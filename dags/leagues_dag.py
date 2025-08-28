from airflow.sdk import dag, task, task_group, Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.sensors.base import PokeReturnValue
from airflow.providers.oracle.hooks.oracle import OracleHook

@dag
def leagues_dag():

    # ------------------------------
    # DDL (matches exactly your table)
    # ------------------------------
    @task.sql(conn_id="oracle_default")
    def create_leagues_table():
        return """
        DECLARE
          e_exists EXCEPTION;
          PRAGMA EXCEPTION_INIT(e_exists, -955);
        BEGIN
          EXECUTE IMMEDIATE '
            CREATE TABLE LEAGUES (
              LEAGUE_ID NUMBER PRIMARY KEY,
              LEAGUE_NAME VARCHAR2(200) NOT NULL,
              LEAGUE_TYPE VARCHAR2(50),
              COUNTRY_ID NUMBER,
              CONSTRAINT FK_LEAGUES_COUNTRY
                FOREIGN KEY (COUNTRY_ID) REFERENCES COUNTRIES(COUNTRY_ID)
            )';
        EXCEPTION
          WHEN e_exists THEN NULL;
        END;
        """
    
    @task
    def fetch_countries() -> list[dict[str, str | int | None]]:
        """
        Read all COUNTRIES from Oracle. Returns a Python list -> XCom.
        Returns list of dicts: {COUNTRY_ID, COUNTRY_NAME, COUNTRY_CODE}
        """
        hook = OracleHook(oracle_conn_id="oracle_default")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNTRY_ID, COUNTRY_NAME, COUNTRY_CODE FROM COUNTRIES")
                countries = cur.fetchall()
        return [{"COUNTRY_ID": country[0], "COUNTRY_NAME": country[1], "COUNTRY_CODE": country[2]} for country in countries]

    @task
    def select_leagues_country(countries: list[dict[str, str | int | None]]) -> dict[str, str | int | None]:
        country_id_indicator = "country_id_indicator"
        country_id_index = int(Variable.get(country_id_indicator, default=0))
        country_ids_len = len(countries)

        if country_id_index >= country_ids_len:
            country_id_index = 0
        Variable.set(country_id_indicator, str(country_id_index))

        return {
            "key": country_id_indicator,
            "country_id": countries[country_id_index]["COUNTRY_ID"],
            "country_name": countries[country_id_index]["COUNTRY_NAME"],
            "idx": country_id_index,
            "total": country_ids_len
        }

    @task.sensor(poke_interval=30, timeout=120)
    def is_api_available(country_name: str) -> PokeReturnValue:
        import requests
        log = LoggingMixin().log
        api_key = Variable.get("API_KEY")
        url = "https://v3.football.api-sports.io/leagues"
        headers = {
            "x-rapidapi-host": "v3.football.api-sports.io",
            "x-rapidapi-key": api_key
        }
        params = {
            "country": country_name
        }
        try:
            r = requests.get(url, headers=headers, params=params)
            log.info("API /leagues status: %s", r.status_code)
            r.raise_for_status()
            payload = r.json()
        except requests.RequestException as e:
            log.warning("API not available yet: %s", e)
            return PokeReturnValue(is_done=False, xcom_value=None)
        return PokeReturnValue(is_done=True, xcom_value=payload)
    
    @task
    def are_leagues_exist(payload: dict) -> list[dict]:
        """
        Validate that leagues exist in the API response.
        """
        from airflow.exceptions import AirflowSkipException
        log = LoggingMixin().log
        available_leagues = payload.get("response", []) or []
        if not available_leagues:
            log.warning("No leagues found in API response.")
            raise AirflowSkipException("No leagues found in API response.")
        log.info("Found %d leagues", len(available_leagues))
        return available_leagues

    @task
    def format_leagues(available_leagues: list[dict], country_id: int) -> list[dict]:
        """
        Build rows for LEAGUES from the API response for the selected country.
        Keeps only LEAGUE_ID, LEAGUE_NAME, LEAGUE_TYPE, COUNTRY_ID.
        """

        formatted_leagues: list[dict] = []
        for leagues in available_leagues:
            league = leagues.get("league", {})
            league_id = league.get("id")
            league_name = league.get("name")
            league_type = league.get("type")

            formatted_leagues.append({
                "LEAGUE_ID": league_id,
                "LEAGUE_NAME": league_name,
                "LEAGUE_TYPE": league_type,
                "COUNTRY_ID": country_id,
            })

        return formatted_leagues
    
    @task_group
    def load_leagues(formatted_leagues: list[dict]):
        @task
        def leagues_to_csv(formatted_leagues: list[dict]) -> str:
            """
            Append-only CSV with final schema.
            """
            import os, csv
            path = "/tmp/leagues.csv"

            fieldnames = ["LEAGUE_ID", "LEAGUE_NAME", "LEAGUE_TYPE", "COUNTRY_ID"]

            existing_ids = set()
            if os.path.exists(path) and os.path.getsize(path) > 0:
                with open(path, newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        existing_ids.add(row.get("LEAGUE_ID", "").strip())

            seen, new_rows = set(), []
            for r in formatted_leagues:
                lid = str(r.get("LEAGUE_ID", "")).strip()
                if not lid or lid in existing_ids or lid in seen:
                    continue
                seen.add(lid)
                new_rows.append(r)

            with open(path, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                if f.tell() == 0:
                    w.writeheader()
                if new_rows:
                    w.writerows(new_rows)

            return f"Appended {len(new_rows)} leagues to {path}"

        @task
        def leagues_to_oracle(formatted_leagues: list[dict]) -> str:
            """
            Insert-only MERGE into LEAGUES (idempotent).
            """
            sql = """
                MERGE INTO LEAGUES t
                USING (
                    SELECT
                        :LEAGUE_ID   AS LEAGUE_ID,
                        :LEAGUE_NAME AS LEAGUE_NAME,
                        :LEAGUE_TYPE AS LEAGUE_TYPE,
                        :COUNTRY_ID  AS COUNTRY_ID
                    FROM dual
                ) s
                ON (t.LEAGUE_ID = s.LEAGUE_ID)
                WHEN NOT MATCHED THEN INSERT (
                    LEAGUE_ID, LEAGUE_NAME, LEAGUE_TYPE, COUNTRY_ID
                ) VALUES (
                    s.LEAGUE_ID, s.LEAGUE_NAME, s.LEAGUE_TYPE, s.COUNTRY_ID
                )
            """
            hook = OracleHook(oracle_conn_id="oracle_default")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(sql, formatted_leagues)
                    inserted = cur.rowcount or 0
                conn.commit()
            return f"Inserted {inserted} new leagues."

        leagues_to_csv(formatted_leagues) >> leagues_to_oracle(formatted_leagues)

leagues_dag()