from airflow.sdk import dag, task, task_group, Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.sensors.base import PokeReturnValue
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.utils.trigger_rule import TriggerRule

@dag
def leagues_dag():

    @task.sql(conn_id="oracle_default")
    def create_leagues_table():
        return """
        DECLARE
          e_exists EXCEPTION;
          PRAGMA EXCEPTION_INIT(e_exists, -955);
        BEGIN
          EXECUTE IMMEDIATE '
            CREATE TABLE LEAGUES (
              LEAGUE_ID   NUMBER PRIMARY KEY,
              LEAGUE_NAME VARCHAR2(200) NOT NULL,
              LEAGUE_TYPE VARCHAR2(50),
              COUNTRY_ID  NUMBER,
              CONSTRAINT FK_LEAGUES_COUNTRY
                FOREIGN KEY (COUNTRY_ID) REFERENCES COUNTRIES(COUNTRY_ID)
            )';
        EXCEPTION WHEN e_exists THEN NULL;
        END;
        """

    @task.sql(conn_id="oracle_default")
    def create_league_seasons_table():
        return """
        DECLARE
          e_exists EXCEPTION;
          PRAGMA EXCEPTION_INIT(e_exists, -955);
        BEGIN
          EXECUTE IMMEDIATE '
            CREATE TABLE LEAGUE_SEASONS (
              LEAGUE_ID   NUMBER NOT NULL,
              SEASON_ID   NUMBER NOT NULL,
              START_DATE  DATE,
              END_DATE    DATE,
              CONSTRAINT PK_LEAGUE_SEASONS PRIMARY KEY (LEAGUE_ID, SEASON_ID),
              CONSTRAINT FK_LS_LEAGUE FOREIGN KEY (LEAGUE_ID) REFERENCES LEAGUES(LEAGUE_ID),
              CONSTRAINT FK_LS_SEASON FOREIGN KEY (SEASON_ID) REFERENCES SEASONS(SEASON_ID)
            )';
        EXCEPTION WHEN e_exists THEN NULL;
        END;
        """

    @task
    def fetch_seasons() -> list[dict[str, int]]:
        hook = OracleHook(oracle_conn_id="oracle_default")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT SEASON_ID, SEASON_YEAR FROM SEASONS")
                seasons = cur.fetchall()
        return [{"SEASON_ID": s[0], "SEASON_YEAR": s[1]} for s in seasons]

    @task
    def fetch_countries() -> list[dict[str, str | int | None]]:
        hook = OracleHook(oracle_conn_id="oracle_default")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNTRY_ID, COUNTRY_NAME, COUNTRY_CODE FROM COUNTRIES")
                countries = cur.fetchall()
        return [{"COUNTRY_ID": r[0], "COUNTRY_NAME": r[1], "COUNTRY_CODE": r[2]} for r in countries]

    @task
    def select_leagues_season(seasons: list[dict[str, int]]) -> dict[str, int]:
        season_id_indicator = "leagues_season_id_indicator"
        idx = int(Variable.get(season_id_indicator, default=0))
        total = len(seasons)
        if idx >= total:
            idx = 0
        return {
            "key": season_id_indicator,
            "season_id": seasons[idx]["SEASON_ID"],
            "season_year": seasons[idx]["SEASON_YEAR"],
            "idx": idx,
            "total": total
        }

    @task
    def select_leagues_country(countries: list[dict[str, str | int | None]]) -> dict[str, str | int | None]:
        country_id_indicator = "leagues_country_id_indicator"
        idx = int(Variable.get(country_id_indicator, default=0))
        total = len(countries)
        if idx >= total:
            idx = 0
        return {
            "key": country_id_indicator,
            "country_id": countries[idx]["COUNTRY_ID"],
            "country_name": countries[idx]["COUNTRY_NAME"],
            "idx": idx,
            "total": total
        }

    @task.sensor(poke_interval=30, timeout=120)
    def is_api_available(country_selection: dict, season_selection: dict) -> PokeReturnValue:
        import requests
        log = LoggingMixin().log
        api_key = Variable.get("API_KEY")
        url = "https://v3.football.api-sports.io/leagues"
        headers = {
            "x-rapidapi-host": "v3.football.api-sports.io",
            "x-rapidapi-key": api_key
        }
        params = {
            "country": country_selection["country_name"],
            "season": season_selection["season_year"]
        }
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            log.info("API /leagues status: %s", r.status_code)
            r.raise_for_status()
            payload = r.json()
        except requests.RequestException as e:
            log.warning("API not available yet: %s", e)
            return PokeReturnValue(is_done=False, xcom_value=None)
        return PokeReturnValue(is_done=True, xcom_value=payload)

    @task.branch
    def are_leagues_exist(payload: dict, **context):
        log = LoggingMixin().log
        available_leagues = payload.get("response", [])
        if not available_leagues:
            log.warning("No leagues found in API response.")
            return "advance_pointers"
        log.info("Found %d leagues", len(available_leagues))
        context['ti'].xcom_push(key='available_leagues', value=available_leagues)
        return "format_leagues"

    @task
    def format_leagues(country_selection, season_selection, **context) -> list[dict]:
        """
        Builds:
        - LEAGUES rows: LEAGUE_ID, LEAGUE_NAME, LEAGUE_TYPE, COUNTRY_ID, SEASON_ID
        - LEAGUE_SEASONS rows: LEAGUE_ID, SEASON_ID, START_DATE(str), END_DATE(str)
        """
        country_id = country_selection["country_id"]
        season_id = season_selection["season_id"]
        target_year = season_selection["season_year"]

        available_leagues = context['ti'].xcom_pull(
            key='available_leagues', task_ids='are_leagues_exist'
        )

        leagues_rows: list[dict] = []
        league_seasons_rows: list[dict] = []

        for entry in available_leagues:
            league = entry.get("league", {}) or {}
            league_id = league.get("id")
            league_name = league.get("name")
            league_type = league.get("type")

            leagues_rows.append({
                "LEAGUE_ID": league_id,
                "LEAGUE_NAME": league_name,
                "LEAGUE_TYPE": league_type,
                "COUNTRY_ID": country_id
            })

            # seasons array from API; pick the one for target_year
            for s in (entry.get("seasons") or []):
                if s.get("year") == target_year:
                    league_seasons_rows.append({
                        "LEAGUE_ID": league_id,
                        "SEASON_ID": season_id,
                        "START_DATE": s.get("start"),   # 'YYYY-MM-DD'
                        "END_DATE": s.get("end")        # 'YYYY-MM-DD'
                    })
                    break  # only one per year

        context['ti'].xcom_push(key='formatted_leagues', value=leagues_rows)
        context['ti'].xcom_push(key='formatted_league_seasons', value=league_seasons_rows)

    @task_group
    def load_leagues():
        @task
        def leagues_to_csv(**context) -> str:
            import os, csv
            rows = context['ti'].xcom_pull(key='formatted_leagues', task_ids='format_leagues')
            path = "/tmp/leagues.csv"
            fieldnames = ["LEAGUE_ID", "LEAGUE_NAME", "LEAGUE_TYPE", "COUNTRY_ID"]

            existing_ids = set()
            if os.path.exists(path) and os.path.getsize(path) > 0:
                with open(path, newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        existing_ids.add(row.get("LEAGUE_ID", "").strip())

            seen, new_rows = set(), []
            for r in rows:
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
        def leagues_to_oracle(**context) -> str:
            rows = context['ti'].xcom_pull(key='formatted_leagues', task_ids='format_leagues')
            rows = [(r["LEAGUE_ID"], r["LEAGUE_NAME"], r["LEAGUE_TYPE"], r["COUNTRY_ID"]) for r in rows]

            sql = """
            MERGE INTO LEAGUES l
            USING (
              SELECT :1 AS LEAGUE_ID,
                     :2 AS LEAGUE_NAME,
                     :3 AS LEAGUE_TYPE,
                     :4 AS COUNTRY_ID
              FROM dual
            ) s
            ON (l.LEAGUE_ID = s.LEAGUE_ID)
            WHEN NOT MATCHED THEN INSERT (
              LEAGUE_ID, LEAGUE_NAME, LEAGUE_TYPE, COUNTRY_ID
            ) VALUES (
              s.LEAGUE_ID, s.LEAGUE_NAME, s.LEAGUE_TYPE, s.COUNTRY_ID
            ) WHERE EXISTS (SELECT 1 FROM COUNTRIES c WHERE c.COUNTRY_ID = s.COUNTRY_ID)
            """

            hook = OracleHook(oracle_conn_id="oracle_default")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(sql, rows)
                    inserted = cur.rowcount or 0
                conn.commit()
            return f"Inserted {inserted} new leagues."

        leagues_to_csv() >> leagues_to_oracle()

    @task_group
    def load_league_seasons():
        @task
        def league_seasons_to_csv(**context) -> str:
            import os, csv
            rows = context['ti'].xcom_pull(key='formatted_league_seasons', task_ids='format_leagues')
            path = "/tmp/league_seasons.csv"
            fieldnames = ["LEAGUE_ID", "SEASON_ID", "START_DATE", "END_DATE"]

            existing_pairs = set()
            if os.path.exists(path) and os.path.getsize(path) > 0:
                with open(path, newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        key = (row.get("LEAGUE_ID", "").strip(), row.get("SEASON_ID", "").strip())
                        existing_pairs.add(key)

            seen, new_rows = set(), []
            for r in rows or []:
                key = (str(r.get("LEAGUE_ID","")).strip(), str(r.get("SEASON_ID","")).strip())
                if not key[0] or not key[1] or key in existing_pairs or key in seen:
                    continue
                seen.add(key)
                new_rows.append(r)

            with open(path, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                if f.tell() == 0:
                    w.writeheader()
                if new_rows:
                    w.writerows(new_rows)
            return f"Appended {len(new_rows)} league-season rows to {path}"

        @task
        def league_seasons_to_oracle(**context) -> str:
            rows = context['ti'].xcom_pull(key='formatted_league_seasons', task_ids='format_leagues') or []
            rows = [
                (r["LEAGUE_ID"], r["SEASON_ID"], r.get("START_DATE"), r.get("END_DATE"))
                for r in rows
                if r.get("LEAGUE_ID") is not None and r.get("SEASON_ID") is not None
            ]
            if not rows:
                return "No league-season rows to insert."

            sql = """
            MERGE INTO LEAGUE_SEASONS ls
            USING (
              SELECT :1 AS LEAGUE_ID,
                     :2 AS SEASON_ID,
                     TO_DATE(:3, 'YYYY-MM-DD') AS START_DATE,
                     TO_DATE(:4, 'YYYY-MM-DD') AS END_DATE
              FROM dual
            ) s
            ON (ls.LEAGUE_ID = s.LEAGUE_ID AND ls.SEASON_ID = s.SEASON_ID)
            WHEN NOT MATCHED THEN INSERT (
              LEAGUE_ID, SEASON_ID, START_DATE, END_DATE
            ) VALUES (
              s.LEAGUE_ID, s.SEASON_ID, s.START_DATE, s.END_DATE
            ) WHERE EXISTS (SELECT 1 FROM LEAGUES l WHERE l.LEAGUE_ID = s.LEAGUE_ID)
                AND EXISTS (SELECT 1 FROM SEASONS se WHERE se.SEASON_ID = s.SEASON_ID)
            """

            hook = OracleHook(oracle_conn_id="oracle_default")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(sql, rows)
                    inserted = cur.rowcount or 0
                conn.commit()
            return f"Inserted {inserted} new league-season rows."

        league_seasons_to_csv() >> league_seasons_to_oracle()

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def advance_pointers(country_selection: dict, season_selection: dict) -> None:
        # Season pointer
        s_key, s_idx, s_total = season_selection["key"], season_selection["idx"], season_selection["total"]
        # Country pointer
        c_key, c_idx, c_total = country_selection["key"], country_selection["idx"], country_selection["total"]

        next_s = s_idx + 1
        if next_s < s_total:
            Variable.set(s_key, str(next_s))
            Variable.set(c_key, str(c_idx))
        else:
            Variable.set(s_key, "0")
            next_c = c_idx + 1
            if next_c >= c_total:
                next_c = 0
            Variable.set(c_key, str(next_c))

    # DAG wiring
    create_leagues_table()

    country_selection = select_leagues_country(fetch_countries())
    season_selection = select_leagues_season(fetch_seasons())

    payload = is_api_available(country_selection=country_selection, season_selection=season_selection)
    update = advance_pointers(country_selection=country_selection, season_selection=season_selection)
    branch = are_leagues_exist(payload)

    branch >> format_leagues(country_selection=country_selection, season_selection=season_selection) >> load_leagues() >> create_league_seasons_table() >> load_league_seasons() >> update
    branch >> update

leagues_dag()
