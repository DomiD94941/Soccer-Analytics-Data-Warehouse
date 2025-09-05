from airflow.sdk import dag, task, task_group, Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.sensors.base import PokeReturnValue
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.utils.trigger_rule import TriggerRule


@dag(
    description="Round-robin ingest of venues by country from Football API into Oracle + CSV.",
    catchup=False,
    tags=["football", "oracle", "venues"]
)
def football_venues_sync():
    """
    Airflow DAG for loading venues into Oracle.
    Steps:
      1. Create VENUES table (if missing)
      2. Select next country (round-robin) from COUNTRIES table
      3. Call football API /venues endpoint for that country
      4. Validate + transform response
      5. Save results to CSV and Oracle DB
      6. Advance country pointer for next DAG run
    """

    @task.sql(conn_id="oracle_default")
    def create_venues_table():
        # Creates VENUES table in Oracle if it does not already exist
        # Columns:
        #   VENUE_ID -> venue ID from API (primary key)
        #   VENUE_NAME -> name of stadium/venue
        #   ADDRESS, CITY -> location info
        #   COUNTRY_ID -> foreign key referencing COUNTRIES(COUNTRY_ID)
        #   CAPACITY -> stadium capacity
        #   SURFACE -> type of playing surface (grass, artificial, etc.)
        return """
        DECLARE
          e_exists EXCEPTION;
          PRAGMA EXCEPTION_INIT(e_exists, -955);
        BEGIN
          EXECUTE IMMEDIATE '
            CREATE TABLE VENUES (
              VENUE_ID      NUMBER PRIMARY KEY,
              VENUE_NAME    VARCHAR2(300) NOT NULL,
              ADDRESS       VARCHAR2(400),
              CITY          VARCHAR2(150),
              COUNTRY_ID    NUMBER,
              CAPACITY      NUMBER,
              SURFACE       VARCHAR2(80),
              CONSTRAINT FK_VENUES_COUNTRY
                FOREIGN KEY (COUNTRY_ID) REFERENCES COUNTRIES(COUNTRY_ID)
            )';
        EXCEPTION
          WHEN e_exists THEN NULL;
        END;
        """

    @task
    def fetch_countries() -> list[dict[str, str | int | None]]:
        # Reads list of countries from COUNTRIES table in Oracle
        # Returns list of dicts with COUNTRY_ID, COUNTRY_NAME, COUNTRY_CODE
        hook = OracleHook(oracle_conn_id="oracle_default")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNTRY_ID, COUNTRY_NAME, COUNTRY_CODE FROM COUNTRIES")
                countries = cur.fetchall()
        return [{"COUNTRY_ID": r[0], "COUNTRY_NAME": r[1], "COUNTRY_CODE": r[2]} for r in countries]

    @task
    def select_venues_country(countries: list[dict[str, str | int | None]]) -> dict[str, str | int | None]:
        # Selects next country for /venues API request
        # Uses Airflow Variable to keep track of round-robin index across runs
        country_id_indicator = "venues_country_id_indicator"
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
    def is_api_available(country_selection: dict) -> PokeReturnValue:
        # Checks if /venues API is reachable for selected country
        # Retries every 30s, times out after 2min
        # Returns payload JSON on success
        import requests
        log = LoggingMixin().log

        country_name = country_selection["country_name"]
        api_key = Variable.get("API_KEY")
        url = "https://v3.football.api-sports.io/venues"
        headers = {
            "x-rapidapi-host": "v3.football.api-sports.io",
            "x-rapidapi-key": api_key
        }
        params = {"country": country_name}

        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            log.info("API /venues status: %s", r.status_code)
            r.raise_for_status()
            payload = r.json()
        except requests.RequestException as e:
            log.warning("API not available yet: %s", e)
            return PokeReturnValue(is_done=False, xcom_value=None)

        return PokeReturnValue(is_done=True, xcom_value=payload)

    @task.branch
    def are_venues_exist(payload: dict, **context):
        # Branching task:
        # - If no venues found, go directly to pointer update
        # - If venues found, go to formatting + load
        log = LoggingMixin().log
        venues = payload.get("response", [])
        if not venues:
            log.warning("No venues found in API response.")
            return "advance_venues_country_id_pointer"
        log.info("Found %d venues", len(venues))
        context['ti'].xcom_push(key='available_venues', value=venues)
        return "format_venues"

    @task
    def format_venues(country_selection: dict, **context) -> list[dict]:
        # Transforms raw venues response into table rows for VENUES table
        # Pushes formatted venues into XCom for downstream tasks
        country_id = country_selection["country_id"]
        available_venues = context['ti'].xcom_pull(key='available_venues', task_ids='are_venues_exist')

        rows: list[dict] = []
        for v in available_venues:
            rows.append({
                "VENUE_ID": v.get("id"),
                "VENUE_NAME": v.get("name"),
                "ADDRESS": v.get("address"),
                "CITY": v.get("city"),
                "COUNTRY_ID": country_id,
                "CAPACITY": v.get("capacity"),
                "SURFACE": v.get("surface"),
            })
        context['ti'].xcom_push(key='formatted_venues', value=rows)

    @task_group
    def load_venues():
        # Task group: saves venues into CSV + Oracle DB

        @task
        def venues_to_csv(**context) -> str:
            # Saves venues into /tmp/venues.csv
            # Skips already existing VENUE_IDs (checks CSV + current batch)
            import os, csv
            formatted = context['ti'].xcom_pull(key='formatted_venues', task_ids='format_venues')
            path = "/tmp/venues.csv"
            fieldnames = ["VENUE_ID", "VENUE_NAME", "ADDRESS", "CITY", "COUNTRY_ID", "CAPACITY", "SURFACE"]

            existing_ids = set()
            if os.path.exists(path) and os.path.getsize(path) > 0:
                with open(path, newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        existing_ids.add(row.get("VENUE_ID", "").strip())

            seen, new_rows = set(), []
            for r in formatted:
                vid = str(r.get("VENUE_ID", "")).strip()
                if not vid or vid in existing_ids or vid in seen:
                    continue
                seen.add(vid)
                new_rows.append(r)

            with open(path, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                if f.tell() == 0:
                    w.writeheader()
                if new_rows:
                    w.writerows(new_rows)

            return f"Appended {len(new_rows)} venues to {path}"

        @task
        def venues_to_oracle(**context) -> str:
            # Inserts venues into Oracle DB using MERGE
            # Ensures COUNTRY_ID exists in COUNTRIES table (FK validation)
            formatted = context['ti'].xcom_pull(key='formatted_venues', task_ids='format_venues')
            rows = [
                (r["VENUE_ID"], r["VENUE_NAME"], r["ADDRESS"], r["CITY"], r["COUNTRY_ID"], r["CAPACITY"], r["SURFACE"])
                for r in formatted]

            sql = """
            MERGE INTO VENUES v
            USING (
              SELECT :1 AS VENUE_ID,
                     :2 AS VENUE_NAME,
                     :3 AS ADDRESS,
                     :4 AS CITY,
                     :5 AS COUNTRY_ID,
                     :6 AS CAPACITY,
                     :7 AS SURFACE
              FROM dual
            ) s
            ON (v.VENUE_ID = s.VENUE_ID)
            WHEN NOT MATCHED THEN INSERT (
              VENUE_ID, VENUE_NAME, ADDRESS, CITY, COUNTRY_ID, CAPACITY, SURFACE
            ) VALUES (
              s.VENUE_ID, s.VENUE_NAME, s.ADDRESS, s.CITY, s.COUNTRY_ID, s.CAPACITY, s.SURFACE
            ) WHERE EXISTS (SELECT 1 FROM COUNTRIES c WHERE c.COUNTRY_ID = s.COUNTRY_ID)
            """

            hook = OracleHook(oracle_conn_id="oracle_default")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(sql, rows)
                    inserted = cur.rowcount or 0
                conn.commit()
            return f"Inserted {inserted} new venues."

        venues_to_csv() >> venues_to_oracle()

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def advance_venues_country_id_pointer(country_selection: dict) -> None:
        # Updates round-robin pointer in Airflow Variable
        # Ensures next DAG run processes next country
        c_key, c_idx, c_total = country_selection["key"], country_selection["idx"], country_selection["total"]
        next_c = c_idx + 1
        if next_c >= c_total:
            next_c = 0
        Variable.set(c_key, str(next_c))

    # DAG wiring (execution order)
    create_venues_table()
    country_selection = select_venues_country(fetch_countries())
    payload = is_api_available(country_selection=country_selection)
    update = advance_venues_country_id_pointer(country_selection=country_selection)
    branch = are_venues_exist(payload)
    branch >> format_venues(country_selection=country_selection) >> load_venues() >> update
    branch >> update


football_venues_sync()
