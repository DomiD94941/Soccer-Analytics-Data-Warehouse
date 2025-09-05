from airflow.sdk import dag, task, task_group, Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.sensors.base import PokeReturnValue
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.utils.trigger_rule import TriggerRule


@dag(
    description="Round-robin ingest of fixtures (league, season) from Football API into Oracle + CSV.",
    catchup=False,
    tags=["football", "oracle", "fixtures"]
)
def football_fixtures_sync():
    """
    Airflow DAG for ingesting match fixtures into Oracle.
    Steps:
      1) Ensure FIXTURES table exists
      2) Round-robin select season + league via Airflow Variables
      3) Call API /fixtures for that (league, season)
      4) If data -> format -> CSV -> Oracle
      5) Advance pointers so next run picks the next slice
    """

    @task.sql(conn_id="oracle_default")
    def create_fixtures_table():
        # Creates FIXTURES if it does not exist (idempotent CREATE block)
        # FK constraints rely on VENUES, LEAGUES, SEASONS, TEAMS being pre-populated
        return """
        DECLARE
          e_exists EXCEPTION;
          PRAGMA EXCEPTION_INIT(e_exists, -955);
        BEGIN
          EXECUTE IMMEDIATE '
            CREATE TABLE FIXTURES (
              FIXTURE_ID           NUMBER PRIMARY KEY,
              REFEREE              VARCHAR2(200),
              TZ                   VARCHAR2(64),
              KICKOFF_UTC          DATE,
              STATUS_LONG          VARCHAR2(100),
              STATUS_SHORT         VARCHAR2(10),
              STATUS_ELAPSED       NUMBER,
              STATUS_EXTRA         VARCHAR2(100),

              ROUND_NAME           VARCHAR2(100),

              HOME_TEAM_ID         NUMBER,
              AWAY_TEAM_ID         NUMBER,

              GOALS_HOME           NUMBER,
              GOALS_AWAY           NUMBER,

              HT_HOME              NUMBER,
              HT_AWAY              NUMBER,
              FT_HOME              NUMBER,
              FT_AWAY              NUMBER,
              ET_HOME              NUMBER,
              ET_AWAY              NUMBER,
              PEN_HOME             NUMBER,
              PEN_AWAY             NUMBER,

              VENUE_ID             NUMBER,
              LEAGUE_ID            NUMBER,
              SEASON_ID            NUMBER,

              CONSTRAINT FK_FIX_VENUE   FOREIGN KEY (VENUE_ID)   REFERENCES VENUES(VENUE_ID),
              CONSTRAINT FK_FIX_LEAGUE  FOREIGN KEY (LEAGUE_ID)  REFERENCES LEAGUES(LEAGUE_ID),
              CONSTRAINT FK_FIX_SEASON  FOREIGN KEY (SEASON_ID)  REFERENCES SEASONS(SEASON_ID),
              CONSTRAINT FK_FIX_HOME    FOREIGN KEY (HOME_TEAM_ID) REFERENCES TEAMS(TEAM_ID),
              CONSTRAINT FK_FIX_AWAY    FOREIGN KEY (AWAY_TEAM_ID) REFERENCES TEAMS(TEAM_ID)
            )';
        EXCEPTION
          WHEN e_exists THEN NULL;
        END;
        """
    
    @task
    def fetch_seasons() -> list[dict[str, int]]:
        # Reads seasons from SEASONS table; returns list of dicts with SEASON_ID/SEASON_YEAR
        hook = OracleHook(oracle_conn_id="oracle_default")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT SEASON_ID, SEASON_YEAR FROM SEASONS ORDER BY SEASON_YEAR")
                rows = cur.fetchall()
        return [{"SEASON_ID": r[0], "SEASON_YEAR": r[1]} for r in rows]

    @task
    def select_fixtures_season(seasons: list[dict[str, int]]) -> dict[str, int]:
        # Round-robin season selector persisted in Airflow Variable
        key = "fixtures_season_id_indicator"
        idx = int(Variable.get(key, default=0))
        total = len(seasons)
        if idx >= total:
            idx = 0
        return {
            "key": key,
            "season_id": seasons[idx]["SEASON_ID"],
            "season_year": seasons[idx]["SEASON_YEAR"],
            "idx": idx,
            "total": total
        }

    @task
    def fetch_leagues() -> list[dict[str, int | str]]:
        # Reads leagues from LEAGUES table; returns list of dicts with LEAGUE_ID/LEAGUE_NAME
        hook = OracleHook(oracle_conn_id="oracle_default")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT LEAGUE_ID, LEAGUE_NAME FROM LEAGUES ORDER BY LEAGUE_NAME")
                rows = cur.fetchall()
        return [{"LEAGUE_ID": r[0], "LEAGUE_NAME": r[1]} for r in rows]

    @task
    def select_fixtures_league(leagues: list[dict[str, int | str]]) -> dict[str, int | str]:
        # Round-robin league selector persisted in Airflow Variable
        key = "fixtures_league_id_indicator"
        idx = int(Variable.get(key, default=0))
        total = len(leagues)
        if total == 0:
            # Handle empty LEAGUES table gracefully
            return {"key": key, "idx": 0, "total": 0}
        if idx >= total:
            idx = 0
        return {
            "key": key,
            "league_id": leagues[idx]["LEAGUE_ID"],
            "league_name": leagues[idx]["LEAGUE_NAME"],
            "idx": idx,
            "total": total
        }

    @task.sensor(poke_interval=30, timeout=120)
    def is_api_available(season_selection: dict, league_selection: dict) -> PokeReturnValue:
        # Sensor to verify /fixtures endpoint availability for selected (league, season)
        # Polls every 30s up to 2min; returns JSON payload via XCom on success
        import requests
        log = LoggingMixin().log

        api_key = Variable.get("API_KEY")
        url = "https://v3.football.api-sports.io/fixtures"
        headers = {
            "x-rapidapi-host": "v3.football.api-sports.io",
            "x-rapidapi-key": api_key
        }
        params = {
            "league": league_selection.get("league_id"),
            "season": season_selection.get("season_year"),
        }

        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            log.info("API /fixtures status: %s (league=%s, season=%s)",
                     r.status_code, league_selection.get("league_id"), season_selection.get("season_year"))
            r.raise_for_status()
            payload = r.json()
        except requests.RequestException as e:
            log.warning("API not available yet: %s", e)
            return PokeReturnValue(is_done=False, xcom_value=None)

        return PokeReturnValue(is_done=True, xcom_value=payload)

    @task.branch
    def are_fixtures_exist(payload: dict, **context):
        # Branching:
        #  - If no fixtures -> jump to pointer advance (skip formatting/loading)
        #  - If fixtures -> push to XCom and continue
        log = LoggingMixin().log
        fixtures = payload.get("response", []) if payload else []
        if not fixtures:
            log.warning("No fixtures found in API response.")
            return "advance_pointers"
        log.info("Found %d fixtures", len(fixtures))
        context['ti'].xcom_push(key='available_fixtures', value=fixtures)
        return "format_fixtures"

    @task
    def format_fixtures(league_selection: dict, season_selection: dict, **context) -> list[dict]:
        # Transforms API payload into FIXTURES table rows (no logos/media fields)
        # Maps ISO datetime to naive UTC for Oracle DATE
        # Pushes formatted rows to XCom for loaders
        from datetime import datetime, timezone

        data = context['ti'].xcom_pull(key='available_fixtures', task_ids='are_fixtures_exist') or []
        league_id = league_selection["league_id"]
        season_id = season_selection["season_id"]
        out: list[dict] = []

        for item in data:
            fx = item.get("fixture", {}) or {}
            lg = item.get("league", {}) or {}
            tm = item.get("teams", {}) or {}
            goals = item.get("goals", {}) or {}
            score = item.get("score", {}) or {}

            # Convert API ISO timestamp to naive UTC datetime (Oracle DATE compatible)
            dt_iso = fx.get("date")
            dt_utc_naive = None
            if dt_iso:
                try:
                    dt = datetime.fromisoformat(dt_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
                    dt_utc_naive = dt.replace(tzinfo=None)
                except Exception:
                    dt_utc_naive = None

            out.append({
                "FIXTURE_ID": fx.get("id"),
                "REFEREE": fx.get("referee"),
                "TZ": fx.get("timezone"),
                "KICKOFF_UTC": dt_utc_naive,
                "STATUS_LONG": (fx.get("status") or {}).get("long"),
                "STATUS_SHORT": (fx.get("status") or {}).get("short"),
                "STATUS_ELAPSED": (fx.get("status") or {}).get("elapsed"),
                "STATUS_EXTRA": (fx.get("status") or {}).get("extra"),

                "ROUND_NAME": lg.get("round"),

                "HOME_TEAM_ID": (tm.get("home") or {}).get("id"),
                "AWAY_TEAM_ID": (tm.get("away") or {}).get("id"),

                "GOALS_HOME": goals.get("home"),
                "GOALS_AWAY": goals.get("away"),

                "HT_HOME": (score.get("halftime") or {}).get("home"),
                "HT_AWAY": (score.get("halftime") or {}).get("away"),
                "FT_HOME": (score.get("fulltime") or {}).get("home"),
                "FT_AWAY": (score.get("fulltime") or {}).get("away"),
                "ET_HOME": (score.get("extratime") or {}).get("home"),
                "ET_AWAY": (score.get("extratime") or {}).get("away"),
                "PEN_HOME": (score.get("penalty") or {}).get("home"),
                "PEN_AWAY": (score.get("penalty") or {}).get("away"),

                "VENUE_ID": (fx.get("venue") or {}).get("id"),
                "LEAGUE_ID": league_id,
                "SEASON_ID": season_id,
            })
        context['ti'].xcom_push(key='formatted_fixtures', value=out)

    @task_group
    def load_fixtures():
        # TaskGroup for saving fixtures to CSV and Oracle

        @task
        def fixtures_to_csv(**context) -> str:
            # Append-only writer to /tmp/fixtures.csv
            # Dedupe by FIXTURE_ID (existing file + current batch)
            import os, csv
            rows = context['ti'].xcom_pull(key='formatted_fixtures', task_ids='format_fixtures') or []
            path = "/tmp/fixtures.csv"
            fieldnames = [
                "FIXTURE_ID","REFEREE","TZ","KICKOFF_UTC","STATUS_LONG","STATUS_SHORT","STATUS_ELAPSED","STATUS_EXTRA",
                "ROUND_NAME","HOME_TEAM_ID","AWAY_TEAM_ID",
                "GOALS_HOME","GOALS_AWAY","HT_HOME","HT_AWAY","FT_HOME","FT_AWAY","ET_HOME","ET_AWAY","PEN_HOME","PEN_AWAY",
                "VENUE_ID","LEAGUE_ID","SEASON_ID"
            ]

            existing = set()
            if os.path.exists(path) and os.path.getsize(path) > 0:
                with open(path, newline="", encoding="utf-8") as f:
                    for r in csv.DictReader(f):
                        existing.add(r.get("FIXTURE_ID", "").strip())

            seen, new_rows = set(), []
            for r in rows:
                fid = str(r.get("FIXTURE_ID", "")).strip()
                if not fid or fid in existing or fid in seen:
                    continue
                seen.add(fid)
                new_rows.append(r)

            with open(path, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                if f.tell() == 0:
                    w.writeheader()
                if new_rows:
                    w.writerows(new_rows)
            return f"Appended {len(new_rows)} fixtures to {path}"

        @task
        def fixtures_to_oracle(**context) -> str:
            
            # MERGE-only insert into FIXTURES (skips existing by FIXTURE_ID)
            # Enforces referential integrity via WHERE EXISTS checks

            formatted = context['ti'].xcom_pull(key='formatted_fixtures', task_ids='format_fixtures') or []
            rows = [
                (
                    r["FIXTURE_ID"], r["REFEREE"], r["TZ"], r["KICKOFF_UTC"],
                    r["STATUS_LONG"], r["STATUS_SHORT"], r["STATUS_ELAPSED"], r["STATUS_EXTRA"],
                    r["ROUND_NAME"], r["HOME_TEAM_ID"], r["AWAY_TEAM_ID"],
                    r["GOALS_HOME"], r["GOALS_AWAY"],
                    r["HT_HOME"], r["HT_AWAY"], r["FT_HOME"], r["FT_AWAY"],
                    r["ET_HOME"], r["ET_AWAY"], r["PEN_HOME"], r["PEN_AWAY"],
                    r["VENUE_ID"], r["LEAGUE_ID"], r["SEASON_ID"]
                )
                for r in formatted
            ]

            sql = """
            MERGE INTO FIXTURES f
            USING (
              SELECT
                :1  AS FIXTURE_ID,
                :2  AS REFEREE,
                :3  AS TZ,
                :4  AS KICKOFF_UTC,
                :5  AS STATUS_LONG,
                :6  AS STATUS_SHORT,
                :7  AS STATUS_ELAPSED,
                :8  AS STATUS_EXTRA,
                :9  AS ROUND_NAME,
                :10 AS HOME_TEAM_ID,
                :11 AS AWAY_TEAM_ID,
                :12 AS GOALS_HOME,
                :13 AS GOALS_AWAY,
                :14 AS HT_HOME,
                :15 AS HT_AWAY,
                :16 AS FT_HOME,
                :17 AS FT_AWAY,
                :18 AS ET_HOME,
                :19 AS ET_AWAY,
                :20 AS PEN_HOME,
                :21 AS PEN_AWAY,
                :22 AS VENUE_ID,
                :23 AS LEAGUE_ID,
                :24 AS SEASON_ID
              FROM dual
            ) s
            ON (f.FIXTURE_ID = s.FIXTURE_ID)
            WHEN NOT MATCHED THEN INSERT (
              FIXTURE_ID, REFEREE, TZ, KICKOFF_UTC, STATUS_LONG, STATUS_SHORT, STATUS_ELAPSED, STATUS_EXTRA,
              ROUND_NAME, HOME_TEAM_ID, AWAY_TEAM_ID,
              GOALS_HOME, GOALS_AWAY, HT_HOME, HT_AWAY, FT_HOME, FT_AWAY, ET_HOME, ET_AWAY, PEN_HOME, PEN_AWAY,
              VENUE_ID, LEAGUE_ID, SEASON_ID
            ) VALUES (
              s.FIXTURE_ID, s.REFEREE, s.TZ, s.KICKOFF_UTC, s.STATUS_LONG, s.STATUS_SHORT, s.STATUS_ELAPSED, s.STATUS_EXTRA,
              s.ROUND_NAME, s.HOME_TEAM_ID, s.AWAY_TEAM_ID,
              s.GOALS_HOME, s.GOALS_AWAY, s.HT_HOME, s.HT_AWAY, s.FT_HOME, s.FT_AWAY, s.ET_HOME, s.ET_AWAY, s.PEN_HOME, s.PEN_AWAY,
              s.VENUE_ID, s.LEAGUE_ID, s.SEASON_ID
            ) WHERE EXISTS (SELECT 1 FROM VENUES v WHERE v.VENUE_ID = s.VENUE_ID)
              AND EXISTS (SELECT 1 FROM LEAGUES l WHERE l.LEAGUE_ID = s.LEAGUE_ID)
              AND EXISTS (SELECT 1 FROM SEASONS se WHERE se.SEASON_ID = s.SEASON_ID)
              AND EXISTS (SELECT 1 FROM TEAMS th WHERE th.TEAM_ID = s.HOME_TEAM_ID)
              AND EXISTS (SELECT 1 FROM TEAMS ta WHERE ta.TEAM_ID = s.AWAY_TEAM_ID)
            """
            hook = OracleHook(oracle_conn_id="oracle_default")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(sql, rows)
                    inserted = cur.rowcount or 0
                conn.commit()
            return f"Inserted {inserted} new fixtures."

        fixtures_to_csv() >> fixtures_to_oracle()

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def advance_pointers(season_selection: dict, league_selection: dict) -> None:
        # Advances round-robin indices:
        #  - Increment season index; if wrapped, increment league index
        #  - Runs even if upstream partially failed (NONE_FAILED_MIN_ONE_SUCCESS)
        s_key, s_idx, s_total = season_selection["key"], season_selection["idx"], season_selection["total"]
        l_key, l_idx, l_total = league_selection["key"], league_selection.get("idx", 0), league_selection.get("total", 0)

        next_s = s_idx + 1
        if next_s < s_total:
            Variable.set(s_key, str(next_s))
            Variable.set(l_key, str(l_idx))
        else:
            Variable.set(s_key, "0")
            next_l = l_idx + 1
            if next_l >= l_total:
                next_l = 0
            Variable.set(l_key, str(next_l))

    # DAG wiring (task dependencies)
    create_fixtures_table()

    # Select (league, season) pair for this run
    league_sel = select_fixtures_league(fetch_leagues())
    season_sel = select_fixtures_season(fetch_seasons())

    # Probe API and branch based on presence of fixtures
    payload = is_api_available(league_selection=league_sel, season_selection=season_sel)
    branch = are_fixtures_exist(payload)
    update = advance_pointers(league_selection=league_sel, season_selection=season_sel)

    # If data exists: format -> load -> advance pointers
    branch >> format_fixtures(league_selection=league_sel, season_selection=season_sel) >> load_fixtures() >> update
    # If no data: advance pointers directly
    branch >> update


football_fixtures_sync()
