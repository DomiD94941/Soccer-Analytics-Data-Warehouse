from airflow.sdk import dag, task, task_group, Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.sensors.base import PokeReturnValue
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.utils.trigger_rule import TriggerRule

@dag
def teams_dag():

    @task.sql(conn_id="oracle_default")
    def create_teams_table():
        return """
        DECLARE
          e_exists EXCEPTION;
          PRAGMA EXCEPTION_INIT(e_exists, -955);
        BEGIN
          EXECUTE IMMEDIATE '
            CREATE TABLE TEAMS (
              TEAM_ID        NUMBER PRIMARY KEY,
              TEAM_NAME      VARCHAR2(200) NOT NULL,
              TEAM_CODE      VARCHAR2(20),
              TEAM_COUNTRY   VARCHAR2(100),
              FOUNDED        NUMBER,
              NATIONAL       VARCHAR2(1),         -- T/F
              VENUE_ID       NUMBER,
              LEAGUE_ID      NUMBER,
              SEASON_ID      NUMBER,
              CONSTRAINT FK_TEAMS_LEAGUE
                FOREIGN KEY (LEAGUE_ID) REFERENCES LEAGUES(LEAGUE_ID),
              CONSTRAINT FK_TEAMS_SEASON
                FOREIGN KEY (SEASON_ID) REFERENCES SEASONS(SEASON_ID),
              CONSTRAINT FK_TEAMS_VENUE
                FOREIGN KEY (VENUE_ID) REFERENCES VENUES(VENUE_ID)
            )';
        EXCEPTION
          WHEN e_exists THEN NULL;
        END;
        """
    @task
    def fetch_seasons() -> list[dict[str, int]]:
        hook = OracleHook(oracle_conn_id="oracle_default")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT SEASON_ID, SEASON_YEAR FROM SEASONS ORDER BY SEASON_YEAR")
                seasons = cur.fetchall()
        return [{"SEASON_ID": r[0], "SEASON_YEAR": r[1]} for r in seasons]
    
    @task
    def select_teams_season(seasons: list[dict[str, int]]) -> dict[str, int]:
        """
        Outer pointer: seasons
        """
        season_id_indicator = "teams_season_id_indicator"
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
    def fetch_leagues() -> list[dict[str, int | str]]:
        """
        Inner domain for the selected season: all leagues recorded for that SEASON_ID.
        """
        hook = OracleHook(oracle_conn_id="oracle_default")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT LEAGUE_ID, LEAGUE_NAME FROM LEAGUES ORDER BY LEAGUE_NAME")
                leagues = cur.fetchall()
        return [{"LEAGUE_ID": r[0], "LEAGUE_NAME": r[1]} for r in leagues]
    
    @task
    def select_teams_league(leagues: list[dict[str, int | str]]) -> dict[str, int | str]:
        """
        Inner pointer: leagues (for the current season).
        """
        league_id_indicator = "teams_league_id_indicator"
        idx = int(Variable.get(league_id_indicator, default=0))
        total = len(leagues)
        if total == 0:
            # No leagues for this season; return an empty selection but keep the pointer info.
            return {"key": league_id_indicator, "idx": 0, "total": 0}
        if idx >= total:
            idx = 0
        return {
            "key": league_id_indicator,
            "league_id": leagues[idx]["LEAGUE_ID"],
            "league_name": leagues[idx]["LEAGUE_NAME"],
            "idx": idx,
            "total": total
        }
    
    @task.sensor(poke_interval=30, timeout=120)
    def is_api_available(season_selection: dict, league_selection: dict) -> PokeReturnValue:
        import requests
        log = LoggingMixin().log

        api_key = Variable.get("API_KEY")
        url = "https://v3.football.api-sports.io/teams"
        headers = {
            "x-rapidapi-host": "v3.football.api-sports.io",
            "x-rapidapi-key": api_key
        }
        params = {
            "league": league_selection["league_id"],
            "season": season_selection["season_year"]
        }

        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            log.info("API /teams status: %s (league=%s, season=%s)", r.status_code, league_selection.get("league_id"), season_selection.get("season_year"))
            r.raise_for_status()
            payload = r.json()
        except requests.RequestException as e:
            log.warning("API not available yet: %s", e)
            return PokeReturnValue(is_done=False, xcom_value=None)

        return PokeReturnValue(is_done=True, xcom_value=payload)
    
    @task.branch
    def are_teams_exist(payload: dict, **context):
        log = LoggingMixin().log
        teams = payload.get("response", [])
        if not teams:
            log.warning("No teams found in API response.")
            return "advance_teams_league_id_pointer"
        log.info("Found %d teams", len(teams))
        context['ti'].xcom_push(key='available_teams', value=teams)
        return "format_teams"

    @task
    def format_teams(league_selection: dict, season_selection: dict, **context) -> list[dict]:
        """
        Build rows for TEAMS:
        TEAM_ID, TEAM_NAME, TEAM_CODE, TEAM_COUNTRY, FOUNDED, NATIONAL, VENUE_ID
        """
        available_teams = context['ti'].xcom_pull(key='available_teams', task_ids='are_teams_exist')
        league_id = league_selection["league_id"]
        season_id = season_selection["season_id"]
        formatted: list[dict] = []
        for entry in available_teams:
            t = entry.get("team")
            v = entry.get("venue")
            formatted.append({
                "TEAM_ID": t.get("id"),
                "TEAM_NAME": t.get("name"),
                "TEAM_CODE": t.get("code"),
                "TEAM_COUNTRY": t.get("country"),
                "FOUNDED": t.get("founded"),
                "NATIONAL": "T" if t.get("national") else "F",
                "LEAGUE_ID": league_id,
                "SEASON_ID": season_id,
                "VENUE_ID": v.get("id")
            })
        context['ti'].xcom_push(key='formatted_teams', value=formatted)

    @task_group
    def load_teams():
        @task
        def teams_to_csv(**context) -> str:
            import os, csv
            rows = context['ti'].xcom_pull(key='formatted_teams', task_ids='format_teams')
            path = "/tmp/teams.csv"
            fieldnames = ["TEAM_ID","TEAM_NAME","TEAM_CODE","TEAM_COUNTRY","FOUNDED","NATIONAL", "LEAGUE_ID", "SEASON_ID", "VENUE_ID"]

            existing = set()
            if os.path.exists(path) and os.path.getsize(path) > 0:
                with open(path, newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        existing.add(row.get("TEAM_ID", "").strip())

            seen, new_rows = set(), []
            for r in rows:
                tid = str(r.get("TEAM_ID","")).strip()
                if not tid or tid in existing or tid in seen:
                    continue
                seen.add(tid)
                new_rows.append(r)

            with open(path, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                if f.tell() == 0:
                    w.writeheader()
                if new_rows:
                    w.writerows(new_rows)
            return f"Appended {len(new_rows)} teams to {path}"

        @task
        def teams_to_oracle(**context) -> str:
            rows = context['ti'].xcom_pull(key='formatted_teams', task_ids='format_teams') or []
            rows = [
                (r["TEAM_ID"], r["TEAM_NAME"], r["TEAM_CODE"], r["TEAM_COUNTRY"], r["FOUNDED"], r["NATIONAL"], r["LEAGUE_ID"], r["SEASON_ID"], r["VENUE_ID"])
                for r in rows if r.get("TEAM_ID") is not None
            ]
            if not rows:
                return "No rows to insert."

            sql = """
            MERGE INTO TEAMS t
            USING (
              SELECT :1 AS TEAM_ID,
                     :2 AS TEAM_NAME,
                     :3 AS TEAM_CODE,
                     :4 AS TEAM_COUNTRY,
                     :5 AS FOUNDED,
                     :6 AS NATIONAL,
                     :7 AS LEAGUE_ID,
                     :8 AS SEASON_ID,
                     :9 AS VENUE_ID
              FROM dual
            ) s
            ON (t.TEAM_ID = s.TEAM_ID)
            WHEN NOT MATCHED THEN INSERT (
              TEAM_ID, TEAM_NAME, TEAM_CODE, TEAM_COUNTRY, FOUNDED, NATIONAL, LEAGUE_ID, SEASON_ID, VENUE_ID
            ) VALUES (
              s.TEAM_ID, s.TEAM_NAME, s.TEAM_CODE, s.TEAM_COUNTRY, s.FOUNDED, s.NATIONAL, s.LEAGUE_ID, s.SEASON_ID, s.VENUE_ID
            ) WHERE EXISTS (SELECT 1 FROM TEAMS t WHERE t.LEAGUE_ID = s.LEAGUE_ID)
              AND EXISTS (SELECT 1 FROM TEAMS t WHERE t.SEASON_ID = s.SEASON_ID)
              AND EXISTS (SELECT 1 FROM TEAMS t WHERE t.VENUE_ID = s.VENUE_ID)
            )
            """

            hook = OracleHook(oracle_conn_id="oracle_default")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(sql, rows)
                    inserted = cur.rowcount or 0
                conn.commit()
            return f"Inserted {inserted} new teams."
        
        teams_to_csv() >> teams_to_oracle()

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def advance_pointers(season_selection: dict, league_selection: dict) -> None:
        # Season pointer
        s_key, s_idx, s_total = season_selection["key"], season_selection["idx"], season_selection["total"]
        # League pointer
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

    # DAG wiring
    create_teams_table()

    league_selection = select_teams_league(fetch_leagues())
    season_selection = select_teams_season(fetch_seasons())

    payload = is_api_available(league_selection=league_selection, season_selection=season_selection)
    update = advance_pointers(league_selection=league_selection, season_selection=season_selection)
    branch = are_teams_exist(payload)

    branch >> format_teams(league_selection=league_selection, season_selection=season_selection) >> load_teams() >> update
    branch >> update


teams_dag()