from airflow.sdk import dag, task, task_group, Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.sensors.base import PokeReturnValue
from airflow.providers.oracle.hooks.oracle import OracleHook

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
              NATIONAL       NUMBER(1),         -- 0/1
              VENUE_ID       NUMBER,
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
        key = "teams_season_idx"
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
    def fetch_leagues_for_season(season_selection: dict) -> list[dict[str, int | str]]:
        """
        Inner domain for the selected season: all leagues recorded for that SEASON_ID.
        """
        season_id = season_selection["season_id"]
        hook = OracleHook(oracle_conn_id="oracle_default")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT LEAGUE_ID, LEAGUE_NAME
                    FROM LEAGUES
                    WHERE SEASON_ID = :sid
                    ORDER BY LEAGUE_ID
                """, [season_id])
                leagues = cur.fetchall()
        return [{"LEAGUE_ID": r[0], "LEAGUE_NAME": r[1]} for r in leagues]
    
    @task
    def select_teams_league(leagues: list[dict[str, int | str]]) -> dict[str, int | str]:
        """
        Inner pointer: leagues (for the current season).
        """
        key = "teams_league_idx"
        idx = int(Variable.get(key, default=0))
        total = len(leagues)
        if total == 0:
            # No leagues for this season; return an empty selection but keep the pointer info.
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
teams_dag()