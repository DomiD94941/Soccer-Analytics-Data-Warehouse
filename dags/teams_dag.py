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
teams_dag()