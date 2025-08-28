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
    def get_countries_from_db() -> list[dict[str, str | int | None]]:
        """
        Load all countries from Oracle COUNTRIES table.
        Returns list of dicts: {COUNTRY_ID, COUNTRY_NAME, COUNTRY_CODE}
        """
        hook = OracleHook(oracle_conn_id="oracle_default")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNTRY_ID, COUNTRY_NAME, COUNTRY_CODE FROM COUNTRIES")
                rows = cur.fetchall()
        return [{"COUNTRY_ID": row[0], "COUNTRY_NAME": row[1], "COUNTRY_CODE": row[2]} for row in rows]
    

