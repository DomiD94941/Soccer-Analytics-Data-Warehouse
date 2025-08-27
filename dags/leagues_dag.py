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