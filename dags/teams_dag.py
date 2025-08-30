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
    
teams_dag()