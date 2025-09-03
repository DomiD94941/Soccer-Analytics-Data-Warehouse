from airflow.sdk import dag, task, task_group, Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.sensors.base import PokeReturnValue
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.utils.trigger_rule import TriggerRule


@dag
def fixture_stats_dag():

    @task.sql(conn_id="oracle_default")
    def create_fixtures_team_stats_table():
        return """
        DECLARE
          e_exists EXCEPTION;
          PRAGMA EXCEPTION_INIT(e_exists, -955);
        BEGIN
          EXECUTE IMMEDIATE '
            CREATE TABLE FIXTURE_TEAM_STATS (
              FIXTURE_ID          NUMBER NOT NULL,
              TEAM_ID             NUMBER NOT NULL,

              SHOTS_ON_GOAL       NUMBER,
              SHOTS_OFF_GOAL      NUMBER,
              TOTAL_SHOTS         NUMBER,
              BLOCKED_SHOTS       NUMBER,
              SHOTS_INSIDE_BOX    NUMBER,
              SHOTS_OUTSIDE_BOX   NUMBER,
              FOULS               NUMBER,
              CORNER_KICKS        NUMBER,
              OFFSIDES            NUMBER,
              BALL_POSSESSION_PCT NUMBER,       
              YELLOW_CARDS        NUMBER,
              RED_CARDS           NUMBER,
              GOALKEEPER_SAVES    NUMBER,
              TOTAL_PASSES        NUMBER,
              PASSES_ACCURATE     NUMBER,
              PASSES_PCT          NUMBER,   

              CONSTRAINT PK_FIXTURE_TEAM_STATS PRIMARY KEY (FIXTURE_ID, TEAM_ID),
              CONSTRAINT FK_FTS_FIXTURE FOREIGN KEY (FIXTURE_ID) REFERENCES FIXTURES(FIXTURE_ID),
              CONSTRAINT FK_FTS_TEAM    FOREIGN KEY (TEAM_ID)    REFERENCES TEAMS(TEAM_ID)
            )';
        EXCEPTION
          WHEN e_exists THEN NULL;
        END;
        """

    @task
    def fetch_fixtures() -> list[dict[str, int]]:
        hook = OracleHook(oracle_conn_id="oracle_default")
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT FIXTURE_ID FROM FIXTURES ORDER BY FIXTURE_ID")
                rows = cur.fetchall()
        return [{"FIXTURE_ID": 215662}]

    @task
    def select_fixture(fixtures: list[dict[str, int]]) -> dict[str, int]:
        key = "fixture_stats_fixture_id_indicator"
        idx = int(Variable.get(key, default=0))
        total = len(fixtures)
        if total == 0:
            return {"key": key, "idx": 0, "total": 0}
        if idx >= total:
            idx = 0
        return {"key": key, "fixture_id": fixtures[idx]["FIXTURE_ID"], "idx": idx, "total": total}

    @task.sensor(poke_interval=30, timeout=120)
    def is_api_available(fixture_selection: dict) -> PokeReturnValue:
        import requests
        log = LoggingMixin().log
        fixture_id = fixture_selection.get("fixture_id")
        if not fixture_id:
            return PokeReturnValue(is_done=True, xcom_value={"response": []})

        api_key = Variable.get("API_KEY")
        url = "https://v3.football.api-sports.io/fixtures/statistics"
        headers = {
            "x-rapidapi-host": "v3.football.api-sports.io",
            "x-rapidapi-key": api_key
        }
        try:
            r = requests.get(url, headers=headers, params={"fixture": fixture_id}, timeout=30)
            log.info("API /fixtures/statistics status: %s (fixture=%s)", r.status_code, fixture_id)
            r.raise_for_status()
            return PokeReturnValue(is_done=True, xcom_value=r.json())
        except requests.RequestException as e:
            log.warning("API not available yet: %s", e)
            return PokeReturnValue(is_done=False, xcom_value=None)

    @task.branch
    def are_stats_exist(payload: dict, **context):
        data = payload.get("response", []) if payload else []
        if not data:
            return "advance_fixtures_stats_fixture_id_pointer"
        context['ti'].xcom_push(key='stats_payload', value=data)
        return "format_stats"

    @task
    def format_stats(fixture_selection: dict, **context):
        """
        Produces one dict per team:
        { FIXTURE_ID, TEAM_ID, SHOTS_ON_GOAL, ..., PASSES_PCT }
        """
        payload = context['ti'].xcom_pull(key='stats_payload', task_ids='are_stats_exist') or []
        fixture_id = fixture_selection["fixture_id"]

        def norm(label: str) -> str:
            # normalize API "type" strings
            return (label or "").lower().replace(" ", "").replace("-", "")

        # map normalized labels to Oracle column names
        COLMAP = {
            "shotsongoal":        "SHOTS_ON_GOAL",
            "shotsoffgoal":       "SHOTS_OFF_GOAL",
            "totalshots":         "TOTAL_SHOTS",
            "blockedshots":       "BLOCKED_SHOTS",
            "shotsinsidebox":     "SHOTS_INSIDE_BOX",
            "shotsoutsidebox":    "SHOTS_OUTSIDE_BOX",
            "fouls":              "FOULS",
            "cornerkicks":        "CORNER_KICKS",
            "offsides":           "OFFSIDES",
            "ballpossession":     "BALL_POSSESSION_PCT",
            "yellowcards":        "YELLOW_CARDS",
            "redcards":           "RED_CARDS",
            "goalkeepersaves":    "GOALKEEPER_SAVES",
            "totalpasses":        "TOTAL_PASSES",
            "passesaccurate":     "PASSES_ACCURATE",
            "passes%":            "PASSES_PCT",
        }

        def to_number(v):
            if v is None: return None
            if isinstance(v, (int, float)): return float(v)
            s = str(v).strip().replace("%", "")
            try: return float(s)
            except Exception: return None

        rows = []
        for team_block in payload:
            team = team_block.get("team") or {}
            team_id = team.get("id")
            # base row with all columns None
            row = {
                "FIXTURE_ID": fixture_id,
                "TEAM_ID": team_id,
                "SHOTS_ON_GOAL": None, "SHOTS_OFF_GOAL": None, "TOTAL_SHOTS": None, "BLOCKED_SHOTS": None,
                "SHOTS_INSIDE_BOX": None, "SHOTS_OUTSIDE_BOX": None, "FOULS": None, "CORNER_KICKS": None,
                "OFFSIDES": None, "BALL_POSSESSION_PCT": None, "YELLOW_CARDS": None, "RED_CARDS": None,
                "GOALKEEPER_SAVES": None, "TOTAL_PASSES": None, "PASSES_ACCURATE": None, "PASSES_PCT": None,
            }
            for stat in team_block.get("statistics") or []:
                col = COLMAP.get(norm(stat.get("type", "")))
                if col:
                    row[col] = to_number(stat.get("value"))
            rows.append(row)

        context['ti'].xcom_push(key='fixture_team_stats_rows', value=rows)


    @task_group
    def load_stats():
        @task
        def fixture_stats_to_csv(**context) -> str:
            import os, csv
            rows = context['ti'].xcom_pull(key='fixture_team_stats_rows', task_ids='format_stats') or []
            path = "/tmp/fixture_team_stats.csv"
            fieldnames = [
                "FIXTURE_ID","TEAM_ID",
                "SHOTS_ON_GOAL","SHOTS_OFF_GOAL","TOTAL_SHOTS","BLOCKED_SHOTS",
                "SHOTS_INSIDE_BOX","SHOTS_OUTSIDE_BOX","FOULS","CORNER_KICKS","OFFSIDES",
                "BALL_POSSESSION_PCT","YELLOW_CARDS","RED_CARDS","GOALKEEPER_SAVES",
                "TOTAL_PASSES","PASSES_ACCURATE","PASSES_PCT"
            ]

            existing = set()
            if os.path.exists(path) and os.path.getsize(path) > 0:
                with open(path, newline="", encoding="utf-8") as f:
                    for r in csv.DictReader(f):
                        existing.add((r.get("FIXTURE_ID","").strip(), r.get("TEAM_ID","").strip()))

            seen, new_rows = set(), []
            for r in rows:
                key = (str(r["FIXTURE_ID"]), str(r["TEAM_ID"]))
                if "" in key or key in existing or key in seen:
                    continue
                seen.add(key)
                new_rows.append(r)

            with open(path, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                if f.tell() == 0:
                    w.writeheader()
                if new_rows:
                    w.writerows(new_rows)
            return f"Appended {len(new_rows)} rows to {path}"

        @task
        def fixture_stats_to_oracle(**context) -> str:
            rows = context['ti'].xcom_pull(key='fixture_team_stats_rows', task_ids='format_stats') or []
            if not rows:
                return "No rows to upsert."

            sql = """
            MERGE INTO FIXTURE_TEAM_STATS t
            USING (
              SELECT
                :1  AS FIXTURE_ID, :2  AS TEAM_ID,
                :3  AS SHOTS_ON_GOAL, :4  AS SHOTS_OFF_GOAL, :5  AS TOTAL_SHOTS, :6  AS BLOCKED_SHOTS,
                :7  AS SHOTS_INSIDE_BOX, :8  AS SHOTS_OUTSIDE_BOX, :9  AS FOULS, :10 AS CORNER_KICKS, :11 AS OFFSIDES,
                :12 AS BALL_POSSESSION_PCT, :13 AS YELLOW_CARDS, :14 AS RED_CARDS, :15 AS GOALKEEPER_SAVES,
                :16 AS TOTAL_PASSES, :17 AS PASSES_ACCURATE, :18 AS PASSES_PCT
              FROM dual
            ) s
            ON (t.FIXTURE_ID = s.FIXTURE_ID AND t.TEAM_ID = s.TEAM_ID)
            WHEN MATCHED THEN UPDATE SET
              t.SHOTS_ON_GOAL       = s.SHOTS_ON_GOAL,
              t.SHOTS_OFF_GOAL      = s.SHOTS_OFF_GOAL,
              t.TOTAL_SHOTS         = s.TOTAL_SHOTS,
              t.BLOCKED_SHOTS       = s.BLOCKED_SHOTS,
              t.SHOTS_INSIDE_BOX    = s.SHOTS_INSIDE_BOX,
              t.SHOTS_OUTSIDE_BOX   = s.SHOTS_OUTSIDE_BOX,
              t.FOULS               = s.FOULS,
              t.CORNER_KICKS        = s.CORNER_KICKS,
              t.OFFSIDES            = s.OFFSIDES,
              t.BALL_POSSESSION_PCT = s.BALL_POSSESSION_PCT,
              t.YELLOW_CARDS        = s.YELLOW_CARDS,
              t.RED_CARDS           = s.RED_CARDS,
              t.GOALKEEPER_SAVES    = s.GOALKEEPER_SAVES,
              t.TOTAL_PASSES        = s.TOTAL_PASSES,
              t.PASSES_ACCURATE     = s.PASSES_ACCURATE,
              t.PASSES_PCT          = s.PASSES_PCT
            WHEN NOT MATCHED THEN INSERT (
              FIXTURE_ID, TEAM_ID,
              SHOTS_ON_GOAL, SHOTS_OFF_GOAL, TOTAL_SHOTS, BLOCKED_SHOTS,
              SHOTS_INSIDE_BOX, SHOTS_OUTSIDE_BOX, FOULS, CORNER_KICKS, OFFSIDES,
              BALL_POSSESSION_PCT, YELLOW_CARDS, RED_CARDS, GOALKEEPER_SAVES,
              TOTAL_PASSES, PASSES_ACCURATE, PASSES_PCT
            ) VALUES (
              s.FIXTURE_ID, s.TEAM_ID,
              s.SHOTS_ON_GOAL, s.SHOTS_OFF_GOAL, s.TOTAL_SHOTS, s.BLOCKED_SHOTS,
              s.SHOTS_INSIDE_BOX, s.SHOTS_OUTSIDE_BOX, s.FOULS, s.CORNER_KICKS, s.OFFSIDES,
              s.BALL_POSSESSION_PCT, s.YELLOW_CARDS, s.RED_CARDS, s.GOALKEEPER_SAVES,
              s.TOTAL_PASSES, s.PASSES_ACCURATE, s.PASSES_PCT
            ) WHERE EXISTS (SELECT 1 FROM FIXTURE_TEAM_STATS f WHERE f.TEAM_ID = s.TEAM_ID)
            """
            bind_rows = [
                (
                    r["FIXTURE_ID"], r["TEAM_ID"],
                    r["SHOTS_ON_GOAL"], r["SHOTS_OFF_GOAL"], r["TOTAL_SHOTS"], r["BLOCKED_SHOTS"],
                    r["SHOTS_INSIDE_BOX"], r["SHOTS_OUTSIDE_BOX"], r["FOULS"], r["CORNER_KICKS"], r["OFFSIDES"],
                    r["BALL_POSSESSION_PCT"], r["YELLOW_CARDS"], r["RED_CARDS"], r["GOALKEEPER_SAVES"],
                    r["TOTAL_PASSES"], r["PASSES_ACCURATE"], r["PASSES_PCT"]
                )
                for r in rows if r.get("FIXTURE_ID") and r.get("TEAM_ID")
            ]

            hook = OracleHook(oracle_conn_id="oracle_default")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(sql, bind_rows)
                    upserted = cur.rowcount or 0
                conn.commit()
            return f"Upserted {upserted} rows."

        fixture_stats_to_csv() >> fixture_stats_to_oracle()

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def advance_fixtures_stats_fixture_id_pointer(fixture_selection: dict) -> None:
        f_key, f_idx, f_total = fixture_selection["key"], fixture_selection["idx"], fixture_selection["total"]
        next_f = f_idx + 1
        if next_f < f_total:
            next_f = 0
        Variable.set(f_key, str(next_f))

    create_fixtures_team_stats_table()
    fixture_sel = select_fixture(fetch_fixtures())
    payload = is_api_available(fixture_selection=fixture_sel)
    branch = are_stats_exist(payload)
    update = advance_fixtures_stats_fixture_id_pointer(fixture_selection=fixture_sel)

    branch >> format_stats(fixture_selection=fixture_sel) >> load_stats() >> update
    branch >> update


fixture_stats_dag()
