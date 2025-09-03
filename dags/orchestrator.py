from datetime import timedelta
from airflow.sdk import dag, Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensorAsync
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

VAR_NAME = "orchestrator_first_run_done"

def is_first_run() -> bool:
    return Variable.get(VAR_NAME, default_var="false").lower() != "true"

def mark_first_run_done():
    Variable.set(VAR_NAME, "true")

@dag(schedule=None, catchup=False, tags=["orchestrator"])
def orchestrator_dag():

    # Decide which path to take
    choose_path = BranchPythonOperator(
        task_id="choose_path",
        python_callable=lambda: "first_run_path" if is_first_run() else "fast_path",
    )

    # Branch heads (simple dummies)
    first_run_path = EmptyOperator(task_id="first_run_path")
    fast_path = EmptyOperator(task_id="fast_path")

    # First-run-only tasks
    run_seasons = TriggerDagRunOperator(
        task_id="run_seasons",
        trigger_dag_id="seasons_dag",
        wait_for_completion=True,
        deferrable=True,
        conf={"orchestrator_run_id": "{{ run_id }}"},
        poke_interval=60,
    )
    gap_after_seasons = TimeDeltaSensorAsync(task_id="gap_after_seasons", delta=timedelta(minutes=2))

    run_countries = TriggerDagRunOperator(
        task_id="run_countries",
        trigger_dag_id="countries_dag",
        wait_for_completion=True,
        deferrable=True,
        conf={"orchestrator_run_id": "{{ run_id }}"},
    )
    gap_after_countries = TimeDeltaSensorAsync(task_id="gap_after_countries", delta=timedelta(minutes=2))

    # Join node: proceed after either branch unblocks
    start_venues = EmptyOperator(
        task_id="start_venues",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Always-run pipeline
    run_venues = TriggerDagRunOperator(
        task_id="run_venues",
        trigger_dag_id="venues_dag",
        wait_for_completion=True,
        deferrable=True,
        conf={"orchestrator_run_id": "{{ run_id }}"},
    )
    gap_after_venues = TimeDeltaSensorAsync(task_id="gap_after_venues", delta=timedelta(minutes=5))

    run_leagues = TriggerDagRunOperator(
        task_id="run_leagues",
        trigger_dag_id="leagues_dag",
        wait_for_completion=True,
        deferrable=True,
        conf={"orchestrator_run_id": "{{ run_id }}"},
    )
    gap_after_leagues = TimeDeltaSensorAsync(task_id="gap_after_leagues", delta=timedelta(minutes=3))

    run_teams = TriggerDagRunOperator(
        task_id="run_teams",
        trigger_dag_id="teams_dag",
        wait_for_completion=True,
        deferrable=True,
        conf={"orchestrator_run_id": "{{ run_id }}"},
    )
    gap_after_teams = TimeDeltaSensorAsync(task_id="gap_after_teams", delta=timedelta(minutes=3))

    run_fixtures = TriggerDagRunOperator(
        task_id="run_fixtures",
        trigger_dag_id="fixtures_dag",
        wait_for_completion=True,
        deferrable=True,
        conf={"orchestrator_run_id": "{{ run_id }}"},
    )
    gap_after_fixtures = TimeDeltaSensorAsync(task_id="gap_after_fixtures", delta=timedelta(minutes=5))

    run_fixture_stats = TriggerDagRunOperator(
        task_id="run_fixture_stats",
        trigger_dag_id="fixture_stats_dag",
        wait_for_completion=True,
        deferrable=True,
        conf={"orchestrator_run_id": "{{ run_id }}"},
    )

    mark_done = PythonOperator(
        task_id="mark_first_run_done",
        python_callable=mark_first_run_done,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Wiring
    choose_path >> [first_run_path, fast_path]

    # First-run path
    first_run_path >> run_seasons >> gap_after_seasons
    first_run_path >> run_countries >> gap_after_countries
    [gap_after_seasons, gap_after_countries] >> start_venues

    # Fast path
    fast_path >> start_venues

    # Common tail
    start_venues >> run_venues >> gap_after_venues >> run_leagues >> gap_after_leagues \
        >> run_teams >> gap_after_teams >> run_fixtures >> gap_after_fixtures \
        >> run_fixture_stats >> mark_done

orchestrator_dag()
