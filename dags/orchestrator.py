from datetime import timedelta
from airflow.sdk import dag, Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensorAsync
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Configurable limit (default 19). Change in UI if needed.
VAR_COUNT = "orchestrator_first_runs_count"
VAR_LIMIT = "orchestrator_first_runs_limit"

def _get_count() -> int:
    return int(Variable.get(VAR_COUNT, default=0))

def _get_limit() -> int:
    return int(Variable.get(VAR_LIMIT, default=19))

def _choose_path() -> str:
    # If we've done the full path fewer than LIMIT times, do it again.
    return "first_run_path" if _get_count() < _get_limit() else "fast_path"

def _inc_count():
    # Increase counter after a successful full-path execution
    Variable.set(VAR_COUNT, str(_get_count() + 1))

@dag(
    schedule="*/5 * * * *",     # every 5 minutes (optional; keep if you want it scheduled)
    catchup=False,
    max_active_runs=1,          # important so the counter doesn't race
    tags=["orchestrator"],
)
def orchestrator_dag():
    # Branch: choose “first_run_path” for the first 19 runs, else “fast_path”
    choose_path = BranchPythonOperator(
        task_id="choose_path",
        python_callable=_choose_path,
    )

    first_run_path = EmptyOperator(task_id="first_run_path")
    fast_path = EmptyOperator(task_id="fast_path")

    # --- first-run-only tasks (counted) ---
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

    # Increment the counter ONLY when the full path has actually completed successfully
    inc_full_path_counter = PythonOperator(
        task_id="inc_full_path_counter",
        python_callable=_inc_count,
        trigger_rule=TriggerRule.ALL_SUCCESS,  # require both gaps to succeed
    )

    # Join node so either branch can proceed
    start_venues = EmptyOperator(
        task_id="start_venues",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # --- always-run tail ---
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

    # Wiring
    choose_path >> [first_run_path, fast_path]

    # Full path (counted): seasons + countries, then bump counter, then join
    first_run_path >> run_seasons >> gap_after_seasons
    first_run_path >> run_countries >> gap_after_countries
    [gap_after_seasons, gap_after_countries] >> inc_full_path_counter >> start_venues

    # Fast path (20th+ run): jump straight to venues
    fast_path >> start_venues

    # Common tail
    start_venues >> run_venues >> gap_after_venues >> run_leagues >> gap_after_leagues \
        >> run_teams >> gap_after_teams >> run_fixtures >> gap_after_fixtures \
        >> run_fixture_stats

orchestrator_dag()
