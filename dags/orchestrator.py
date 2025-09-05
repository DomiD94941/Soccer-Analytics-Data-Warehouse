from datetime import timedelta
from airflow.sdk import task, dag, Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# Orchestrator configuration

# Persistent counters kept in Airflow Variables so logic survives scheduler restarts.
# VAR_COUNT: how many times we've executed the "first run path" successfully.
# VAR_LIMIT: how many times we should take the first run path before switching to fast path.
VAR_COUNT = "orchestrator_first_runs_count"
VAR_LIMIT = "orchestrator_first_runs_limit"

def _get_count() -> int:
    # Current number of completed first-run cycles
    return int(Variable.get(VAR_COUNT, default=0))

def _get_limit() -> int:
    # Threshold after which we stop running the heavy bootstrap path
    return int(Variable.get(VAR_LIMIT, default=19))

@dag(
    schedule="*/5 * * * *",   # Run every 5 minutes (optional—tune as needed)
    catchup=False,            # Do not backfill historical runs
    max_active_runs=1,        # Prevent concurrent runs to avoid counter races
    tags=["orchestrator"]
)
def orchestrator_dag():
    # Decide which branch to take:
    # - For the first N (default 19) executions -> "first_run_path" (bootstraps foundational data)
    # - Afterwards -> "fast_path" (skip the heavy bootstrap)
    @task.branch
    def choose_path():
        return "first_run_path" if _get_count() < _get_limit() else "fast_path"

    # Branch heads (used purely for graph readability and branching)
    first_run_path = EmptyOperator(task_id="first_run_path")
    fast_path = EmptyOperator(task_id="fast_path")

    
    # First-run-only section (counted)
   
    # Run SEASONS DAG (blocks until completion). Using deferrable operator to save scheduler resources.
    run_seasons = TriggerDagRunOperator(
        task_id="run_seasons",
        trigger_dag_id="seasons_dag",
        conf={"orchestrator_run_id": "{{ run_id }}"} # propagate orchestrator run id for traceability
    )

    # Run COUNTRIES DAG (also deferrable + blocking)
    run_countries = TriggerDagRunOperator(
        task_id="run_countries",
        trigger_dag_id="countries_dag",
        conf={"orchestrator_run_id": "{{ run_id }}"}
    )

    # Only increment the first-run counter when BOTH streams (seasons + countries) fully succeed
    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def inc_full_path_counter():
        Variable.set(VAR_COUNT, str(_get_count() + 1))

    # Join node so either branch (first/fast) can converge before the common tail
    start_venues = EmptyOperator(
        task_id="start_venues",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS # be tolerant to partial upstream branches
    )

    
    # Always-run tail (executed on every orchestrator run)
  
    # Venues -> Leagues -> Teams -> Fixtures -> Fixture Stats
    
    run_venues = TriggerDagRunOperator(
        task_id="run_venues",
        trigger_dag_id="venues_dag",
        conf={"orchestrator_run_id": "{{ run_id }}"}
    )

    run_leagues = TriggerDagRunOperator(
        task_id="run_leagues",
        trigger_dag_id="leagues_dag",
        conf={"orchestrator_run_id": "{{ run_id }}"}
    )

    run_teams = TriggerDagRunOperator(
        task_id="run_teams",
        trigger_dag_id="teams_dag",
        conf={"orchestrator_run_id": "{{ run_id }}"}
    )

    run_fixtures = TriggerDagRunOperator(
        task_id="run_fixtures",
        trigger_dag_id="fixtures_dag",
        conf={"orchestrator_run_id": "{{ run_id }}"}
    )

    run_fixture_stats = TriggerDagRunOperator(
        task_id="run_fixture_stats",
        trigger_dag_id="fixture_stats_dag",
        conf={"orchestrator_run_id": "{{ run_id }}"}
    )

    gap_after_seasons = BashOperator(task_id="gap_after_seasons", bash_command="sleep 60")
    gap_after_countries = BashOperator(task_id="gap_after_countries", bash_command="sleep 60")
    gap_after_venues = BashOperator(task_id="gap_after_venues", bash_command="sleep 60")
    gap_after_leagues = BashOperator(task_id="gap_after_leagues", bash_command="sleep 60")
    gap_after_teams = BashOperator(task_id="gap_after_teams", bash_command="sleep 60")
    gap_after_fixtures = BashOperator(task_id="gap_after_fixtures", bash_command="sleep 60")

    # Wiring (task dependencies)

    # Branch to either first-run or fast path
    branch_choice = choose_path()
    incr_counter_task = inc_full_path_counter()

    branch_choice >> [first_run_path, fast_path]

    # Full bootstrap path: run seasons + countries in parallel, then bump counter, then join
    first_run_path >> run_seasons >> gap_after_seasons
    first_run_path >> run_countries >> gap_after_countries
    [gap_after_seasons, gap_after_countries] >> incr_counter_task >> start_venues

    # Fast path: skip bootstrap and jump straight to venues
    fast_path >> start_venues

    # Common tail executed on every run (sequential with gaps between)
    start_venues >> run_venues >> gap_after_venues \
        >> run_leagues >> gap_after_leagues \
        >> run_teams >> gap_after_teams \
        >> run_fixtures >> gap_after_fixtures \
        >> run_fixture_stats


orchestrator_dag()
