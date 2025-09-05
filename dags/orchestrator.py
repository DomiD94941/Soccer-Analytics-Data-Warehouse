from airflow.sdk import task, dag, Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import pendulum

# --- Orchestrator configuration / state Variables ---
VAR_COUNT = "orchestrator_first_runs_count"     # how many times we executed the "first_run_path"
VAR_LIMIT = "orchestrator_first_runs_limit"     # maximum allowed executions of first_run_path
VAR_DAY = "orchestrator_daily_key"              # stores the YYYY-MM-DD date key for daily quota
VAR_DAY_COUNT = "orchestrator_daily_count"      # how many orchestrator runs have been executed today
VAR_DAY_LIMIT = "orchestrator_daily_limit"      # daily limit of orchestrator runs

TZ = "Europe/Warsaw"  # timezone used for daily reset

def _get_count() -> int:
    # Get the number of times first_run_path has been executed
    return int(Variable.get(VAR_COUNT, default=0))

def _get_limit() -> int:
    # Threshold for switching from first_run_path to fast_path
    return int(Variable.get(VAR_LIMIT, default=1))  # only 1 heavy bootstrap run

def _today_key() -> str:
    # Current date string in YYYY-MM-DD format
    return pendulum.now(TZ).date().isoformat()

def _get_daily_limit() -> int:
    # Maximum orchestrator runs allowed per day
    return int(Variable.get(VAR_DAY_LIMIT, default=20))

def _get_daily_state():
    """
    Returns (today_key, current_count).
    If a new day started (different key than stored), reset counter to 0.
    """
    today = _today_key()
    stored_key = Variable.get(VAR_DAY, default=0)
    if stored_key != today:
        Variable.set(VAR_DAY, str(today))
        Variable.set(VAR_DAY_COUNT, "0")
        return today, 0
    return today, int(Variable.get(VAR_DAY_COUNT, default=0))

@dag(
    description=(
        "Top-level orchestrator for football ingestion DAGs. "
        "On first run(s), performs a full bootstrap (seasons + countries) before continuing. "
        "After bootstrap, switches to a fast path that only runs downstream DAGs "
        "(venues → leagues → teams → fixtures → fixture_stats). "
        "Daily and first-run counters are enforced via Airflow Variables to prevent overloading "
        "the API and Oracle DB (e.g. max 1 bootstrap run, max 20 orchestrator runs/day)."
    ),    
    schedule=None,  # Run every 5 minutes (the daily limit will still enforce max 20/day)
    catchup=False,  # Do not backfill historical runs
    max_active_runs=1,  # Prevent concurrent runs (avoids counter race conditions)
    tags=["orchestrator", "football", "control"]
)
def football_orchestrator():

    # --- Branch 0: daily quota check ---
    @task.branch
    def check_daily_quota():
        _, count = _get_daily_state()
        limit = _get_daily_limit()
        return "stop_today" if count >= limit else "choose_path"

    stop_today = EmptyOperator(task_id="stop_today")

    # --- Branch 1: decide between first_run_path vs fast_path ---
    @task.branch
    def choose_path():
        return "first_run_path" if _get_count() < _get_limit() else "fast_path"

    first_run_path = EmptyOperator(task_id="first_run_path")
    fast_path = EmptyOperator(task_id="fast_path")

    # --- First-run-only section (executed only once, at the very beginning) ---
    run_seasons = TriggerDagRunOperator(
        task_id="run_seasons",
        trigger_dag_id="seasons_dag",
        conf={"orchestrator_run_id": "{{ run_id }}"},  # propagate orchestrator run id for traceability
        wait_for_completion=True,  # block until target DAG finishes
        deferrable=True,           # save scheduler resources while waiting
    )
    run_countries = TriggerDagRunOperator(
        task_id="run_countries",
        trigger_dag_id="countries_dag",
        conf={"orchestrator_run_id": "{{ run_id }}"},
        wait_for_completion=True,
        deferrable=True,
    )

    # Increment first-run counter only if both SEASONS and COUNTRIES succeed
    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def inc_full_path_counter():
        Variable.set(VAR_COUNT, str(_get_count() + 1))

    # Join node so either branch (first/fast) converges before the common tail
    start_venues = EmptyOperator(
        task_id="start_venues",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS  # tolerant join
    )

    # --- Always-run tail (executed on every orchestrator run if daily quota allows) ---
    run_venues = TriggerDagRunOperator(
        task_id="run_venues",
        trigger_dag_id="venues_dag",
        conf={"orchestrator_run_id": "{{ run_id }}"},
        wait_for_completion=True,
        deferrable=True,
    )
    run_leagues = TriggerDagRunOperator(
        task_id="run_leagues",
        trigger_dag_id="leagues_dag",
        conf={"orchestrator_run_id": "{{ run_id }}"},
        wait_for_completion=True,
        deferrable=True,
    )
    run_teams = TriggerDagRunOperator(
        task_id="run_teams",
        trigger_dag_id="teams_dag",
        conf={"orchestrator_run_id": "{{ run_id }}"},
        wait_for_completion=True,
        deferrable=True,
    )
    run_fixtures = TriggerDagRunOperator(
        task_id="run_fixtures",
        trigger_dag_id="fixtures_dag",
        conf={"orchestrator_run_id": "{{ run_id }}"},
        wait_for_completion=True,
        deferrable=True,
    )
    run_fixture_stats = TriggerDagRunOperator(
        task_id="run_fixture_stats",
        trigger_dag_id="fixture_stats_dag",
        conf={"orchestrator_run_id": "{{ run_id }}"},
        wait_for_completion=True,
        deferrable=True,
    )

    # Optional gaps (sleep) between DAG triggers; might not be needed if wait_for_completion=True
    gap_after_seasons = BashOperator(task_id="gap_after_seasons", bash_command="sleep 60")
    gap_after_countries = BashOperator(task_id="gap_after_countries", bash_command="sleep 60")
    gap_after_venues = BashOperator(task_id="gap_after_venues", bash_command="sleep 60")
    gap_after_leagues = BashOperator(task_id="gap_after_leagues", bash_command="sleep 60")
    gap_after_teams = BashOperator(task_id="gap_after_teams", bash_command="sleep 60")
    gap_after_fixtures = BashOperator(task_id="gap_after_fixtures", bash_command="sleep 60")

    # Increment daily counter only after the whole tail (venues -> stats) finishes successfully
    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def inc_daily_counter():
        today, count = _get_daily_state()
        Variable.set(VAR_DAY, today)
        Variable.set(VAR_DAY_COUNT, str(count + 1))

    # --- Task wiring ---

    # Check daily quota first
    quota = check_daily_quota()

    # Create the branch task object (don’t use a string here)
    branch_choice = choose_path()

    # Fan out to either stop or the next branch task
    quota >> [stop_today, branch_choice]

    # First-run vs fast path (this part was already fine)
    branch_choice >> [first_run_path, fast_path]

    incr_counter_task = inc_full_path_counter()

    # Full bootstrap path: SEASONS + COUNTRIES, then increment counter, then join
    first_run_path >> run_seasons >> gap_after_seasons
    first_run_path >> run_countries >> gap_after_countries
    [gap_after_seasons, gap_after_countries] >> incr_counter_task >> start_venues

    # Fast path: skip bootstrap and go directly to venues
    fast_path >> start_venues

    # Common tail (executed sequentially with optional gaps)
    start_venues >> run_venues >> gap_after_venues \
        >> run_leagues >> gap_after_leagues \
        >> run_teams >> gap_after_teams \
        >> run_fixtures >> gap_after_fixtures \
        >> run_fixture_stats >> inc_daily_counter()

football_orchestrator()
