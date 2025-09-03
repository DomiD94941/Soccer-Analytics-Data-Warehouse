from datetime import timedelta
from airflow.sdk import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.time_delta import TimeDeltaSensorAsync  # deferrable

@dag(schedule=None, catchup=False, tags=["orchestrator"])
def orchestrator_dag():

    run_seasons = TriggerDagRunOperator(
        task_id="run_seasons",
        trigger_dag_id="seasons_dag",
        wait_for_completion=True,
        deferrable=True,
        conf={"orchestrator_run_id": "{{ run_id }}"},
        poke_interval=60, 
    )
    gap_after_seasons = TimeDeltaSensorAsync(
        task_id="gap_after_seasons",
        delta=timedelta(minutes=2),
    )

    run_countries = TriggerDagRunOperator(
        task_id="run_countries",
        trigger_dag_id="countries_dag",
        wait_for_completion=True,
        deferrable=True,
        conf={"orchestrator_run_id": "{{ run_id }}"},
    )
    gap_after_countries = TimeDeltaSensorAsync(
        task_id="gap_after_countries",
        delta=timedelta(minutes=2),
    )

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

    run_seasons >> gap_after_seasons
    run_countries >> gap_after_countries

    [gap_after_seasons, gap_after_countries] >> run_venues >> gap_after_venues >> run_leagues >> gap_after_leagues \
    >> run_teams >> gap_after_teams >> run_fixtures >> gap_after_fixtures \
    >> run_fixture_stats

orchestrator_dag()
