from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime


with DAG(
    dag_id="taxi_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
) as dag:

    detect_snapshot = BashOperator(
        task_id="detect_snapshot",
        bash_command="""
    python /opt/airflow/dags/scripts/detect_snapshot.py
    """,
    )

    incremental_load = BashOperator(
        task_id="incremental_load",
        bash_command="""
    python /opt/airflow/dags/scripts/incremental_load.py
    """,
    )

    mark_snapshot_processed = BashOperator(
        task_id="mark_snapshot_processed",
        bash_command="""
    python /opt/airflow/dags/scripts/mark_snapshot_processed.py
    """,
    )

    refresh_marts = BashOperator(
        task_id="refresh_marts",
        bash_command="""
    python /opt/airflow/dags/scripts/refresh_marts.py
    """,
    )
    rollback = BashOperator(
        task_id="rollback",
        trigger_rule=TriggerRule.ONE_FAILED,
        bash_command="""
    python /opt/airflow/dags/scripts/rollback.py
    """,
    )
    detect_snapshot >> incremental_load >> refresh_marts >> mark_snapshot_processed

    incremental_load >> rollback
    refresh_marts >> rollback