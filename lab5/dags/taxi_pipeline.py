from airflow import DAG
from airflow.operators.bash import BashOperator

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

    dbt_run = BashOperator(
        task_id="dbt_run",

        bash_command="""
        docker exec dbt bash -c '
            cd /usr/app/s3_analytics &&
            dbt run
        '
        """
    )

    mark_snapshot_processed = BashOperator(
        task_id="mark_snapshot_processed",
        bash_command="""
    python /opt/airflow/dags/scripts/mark_snapshot_processed.py
    """,
    )

    detect_snapshot >> dbt_run  >> mark_snapshot_processed
    # Это
