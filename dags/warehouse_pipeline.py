from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="warehouse_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    run_warehouse_etl = BashOperator(
        task_id="run_warehouse_etl",
        bash_command="python /opt/workspace/spark_jobs/warehouse_etl.py",
    )

    run_warehouse_etl

