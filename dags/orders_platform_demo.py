from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="orders_platform_demo",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "ecommerce"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_raw_orders = BashOperator(
        task_id="check_raw_orders",
        bash_command="test -f /opt/airflow/data/raw/orders.csv && echo 'orders.csv is available'",
    )

    show_project_files = BashOperator(
        task_id="show_processed_files",
        bash_command="ls -la /opt/airflow/data/processed",
    )

    finish = EmptyOperator(task_id="finish")

    start >> check_raw_orders >> show_project_files >> finish
