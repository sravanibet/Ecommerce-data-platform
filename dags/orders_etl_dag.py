from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='orders_etl_dag',
    default_args={'owner': 'airflow'},
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    run_orders_etl = BashOperator(
        task_id='run_orders_etl',
        bash_command='python /opt/workspace/spark_jobs/orders_etl.py',
    )

    run_orders_etl
