from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id='orders_etl_dag',
    default_args=default_args,
    description='Orders ETL Spark Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Run Spark ETL Job
    run_orders_etl = BashOperator(
        task_id='run_orders_etl',
        bash_command='python /opt/spark_jobs/orders_etl.py'
    )

    run_orders_etl