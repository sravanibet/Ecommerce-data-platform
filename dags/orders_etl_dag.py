from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'orders_etl_dag',
    default_args=default_args,
    description='DAG to trigger orders ETL Spark job',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task to run the Spark job
    run_spark_job = BashOperator(
        task_id='run_orders_etl',
        bash_command='spark-submit --master spark://spark-master:7077 /opt/spark_jobs/orders_etl.py'
    )

    run_spark_job