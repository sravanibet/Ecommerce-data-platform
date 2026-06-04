from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup
from kafka import KafkaAdminClient


def kafka_topic_ready() -> bool:
    try:
        admin = KafkaAdminClient(bootstrap_servers="kafka:9092", request_timeout_ms=10000)
        topics = admin.list_topics()
        return "orders" in topics
    except Exception:
        return False


def create_dag():
    default_args = {
        "owner": "airflow",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "sla": timedelta(minutes=15),
    }

    with DAG(
        dag_id="streaming_pipeline",
        default_args=default_args,
        start_date=datetime(2026, 6, 1),
        schedule="@hourly",
        catchup=True,
        max_active_runs=1,
        tags=["streaming", "delta", "kafka"],
    ) as dag:
        start = EmptyOperator(task_id="start")

        wait_for_kafka = PythonSensor(
            task_id="wait_for_kafka",
            python_callable=kafka_topic_ready,
            poke_interval=20,
            timeout=300,
            mode="reschedule",
        )

        with TaskGroup(group_id="streaming_layers") as streaming_layers:
            previous = None
            for layer in ["bronze", "silver", "gold"]:
                task = BashOperator(
                    task_id=f"run_{layer}_layer",
                    bash_command=(
                        f"python /opt/workspace/spark_jobs/streaming_pipeline.py "
                        f"--duration 90 --kafka-bootstrap kafka:9092 --topic orders --layer {layer}"
                    ),
                )

                if previous:
                    previous >> task
                previous = task

        validate = BashOperator(
            task_id="validate_streaming_outputs",
            bash_command="python /opt/workspace/streaming/validate_streams.py",
        )

        end = EmptyOperator(task_id="end")

        start >> wait_for_kafka >> streaming_layers >> validate >> end

    return dag


global_dag = create_dag()
