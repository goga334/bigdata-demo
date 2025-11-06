from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys

# додаємо /opt/app в path
sys.path.append("/opt/app")

from producer import run_producer
from consumer_to_bronze import run_consumer
from build_silver_gold import build_silver, build_gold

default_args = {
    "owner": "data_team",
    "retries": 0,
    "depends_on_past": False,
}

with DAG(
    dag_id="shop_events_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    t0_generate = PythonOperator(
        task_id="generate_events",
        python_callable=run_producer,
        op_kwargs={"num_events": 20_000},
    )

    t1_bronze = PythonOperator(
        task_id="consume_to_bronze",
        python_callable=run_consumer,
        op_kwargs={"max_messages": 20_000, "batch_size": 5_000},
    )

    t2_silver = PythonOperator(
        task_id="build_silver",
        python_callable=build_silver,
    )

    t3_gold = PythonOperator(
        task_id="build_gold",
        python_callable=build_gold,
    )

    t0_generate >> t1_bronze >> t2_silver >> t3_gold
