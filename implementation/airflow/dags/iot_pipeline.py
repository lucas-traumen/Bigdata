"""Hourly Gold aggregation DAG.

Runs the SAME batch program as manual mode
(``/opt/spark/run/run-batch.sh --start <UTC> --end <UTC>``) through a
LocalExecutor subprocess inside this container (spark-submit local[2], driver
heap 1g — covered by the scheduler's mem_limit in compose.yaml).

Guarantees (plan 3.1 / acceptance #10):
  * LocalExecutor, no Celery, no webserver in this demo image;
  * ``max_active_runs=1`` + core parallelism 1 => at most ONE batch job at a
    time;
  * data_interval boundaries of an hourly schedule are UTC hour boundaries,
    matching the batch [start, end) contract;
  * retries are bounded (2 x 2 min) and idempotent (full-replace upsert).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

BATCH_PROGRAM = "/opt/spark/run/run-batch.sh"

default_args = {
    "owner": "bigdata-demo",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="iot_gold_hourly",
    description=(
        "Hourly gold.sensor_hourly aggregation from Silver Parquet "
        "(UTC [start,end) — same batch program as manual mode)"
    ),
    schedule="0 * * * *",  # hourly; Airflow uses UTC by default
    start_date=pendulum.datetime(2026, 9, 14, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    default_args=default_args,
    tags=["iot", "gold", "hourly"],
) as dag:
    gold_batch = BashOperator(
        task_id="spark_batch_gold_hourly",
        bash_command=(
            f"{BATCH_PROGRAM} "
            "--start '{{ data_interval_start }}' "
            "--end '{{ data_interval_end }}'"
        ),
        # PG* credentials and bounds are inherited from the scheduler
        # container environment (set in compose.yaml) so the spark-submit
        # child writes to the same PostgreSQL app DB.
    )

    gold_batch
