"""Shared constants, schemas and session builder for the Spark jobs.

Everything is driven by environment variables so the same code runs inside
the spark-stream container, the spark-batch one-shot container and the
Airflow scheduler's LocalExecutor children. No Delta Lake, no HDFS: paths are
plain local Parquet under DATA_ROOT (bind-mounted at /data/bigdata).
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ------------------------------------------------------------- endpoints ----
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sensor_raw")

# ------------------------------------------------------------- data paths ----
# Container-internal mount point; host side is ${DATA_ROOT} from .env.
DATA_ROOT = os.environ.get("DATA_ROOT", "/data/bigdata")

BRONZE_PATH = f"{DATA_ROOT}/bronze/sensor"
SILVER_PATH = f"{DATA_ROOT}/silver/sensor"
QUARANTINE_PATH = f"{DATA_ROOT}/quarantine/sensor"

CHECKPOINT_BASE = f"{DATA_ROOT}/checkpoints"
CP_Q1 = f"{CHECKPOINT_BASE}/q1"
CP_Q2A = f"{CHECKPOINT_BASE}/q2a"
CP_Q2B = f"{CHECKPOINT_BASE}/q2b"
CP_Q3 = f"{CHECKPOINT_BASE}/q3"

PROGRESS_LOG = f"{DATA_ROOT}/logs/stream-progress.jsonl"
HEARTBEAT_FILE = f"{DATA_ROOT}/control/stream-heartbeat"

# ---------------------------------------------------------------- schemas ----
# Q1 output (Bronze): raw bytes + Kafka provenance. No parsing, no dedup:
# malformed JSON is preserved exactly as received (plan 3.2, acceptance #5).
BRONZE_SCHEMA = StructType([
    StructField("raw_payload", StringType(), True),
    StructField("mqtt_topic", StringType(), True),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("received_at_utc", TimestampType(), True),
])

# Q2a output (Silver): normalized + validated rows. Duplicates by event_id are
# allowed here; consumers (Q3, batch) dedup deterministically.
SILVER_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("event_time_utc", TimestampType(), True),
    StructField("event_time_src", StringType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("ingest_time_utc", TimestampType(), True),
    StructField("ingest_time_src", StringType(), True),
    StructField("sensor_type", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("location", StringType(), True),
    StructField("sequence_no", StringType(), True),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("received_at_utc", TimestampType(), True),
    StructField("is_late", BooleanType(), True),
])

# Q2b output (Quarantine): invalid rows + machine-readable reason. record_key
# falls back to topic-partition-offset when event_id is missing so no record
# is ever lost silently (plan 3.2).
QUARANTINE_SCHEMA = StructType([
    StructField("record_key", StringType(), True),
    StructField("error_reason", StringType(), True),
    StructField("raw_payload", StringType(), True),
    StructField("mqtt_topic", StringType(), True),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("received_at_utc", TimestampType(), True),
])


def get_spark(app_name: str) -> SparkSession:
    """Build the SparkSession for local-mode jobs.

    Notes:
      * session timezone UTC — every timestamp is normalized to UTC (plan 3.2);
      * ANSI mode is explicitly OFF: casts of malformed strings must yield
        NULL (quarantine path) instead of raising (Spark 4 defaults to ANSI);
      * shuffle partitions 2 matches local[2] and the demo data volume.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
