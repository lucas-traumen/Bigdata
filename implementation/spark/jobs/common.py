"""Shared constants, metric registry, schemas and session builder for the Spark jobs.

Everything is driven by environment variables so the same code runs inside
the spark-stream container and the spark-batch one-shot container. No Delta
Lake, no HDFS: paths are plain local Parquet under DATA_ROOT (bind-mounted
at /data/bigdata).

IMPORTANT (module contract): the module-level code is deliberately PySpark-free.
The metric registry (METRIC_CONFIG / METRIC_NAMES / METRIC_ALIASES) is the
SINGLE SOURCE OF TRUTH shared by validation.py, pg_sink.py, batch.py and the
unit tests, which all import it WITHOUT pyspark installed. The Parquet schemas
are therefore built by cached functions (bronze_schema()/silver_schema()/
quarantine_schema()) instead of module-level constants; the streaming app calls
them at startup.
"""

import os
from functools import lru_cache

# ------------------------------------------------------------- endpoints ----
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sensor_raw")
SPARK_SHUFFLE_PARTITIONS = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "2")

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

# -------------------------------------------------------- metric registry ----
# Single source of truth for the six environmental metrics (plan E3.2):
# validation bounds, alert threshold/direction and the Silver / staging /
# sensor_latest / Gold schema columns. Adding a metric = adding ONE row here
# (plus its schema column) — the validation, alert and Gold logic stay
# table-driven and unchanged.
#
# Per metric: (min_bound, max_bound, alert_threshold, alert_direction).
#   * bounds are inclusive and physical (outside => per-metric issue, metric
#     masked to NULL in Silver, row still valid);
#   * alert fires when value > threshold (direction "high") or
#     value < threshold (direction "low"), evaluated in Q3 for every staged
#     event; None threshold = informational metric without alerts;
#   * direction is a stored alert attribute so the dashboard/API can render
#     "above/below" without re-deriving it.
METRIC_CONFIG = {
    "temperature_c": (-50.0, 200.0, 35.0, "high"),
    "humidity_pct": (0.0, 100.0, 80.0, "high"),
    "co2_ppm": (0.0, 5000.0, 1000.0, "high"),
    "pressure_hpa": (300.0, 1100.0, 950.0, "low"),
    "pm25_ugm3": (0.0, 1000.0, 35.0, "high"),
    "light_lux": (0.0, 200000.0, None, None),
}
# Stable iteration order (payload/schema column order follow this tuple).
METRIC_NAMES = tuple(METRIC_CONFIG)

# Legacy JSON field accepted as an alias of a canonical metric (empty/missing
# canonical field falls through to the alias — documented in README).
METRIC_ALIASES = {"temperature_c": ("value",)}


def metric_bounds(name: str) -> tuple[float, float]:
    """Inclusive physical bounds (min, max) for one metric name."""
    cfg = METRIC_CONFIG[name]
    return cfg[0], cfg[1]


def alert_metrics() -> tuple[str, ...]:
    """Metric names that have an alert threshold configured."""
    return tuple(name for name in METRIC_NAMES if METRIC_CONFIG[name][2] is not None)


def alert_rule(name: str) -> tuple[float, str]:
    """(threshold, direction) for one alertable metric (ValueError otherwise)."""
    cfg = METRIC_CONFIG[name]
    if cfg[2] is None:
        raise ValueError(f"metric {name!r} has no alert threshold configured")
    return cfg[2], cfg[3]


# ---------------------------------------------------------------- schemas ----
# Q1 output (Bronze): raw bytes + Kafka provenance. No parsing, no dedup:
# malformed JSON is preserved exactly as received (plan 3.2, acceptance #5).
@lru_cache(maxsize=1)
def bronze_schema():
    from pyspark.sql.types import (
        IntegerType, LongType, StringType, StructField, StructType, TimestampType,
    )
    return StructType([
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
#
# One event is a data-fusion VECTOR: the six metric columns (explicit, in
# METRIC_NAMES order so the JDBC column guard test in
# implementation/tests/test_jdbc_columns.py can parse them statically) carry
# the metrics the sensor published; absent metrics stay NULL (station subsets
# are normal, not an error). ``metric_issues`` joins the per-metric reason
# vocabulary ("value_out_of_bounds:<metric>", "non_numeric:<metric>") with
# ';' when a metric had to be masked to NULL.
@lru_cache(maxsize=1)
def silver_schema():
    from pyspark.sql.types import (
        BooleanType, DoubleType, IntegerType, LongType, StringType,
        StructField, StructType, TimestampType,
    )
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("event_time_utc", TimestampType(), True),
        StructField("event_time_src", StringType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("humidity_pct", DoubleType(), True),
        StructField("co2_ppm", DoubleType(), True),
        StructField("pressure_hpa", DoubleType(), True),
        StructField("pm25_ugm3", DoubleType(), True),
        StructField("light_lux", DoubleType(), True),
        StructField("metric_issues", StringType(), True),
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
# is ever lost silently (plan 3.2). Only IDENTITY errors land here
# (missing/bad event_id, sensor_id or timestamps); per-metric problems stay in
# Silver with the metric masked to NULL (see silver_schema).
@lru_cache(maxsize=1)
def quarantine_schema():
    from pyspark.sql.types import (
        IntegerType, LongType, StringType, StructField, StructType, TimestampType,
    )
    return StructType([
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


def get_spark(app_name: str):
    """Build the SparkSession for local-mode jobs.

    Notes:
      * session timezone UTC — every timestamp is normalized to UTC (plan 3.2);
      * ANSI mode is explicitly OFF: casts of malformed strings must yield
        NULL (quarantine path) instead of raising (Spark 4 defaults to ANSI);
      * shuffle partitions 2 matches local[2] and the demo data volume.
    """
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
