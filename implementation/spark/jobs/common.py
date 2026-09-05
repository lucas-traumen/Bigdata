"""Shared constants and helpers for the Bronze/Silver/Gold Spark jobs.

All tables are Delta tables stored on HDFS. Paths are fully qualified so the
jobs do not depend on fs.defaultFS. Hostnames resolve via the Docker Compose
bridge network (service names).
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

# ------------------------------------------------------------- endpoints ----
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sensor.raw")
SPARK_MASTER = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")

HDFS_BASE = os.environ.get("HDFS_BASE", "hdfs://hdfs-namenode:9000")

BRONZE_PATH = f"{HDFS_BASE}/delta/bronze/sensor"
SILVER_PATH = f"{HDFS_BASE}/delta/silver/sensor"
QUARANTINE_PATH = f"{HDFS_BASE}/delta/silver/sensor_quarantine"
GOLD_1M_PATH = f"{HDFS_BASE}/delta/gold/agg_1m"
GOLD_5M_PATH = f"{HDFS_BASE}/delta/gold/agg_5m"

CHECKPOINT_BASE = f"{HDFS_BASE}/_checkpoints"
BRONZE_CHECKPOINT = f"{CHECKPOINT_BASE}/bronze"

# --------------------------------------------------------------- schemas ----
# Bronze keeps the raw payload as strings (medallion convention: cast late).
# from_json never fails on type mismatch this way; Silver does the casting and
# the physical-bounds validation.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("sensor_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("ingest_time", StringType(), True),
    StructField("sensor_type", StringType(), True),
    StructField("value", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("location", StringType(), True),
    StructField("sequence_no", StringType(), True),
])


def get_spark(app_name: str) -> SparkSession:
    """Build a SparkSession configured for Delta Lake.

    The master is NOT set here: spark-submit's --master flag wins, so the
    same jobs run on the standalone cluster (run/*.sh) or in local mode
    (count-tables.py from smoke tests).
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
