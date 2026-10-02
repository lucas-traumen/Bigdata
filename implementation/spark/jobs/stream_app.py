#!/usr/bin/env python3
"""Single SparkSession running the whole streaming pipeline (local[2]).

Queries, each with its own checkpoint directory (plan 3.1 / 4.2):

  Q1  kafka `sensor_raw`  -> Bronze Parquet   (raw_payload + Kafka/MQTT
      provenance + received_at_utc; NO parsing, NO dedup — malformed JSON is
      preserved exactly as received)
  Q2a Bronze file stream  -> Silver Parquet   (validated rows only)
  Q2b Bronze file stream  -> Quarantine Parquet (invalid rows + error_reason;
      complementary predicates of Q2a, so the branches are exclusive)
  Q3  Silver file stream  -> PostgreSQL       (sensor_latest upsert, per-metric
      alerts insert-idempotent, processed_events replay marker) via pg_sink.py

Operational behavior:
  * one trigger interval for all queries (default 5s), bounded input via
    maxOffsetsPerTrigger (Kafka) / maxFilesPerTrigger (file streams);
  * heartbeat file refreshed for the container healthcheck;
  * per-query progress (incl. Kafka end offsets) appended as JSONL to
    DATA_ROOT/logs/stream-progress.jsonl — scripts/run-mode.sh uses this to
    verify a clean drain before batch runs;
  * any terminated query aborts the process so the container restart policy
    kicks in (checkpoints make the resume safe).
"""

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import (  # noqa: E402
    BRONZE_PATH,
    CP_Q1,
    CP_Q2A,
    CP_Q2B,
    CP_Q3,
    HEARTBEAT_FILE,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    PROGRESS_LOG,
    QUARANTINE_PATH,
    SILVER_PATH,
    METRIC_NAMES,
    bronze_schema,
    silver_schema,
    get_spark,
)
import pg_sink  # noqa: E402
import validation  # noqa: E402

TRIGGER_INTERVAL = os.environ.get(
    "STREAM_TRIGGER", os.environ.get("SPARK_TRIGGER_INTERVAL", "5 seconds")
)
MAX_OFFSETS_PER_TRIGGER = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "20000")
MAX_FILES_PER_TRIGGER = os.environ.get("MAX_FILES_PER_TRIGGER", "1000")
MAX_FILES_PER_TRIGGER_Q3 = os.environ.get("MAX_FILES_PER_TRIGGER_Q3", "500")
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "10"))

MQTT_TOPIC_EXPR = """
CASE WHEN size(filter(headers, h -> h.key = 'mqtt_topic')) > 0
     THEN cast(element_at(filter(headers, h -> h.key = 'mqtt_topic'), 1).value AS string)
     ELSE NULL END
"""


def build_bronze_df(kafka_df):
    """Project the Kafka source into the Bronze schema (lossless)."""
    from pyspark.sql import functions as F

    return kafka_df.select(
        F.col("value").alias("raw_payload"),
        F.expr(MQTT_TOPIC_EXPR).alias("mqtt_topic"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("received_at_utc"),
    )


def silver_projection(df):
    """Valid rows -> Silver columns (identity + metric vector + provenance)."""
    from pyspark.sql import functions as F
    return df.where(F.col("is_valid")).select(
        "event_id", "sensor_id", "event_time_utc", "event_time_src",
        *METRIC_NAMES,
        "metric_issues",
        "ingest_time_utc", "ingest_time_src",
        "sensor_type", "unit", "location", "sequence_no",
        "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        "received_at_utc", "is_late",
    )


def quarantine_projection(df):
    from pyspark.sql import functions as F
    return df.where(~F.col("is_valid")).select(
        "record_key", "error_reason", "raw_payload", "mqtt_topic",
        "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        "received_at_utc",
    )


def main() -> int:
    spark = get_spark("iot-stream-app")

    # ---------------- Q1: Kafka -> Bronze (raw, no dedup) ----------------
    kafka_src = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("includeHeaders", "true")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        # demo topic has short retention; do not restart-loop on expired
        # offsets (data loss is reported by the recovery test instead)
        .option("failOnDataLoss", "false")
        .load()
    )
    q1 = (
        build_bronze_df(kafka_src).writeStream
        .format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CP_Q1)
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("q1_bronze")
        .start()
    )

    # ------------- Q2a/Q2b: Bronze -> Silver / Quarantine ----------------
    bronze_src = (
        spark.readStream.format("parquet")
        .schema(bronze_schema())
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .load(BRONZE_PATH)
    )
    validated = validation.build_validity_columns(bronze_src)

    q2a = (
        silver_projection(validated).writeStream
        .format("parquet")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", CP_Q2A)
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("q2a_silver")
        .start()
    )

    q2b = (
        quarantine_projection(validated).writeStream
        .format("parquet")
        .option("path", QUARANTINE_PATH)
        .option("checkpointLocation", CP_Q2B)
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("q2b_quarantine")
        .start()
    )

    # ------------------ Q3: Silver -> PostgreSQL -------------------------
    silver_src = (
        spark.readStream.format("parquet")
        .schema(silver_schema())
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER_Q3)
        .load(SILVER_PATH)
    )
    q3 = (
        silver_src.writeStream
        .foreachBatch(pg_sink.apply_stream_batch)
        .option("checkpointLocation", CP_Q3)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName("q3_pg")
        .start()
    )

    queries = [q1, q2a, q2b, q3]

    def _graceful_stop(signum, frame):  # noqa: ARG001
        print(f"[stream] signal {signum}: stopping streaming queries gracefully",
              flush=True)
        for q in queries:
            try:
                q.stop()
            except Exception as exc:  # noqa: BLE001
                print(f"[stream] stop {q.name} failed: {exc}", flush=True)
        spark.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _graceful_stop)
    signal.signal(signal.SIGINT, _graceful_stop)

    os.makedirs(os.path.dirname(PROGRESS_LOG), exist_ok=True)
    os.makedirs(os.path.dirname(HEARTBEAT_FILE), exist_ok=True)
    progress_fh = open(PROGRESS_LOG, "a", encoding="utf-8", buffering=1)
    last_batch_seen: dict[str, object] = {}
    last_heartbeat = 0.0

    print(f"[stream] running: {[q.name for q in queries]} "
          f"trigger={TRIGGER_INTERVAL}", flush=True)

    while True:
        time.sleep(2)

        now = time.monotonic()
        if now - last_heartbeat >= HEARTBEAT_EVERY_S:
            last_heartbeat = now
            try:
                with open(HEARTBEAT_FILE, "w", encoding="utf-8") as fh:
                    fh.write(datetime.now(timezone.utc).isoformat() + "\n")
            except OSError as exc:
                print(f"[stream] heartbeat write failed: {exc}", flush=True)

        for q in queries:
            if not q.isActive:
                exc = q.exception()
                print(f"[stream] FATAL query {q.name} terminated: {exc}", flush=True)
                progress_fh.close()
                spark.stop()
                return 1
            p = q.lastProgress
            if not p:
                continue
            batch_id = p.get("batchId")
            if batch_id is not None and last_batch_seen.get(q.name) == batch_id:
                continue
            last_batch_seen[q.name] = batch_id
            record = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "query": q.name,
                "batch_id": batch_id,
                "num_input_rows": p.get("numInputRows"),
                "duration_ms": (p.get("durationMs") or {}).get("triggerExecution"),
                "sources": [
                    {
                        "description": s.get("description"),
                        "start_offset": s.get("startOffset"),
                        "end_offset": s.get("endOffset"),
                    }
                    for s in p.get("sources", [])
                ],
                "sink": p.get("sink"),
            }
            progress_fh.write(json.dumps(record, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    sys.exit(main())
