#!/usr/bin/env python3
"""Hourly Gold batch: Silver Parquet -> gold.sensor_hourly (UTC [start,end)).

Usage:
    batch.py --start 2026-09-15T01:00:00Z --end 2026-09-15T03:00:00Z

Contract (plan 3.3 / E3.3):
  * --start/--end are UTC ISO-8601; BOTH must sit exactly on hour boundaries
    and the window [start, end) must contain at least one full hour;
  * invalid input is rejected BEFORE any write happens;
  * reads Silver directly; dedups by event_id with a deterministic tie-break
    (received_at_utc desc, then kafka_partition desc, then kafka_offset desc)
    so rerunning the same window yields identical aggregates;
  * aggregates PER METRIC (dimension from METRIC_CONFIG in common.py): each
    event contributes a reading to every metric it carries; the result grain
    is (sensor_id, hour_start, metric) with count/avg/min/max;
  * writes gold.sensor_hourly through a full-replace upsert => reruns never
    accumulate; late data is fixed by rerunning the affected hour(s);
  * no collect()/toPandas() of history: rows go to PostgreSQL through the
    JDBC writer, only scalar counts return to the driver.

The pure helpers (parse_utc, validate_window, truncate_hour, metric_stack_expr)
live at module level WITHOUT pyspark imports so unit tests can import them
standalone.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import METRIC_NAMES  # noqa: E402  (module-level code is pyspark-free)


def parse_utc(s: str) -> datetime:
    """Parse --start/--end into an aware UTC datetime.

    Accepts 'Z' or an explicit offset (e.g. '+07:00', converted to UTC).
    Rejects naive timestamps: the batch contract is explicitly UTC.
    """
    if not isinstance(s, str) or not s.strip():
        raise ValueError("timestamp must be a non-empty ISO-8601 UTC string")
    try:
        dt = datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(
            f"invalid ISO-8601 timestamp {s!r}; expected e.g. 2026-09-15T01:00:00Z"
        ) from None
    if dt.tzinfo is None:
        raise ValueError(
            f"timestamp {s!r} must carry an explicit UTC offset (Z or +hh:mm)"
        )
    return dt.astimezone(timezone.utc)


def validate_window(start: datetime, end: datetime) -> None:
    """[start,end) contract: hour boundaries, end > start, >= 1 full hour."""
    if (start.minute, start.second, start.microsecond) != (0, 0, 0):
        raise ValueError(f"start {start.isoformat()} is not on an hour boundary")
    if (end.minute, end.second, end.microsecond) != (0, 0, 0):
        raise ValueError(f"end {end.isoformat()} is not on an hour boundary")
    if end <= start:
        raise ValueError(
            "end must be after start; got "
            f"[start={start.isoformat()}, end={end.isoformat()})"
        )
    if (end - start).total_seconds() < 3600:
        raise ValueError("window must contain at least one full hour")


def truncate_hour(dt: datetime) -> datetime:
    """Python mirror of Spark date_trunc('hour', ts) (session tz UTC)."""
    return dt.replace(minute=0, second=0, microsecond=0)


def metric_stack_expr(metrics: tuple[str, ...] = METRIC_NAMES) -> str:
    """Spark stack() expression unpivoting the metric vector into rows.

    Returns e.g. ``stack(2, 'temperature_c', temperature_c, 'co2_ppm',
    co2_ppm) AS (metric, value)``. Generated from METRIC_CONFIG so adding a
    metric extends the Gold dimension without code changes.
    """
    if not metrics:
        raise ValueError("metric_stack_expr requires at least one metric")
    pairs = ", ".join(f"'{m}', {m}" for m in metrics)
    return f"stack({len(metrics)}, {pairs}) AS (metric, value)"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Hourly Gold aggregation over Silver Parquet (UTC [start,end))")
    ap.add_argument("--start", required=True,
                    help="UTC ISO-8601 hour boundary, e.g. 2026-09-15T01:00:00Z")
    ap.add_argument("--end", required=True,
                    help="UTC ISO-8601 hour boundary, exclusive, e.g. 2026-09-15T03:00:00Z")
    args = ap.parse_args(argv)

    # reject invalid input BEFORE touching Spark or PostgreSQL
    try:
        start = parse_utc(args.start)
        end = parse_utc(args.end)
        validate_window(start, end)
    except ValueError as exc:
        print(f"[batch] REJECTED: {exc}", flush=True)
        return 2

    # heavy imports only after validation (keeps --help and arg errors cheap)
    from pyspark.sql import SparkSession
    from pyspark.sql import Window, functions as F

    from common import SILVER_PATH
    import pg_sink

    spark = (
        SparkSession.builder
        .appName("iot-batch-gold-hourly")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        if not os.path.isdir(SILVER_PATH) or not any(os.scandir(SILVER_PATH)):
            print(f"[batch] no Silver data at {SILVER_PATH}; nothing to do",
                  flush=True)
            return 0

        silver = spark.read.parquet(SILVER_PATH)
        in_range = silver.where(
            "event_id IS NOT NULL AND sensor_id IS NOT NULL "
            "AND event_time_utc IS NOT NULL"
        ).where(
            (F.col("event_time_utc") >= F.lit(start))
            & (F.col("event_time_utc") < F.lit(end))
        )

        # deterministic per-event_id pick (see module docstring)
        w = Window.partitionBy("event_id").orderBy(
            F.col("received_at_utc").desc_nulls_last(),
            F.col("kafka_partition").desc_nulls_last(),
            F.col("kafka_offset").desc_nulls_last(),
        )
        deduped = (
            in_range.withColumn("_rn", F.row_number().over(w))
            .where(F.col("_rn") == 1)
            .drop("_rn")
        )

        # unpivot the metric vector: one reading row per (event, metric);
        # rows whose metric is NULL (station subset) contribute nothing.
        stacked = deduped.select(
            "sensor_id",
            F.date_trunc("hour", F.col("event_time_utc")).alias("hour_start"),
            F.expr(metric_stack_expr()).alias("metric", "value"),
        ).where(F.col("value").isNotNull())

        agg = (
            stacked
            .groupBy("sensor_id", "hour_start", "metric")
            .agg(
                F.count(F.lit(1)).alias("event_count"),
                F.avg("value").alias("avg_value"),
                F.min("value").alias("min_value"),
                F.max("value").alias("max_value"),
            )
        )

        rows, events = pg_sink.apply_hourly_batch(agg, start, end)
        print(f"[batch] DONE window=[{start.isoformat()}, {end.isoformat()}): "
              f"{rows} sensor-hour-metric rows backed by {events} readings", flush=True)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
