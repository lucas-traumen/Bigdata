"""Gold job: Silver -> 1m/5m window aggregates (streaming).

Computes avg/min/max/count of value over tumbling windows (1 minute and
5 minutes) per location x sensor_type, reading the Silver Delta table as a
streaming source and appending to two Gold Delta tables.

Trigger modes (--trigger flag):
  processing     (default) a micro-batch every 10 seconds, long-running
                 (Phase 1 behaviour, mode A).
  available-now  process all data currently available, then stop. Used by
                 the Airflow `gold_refresh` task (mode B) so each scheduled
                 run has a clear start/end like a batch job.
"""

import argparse

from pyspark.sql.functions import col, lit, window

from common import (
    CHECKPOINT_BASE,
    GOLD_1M_PATH,
    GOLD_5M_PATH,
    SILVER_PATH,
    get_spark,
)

WINDOWS = [("1 minute", GOLD_1M_PATH), ("5 minutes", GOLD_5M_PATH)]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--trigger",
        choices=["processing", "available-now"],
        default="processing",
        help="streaming trigger mode (default: processing)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("gold-window-aggregates")

    silver_stream = (
        spark.readStream
        .format("delta")
        .load(SILVER_PATH)
    )

    queries = []
    for duration, out_path in WINDOWS:
        agg = (
            silver_stream
            .withWatermark("event_time", "10 minutes")
            .groupBy(
                window(col("event_time"), duration),
                col("location"),
                col("sensor_type"),
            )
            .avg("value")
            .min("value")
            .max("value")
            .count()
            .withColumnRenamed("avg(value)", "value_avg")
            .withColumnRenamed("min(value)", "value_min")
            .withColumnRenamed("max(value)", "value_max")
            .withColumn("window_start", col("window.start"))
            .withColumn("window_end", col("window.end"))
            .withColumn("window_duration", lit(duration))
            .drop("window")
        )

        writer = (
            agg.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold-{duration.replace(' ', '')}")
        )
        if args.trigger == "available-now":
            # Drain everything currently available, then terminate.
            writer = writer.trigger(availableNow=True)
        else:
            writer = writer.trigger(processingTime="10 seconds")
        q = writer.start(out_path)
        queries.append(q)

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
