"""Bronze job: Kafka -> Delta (streaming).

Reads sensor events from Kafka topic `sensor.raw`, parses the JSON payload,
deduplicates by event_id within a 10-minute event-time watermark, and appends
to the Bronze Delta table on HDFS, partitioned by event_date.

Exactly-once semantics come from Structured Streaming + Delta checkpoint
stored on HDFS (_checkpoints/bronze).
"""

from pyspark.sql.functions import col, current_timestamp, from_json, split, when

from common import (
    BRONZE_CHECKPOINT,
    BRONZE_PATH,
    EVENT_SCHEMA,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    get_spark,
)


def main() -> None:
    spark = get_spark("bronze-kafka-to-delta")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("kafka.max.poll.records", 500)
        .load()
        .selectExpr("CAST(value AS STRING) AS json_value", "timestamp AS kafka_ts")
    )

    parsed = raw.select(
        from_json(col("json_value"), EVENT_SCHEMA).alias("e"),
        col("kafka_ts"),
    ).select(
        col("e.event_id").alias("event_id"),
        col("e.sensor_id").alias("sensor_id"),
        col("e.event_time").alias("event_time"),
        col("e.ingest_time").alias("ingest_time"),
        col("e.sensor_type").alias("sensor_type"),
        col("e.value").alias("value"),
        col("e.unit").alias("unit"),
        col("e.location").alias("location"),
        col("e.sequence_no").alias("sequence_no"),
        col("kafka_ts"),
    )

    # Watermark bounds the dedup state (10 minutes) so it does not grow forever.
    # Late events beyond the watermark are still ingested (they land in Bronze);
    # dedup only applies inside the watermark window.
    with_event_ts = parsed.withColumn(
        "event_ts",
        when(col("event_time").isNotNull(), col("event_time").cast("timestamp"))
        .otherwise(col("kafka_ts")),
    )

    deduped = (
        with_event_ts
        .withWatermark("event_ts", "10 minutes")
        .dropDuplicates(["event_id"])
    )

    out = (
        deduped
        .withColumn("ingest_time_spark", current_timestamp())
        .withColumn("event_date", split(col("event_time"), "T").getItem(0))
        # Drop the helper columns used only for watermarking.
        .drop("event_ts", "kafka_ts")
    )

    query = (
        out.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", BRONZE_CHECKPOINT)
        .trigger(processingTime="5 seconds")
        .partitionBy("event_date")
        .start(BRONZE_PATH)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
