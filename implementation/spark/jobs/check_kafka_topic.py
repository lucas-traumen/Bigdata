"""Check that the Kafka topic exists (exit 0) or not (exit 1).

Used by run-bronze.sh to wait until init-cluster.sh has created `sensor.raw`.
"""

import sys

from pyspark.sql import SparkSession


def main() -> int:
    from common import KAFKA_BOOTSTRAP, KAFKA_TOPIC

    spark = (
        SparkSession.builder
        .appName("check-kafka-topic")
        .master(sys.argv[1] if len(sys.argv) > 1 else "spark://spark-master:7077")
        .getOrCreate()
    )
    try:
        df = (
            spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", KAFKA_TOPIC)
            .load()
        )
        df.limit(0).collect()
    except Exception as exc:
        print(f"topic not ready: {exc}", file=sys.stderr)
        spark.stop()
        return 1
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
