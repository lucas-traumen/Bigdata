#!/usr/bin/env bash
# One-shot hourly Gold batch over Silver Parquet.
# Usage: run-batch.sh --start <UTC hour> --end <UTC hour>   (window [start,end))
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
SPARK_MASTER="${SPARK_MASTER:-local[2]}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-1g}"
SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-2}"

exec "$SPARK_HOME/bin/spark-submit" \
  --master "$SPARK_MASTER" \
  --deploy-mode client \
  --name "iot-batch-gold-hourly" \
  --driver-memory "$SPARK_DRIVER_MEMORY" \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.ansi.enabled=false \
  --conf "spark.sql.shuffle.partitions=$SPARK_SHUFFLE_PARTITIONS" \
  --conf spark.driver.extraJavaOptions="-XX:+ExitOnOutOfMemoryError" \
  "$SPARK_HOME/jobs/batch.py" "$@"
