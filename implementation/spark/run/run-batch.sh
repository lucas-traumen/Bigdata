#!/usr/bin/env bash
# One-shot hourly Gold batch over Silver Parquet.
# Usage: run-batch.sh --start <UTC hour> --end <UTC hour>   (window [start,end))
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/opt/spark}"

exec "$SPARK_HOME/bin/spark-submit" \
  --master "local[2]" \
  --deploy-mode client \
  --name "iot-batch-gold-hourly" \
  --driver-memory 1g \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.ansi.enabled=false \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.driver.extraJavaOptions="-XX:+ExitOnOutOfMemoryError" \
  "$SPARK_HOME/jobs/batch.py" "$@"
