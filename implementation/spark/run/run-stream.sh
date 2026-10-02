#!/usr/bin/env bash
# Launch the single streaming Spark app (local[2], one SparkSession, 4 queries).
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
SPARK_MASTER="${SPARK_MASTER:-local[2]}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-1g}"
SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-2}"
DATA_ROOT="${DATA_ROOT:-/data/bigdata}"

# File stream sources need the input directories to exist (even when empty).
mkdir -p "$DATA_ROOT/bronze/sensor" "$DATA_ROOT/silver/sensor" \
         "$DATA_ROOT/quarantine/sensor" \
         "$DATA_ROOT/checkpoints/q1" "$DATA_ROOT/checkpoints/q2a" \
         "$DATA_ROOT/checkpoints/q2b" "$DATA_ROOT/checkpoints/q3" \
         "$DATA_ROOT/logs" "$DATA_ROOT/control"

exec "$SPARK_HOME/bin/spark-submit" \
  --master "$SPARK_MASTER" \
  --deploy-mode client \
  --name "iot-stream-app" \
  --driver-memory "$SPARK_DRIVER_MEMORY" \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.ansi.enabled=false \
  --conf "spark.sql.shuffle.partitions=$SPARK_SHUFFLE_PARTITIONS" \
  --conf spark.driver.extraJavaOptions="-XX:+ExitOnOutOfMemoryError" \
  "$SPARK_HOME/jobs/stream_app.py"
