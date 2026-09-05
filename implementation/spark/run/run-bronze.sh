#!/bin/sh
# Submit the Bronze streaming job in client mode (driver runs in this container).
#
# Memory is read from env (with Phase 1 defaults) so mode B (Airflow compose
# override) can tune it without editing this script.
set -e

DRIVER_MEM="${SPARK_DRIVER_MEMORY:-512m}"
EXECUTOR_MEM="${SPARK_EXECUTOR_MEMORY:-640m}"
PYSPARK_MEM="${SPARK_EXECUTOR_PYSPARK_MEMORY:-256m}"

echo "[run-bronze] waiting for Kafka topic sensor.raw..."
# NOTE: driver memory must be >= ~450MB or Spark refuses to start
# (INVALID_DRIVER_MEMORY), so even this tiny check uses 512m.
until /opt/spark/bin/spark-submit \
    --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
    --conf spark.driver.memory="$DRIVER_MEM" \
    --conf spark.executor.memory="$EXECUTOR_MEM" \
    /opt/spark/jobs/check_kafka_topic.py >/dev/null 2>&1; do
  sleep 10
done

exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --deploy-mode client \
  --conf spark.driver.memory="$DRIVER_MEM" \
  --conf spark.executor.memory="$EXECUTOR_MEM" \
  --conf spark.executor.pyspark.memory="$PYSPARK_MEM" \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.sql.session.timeZone=UTC \
  /opt/spark/jobs/bronze.py "$@"
