#!/bin/sh
# Submit the Silver batch job (client mode).
#
# Modes:
#   run-silver.sh           loop forever: process Bronze -> Silver every
#                           SILVER_INTERVAL seconds (default 30)
#   run-silver.sh --once    single pass (used by smoke tests / Airflow mode B)
#
# Waits until the Bronze table exists before the first run.
# Memory is read from env (with Phase 1 defaults) so mode B (Airflow compose
# override) can tune it without editing this script.
set -e

DRIVER_MEM="${SPARK_DRIVER_MEMORY:-512m}"
EXECUTOR_MEM="${SPARK_EXECUTOR_MEMORY:-640m}"
PYSPARK_MEM="${SPARK_EXECUTOR_PYSPARK_MEMORY:-256m}"

INTERVAL="${SILVER_INTERVAL:-30}"
ONCE=0
if [ "${1:-}" = "--once" ]; then ONCE=1; shift; fi

NN=${HDFS_NAMENODE:-hdfs-namenode:9870}
check_url() {
  curl -s -o /dev/null -w "%{http_code}" \
    "http://$NN/webhdfs/v1$1?op=GETFILESTATUS"
}

echo "[run-silver] waiting for Bronze table (_delta_log)..."
until [ "$(check_url /delta/bronze/sensor/_delta_log)" = "200" ]; do
  sleep 10
done

while true; do
  /opt/spark/bin/spark-submit \
    --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
    --deploy-mode client \
    --conf spark.driver.memory="$DRIVER_MEM" \
    --conf spark.executor.memory="$EXECUTOR_MEM" \
    --conf spark.executor.pyspark.memory="$PYSPARK_MEM" \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.sql.session.timeZone=UTC \
    /opt/spark/jobs/silver.py "$@" || echo "[run-silver] pass failed, retrying"
  if [ "$ONCE" = "1" ]; then break; fi
  sleep "$INTERVAL"
done
