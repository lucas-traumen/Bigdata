#!/bin/sh
# Submit the Gold streaming job (client mode).
# Waits until the Silver table exists before starting.
#
# Extra args are forwarded to gold.py, e.g.:
#   run-gold.sh --trigger available-now   (one drain-and-stop run, Airflow mode B)
# Memory is read from env (with Phase 1 defaults) so mode B can tune it
# without editing this script.
set -e

DRIVER_MEM="${SPARK_DRIVER_MEMORY:-512m}"
EXECUTOR_MEM="${SPARK_EXECUTOR_MEMORY:-640m}"
PYSPARK_MEM="${SPARK_EXECUTOR_PYSPARK_MEMORY:-256m}"

NN=${HDFS_NAMENODE:-hdfs-namenode:9870}
check_url() {
  curl -s -o /dev/null -w "%{http_code}" \
    "http://$NN/webhdfs/v1$1?op=GETFILESTATUS"
}

echo "[run-gold] waiting for Silver table (_delta_log)..."
until [ "$(check_url /delta/silver/sensor/_delta_log)" = "200" ]; do
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
  /opt/spark/jobs/gold.py "$@"
