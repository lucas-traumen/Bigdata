#!/usr/bin/env bash
# Reset pipeline data (idempotent). Use before a clean smoke test so that
# Bronze/Silver/Gold/quarantine counts start from zero.
#
# WARNING: this deletes all Delta tables and Spark checkpoints on HDFS.
# Kafka topic offsets are NOT reset (truncate if needed, see note below).
#
#   cd implementation
#   ./scripts/reset-pipeline.sh
set -euo pipefail

cd "$(dirname "$0")/.."
NN=hdfs-namenode

echo "== Removing HDFS Delta tables and checkpoints"
docker exec "$NN" hdfs dfs -rm -r -skipTrash /delta /_checkpoints 2>/dev/null || true

echo "== Recreating HDFS roots"
docker exec "$NN" hdfs dfs -mkdir -p /delta /_checkpoints

echo "== Reset done. (Kafka topic sensor.raw data kept; delete & recreate it"
echo "   if you also need offsets at 0.)"
