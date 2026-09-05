#!/usr/bin/env bash
# One-shot cluster init: create HDFS directories and the Kafka topic.
# Run after `docker compose up -d` and once hdfs-namenode + kafka-1 are healthy.
#
#   cd implementation/infra
#   ../scripts/init-cluster.sh
#
# Idempotent: re-running is safe.
set -euo pipefail

cd "$(dirname "$0")/../infra"

NN=hdfs-namenode
KAFKA=kafka-1

echo "== Wait for HDFS NameNode safe mode off"
for i in $(seq 1 60); do
  if docker exec "$NN" hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF; then
    break
  fi
  sleep 2
done

echo "== Create HDFS directories"
# Only the two root dirs from the plan. The Delta tables create their own
# leaf directories (bronze/sensor, silver/..., gold/...) on first write;
# pre-creating them as empty non-Delta dirs would make the first write fail.
for d in /delta /_checkpoints; do
  docker exec "$NN" hdfs dfs -mkdir -p "$d"
done
echo "OK: HDFS roots created"

echo "== Create Kafka topic sensor.raw (3 partitions)"
# NOTE: RF=2 is impossible with a single broker; RF=1 is used instead
# (documented in implementation/README.md "Do lech so voi bao cao").
docker exec -e JMX_PORT= "$KAFKA" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic sensor.raw \
  --partitions 3 \
  --replication-factor 1

docker exec -e JMX_PORT= "$KAFKA" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic sensor.raw

echo "== Done"
