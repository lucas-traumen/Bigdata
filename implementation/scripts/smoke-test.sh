#!/usr/bin/env bash
# End-to-end smoke test (plan acceptance criterion #2):
#   1. Reset pipeline data (clean Bronze/Silver/Gold/quarantine).
#   2. Produce ~10k CLEAN events into Kafka + 10 fast-forward events that
#      push the event-time watermark forward so Gold windows close quickly.
#   3. Count messages that landed in Kafka topic sensor.raw.
#   4. Count rows in Bronze / Silver / Gold Delta tables via spark-submit.
#   5. Verify silver_quarantine is EMPTY (no fault injection in this run).
#
# Prerequisites: stack is up (docker compose up -d), init-cluster.sh ran,
# spark-bronze / spark-silver / spark-gold / spark-worker-* are running.
#
# Usage:
#   cd implementation
#   ./scripts/smoke-test.sh
#
# Tunables: SMOKE_EVENTS (10000), SMOKE_RATE (2000 evt/s).
set -euo pipefail

cd "$(dirname "$0")/.."

EVENTS="${SMOKE_EVENTS:-10000}"
RATE="${SMOKE_RATE:-2000}"
KAFKA=kafka-1
SPARK_IMAGE=bigdata/spark:4.2.0-delta-4.4.0

echo "=========================================="
echo "SMOKE TEST: $EVENTS events @ $RATE evt/s"
echo "=========================================="

# ------------------------------------------------------------- 0. reset ----
echo "[0/6] Stopping jobs and resetting pipeline data (HDFS + Kafka topic)..."
(cd infra && docker compose stop spark-bronze spark-silver spark-gold)
./scripts/reset-pipeline.sh
# Recreate the topic so Bronze's `startingOffsets=earliest` replay is clean
# (old dirty events from previous runs would otherwise be re-ingested).
docker exec -e JMX_PORT= "$KAFKA" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic sensor.raw || true
sleep 5
docker exec -e JMX_PORT= "$KAFKA" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --if-not-exists \
  --topic sensor.raw --partitions 3 --replication-factor 1
(cd infra && docker compose start spark-bronze spark-silver spark-gold)

# ------------------------------------------------------------- produce ----
echo "[1/6] Producing $EVENTS clean events (+10 fast-forward)..."
docker run --rm \
  --network bigdata_bigdata \
  -e BOOTSTRAP_SERVERS=kafka-1:9092 \
  -e TOPIC=sensor.raw \
  -e RATE="$RATE" \
  -e EVENTS="$EVENTS" \
  -e FAULT_INJECT=0 \
  -e FAST_FORWARD=700 \
  bigdata/sensor-simulator:latest
echo "[1/6] Produce done."

# ------------------------------------------------------ count in Kafka ----
echo "[2/6] Counting messages in Kafka sensor.raw..."
# JMX_PORT= empty: stop the CLI tool from starting its own JMX agent, which
# would collide with the broker's JMX port (5555).
KAFKA_COUNT=$(docker exec -e JMX_PORT= "$KAFKA" /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic sensor.raw \
  | awk -F: '{sum += $NF} END {print sum}')
echo "[2/6] Kafka offset sum (total messages in topic): $KAFKA_COUNT"

# --------------------------------------------------- wait for ingestion ----
count_tables() {
  docker run --rm --network bigdata_bigdata \
    --entrypoint /opt/spark/bin/spark-submit \
    "$SPARK_IMAGE" \
    --master local[1] \
    --conf spark.driver.memory=512m \
    /opt/spark/jobs/count-tables.py 2>/dev/null | grep '^TABLE:' || true
}

echo "[3/6] Waiting for Bronze to ingest >= $EVENTS rows..."
BRONZE=0
for i in $(seq 1 60); do
  BRONZE=$(count_tables | awk -F: '/^TABLE:bronze/{print $3}')
  BRONZE=${BRONZE:--1}
  if [ "$BRONZE" -ge "$EVENTS" ]; then break; fi
  echo "      bronze rows so far: $BRONZE"
  sleep 10
done

echo "[4/6] Waiting for Silver + Gold (quarantine must stay empty)..."
GOLD1M=0
for i in $(seq 1 60); do
  COUNTS=$(count_tables)
  SILVER=$(echo "$COUNTS" | awk -F: '/^TABLE:silver/{print $3}'); SILVER=${SILVER:--1}
  QUARANTINE=$(echo "$COUNTS" | awk -F: '/^TABLE:quarantine/{print $3}'); QUARANTINE=${QUARANTINE:--1}
  GOLD1M=$(echo "$COUNTS" | awk -F: '/^TABLE:gold_1m/{print $3}'); GOLD1M=${GOLD1M:--1}
  GOLD5M=$(echo "$COUNTS" | awk -F: '/^TABLE:gold_5m/{print $3}'); GOLD5M=${GOLD5M:--1}
  if [ "$SILVER" -ge "$EVENTS" ] && [ "$GOLD1M" -ge 1 ]; then break; fi
  echo "      silver=$SILVER quarantine=$QUARANTINE gold1m=$GOLD1M gold5m=$GOLD5M"
  sleep 10
done

# ----------------------------------------------------- final tally --------
echo "[5/6] Final table counts:"
count_tables

# ------------------------------------------------------------ verdicts ----
echo "[6/6] Verdicts"
PASS=0
FAIL=0
check() {
  local label="$1" ok="$2"
  if [ "$ok" = "1" ]; then echo "  PASS: $label"; PASS=$((PASS + 1))
  else echo "  FAIL: $label"; FAIL=$((FAIL + 1)); fi
}

EXPECTED_KAFKA=$((EVENTS + 10))   # + fast-forward events
check "Kafka received >= $EXPECTED_KAFKA messages (got $KAFKA_COUNT)" \
  "$([ "${KAFKA_COUNT:-0}" -ge "$EXPECTED_KAFKA" ] && echo 1 || echo 0)"
check "Bronze >= $EVENTS rows (got ${BRONZE:-0})" \
  "$([ "${BRONZE:-0}" -ge "$EVENTS" ] && echo 1 || echo 0)"
check "Quarantine empty without fault-inject (got ${QUARANTINE:-0})" \
  "$([ "${QUARANTINE:-0}" -eq 0 ] && echo 1 || echo 0)"
check "Silver >= $EVENTS rows (got ${SILVER:-0})" \
  "$([ "${SILVER:-0}" -ge "$EVENTS" ] && echo 1 || echo 0)"
check "Gold 1m has >= 1 aggregate row (got ${GOLD1M:-0})" \
  "$([ "${GOLD1M:-0}" -ge 1 ] && echo 1 || echo 0)"
check "Gold 5m has >= 1 aggregate row (got ${GOLD5M:-0})" \
  "$([ "${GOLD5M:-0}" -ge 1 ] && echo 1 || echo 0)"

echo "=========================================="
echo "SMOKE RESULT: $PASS passed, $FAIL failed"
echo "=========================================="
[ "$FAIL" -eq 0 ]
