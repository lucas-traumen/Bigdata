#!/usr/bin/env bash
# test-recovery.sh — kill-the-pipeline recovery check against a RUNNING live
# pipeline (RQ-C, plan acceptance #11).
#
# Scenario:
#   1. baseline: current Kafka end offsets + last q1 progress batch_id/offsets
#   2. crash the spark-stream app IN-CONTAINER (SIGKILL the stream_app.py
#      driver): spark-submit's JVM then exits non-zero -> the container exits
#      -> restart policy (unless-stopped) restarts it -> checkpoints resume the
#      queries with no graceful finalize (hardest case).
#      NOTE: we deliberately do NOT use `docker kill bigdata-spark-stream`:
#      Docker treats an operator-issued `docker kill`/`docker stop` as a manual
#      stop and SUPPRESSES the restart policy (verified: neither `always` nor
#      `unless-stopped` restarts after `docker kill`). That models an operator
#      action, not a process crash, so it would never resume on its own.
#   3. restart bridge (graceful) -> persistent MQTT session redelivers any
#      unacked messages
#   4. verify: progress advances past baseline (pipeline resumed), bronze
#      files grow; report the observed loss window honestly.

#
# PASS = pipeline resumed and continued processing. The reported gap between
# simulator manifest events and Kafka-delivered event_ids is EVIDENCE, not a
# pass/fail criterion — this pipeline does NOT claim exactly-once.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMPL_DIR="$(dirname "$SCRIPT_DIR")"
cd "$IMPL_DIR"
if [ -f .env ]; then set -a; . ./.env; set +a; fi
DATA_ROOT="${DATA_ROOT:-/data/bigdata}"
KAFKA_TOPIC="${KAFKA_TOPIC:-sensor_raw}"
PROGRESS_LOG="${DATA_ROOT}/logs/stream-progress.jsonl"
RESUME_TIMEOUT_S=240
COMPOSE=(docker compose -f "$IMPL_DIR/compose.yaml")

PASS=0; FAIL=0
check() { # name, ok_flag(1=pass)
  if [ "$2" -eq 1 ]; then echo "  [PASS] $1"; PASS=$((PASS+1));
  else echo "  [FAIL] $1"; FAIL=$((FAIL+1)); fi; }

kafka_sum() {
  "${COMPOSE[@]}" --profile live exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "$KAFKA_TOPIC" 2>/dev/null \
    | awk -F: '{s+=$NF} END {print s+0}'
}

progress_snapshot() {
  # prints "<batch_id> <offsets_sum>" of the last q1 progress line
  if [ -r "$PROGRESS_LOG" ] && command -v python3 >/dev/null 2>&1; then
    grep '"query": *"q1_bronze"' "$PROGRESS_LOG" 2>/dev/null | tail -n1 \
      | KAFKA_TOPIC="$KAFKA_TOPIC" python3 -c '
import json, os, sys
try:
    rec = json.loads(sys.stdin.readline())
    topic = os.environ.get("KAFKA_TOPIC", "sensor_raw")
    total = 0
    for src in rec.get("sources", []):
        end = src.get("end_offset") or {}
        # end_offset is logged as a JSON-encoded string; accept dict too.
        if isinstance(end, str):
            try:
                end = json.loads(end)
            except Exception:
                end = {}
        if isinstance(end, dict):
            offs = end.get(topic)
            if isinstance(offs, dict):
                total += sum(int(v) for v in offs.values())
    print(rec.get("batch_id", -1), total)
except Exception:
    print("-1 0")
'
  else
    echo "-1 0"
  fi
}

echo "== test-recovery start ($(date -u +%H:%M:%SZ)) =="

echo "== 0/5 ensuring live pipeline is up =="
if ! docker inspect -f '{{.State.Status}}' bigdata-spark-stream >/dev/null 2>&1; then
  "$SCRIPT_DIR/run-mode.sh" live
fi
"$SCRIPT_DIR/wait-for-health.sh" --timeout 240 spark-stream kafka
# make sure traffic exists: simulator should be running (live mode)
if [ "$(docker inspect -f '{{.State.Status}}' bigdata-simulator 2>/dev/null)" != "running" ]; then
  echo "simulator not running; starting a bounded publisher in background"
  "${COMPOSE[@]}" --profile live run -d --name bigdata-simulator-recovery simulator \
    python -u /app/producer.py --rate 5 --duration 180 \
    --manifest /data/bigdata/logs/recovery-manifest.jsonl
  SIM_CONTAINER=bigdata-simulator-recovery
else
  SIM_CONTAINER=bigdata-simulator
fi
sleep 10

echo "== 1/5 baseline =="
base_batch="$(progress_snapshot | awk '{print $1}')"
base_progress="$(progress_snapshot | awk '{print $2}')"
base_kafka="$(kafka_sum)"
bronze_before="$(find "$DATA_ROOT/bronze/sensor" -name '*.parquet' 2>/dev/null | wc -l)"
echo "  baseline: q1 batch_id=${base_batch} progress_offsets=${base_progress} kafka_end=${base_kafka} bronze_files=${bronze_before}"

echo "== 2/5 crash the spark-stream app in-container (SIGKILL the driver) =="
# Kill the Python driver inside the container; spark-submit's JVM then exits
# non-zero, the container exits, and the `unless-stopped` policy restarts it.
# (docker kill <container> would be treated as an operator stop and would NOT
# restart — see the scenario note at the top.)
docker exec bigdata-spark-stream bash -c 'pkill -9 -f stream_app.py' >/dev/null 2>&1 || true
sleep 5
state="$(docker inspect -f '{{.State.Status}}' bigdata-spark-stream 2>/dev/null || echo missing)"
echo "  container state after kill: ${state} (restart policy: unless-stopped)"

echo "== 3/5 waiting for resume (max ${RESUME_TIMEOUT_S}s) =="
resumed=0
deadline=$(( $(date +%s) + RESUME_TIMEOUT_S ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  snap="$(progress_snapshot)"
  b="$(echo "$snap" | awk '{print $1}')"; p="$(echo "$snap" | awk '{print $2}')"
  if [ "$b" != "$base_batch" ] || [ "$p" -gt "$base_progress" ]; then
    resumed=1; echo "  resumed: q1 batch_id=${b} progress_offsets=${p}"; break
  fi
  sleep 5
done
check "spark-stream resumed from checkpoint (progress advanced)" "$resumed"

echo "== 4/5 restart bridge (persistent MQTT session redelivery) =="
docker restart bigdata-bridge >/dev/null
sleep 15
bridge_alive="$(docker inspect -f '{{.State.Status}}' bigdata-bridge 2>/dev/null || echo missing)"
bridge_ok=0; [ "$bridge_alive" = "running" ] && bridge_ok=1
check "bridge running after restart" "$bridge_ok"

echo "== 5/5 post-recovery flow =="
kafka_after="$(kafka_sum)"
bronze_after="$(find "$DATA_ROOT/bronze/sensor" -name '*.parquet' 2>/dev/null | wc -l)"
echo "  after: kafka_end=${kafka_after} bronze_files=${bronze_after}"
kafka_ok=0; [ "${kafka_after:-0}" -gt "${base_kafka:-0}" ] && kafka_ok=1
check "kafka offsets advanced after recovery" "$kafka_ok"
bronze_ok=0; [ "${bronze_after:-0}" -gt "${bronze_before:-0}" ] && bronze_ok=1
check "bronze files grew after recovery" "$bronze_ok"

# loss-window evidence: sent = all per-source manifests (zone simulators write
# simulator-<zone>.jsonl; ad-hoc runs may add others), delivered = Kafka log
if ls "${DATA_ROOT}"/logs/simulator-*.jsonl >/dev/null 2>&1; then
  sent="$(cat "${DATA_ROOT}"/logs/simulator-*.jsonl | wc -l)"
  delivered="$(grep -ho '"event_id":"[^"]*"' "${DATA_ROOT}/logs/bridge-deliveries.jsonl" 2>/dev/null | sort -u | wc -l)"
  echo "  [info] loss-window evidence (cumulative, all runs):"
  echo "         manifest lines (sent): ${sent}"
  echo "         distinct event_ids delivered to Kafka: ${delivered}"
  echo "         => cumulative un-delivered/lost window: $((sent - delivered))"
  echo "         (includes events sent while bridge was down + broker queue drops)"
fi
if [ -n "${SIM_CONTAINER:-}" ] && [ "$SIM_CONTAINER" != "bigdata-simulator" ]; then
  docker rm -f "$SIM_CONTAINER" >/dev/null 2>&1 || true
fi

echo "== RESULT: PASS=${PASS} FAIL=${FAIL} =="
echo "NOTE: recovery success here does NOT imply exactly-once delivery."
[ "$FAIL" -eq 0 ]
