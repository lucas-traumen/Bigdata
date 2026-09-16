#!/usr/bin/env bash
# test-e2e.sh — end-to-end fixture check against a RUNNING live pipeline.
# Requires: init.sh done, run-mode.sh live up (starts it if missing).
#
# Steps:
#   1. one-shot simulator: 200 events, rate 20/s, fault-inject, seed 42,
#      manifest at ${DATA_ROOT}/logs/e2e-manifest.jsonl
#   2. poll the API until processed_events settles
#   3. assert: manifest rows >= 200; alerts >= 1 (spikes cross 35 C);
#      silver + quarantine parquet files exist; latest data visible.
# NOTE (honesty): assertions use generous lower bounds; exact counts depend on
# the seeded RNG stream and are reported, not assumed.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMPL_DIR="$(dirname "$SCRIPT_DIR")"
cd "$IMPL_DIR"
if [ -f .env ]; then set -a; . ./.env; set +a; fi
DATA_ROOT="${DATA_ROOT:-/data/bigdata}"
API="http://localhost:${BACKEND_HOST_PORT:-8000}"
COMPOSE=(docker compose -f "$IMPL_DIR/compose.yaml")
MANIFEST="${DATA_ROOT}/logs/e2e-manifest.jsonl"
EVENTS=200
POLL_TIMEOUT_S=180

PASS=0; FAIL=0
check() { # name, ok_flag(1=pass)
  if [ "$2" -eq 1 ]; then
    echo "  [PASS] $1"; PASS=$((PASS+1))
  else
    echo "  [FAIL] $1"; FAIL=$((FAIL+1))
  fi
}

api() { curl -fsS --max-time 5 "$API$1" 2>/dev/null || echo '{}'; }

json_field() { # json_body, python-expr over dict d
  python3 -c "
import json, sys
try:
    d = json.loads(sys.argv[1])
    print(eval(sys.argv[2]))
except Exception as e:
    print(0)
" "$1" "$2" 2>/dev/null || echo 0
}

echo "== test-e2e start ($(date -u +%H:%M:%SZ)) =="

echo "== 1/4 ensuring live pipeline is up =="
if ! docker inspect -f '{{.State.Status}}' bigdata-spark-stream >/dev/null 2>&1; then
  echo "live pipeline not running — starting via run-mode.sh live"
  "$SCRIPT_DIR/run-mode.sh" live
fi
"$SCRIPT_DIR/wait-for-health.sh" --timeout 240 spark-stream postgres backend

echo "== 2/4 sending ${EVENTS} events (fault-inject, seed 42) =="
rm -f "$MANIFEST"
"${COMPOSE[@]}" --profile live run --rm --name e2e-simulator simulator \
  python -u /app/producer.py --events "$EVENTS" --rate 20 --fault-inject \
  --seed 42 --manifest /data/bigdata/logs/e2e-manifest.jsonl

manifest_lines=0
[ -f "$MANIFEST" ] && manifest_lines="$(wc -l < "$MANIFEST")"
manifest_ok=0; [ "${manifest_lines:-0}" -ge "$EVENTS" ] && manifest_ok=1
check "manifest rows >= ${EVENTS} (got ${manifest_lines})" "$manifest_ok"

echo "== 3/4 waiting for the pipeline to settle (max ${POLL_TIMEOUT_S}s) =="
deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
prev=-1; stable=0; processed=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  processed="$(json_field "$(api /api/progress)" "d.get('events', 0)")"
  if [ "${processed:-0}" -ge 1 ] && [ "$processed" -eq "$prev" ]; then
    stable=$((stable+1))
    if [ "$stable" -ge 3 ]; then break; fi   # 3 consecutive equal samples
  else
    stable=0
  fi
  prev="$processed"
  sleep 5
done
processed_ok=0; [ "${processed:-0}" -ge 1 ] && processed_ok=1
check "processed_events > 0 (got ${processed})" "$processed_ok"

dup_check="$(json_field "$(api /api/progress)" "d.get('duplicates', 0)")"
echo "  [info] duplicates seen (replayed/dup events): ${dup_check}"

echo "== 4/4 assertions =="
alerts_total="$(json_field "$(api "/api/alerts?limit=1")" "d.get('total', 0)")"
alerts_ok=0; [ "${alerts_total:-0}" -ge 1 ] && alerts_ok=1
check "alerts >= 1 (got ${alerts_total})" "$alerts_ok"

latest_total="$(json_field "$(api "/api/sensors/latest?limit=1")" "d.get('total', 0)")"
latest_ok=0; [ "${latest_total:-0}" -ge 1 ] && latest_ok=1
check "sensor_latest rows >= 1 (got ${latest_total})" "$latest_ok"

quar_files="$(find "$DATA_ROOT/quarantine/sensor" -name '*.parquet' 2>/dev/null | wc -l)"
quar_ok=0; [ "${quar_files:-0}" -ge 1 ] && quar_ok=1
check "quarantine parquet files >= 1 (got ${quar_files})" "$quar_ok"

silver_files="$(find "$DATA_ROOT/silver/sensor" -name '*.parquet' 2>/dev/null | wc -l)"
silver_ok=0; [ "${silver_files:-0}" -ge 1 ] && silver_ok=1
check "silver parquet files >= 1 (got ${silver_files})" "$silver_ok"

# gap report (honesty): manifest events vs distinct delivered event ids
if [ -f "${DATA_ROOT}/logs/bridge-deliveries.jsonl" ]; then
  delivered="$(grep -o '"event_id":"[^"]*"' "${DATA_ROOT}/logs/bridge-deliveries.jsonl" \
    | sort -u | wc -l)"
  echo "  [info] distinct event_ids delivered to Kafka so far: ${delivered}"
  echo "  [info] difference vs manifest lines (includes earlier runs/faults) is reported, not asserted"
fi

echo "== RESULT: PASS=${PASS} FAIL=${FAIL} =="
[ "$FAIL" -eq 0 ]
