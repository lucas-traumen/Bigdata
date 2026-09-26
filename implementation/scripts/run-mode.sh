#!/usr/bin/env bash
# run-mode.sh — switch the stack between its two run modes WITHOUT ever
# deleting volumes, checkpoints or data (plan 4.1).
#
# Usage:
#   run-mode.sh live
#       start/refresh the streaming pipeline (mqtt, simulator, bridge, kafka,
#       spark-stream) together with the shared serving layer.
#   run-mode.sh batch --start <UTC hour> --end <UTC hour>
#       (or BATCH_START/BATCH_END env) — drain the live pipeline, then run the
#       one-shot hourly Gold batch; PostgreSQL/dashboard stay up; all
#       volumes/checkpoints are preserved.
#   run-mode.sh status
#       show container states and the tail of the stream progress log.
#
# Guarantees:
#   * stops the services of the previous mode first (no overlapping modes);
#   * never runs `docker compose down` or `down -v` (see reset-pipeline.sh
#     for the explicit destructive path);
#   * drain order: stop simulator + bridge, wait until Spark consumed the
#     Kafka backlog (progress log vs broker end offsets), then SIGTERM
#     spark-stream (graceful checkpoint finalize).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMPL_DIR="$(dirname "$SCRIPT_DIR")"
cd "$IMPL_DIR"
if [ -f .env ]; then set -a; . ./.env; set +a; fi
DATA_ROOT="${DATA_ROOT:-/data/bigdata}"
KAFKA_TOPIC="${KAFKA_TOPIC:-sensor_raw}"
PROGRESS_LOG="${DATA_ROOT}/logs/stream-progress.jsonl"
COMPOSE=(docker compose -f "$IMPL_DIR/compose.yaml")

usage() {
  cat <<'USAGE'
Usage:
  run-mode.sh live
      start/refresh the streaming pipeline (mqtt, simulator, bridge, kafka,
      spark-stream) together with the shared serving layer.
  run-mode.sh batch --start <UTC hour> --end <UTC hour>
      (or BATCH_START/BATCH_END env) — drain the live pipeline, then run the
      one-shot hourly Gold batch; PostgreSQL/dashboard stay up; volumes and
      checkpoints are preserved.
  run-mode.sh status
      show container states and the tail of the stream progress log.
USAGE
  exit 1
}

# ---------------------------------------------------------------------------
kafka_end_offsets_sum() {
  # total end offsets of the topic across partitions (0 when kafka is down)
  # NOTE: --profile is required to address profiled services with exec
  "${COMPOSE[@]}" --profile live exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "$KAFKA_TOPIC" 2>/dev/null \
    | awk -F: '{s+=$NF} END {print s+0}'
}

progress_q1_offsets_sum() {
  # sum of Kafka end offsets recorded in the LAST q1 progress line
  [ -r "$PROGRESS_LOG" ] || { echo 0; return; }
  grep '"query": *"q1_bronze"' "$PROGRESS_LOG" 2>/dev/null | tail -n1 \
    | KAFKA_TOPIC="$KAFKA_TOPIC" python3 -c '
import json, os, sys
try:
    rec = json.loads(sys.stdin.readline())
    topic = os.environ.get("KAFKA_TOPIC", "sensor_raw")
    total = 0
    for src in rec.get("sources", []):
        end = src.get("end_offset") or {}
        if isinstance(end, dict):
            offsets = end.get(topic)
            if isinstance(offsets, dict):
                total += sum(int(v) for v in offsets.values())
    print(total)
except Exception:
    print(0)
' 2>/dev/null || echo 0
}

wait_for_drain() {
  # Wait (max WAIT seconds) until Spark's recorded Kafka offsets catch up
  # with the broker's end offsets. Best effort: python3 required, otherwise
  # a fixed settle delay is used with a warning.
  local wait_s="${DRAIN_WAIT_S:-60}" i k p
  if ! command -v python3 >/dev/null 2>&1; then
    echo "[mode] python3 unavailable; settling ${wait_s}s without offset check"
    sleep "$wait_s"
    return 0
  fi
  for ((i = 0; i < wait_s; i += 3)); do
    k="$(kafka_end_offsets_sum)"
    p="$(progress_q1_offsets_sum)"
    echo "[mode] drain check ${i}/${wait_s}s: kafka_end=${k} spark_progress=${p}"
    if [ "$k" -le "$p" ]; then
      echo "[mode] drain confirmed (spark consumed all broker offsets)"
      return 0
    fi
    sleep 3
  done
  echo "[mode] WARNING: drain not confirmed after ${wait_s}s — proceeding "
  echo "[mode] (unconsumed events stay in Kafka; restart live to resume)"
  return 0
}

stop_live() {
  echo "== Stopping simulator containers (SIGTERM, graceful flush) =="
  "$SCRIPT_DIR/run-multi-sim.sh" stop 2>/dev/null || true
  echo "== Stopping template simulator + bridge =="
  "${COMPOSE[@]}" --profile live stop simulator bridge 2>/dev/null || true
  echo "== Waiting for Spark to consume the Kafka backlog =="
  wait_for_drain
  echo "== Stopping spark-stream (SIGTERM -> graceful checkpoint finalize) =="
  "${COMPOSE[@]}" --profile live stop spark-stream 2>/dev/null || true
  # remove the exited one-shot batch container if a previous run left one
  "${COMPOSE[@]}" --profile batch rm -f spark-batch 2>/dev/null || true
  if [ -r "$PROGRESS_LOG" ]; then
    echo "[mode] last progress lines (drain evidence):"
    tail -n 3 "$PROGRESS_LOG" | cut -c1-200
  fi
}

stop_batch() {
  "${COMPOSE[@]}" --profile batch rm -f spark-batch 2>/dev/null || true
}

ensure_shared() {
  "${COMPOSE[@]}" up -d postgres backend dashboard
  "$SCRIPT_DIR/wait-for-health.sh" --timeout 120 postgres
}

# ---------------------------------------------------------------------------

MODE="${1:-}"
shift || true
case "$MODE" in
  live)
    stop_batch
    ensure_shared
    echo "== Starting live pipeline =="
    "${COMPOSE[@]}" --profile live up -d kafka mqtt
    "$SCRIPT_DIR/wait-for-health.sh" --timeout 180 kafka
    "${COMPOSE[@]}" --profile live run --rm --name kafka-init-once kafka-init   # topic create, idempotent
    "${COMPOSE[@]}" --profile live up -d bridge spark-stream
    "$SCRIPT_DIR/wait-for-health.sh" --timeout 240 spark-stream
    echo "== Starting 5 zone simulators (zoneA..zoneE) via run-multi-sim.sh =="
    "$SCRIPT_DIR/run-multi-sim.sh" start
    echo "== live mode up; dashboard: http://localhost:${DASHBOARD_HOST_PORT:-8088} =="
    ;;

  batch)
    START="${BATCH_START:-}"
    END_="${BATCH_END:-}"
    # accept both "--start X --end Y" and "--start=X --end=Y"
    while [ $# -gt 0 ]; do
      case "$1" in
        --start)  START="$2";  shift 2 ;;
        --start=*) START="${1#--start=}"; shift ;;
        --end)    END_="$2";   shift 2 ;;
        --end=*)  END_="${1#--end=}"; shift ;;
        *) echo "unknown argument: $1" >&2; usage ;;
      esac
    done
    if [ -z "$START" ] || [ -z "$END_" ]; then
      echo "batch mode requires --start and --end (UTC hour boundaries) or BATCH_START/BATCH_END" >&2
      usage
    fi
    echo "== Draining live pipeline before batch =="
    stop_live
    ensure_shared
    echo "== Running Gold batch window [${START}, ${END_}) =="
    "${COMPOSE[@]}" --profile batch run --rm --name spark-batch-once spark-batch \
      /opt/spark/run/run-batch.sh --start "$START" --end "$END_"
    echo "== batch done; volumes/checkpoints preserved =="
    echo "== back to streaming with: run-mode.sh live =="
    ;;

  status)
    "${COMPOSE[@]}" --profile live --profile batch ps -a
    if [ -r "$PROGRESS_LOG" ]; then
      echo; echo "== last 3 stream progress lines =="
      tail -n 3 "$PROGRESS_LOG" | cut -c1-240
    fi
    ;;

  *)
    usage
    ;;
esac
