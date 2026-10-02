#!/usr/bin/env bash
# Run a bounded benchmark workload against an already-running live stack.
#
# This helper never resets data, removes volumes, or starts the 3-hour B7 load
# automatically. Each invocation uses a unique run id and source prefix:
#   benchmark-run.sh run --run-id pilot01 --rate 50 --duration 600 --sensors 5
#   benchmark-run.sh status --run-id pilot01
#   benchmark-run.sh stop --run-id pilot01
#
# The run command records metadata, source manifests, resource samples and
# before/after snapshots. It waits for source containers to finish, then writes
# a machine-readable summary. The live stack must already be up.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMPL_DIR="$(dirname "$SCRIPT_DIR")"
cd "$IMPL_DIR"
if [ -f .env ]; then set -a; . ./.env; set +a; fi

DATA_ROOT="${DATA_ROOT:-/data/bigdata}"
API="http://localhost:${BACKEND_HOST_PORT:-8000}"
KAFKA_TOPIC="${KAFKA_TOPIC:-sensor_raw}"
COMPOSE=(docker compose -f "$IMPL_DIR/compose.yaml")

usage() {
  local status="${1:-2}"
  cat <<'USAGE'
Usage:
  benchmark-run.sh run --run-id ID --rate EVENTS_PER_SECOND --duration SECONDS [options]
      --sensors N              sensors per source (default: SIM_SENSORS or 5)
      --zones "A B C"           source zones (default: A B C D E)
      --fault-inject            enable simulator fault injection
      --spike-pct P             alert spike percentage (default: SIM_SPIKE_PCT or 2)
      --resource-interval SEC   resource sample interval (default: 30)
      --seed N                  deterministic seed base (zone number is added)
  benchmark-run.sh status --run-id ID
  benchmark-run.sh stop --run-id ID
  benchmark-run.sh snapshot --run-id ID
USAGE
  exit "$status"
}

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
  usage 0
fi

require_run_id() {
  case "$1" in
    ''|*[!A-Za-z0-9_-]*) echo "run-id must contain only letters, digits, _ or -" >&2; exit 2 ;;
  esac
}

run_dir() { echo "${DATA_ROOT}/logs/bench-$1"; }
meta_path() { echo "$(run_dir "$1")-metadata.json"; }

write_snapshot() {
  local run_id="$1" label="$2" dir
  dir="$(run_dir "$run_id")"
  mkdir -p "$dir"
  date -u +%Y-%m-%dT%H:%M:%SZ > "${dir}/${label}-utc.txt"
  docker ps -a --filter label="iot.benchmark.run_id=${run_id}" \
    --format '{{.Names}}\t{{.Status}}\t{{.Image}}' \
    > "${dir}/${label}-containers.tsv" 2>&1 || true
  docker system df -v > "${dir}/${label}-docker-system-df.txt" 2>&1 || true
  df -h "$DATA_ROOT" > "${dir}/${label}-df.txt" 2>&1 || true
  du -sb "$DATA_ROOT" > "${dir}/${label}-du.txt" 2>&1 || true
  curl -fsS --max-time 10 "$API/api/progress" \
    > "${dir}/${label}-progress.json" 2>"${dir}/${label}-progress.err" || true
  curl -fsS --max-time 10 "$API/health" \
    > "${dir}/${label}-health.json" 2>"${dir}/${label}-health.err" || true
  "${COMPOSE[@]}" --profile live exec -T kafka \
    /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
    --topic "$KAFKA_TOPIC" \
    > "${dir}/${label}-kafka-offsets.txt" 2>&1 || true
}

write_metadata() {
  local run_id="$1" rate="$2" duration="$3" sensors="$4" zones="$5"
  local spike="$6" faults="$7" interval="$8" seed="$9" path
  path="$(meta_path "$run_id")"
  python3 - "$path" "$run_id" "$rate" "$duration" "$sensors" "$zones" \
    "$spike" "$faults" "$interval" "$seed" "$DATA_ROOT" <<'PY'
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone

path, run_id, rate, duration, sensors, zones, spike, faults, interval, seed, data_root = sys.argv[1:]
try:
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL
    ).strip()
except Exception:
    commit = None
record = {
    "run_id": run_id,
    "start_utc": datetime.now(timezone.utc).isoformat(),
    "end_utc": None,
    "git_commit": commit,
    "host": platform.node(),
    "platform": platform.platform(),
    "python": platform.python_version(),
    "data_root": data_root,
    "rate_per_source": float(rate),
    "duration_seconds": float(duration),
    "sensors_per_source": int(sensors),
    "zones": zones.split(),
    "spike_pct": float(spike),
    "fault_inject": faults == "1",
    "resource_interval_seconds": int(interval),
    "seed_base": int(seed) if seed else None,
}
with open(path, "w", encoding="utf-8") as fh:
    json.dump(record, fh, indent=2, sort_keys=True)
    fh.write("\n")
PY
}

finish_metadata() {
  local run_id="$1" path
  path="$(meta_path "$run_id")"
  python3 - "$path" <<'PY'
import json
import sys
from datetime import datetime, timezone
path = sys.argv[1]
with open(path, encoding="utf-8") as fh:
    record = json.load(fh)
record["end_utc"] = datetime.now(timezone.utc).isoformat()
with open(path, "w", encoding="utf-8") as fh:
    json.dump(record, fh, indent=2, sort_keys=True)
    fh.write("\n")
PY
}

container_state() {
  local state
  state="$(docker inspect -f '{{.State.Status}}' "$1" 2>/dev/null || true)"
  if [ -n "$state" ]; then
    printf '%s\n' "$state"
  else
    printf '%s\n' missing
  fi
}

stop_run_containers() {
  local run_id="$1"
  docker ps -aq --filter "label=iot.benchmark.run_id=${run_id}" \
    | xargs -r docker rm -f >/dev/null 2>&1 || true
}

run_workload() {
  local run_id="$1" rate="$2" duration="$3" sensors="$4" zones="$5"
  local spike="$6" fault_inject="$7" interval="$8" seed="$9"
  local dir resource_pid count zone zone_no state remaining
  dir="$(run_dir "$run_id")"
  mkdir -p "$dir"
  write_metadata "$run_id" "$rate" "$duration" "$sensors" "$zones" \
    "$spike" "$fault_inject" "$interval" "$seed"
  write_snapshot "$run_id" before

  if [ "$(container_state bigdata-spark-stream)" != "running" ] || \
     [ "$(container_state bigdata-bridge)" != "running" ]; then
    echo "live stack is not running; start it with scripts/run-mode.sh live" >&2
    exit 1
  fi

  count=$(( (duration + interval - 1) / interval + 6 ))
  "$SCRIPT_DIR/resource-report.sh" --interval "$interval" --count "$count" \
    --jsonl-only > "${dir}/resource-report.stdout.log" 2>&1 &
  resource_pid=$!
  printf '%s\n' "$resource_pid" > "${dir}/resource-report.pid"

  trap 'stop_run_containers "$run_id"; kill "$resource_pid" 2>/dev/null || true; exit 130' INT TERM
  zone_no=0
  for zone in $zones; do
    zone_no=$((zone_no + 1))
    state="$(container_state "sim-${run_id}-${zone}")"
    if [ "$state" != "missing" ]; then
      echo "container sim-${run_id}-${zone} already exists" >&2
      stop_run_containers "$run_id"
      kill "$resource_pid" 2>/dev/null || true
      exit 1
    fi
    args=(python -u /app/producer.py
      --rate "$rate" --duration "$duration" --sensors "$sensors"
      --spike-pct "$spike"
      --manifest "/data/bigdata/logs/simulator-${run_id}-${zone}.jsonl")
    if [ "$fault_inject" = "1" ]; then
      args+=(--fault-inject)
    else
      args+=(--no-fault-inject)
    fi
    if [ -n "$seed" ]; then
      args+=(--seed "$((seed + zone_no - 1))")
    fi
    "${COMPOSE[@]}" --profile live run -d --rm \
      --name "sim-${run_id}-${zone}" \
      --label "iot.simulator=1" \
      --label "iot.benchmark.run_id=${run_id}" \
      -e "SENSOR_PREFIX=${run_id}_${zone}" \
      -e "MANIFEST_FILE=/data/bigdata/logs/simulator-${run_id}-${zone}.jsonl" \
      simulator "${args[@]}"
  done

  echo "benchmark ${run_id} running for ${duration}s at ${rate} events/s/source"
  while :; do
    remaining=0
    for zone in $zones; do
      state="$(container_state "sim-${run_id}-${zone}")"
      [ "$state" = "running" ] && remaining=1
    done
    [ "$remaining" -eq 0 ] && break
    sleep 5
  done

  write_snapshot "$run_id" sources-finished
  echo "benchmark ${run_id} sources finished; waiting for q1/q2/q3 to settle"
  "$SCRIPT_DIR/run-mode.sh" settle
  write_snapshot "$run_id" pipeline-settled
  finish_metadata "$run_id"
  kill "$resource_pid" 2>/dev/null || true
  wait "$resource_pid" 2>/dev/null || true
  rm -f "${dir}/resource-report.pid"
  python3 "$SCRIPT_DIR/benchmark_summary.py" --run-id "$run_id" \
    --data-root "$DATA_ROOT" --output "${dir}/summary.json"
  trap - INT TERM
  echo "benchmark complete: ${dir}/summary.json"
}

RUN_ID=""
COMMAND="${1:-}"
shift || true
case "$COMMAND" in
  run)
    RATE=""; DURATION=""; SENSORS="${SIM_SENSORS:-5}"
    ZONES="A B C D E"; SPIKE="${SIM_SPIKE_PCT:-2}"; FAULTS=0
    INTERVAL=30; SEED=""
    while [ $# -gt 0 ]; do
      case "$1" in
        --run-id) RUN_ID="$2"; shift 2 ;;
        --rate|--rate-per-source) RATE="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --sensors) SENSORS="$2"; shift 2 ;;
        --zones) ZONES="$2"; shift 2 ;;
        --spike-pct) SPIKE="$2"; shift 2 ;;
        --fault-inject) FAULTS=1; shift ;;
        --resource-interval) INTERVAL="$2"; shift 2 ;;
        --seed) SEED="$2"; shift 2 ;;
        *) usage ;;
      esac
    done
    [ -n "$RUN_ID" ] && [ -n "$RATE" ] && [ -n "$DURATION" ] || usage
    require_run_id "$RUN_ID"
    run_workload "$RUN_ID" "$RATE" "$DURATION" "$SENSORS" "$ZONES" \
      "$SPIKE" "$FAULTS" "$INTERVAL" "$SEED"
    ;;
  status)
    [ "$#" -eq 2 ] && [ "$1" = "--run-id" ] || usage
    RUN_ID="$2"; require_run_id "$RUN_ID"
    docker ps -a --filter "label=iot.benchmark.run_id=${RUN_ID}" \
      --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'
    [ -f "$(run_dir "$RUN_ID")/summary.json" ] && cat "$(run_dir "$RUN_ID")/summary.json"
    ;;
  stop)
    [ "$#" -eq 2 ] && [ "$1" = "--run-id" ] || usage
    RUN_ID="$2"; require_run_id "$RUN_ID"
    stop_run_containers "$RUN_ID"
    if [ -f "$(run_dir "$RUN_ID")/resource-report.pid" ]; then
      kill "$(cat "$(run_dir "$RUN_ID")/resource-report.pid")" 2>/dev/null || true
      rm -f "$(run_dir "$RUN_ID")/resource-report.pid"
    fi
    echo "stopped benchmark containers for ${RUN_ID}"
    ;;
  snapshot)
    [ "$#" -eq 2 ] && [ "$1" = "--run-id" ] || usage
    RUN_ID="$2"; require_run_id "$RUN_ID"
    write_snapshot "$RUN_ID" "snapshot-$(date -u +%Y%m%dT%H%M%SZ)"
    ;;
  *) usage ;;
esac
