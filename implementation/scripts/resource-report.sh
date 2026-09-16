#!/usr/bin/env bash
# resource-report.sh — sample resource usage of the stack (plan: budget evidence).
#
# Usage:
#   resource-report.sh                    # one sample, printed + appended to JSONL
#   resource-report.sh --interval 30 --count 20
#   resource-report.sh --jsonl-only       # no per-sample stdout (for long runs)
#
# Each sample records: per-container CPU/memory (docker stats), host available
# RAM, disk free under DATA_ROOT, Kafka end offsets, Parquet file counts and
# PostgreSQL row counts. Appends one JSON line to
# ${DATA_ROOT}/logs/resource-report.jsonl.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMPL_DIR="$(dirname "$SCRIPT_DIR")"
cd "$IMPL_DIR"
if [ -f .env ]; then set -a; . ./.env; set +a; fi
DATA_ROOT="${DATA_ROOT:-/data/bigdata}"
KAFKA_TOPIC="${KAFKA_TOPIC:-sensor_raw}"
PG_USER="${POSTGRES_USER:-bigdata}"
PG_DB="${APP_DB:-app}"
COMPOSE=(docker compose -f "$IMPL_DIR/compose.yaml")

INTERVAL=0
COUNT=1
JSONL_ONLY=0
while [ $# -gt 0 ]; do
  case "$1" in
    --interval) INTERVAL="$2"; shift 2 ;;
    --count)    COUNT="$2"; shift 2 ;;
    --jsonl-only) JSONL_ONLY=1; shift ;;
    *) echo "unknown flag: $1" >&2; exit 1 ;;
  esac
done

OUT="${DATA_ROOT}/logs/resource-report.jsonl"
mkdir -p "$(dirname "$OUT")"

sample() {
  local ts; ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  local stats
  # TAB-delimited format: MemUsage contains spaces ("50MiB / 1GiB"), so a
  # space-separated format would misalign the mem_pct/cpu_pct columns.
  # Parsing lives in scripts/stats_parse.py (unit-tested against canned rows).
  stats="$(docker stats --no-stream --format '{{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}' \
           2>/dev/null | grep '^bigdata-' || true)"

  local mem_avail="null"
  if [ -r /proc/meminfo ]; then
    mem_avail="$(awk '/^MemAvailable:/{printf "%.0f", $2/1024}' /proc/meminfo)"
  fi
  local disk_free="null"
  disk_free="$(df -BG --output=avail "$DATA_ROOT" 2>/dev/null | tail -n1 | tr -dc '0-9')"

  local kafka_offsets="null"
  # NOTE: --profile is required to address the profiled kafka service with exec
  kafka_offsets="$("${COMPOSE[@]}" --profile live exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "$KAFKA_TOPIC" 2>/dev/null \
    | tr '\n' ';' || true)"
  [ -z "$kafka_offsets" ] && kafka_offsets="null"

  local f_bronze=0 f_silver=0 f_quar=0
  [ -d "$DATA_ROOT/bronze/sensor" ]     && f_bronze="$(find "$DATA_ROOT/bronze/sensor" -name '*.parquet' 2>/dev/null | wc -l)"
  [ -d "$DATA_ROOT/silver/sensor" ]     && f_silver="$(find "$DATA_ROOT/silver/sensor" -name '*.parquet' 2>/dev/null | wc -l)"
  [ -d "$DATA_ROOT/quarantine/sensor" ] && f_quar="$(find "$DATA_ROOT/quarantine/sensor" -name '*.parquet' 2>/dev/null | wc -l)"

  local pg_counts="null"
  pg_counts="$("${COMPOSE[@]}" exec -T postgres psql -U "$PG_USER" -d "$PG_DB" -tAc \
    "SELECT json_build_object(
        'sensor_latest', (SELECT count(*) FROM sensor_latest),
        'alerts',        (SELECT count(*) FROM alerts),
        'processed',     (SELECT count(*) FROM processed_events),
        'hourly_rows',   (SELECT count(*) FROM gold.sensor_hourly))" 2>/dev/null || true)"
  [ -z "$pg_counts" ] && pg_counts="null"

  local line
  line="$(python3 "$SCRIPT_DIR/stats_parse.py" "$ts" "$mem_avail" "$disk_free" \
        "$kafka_offsets" "$f_bronze" "$f_silver" "$f_quar" "$pg_counts" "$stats")"

  echo "$line" >> "$OUT"
  if [ "$JSONL_ONLY" -ne 1 ]; then
    echo "$line" | python3 -m json.tool
  fi
}

for ((i = 0; i < COUNT; i++)); do
  [ "$i" -gt 0 ] && [ "$INTERVAL" -gt 0 ] && sleep "$INTERVAL"
  sample
done
echo "report appended to: $OUT"
