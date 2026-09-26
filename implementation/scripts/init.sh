#!/usr/bin/env bash
# init.sh — one-time initialization of the stack:
#   1. .env + host directories (delegates to prepare-host.sh)
#   2. builds all images (requires internet for base images + Maven jars)
#   3. starts PostgreSQL and verifies the app database + schema
#   4. starts Kafka (live profile) and creates the sensor_raw topic
# Does NOT start the full live pipeline — use run-mode.sh live next.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMPL_DIR="$(dirname "$SCRIPT_DIR")"
cd "$IMPL_DIR"
if [ -f .env ]; then set -a; . ./.env; set +a; fi
DATA_ROOT="${DATA_ROOT:-/data/bigdata}"
KAFKA_TOPIC="${KAFKA_TOPIC:-sensor_raw}"
COMPOSE=(docker compose -f "$IMPL_DIR/compose.yaml")

echo "== 1/5 Host preparation =="
"$SCRIPT_DIR/prepare-host.sh"

echo "== 2/5 Building images (simulator, bridge, spark, backend, dashboard) =="
# Profiles must be enabled for `compose build` (Compose v2 semantics):
# without them only the two unprofiled services (backend, dashboard) are
# built, while simulator/bridge/spark-stream (live) and spark-batch (batch)
# would be skipped.
"${COMPOSE[@]}" --profile live --profile batch build

echo "== 3/5 PostgreSQL + schema =="
"${COMPOSE[@]}" up -d postgres
"$SCRIPT_DIR/wait-for-health.sh" --timeout 120 postgres

"${COMPOSE[@]}" exec -T postgres psql -U "${POSTGRES_USER:-bigdata}" -d "${APP_DB:-app}" -tAc \
  "SELECT 'schema check:', count(*) FROM information_schema.tables
   WHERE table_schema = 'public' OR table_schema = 'gold'"
"${COMPOSE[@]}" exec -T postgres psql -U "${POSTGRES_USER:-bigdata}" -d "${APP_DB:-app}" -tAc \
  "SELECT 'serving tables:', string_agg(table_schema || '.' || table_name, ', ')
   FROM information_schema.tables
   WHERE (table_schema || '.' || table_name) IN
     ('public.sensor_latest','public.alerts','public.processed_events',
      'gold.sensor_hourly','public.staging_stream_events','public.staging_hourly_agg')"

echo "== 4/5 Kafka + topic =="
"${COMPOSE[@]}" --profile live up -d kafka
"$SCRIPT_DIR/wait-for-health.sh" --timeout 180 kafka
"${COMPOSE[@]}" --profile live run --rm --name kafka-init-once kafka-init
"${COMPOSE[@]}" --profile live exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic "$KAFKA_TOPIC"

echo "== 5/5 Done =="
cat <<'EOF'
Next steps:
  implementation/scripts/run-mode.sh live          # start the streaming pipeline (5 zone simulators)
  implementation/scripts/test-e2e.sh               # end-to-end fixture check
  implementation/scripts/run-mode.sh batch --start <UTC> --end <UTC>
  implementation/scripts/run-batch-hourly.sh       # hourly Gold batch (host cron trigger)
EOF
