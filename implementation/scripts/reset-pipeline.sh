#!/usr/bin/env bash
# reset-pipeline.sh — the ONLY sanctioned destructive path. Ordinary restarts
# (run-mode.sh) never delete volumes, checkpoints or data.
#
# Usage:
#   reset-pipeline.sh                # stop + remove containers (data kept)
#   reset-pipeline.sh --data         # also wipe DATA_ROOT contents
#                                    # (bronze/silver/quarantine/checkpoints/logs/control)
#   reset-pipeline.sh --volumes      # also remove the named Kafka/PostgreSQL volumes
#   reset-pipeline.sh --data --volumes --yes
#
# Flags compose: --data implies containers; --volumes implies containers.
# Confirmation prompt unless --yes. NEVER touches other Docker projects.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMPL_DIR="$(dirname "$SCRIPT_DIR")"
cd "$IMPL_DIR"
if [ -f .env ]; then set -a; . ./.env; set +a; fi
DATA_ROOT="${DATA_ROOT:-/data/bigdata}"
COMPOSE=(docker compose -f "$IMPL_DIR/compose.yaml")

WIPE_DATA=0
WIPE_VOLUMES=0
ASSUME_YES=0
for arg in "$@"; do
  case "$arg" in
    --data)    WIPE_DATA=1 ;;
    --volumes) WIPE_VOLUMES=1 ;;
    --yes|-y)  ASSUME_YES=1 ;;
    *) echo "unknown flag: $arg" >&2; exit 1 ;;
  esac
done

echo "This will stop and remove the bigdata-iot containers"
[ "$WIPE_DATA" -eq 1 ]    && echo "  AND delete all files under ${DATA_ROOT} (parquet/checkpoints/logs)"
[ "$WIPE_VOLUMES" -eq 1 ] && echo "  AND delete named volumes: bigdata-iot_pg_data, bigdata-iot_kafka_data"
if [ "$ASSUME_YES" -ne 1 ]; then
  read -r -p "Type exactly 'reset' to continue: " answer
  [ "$answer" = "reset" ] || { echo "aborted"; exit 1; }
fi

echo "== Stopping and removing project containers (all profiles) =="
"${COMPOSE[@]}" --profile live --profile batch --profile airflow down --remove-orphans || true

if [ "$WIPE_DATA" -eq 1 ]; then
  echo "== Wiping ${DATA_ROOT} contents =="
  for d in bronze silver quarantine checkpoints logs control; do
    if [ -d "$DATA_ROOT/$d" ]; then
      find "$DATA_ROOT/$d" -mindepth 1 -delete
    fi
  done
  # recreate the empty layout with demo permissions
  "$SCRIPT_DIR/prepare-host.sh"
fi

if [ "$WIPE_VOLUMES" -eq 1 ]; then
  echo "== Removing named volumes =="
  docker volume rm -f bigdata-iot_pg_data bigdata-iot_kafka_data
fi

echo "== Reset complete. Next: scripts/init.sh (after --volumes) or run-mode.sh live =="
