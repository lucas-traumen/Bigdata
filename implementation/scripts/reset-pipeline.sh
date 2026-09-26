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
# Compose project name, resolved exactly like docker compose does for the
# file passed to COMPOSE above: COMPOSE_PROJECT_NAME (env / sourced .env)
# wins, otherwise the top-level `name: bigdata-iot` pinned in compose.yaml.
# Used below to scope the sim-* cleanup to THIS project's containers.
PROJECT_NAME="${COMPOSE_PROJECT_NAME:-bigdata-iot}"

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
"${COMPOSE[@]}" --profile live --profile batch down --remove-orphans || true
# per-zone simulator containers spawned by run-multi-sim.sh (ad-hoc names
# sim-<zone>): the anchored name filter is ANDed with the standard
# com.docker.compose.project label that compose stamps on every container it
# creates (including `docker compose run` one-offs), so a sim-* container of
# ANOTHER compose project — or any unrelated container that merely has "sim-"
# in its name — can never be matched or removed here. (run-multi-sim.sh also
# tags its containers with iot.simulator=1 for ad-hoc inspection.)
for sim in $(docker ps -aq \
    --filter 'name=^sim-' \
    --filter "label=com.docker.compose.project=${PROJECT_NAME}" 2>/dev/null); do
  docker rm -f "$sim" >/dev/null 2>&1 || true
done

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
