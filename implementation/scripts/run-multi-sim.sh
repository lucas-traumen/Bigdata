#!/usr/bin/env bash
# run-multi-sim.sh — spawn/stop the N parallel source simulators of the
# multi-source demo (plan E3.1): one container per zone prefix (zoneA..zoneE)
# so event_ids never collide across sources (event_id = {prefix}-{seq}).
#
# Usage:
#   run-multi-sim.sh start     # docker compose run -d --name sim-<zone> ...
#   run-multi-sim.sh stop      # remove the per-zone containers
#   run-multi-sim.sh status    # list sim-* containers
#
# Prerequisite: mqtt (and ideally the whole `run-mode.sh live` stack) up.
# Each container writes its own manifest JSONL:
#   ${DATA_ROOT}/logs/simulator-<zone>.jsonl   ("what this source sent")
# Rate/sensors/fault profile come from .env (SIM_RATE, SIM_SENSORS,
# SIM_SPIKE_PCT, FAULT_INJECT) exactly like the template simulator service.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMPL_DIR="$(dirname "$SCRIPT_DIR")"
cd "$IMPL_DIR"
if [ -f .env ]; then set -a; . ./.env; set +a; fi
COMPOSE=(docker compose -f "$IMPL_DIR/compose.yaml")

# zone prefixes; override with SIM_ZONES="zoneA zoneB" for fewer/more sources
ZONES="${SIM_ZONES:-zoneA zoneB zoneC zoneD zoneE}"
ZONE_COUNT="$(echo $ZONES | wc -w)"

case "${1:-}" in
  start)
    echo "== Starting ${ZONE_COUNT} zone simulators: ${ZONES} =="
    for zone in $ZONES; do
      # skip only when a container with this name is actually RUNNING; an
      # exited leftover (e.g. --rm removal interrupted) is replaced
      state="$(docker inspect -f '{{.State.Status}}' "sim-${zone}" 2>/dev/null || echo missing)"
      if [ "$state" = "running" ]; then
        echo "[multi-sim] sim-${zone} already running — skipping"
        continue
      fi
      if [ "$state" != "missing" ]; then
        echo "[multi-sim] removing leftover sim-${zone} (state=${state})"
        docker rm -f "sim-${zone}" >/dev/null 2>&1 || true
      fi
      # unique SENSOR_PREFIX => unique sensor_id/event_id namespace;
      # unique MQTT client id (producer derives it from the prefix) and
      # per-zone manifest file for sent-vs-delivered reconciliation.
      # The explicit label marks these ad-hoc containers, on top of the
      # standard com.docker.compose.project label that `docker compose run`
      # assigns itself — both are scoped to THIS compose file/project
      # (compose.yaml `name: bigdata-iot`), which is what project-scoped
      # cleanup (reset-pipeline.sh) filters on so another project's sim-*
      # containers can never be touched.
      "${COMPOSE[@]}" --profile live run -d --rm --name "sim-${zone}" \
        --label iot.simulator=1 \
        -e "SENSOR_PREFIX=${zone}" \
        -e "MANIFEST_FILE=/data/bigdata/logs/simulator-${zone}.jsonl" \
        simulator
      echo "[multi-sim] started sim-${zone} (manifest: logs/simulator-${zone}.jsonl)"
    done
    echo "== zone simulators up =="
    ;;

  stop)
    for zone in $ZONES; do
      # --rm on `run` removes the container once it stops; rm -f covers both
      # the still-running and the already-exited case, never other projects.
      docker rm -f "sim-${zone}" >/dev/null 2>&1 \
        && echo "[multi-sim] removed sim-${zone}" \
        || echo "[multi-sim] sim-${zone} not present"
    done
    ;;

  status)
    docker ps -a --filter 'name=sim-' \
      --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'
    ;;

  *)
    echo "usage: $0 {start|stop|status}" >&2
    exit 1
    ;;
esac
