#!/usr/bin/env bash
# run-batch-hourly.sh — hourly Gold batch trigger WITHOUT a scheduler stack
# (plan E3.3, user decision 2026-09-18): computes the UTC hour window that
# just ended [hh:00, hh+1:00) and runs the one-shot batch for it.
# Intended to be called from HOST cron, e.g. (crontab -e):
#
#   0 * * * * /path/to/implementation/scripts/run-batch-hourly.sh \
#       >> ${DATA_ROOT}/logs/batch-hourly.log 2>&1
#
# Concurrency guard: a mkdir-based lock under ${DATA_ROOT}/control/ refuses a
# new run while the previous batch is still going (stale locks from crashed
# runs are detected via the recorded PID and cleaned up).
# Reruns are always safe: the batch upsert is full-replace per
# (sensor_id, hour_start, metric); events that land in Silver slightly after
# the hour can be captured by rerunning the window via run-mode.sh batch.
#
# Portability: the UTC window math below uses GNU date syntax
# (`date -u -d "@<epoch>"`). Requires GNU date (Linux); BSD/macOS date is
# NOT supported. This is deliberate — the plan targets a single Linux host
# with Docker Engine + Compose plugin (plan §1), so no cross-platform date
# fallback is added here.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMPL_DIR="$(dirname "$SCRIPT_DIR")"
cd "$IMPL_DIR"
if [ -f .env ]; then set -a; . ./.env; set +a; fi
DATA_ROOT="${DATA_ROOT:-/data/bigdata}"
GRACE_S="${BATCH_GRACE_S:-60}"
LOCK_DIR="${DATA_ROOT}/control/batch-hourly.lock"
COMPOSE=(docker compose -f "$IMPL_DIR/compose.yaml")

cleanup_lock() {
  rm -rf "$LOCK_DIR"
}

# ---------------------------------------------------------------------------
# 1) lock: refuse to overlap with a still-running batch
if mkdir "$LOCK_DIR" 2>/dev/null; then
  echo $$ > "$LOCK_DIR/pid"
else
  lock_pid="$(cat "$LOCK_DIR/pid" 2>/dev/null || echo '')"
  if [ -n "$lock_pid" ] && kill -0 "$lock_pid" 2>/dev/null; then
    echo "[batch-hourly] previous batch still running (pid ${lock_pid}) — skipping"
    exit 1
  fi
  echo "[batch-hourly] removing stale lock (pid ${lock_pid:-?} not running)"
  rm -rf "$LOCK_DIR"
  mkdir "$LOCK_DIR"
  echo $$ > "$LOCK_DIR/pid"
fi
# remove the lock on ANY exit path — registered only AFTER we hold the lock,
# so a refused run never deletes the holder's lock directory
trap cleanup_lock EXIT

# ---------------------------------------------------------------------------
# 2) window: the UTC hour that just ended, [start, end) on hour boundaries
now_epoch="$(date -u +%s)"
end_epoch=$((now_epoch - now_epoch % 3600))
start_epoch=$((end_epoch - 3600))
start="$(date -u -d "@${start_epoch}" +%Y-%m-%dT%H:00:00Z)"
end="$(date -u -d "@${end_epoch}" +%Y-%m-%dT%H:00:00Z)"

# 3) grace delay so events published near the boundary can reach Silver
if [ "$GRACE_S" -gt 0 ]; then
  echo "[batch-hourly] waiting ${GRACE_S}s for boundary events to land in Silver"
  sleep "$GRACE_S"
fi

echo "[batch-hourly] running Gold batch window [${start}, ${end})"
"${COMPOSE[@]}" --profile batch run --rm --name spark-batch-hourly spark-batch \
  /opt/spark/run/run-batch.sh --start "$start" --end "$end"

echo "[batch-hourly] done: window [${start}, ${end})"
