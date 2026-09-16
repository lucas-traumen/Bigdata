#!/usr/bin/env bash
# prepare-host.sh — idempotent host preparation:
#   * creates DATA_ROOT subdirectories (bind-mount targets),
#   * makes them writable for the demo (a+rwX; single-user demo machine),
#   * bootstraps .env from .env.example when missing.
# NOTE: creating /data may require sudo on the target machine:
#   sudo mkdir -p /data && sudo chown "$USER" /data
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMPL_DIR="$(dirname "$SCRIPT_DIR")"
cd "$IMPL_DIR"
if [ -f .env ]; then set -a; . ./.env; set +a; fi
DATA_ROOT="${DATA_ROOT:-/data/bigdata}"

SUBDIRS=(
  bronze/sensor
  silver/sensor
  quarantine/sensor
  checkpoints/q1
  checkpoints/q2a
  checkpoints/q2b
  checkpoints/q3
  logs/airflow
  control
)

echo "== Preparing host directories under ${DATA_ROOT} =="
mkdir -p "$DATA_ROOT"
for d in "${SUBDIRS[@]}"; do
  mkdir -p "$DATA_ROOT/$d"
done
# Demo permissions: any container UID may read/write (documented trade-off).
chmod -R a+rwX "$DATA_ROOT" 2>/dev/null || true

if [ ! -f .env ]; then
  cp .env.example .env
  echo "== Created .env from .env.example (edit credentials/ports there) =="
fi

echo "Created/verified:"
printf '  %s\n' "$DATA_ROOT" "${SUBDIRS[@]/#/$DATA_ROOT/}"
echo "Done."
