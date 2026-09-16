#!/usr/bin/env bash
# wait-for-health.sh — block until the given services report healthy.
# Usage: wait-for-health.sh [--timeout SECONDS] SERVICE [SERVICE...]
#   SERVICE is the compose service name; container names are bigdata-<service>.
# Exit codes: 0 all healthy/running; 1 timeout or docker error.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMPL_DIR="$(dirname "$SCRIPT_DIR")"
cd "$IMPL_DIR"
if [ -f .env ]; then set -a; . ./.env; set +a; fi

TIMEOUT=120
SERVICES=()
while [ $# -gt 0 ]; do
  case "$1" in
    --timeout) TIMEOUT="$2"; shift 2 ;;
    *) SERVICES+=("$1"); shift ;;
  esac
done
if [ ${#SERVICES[@]} -eq 0 ]; then
  echo "usage: $0 [--timeout SECONDS] SERVICE [SERVICE...]" >&2
  exit 1
fi

deadline=$(( $(date +%s) + TIMEOUT ))
pending=("${SERVICES[@]}")
while [ ${#pending[@]} -gt 0 ]; do
  remaining=()
  for svc in "${pending[@]}"; do
    container="bigdata-${svc}"
    state="$(docker inspect -f '{{.State.Status}}' "$container" 2>/dev/null || echo missing)"
    if [ "$state" != "running" ]; then
      echo "[wait] $container is not running (state=$state)"
      exit 1
    fi
    health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container" 2>/dev/null || echo none)"
    if [ "$health" = "healthy" ] || [ "$health" = "none" ]; then
      echo "[wait] $container OK (health=$health)"
    else
      remaining+=("$svc")
    fi
  done
  if [ ${#remaining[@]} -eq 0 ]; then
    break
  fi
  pending=("${remaining[@]}")
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "[wait] TIMEOUT after ${TIMEOUT}s; still waiting for: ${pending[*]}" >&2
    exit 1
  fi
  sleep 3
done
exit 0
