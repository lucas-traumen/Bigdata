#!/usr/bin/env bash
# preflight.sh — read-only host capability check for the IoT demo stack.
# Verifies Docker/Compose, OS/arch, CPU/RAM/disk against the budget and host
# port availability, and prints a verdict. It never starts or stops anything.
#
# Exit codes: 0 = ready; 1 = hard blocker (Docker/Compose missing);
#             2 = under budget (ports/resource warnings — fix or override).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMPL_DIR="$(dirname "$SCRIPT_DIR")"
cd "$IMPL_DIR"
if [ -f .env ]; then set -a; . ./.env; set +a; fi
DATA_ROOT="${DATA_ROOT:-/data/bigdata}"

HARD_FAIL=0
WARN=0
err()   { echo "  [FAIL] $*"; HARD_FAIL=1; }
warn()  { echo "  [WARN] $*"; WARN=1; }
ok()    { echo "  [OK]   $*"; }

echo "== Preflight for bigdata-iot stack ($(date -u +%Y-%m-%dT%H:%M:%SZ)) =="

echo "-- Docker --"
if command -v docker >/dev/null 2>&1; then
  ok "docker CLI: $(docker --version 2>/dev/null || echo '?')"
  if docker info >/dev/null 2>&1; then
    ok "docker daemon reachable"
  else
    err "docker daemon not reachable (is the service running?)"
  fi
else
  err "docker CLI not found — install Docker Engine first"
fi
if docker compose version >/dev/null 2>&1; then
  ok "compose plugin: $(docker compose version 2>/dev/null | head -n1)"
else
  err "docker compose plugin not available"
fi

echo "-- OS / arch / CPU --"
ok "kernel: $(uname -sr), arch: $(uname -m)"
CPUS="$(nproc 2>/dev/null || echo '?')"
ok "logical CPUs: $CPUS"

echo "-- Memory (budget: total >= 8 GB, available >= 4.6 GB) --"
if [ -r /proc/meminfo ]; then
  total_kb="$(awk '/^MemTotal:/{print $2}' /proc/meminfo)"
  avail_kb="$(awk '/^MemAvailable:/{print $2}' /proc/meminfo)"
  total_gb="$(awk -v k="$total_kb" 'BEGIN{printf "%.1f", k/1048576}')"
  avail_gb="$(awk -v k="$avail_kb" 'BEGIN{printf "%.1f", k/1048576}')"
  ok "RAM total: ${total_gb} GB, available: ${avail_gb} GB"
  if awk -v a="$avail_kb" 'BEGIN{exit !(a < 4718592)}'; then
    warn "available RAM below the 4.6 GB stack budget — close apps or use the target machine"
  fi
else
  warn "cannot read /proc/meminfo"
fi

echo "-- Disk (budget: >= 10 GB free; images ~3 GB + data) --"
data_dir="$(dirname "$DATA_ROOT")"
for d in "$data_dir" "/var/lib/docker"; do
  if df_out="$(df -BG --output=avail "$d" 2>/dev/null | tail -n1 | tr -dc '0-9')"; then
    if [ -n "$df_out" ]; then
      ok "free space on $d: ${df_out} GB"
      if [ "$df_out" -lt 10 ]; then
        warn "less than 10 GB free on $d — Docker builds/pulls may fail"
      fi
    fi
  else
    warn "cannot stat free space for $d"
  fi
done

echo "-- DATA_ROOT --"
echo "  DATA_ROOT=${DATA_ROOT}"
if [ -d "$DATA_ROOT" ]; then
  if [ -w "$DATA_ROOT" ]; then ok "exists and writable"
  else warn "exists but not writable by the current user (prepare-host.sh may need sudo)"
  fi
else
  warn "does not exist yet — run prepare-host.sh (or set DATA_ROOT to a writable path, e.g. \$PWD/data)"
fi

echo "-- Host ports (change via .env if occupied) --"
port_owner() {
  # prints a listener hint or empty
  if command -v ss >/dev/null 2>&1; then
    ss -ltnH "sport = :$1" 2>/dev/null | awk '{print $4}' | head -n1
  fi
}
check_port() {
  local port="$1" name="$2" envvar="$3"
  if port_owner "$port" >/dev/null 2>&1 && [ -n "$(port_owner "$port")" ]; then
    warn "TCP $port (${name}) appears busy: $(port_owner "$port") — set ${envvar} in .env to another port"
  else
    ok "TCP $port (${name}) free"
  fi
}
check_port "${MQTT_HOST_PORT:-1883}"      "mqtt"      "MQTT_HOST_PORT"
check_port "${KAFKA_HOST_PORT:-9092}"     "kafka"     "KAFKA_HOST_PORT"
check_port "${BACKEND_HOST_PORT:-8000}"   "backend"   "BACKEND_HOST_PORT"
check_port "${DASHBOARD_HOST_PORT:-8088}" "dashboard" "DASHBOARD_HOST_PORT"
check_port "${POSTGRES_HOST_PORT:-5432}"  "postgres"  "POSTGRES_HOST_PORT"

echo "-- Verdict --"
if [ "$HARD_FAIL" -eq 1 ]; then
  echo "BLOCKED: hard blockers present (see [FAIL] above)."
  exit 1
elif [ "$WARN" -eq 1 ]; then
  echo "READY WITH WARNINGS: the stack can start, but review [WARN] items first."
  exit 2
fi
echo "READY: host satisfies the documented budget."
exit 0
