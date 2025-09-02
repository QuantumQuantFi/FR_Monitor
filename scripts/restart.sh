#!/usr/bin/env bash
set -euo pipefail

# Restart WLFI Funding Rate Monitor (simple_app.py on port 4002)
# Usage:
#   APP_DIR="/root/jerry/wlfi/FR_Monitor" ./scripts/restart.sh
# or run from repo root:
#   ./scripts/restart.sh

APP_DIR=${APP_DIR:-"$(cd "$(dirname "$0")/.." && pwd)"}
PORTS=(4002 5000)

echo "[info] Using APP_DIR=$APP_DIR"
cd "$APP_DIR"

stop_port() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    PID=$(lsof -t -i:"$port" -sTCP:LISTEN 2>/dev/null || true)
    if [ -n "${PID:-}" ]; then
      echo "[info] Stopping process on port $port (pid=$PID)"
      kill -TERM "$PID" 2>/dev/null || true
      sleep 2
      kill -9 "$PID" 2>/dev/null || true
    fi
  elif command -v fuser >/dev/null 2>&1; then
    echo "[info] Stopping process on port $port via fuser"
    fuser -k "$port"/tcp || true
  else
    echo "[warn] Neither lsof nor fuser found; skipping port $port stop"
  fi
}

for p in "${PORTS[@]}"; do
  stop_port "$p"
done

echo "[info] Clearing dynamic symbol cache"
rm -f dynamic_symbols_cache.json || true

LOG_FILE="run.log"
echo "[info] Starting simple_app.py with nohup (logging to $LOG_FILE)"
nohup python3 simple_app.py > "$LOG_FILE" 2>&1 &
NEW_PID=$!
sleep 2
echo "[ok] Started simple_app.py (pid=$NEW_PID)"

echo "[tail] Last 40 lines of $LOG_FILE:"
tail -n 40 "$LOG_FILE" || true

echo "[done] Visit: http://<host>:4002/ and /api/system/status"

