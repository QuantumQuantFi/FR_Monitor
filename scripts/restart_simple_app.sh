#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")/.."

echo "[restart] Checking for existing simple_app.py process..."
if pgrep -f "python(3)?\s+-u?\s*simple_app.py" >/dev/null 2>&1; then
    echo "[restart] Stopping existing simple_app.py..."
    pgrep -f "python(3)?\s+-u?\s*simple_app.py" | xargs -r kill -TERM || true
    # wait up to 5s for graceful stop
    for i in {1..5}; do
        if pgrep -f "python(3)?\s+-u?\s*simple_app.py" >/dev/null 2>&1; then
            sleep 1
        else
            break
        fi
    done
    # force kill if still present
    if pgrep -f "python(3)?\s+-u?\s*simple_app.py" >/dev/null 2>&1; then
        echo "[restart] Forcing stop..."
        pgrep -f "python(3)?\s+-u?\s*simple_app.py" | xargs -r kill -KILL || true
    fi
else
    echo "[restart] No existing process found."
fi

echo "[restart] Starting simple_app.py with nohup..."
rm -f simple_app.pid

# Choose python interpreter
PY_CMD="python"
if ! command -v "$PY_CMD" >/dev/null 2>&1; then
  if command -v python3 >/dev/null 2>&1; then
    PY_CMD="python3"
  fi
fi

nohup "$PY_CMD" -u simple_app.py > run.log 2>&1 &
PID=$!
echo "$PID" > simple_app.pid
echo "[restart] simple_app.py started with PID $PID"

echo "[restart] Tail logs: tail -n 100 -f run.log"
echo "[restart] Verify port: ss -ltnp | grep ':4002 ' || true"
