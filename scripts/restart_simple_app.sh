#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${SCRIPT_DIR}/stop_simple_app.sh" || true
exec "${SCRIPT_DIR}/run_simple_app.sh" "$@"
