#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

LOG_DIR="${SIMPLE_APP_LOG_DIR:-${PROJECT_ROOT}/logs/simple_app}"
RUNTIME_DIR="${SIMPLE_APP_RUNTIME_DIR:-${PROJECT_ROOT}/runtime/simple_app}"

mkdir -p "${LOG_DIR}" "${RUNTIME_DIR}"

export SIMPLE_APP_LOG_DIR="${LOG_DIR}"
export SIMPLE_APP_RUNTIME_DIR="${RUNTIME_DIR}"

exec python "${PROJECT_ROOT}/simple_app.py" "$@"
