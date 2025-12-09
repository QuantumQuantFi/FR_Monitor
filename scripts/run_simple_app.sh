#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

LOG_DIR="${SIMPLE_APP_LOG_DIR:-${PROJECT_ROOT}/logs/simple_app}"
RUNTIME_DIR="${SIMPLE_APP_RUNTIME_DIR:-${PROJECT_ROOT}/runtime/simple_app}"
PYTHON_BIN="${PYTHON_BIN:-${PROJECT_ROOT}/venv/bin/python}"
OUTCOME_WORKER_ENABLED="${OUTCOME_WORKER_ENABLED:-1}"
OUTCOME_WORKER_INTERVAL_SEC="${OUTCOME_WORKER_INTERVAL_SEC:-600}"

mkdir -p "${LOG_DIR}" "${RUNTIME_DIR}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
    PYTHON_BIN="$(command -v python3 || command -v python || true)"
fi
if [[ -z "${PYTHON_BIN}" ]]; then
    PYTHON_BIN="python"
fi

export SIMPLE_APP_LOG_DIR="${LOG_DIR}"
export SIMPLE_APP_RUNTIME_DIR="${RUNTIME_DIR}"

OUTCOME_WORKER_PID_FILE="${RUNTIME_DIR}/outcome_worker.pid"
OUTCOME_WORKER_LOG="${LOG_DIR}/outcome_worker.log"

maybe_start_outcome_worker() {
    if [[ "${OUTCOME_WORKER_ENABLED}" != "1" ]]; then
        echo "[run] outcome worker disabled (OUTCOME_WORKER_ENABLED=${OUTCOME_WORKER_ENABLED})"
        return
    fi

    # 避免重复启动
    if [[ -f "${OUTCOME_WORKER_PID_FILE}" ]]; then
        existing_pid="$(<"${OUTCOME_WORKER_PID_FILE}")"
        if [[ -n "${existing_pid}" && "${existing_pid}" =~ ^[0-9]+$ && -e "/proc/${existing_pid}" ]]; then
            echo "[run] outcome worker 已在运行 (PID=${existing_pid})，跳过重新启动"
            return
        fi
    fi

    echo "[run] 启动 outcome worker loop，周期 ${OUTCOME_WORKER_INTERVAL_SEC}s"
    mkdir -p "$(dirname "${OUTCOME_WORKER_LOG}")"
    nohup bash -c "while true; do date --iso-8601=seconds >> '${OUTCOME_WORKER_LOG}'; '${PYTHON_BIN}' '${PROJECT_ROOT}/watchlist_outcome_worker.py' --loop-once >> '${OUTCOME_WORKER_LOG}' 2>&1; sleep ${OUTCOME_WORKER_INTERVAL_SEC}; done" >/dev/null 2>&1 &
    echo $! > "${OUTCOME_WORKER_PID_FILE}"
}

maybe_start_outcome_worker

if [[ ! -x "${PYTHON_BIN}" ]]; then
    PYTHON_BIN="python"
fi

exec "${PYTHON_BIN}" "${PROJECT_ROOT}/simple_app.py" "$@"
