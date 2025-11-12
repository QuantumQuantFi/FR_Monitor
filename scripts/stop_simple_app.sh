#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUNTIME_DIR="${SIMPLE_APP_RUNTIME_DIR:-${PROJECT_ROOT}/runtime/simple_app}"
PID_FILE="${RUNTIME_DIR}/simple_app.pid"

mkdir -p "${RUNTIME_DIR}"

declare -a TARGET_PIDS=()
declare -A SEEN_PIDS=()

add_pid() {
    local pid="$1"
    if [[ -z "${pid}" || ! "${pid}" =~ ^[0-9]+$ ]]; then
        return
    fi
    if [[ -z "${SEEN_PIDS[${pid}]:-}" ]]; then
        SEEN_PIDS["${pid}"]=1
        TARGET_PIDS+=("${pid}")
    fi
}

if [[ -f "${PID_FILE}" ]]; then
    PID_FROM_FILE="$(<"${PID_FILE}")"
    add_pid "${PID_FROM_FILE}"
fi

if command -v pgrep >/dev/null 2>&1; then
    while read -r pid; do
        add_pid "${pid}"
    done < <(pgrep -f "[p]ython[0-9.]* .*simple_app\\.py" 2>/dev/null || true)
fi

stop_pid() {
    local pid="$1"
    if ! kill -0 "${pid}" 2>/dev/null; then
        echo "[stop] PID ${pid} 已不在运行"
        return
    fi

    echo "[stop] 正在停止 PID ${pid}"
    kill -TERM "${pid}" 2>/dev/null || true
    for _ in {1..10}; do
        if kill -0 "${pid}" 2>/dev/null; then
            sleep 0.5
        else
            echo "[stop] PID ${pid} 已停止"
            return
        fi
    done

    echo "[stop] PID ${pid} 响应缓慢，发送 KILL"
    kill -KILL "${pid}" 2>/dev/null || true
}

if [[ ${#TARGET_PIDS[@]} -eq 0 ]]; then
    echo "[stop] 未找到正在运行的 simple_app.py 进程"
else
    for pid in "${TARGET_PIDS[@]}"; do
        stop_pid "${pid}"
    done
fi

rm -f "${PID_FILE}"
echo "[stop] 运行目录: ${RUNTIME_DIR}"
