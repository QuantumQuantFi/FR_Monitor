#!/usr/bin/env bash
set -euo pipefail

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNIT_SRC="${SRC_DIR}/simple_app_watchdog.service"
UNIT_DST="/etc/systemd/system/simple_app_watchdog.service"

if [[ ! -f "${UNIT_SRC}" ]]; then
  echo "missing unit file: ${UNIT_SRC}" >&2
  exit 1
fi

install -m 0644 "${UNIT_SRC}" "${UNIT_DST}"
systemctl daemon-reload
systemctl enable --now simple_app_watchdog.service
systemctl status --no-pager simple_app_watchdog.service || true

