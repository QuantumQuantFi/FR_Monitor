#!/usr/bin/env bash
# Rolling partition maintenance for watchlist (raw + series).
# Creates partitions for the next N days and drops partitions older than retention.
# Usage: RETAIN_DAYS_RAW=14 RETAIN_DAYS_SERIES=7 ./scripts/manage_watchlist_partitions.sh

set -euo pipefail

PSQL_BIN="${PSQL_BIN:-psql}"
PG_DSN="${PG_DSN:-postgresql://wl_writer:wl_writer_A3f9xB2@127.0.0.1:5432/watchlist}"

RETAIN_DAYS_RAW="${RETAIN_DAYS_RAW:-14}"
RETAIN_DAYS_SERIES="${RETAIN_DAYS_SERIES:-7}"
PRECREATE_DAYS="${PRECREATE_DAYS:-2}"

run_psql() {
  "${PSQL_BIN}" "${PG_DSN}" -v ON_ERROR_STOP=1 "$@"
}

run_psql <<'SQL'
SET client_min_messages=warning;
SET search_path=watchlist,public;
SQL

today=$(date -u +%Y-%m-%d)

create_partitions() {
  local table=$1
  local days=$2
  for i in $(seq 0 "${days}"); do
    local d=$(date -u -d "${today} +${i} day" +%Y-%m-%d)
    local next=$(date -u -d "${d} +1 day" +%Y-%m-%d)
    local suffix=$(date -u -d "${d}" +%Y%m%d)
    run_psql -c "CREATE TABLE IF NOT EXISTS ${table}_p${suffix} PARTITION OF ${table} FOR VALUES FROM ('${d}') TO ('${next}');"
  done
}

drop_old_partitions() {
  local table=$1
  local retain=$2
  local cutoff=$(date -u -d "${today} -${retain} day" +%Y-%m-%d)
  # list partitions and drop older than cutoff start date
  run_psql -At -c "SELECT relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='watchlist' AND relname LIKE '${table}_p%' ORDER BY relname;" | \
  while read -r rel; do
    local stamp=${rel#"${table}_p"}
    if [[ ${#stamp} -eq 8 ]]; then
      local part_date=$(date -u -d "${stamp}" +%Y-%m-%d)
      if [[ "${part_date}" < "${cutoff}" ]]; then
        run_psql -c "DROP TABLE IF EXISTS ${rel};"
      fi
    fi
  done
}

create_partitions "watch_signal_raw" "${PRECREATE_DAYS}"
create_partitions "watchlist_series_agg" "${PRECREATE_DAYS}"
drop_old_partitions "watch_signal_raw" "${RETAIN_DAYS_RAW}"
drop_old_partitions "watchlist_series_agg" "${RETAIN_DAYS_SERIES}"

echo "Partition maintenance done for ${today} (raw retain ${RETAIN_DAYS_RAW}d, series retain ${RETAIN_DAYS_SERIES}d, precreated ${PRECREATE_DAYS}d)."
