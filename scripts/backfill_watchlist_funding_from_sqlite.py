#!/usr/bin/env python3
"""
Backfill funding schedule fields in Postgres watch_signal_raw from SQLite market_data.db.

Target:
- leg_a_next_funding_time / leg_b_next_funding_time for perp legs.
- top-level funding_interval_hours / next_funding_time for multi-exchange rows,
  derived from perp legs when missing.

Usage:
  venv/bin/python scripts/backfill_watchlist_funding_from_sqlite.py \
      --pg-dsn "postgresql://..." --sqlite-path market_data.db --days 7
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        # fallback for 'YYYY-mm-dd HH:MM:SS'
        try:
            dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None


def _sqlite_ts(dt: datetime) -> str:
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _to_float(x: Any) -> Optional[float]:
    try:
        f = float(x)
        return f
    except Exception:
        return None


def _get_sqlite_funding(
    sqlite_conn: sqlite3.Connection, exchange: str, symbol: str, target_ts: datetime
) -> Tuple[Optional[float], Optional[datetime]]:
    ts_str = _sqlite_ts(target_ts)
    cur = sqlite_conn.execute(
        """
        SELECT funding_interval_hours, next_funding_time
        FROM price_data_1min
        WHERE exchange=? AND symbol=? AND timestamp<=?
        ORDER BY timestamp DESC
        LIMIT 1;
        """,
        (exchange, symbol, ts_str),
    )
    row = cur.fetchone()
    if not row:
        cur = sqlite_conn.execute(
            """
            SELECT funding_interval_hours, next_funding_time
            FROM price_data
            WHERE exchange=? AND symbol=? AND timestamp<=?
            ORDER BY timestamp DESC
            LIMIT 1;
            """,
            (exchange, symbol, ts_str),
        )
        row = cur.fetchone()
    if not row:
        return None, None
    interval = _to_float(row[0])
    if interval is not None and interval <= 0:
        interval = None
    next_ft = _parse_ts(row[1])
    return interval, next_ft


def iter_missing_rows(
    pg_conn, since: datetime, limit: Optional[int] = None
) -> Iterable[Dict[str, Any]]:
    sql = """
    SELECT id, ts, exchange, symbol, signal_type,
           funding_interval_hours, next_funding_time,
           leg_a_exchange, leg_a_symbol, leg_a_kind, leg_a_next_funding_time,
           leg_b_exchange, leg_b_symbol, leg_b_kind, leg_b_next_funding_time
    FROM watchlist.watch_signal_raw
    WHERE ts >= %s
      AND (exchange='multi' OR signal_type IN ('B','C'))
      AND leg_a_exchange IS NOT NULL AND leg_b_exchange IS NOT NULL
      AND (
            leg_a_next_funding_time IS NULL
         OR leg_b_next_funding_time IS NULL
         OR funding_interval_hours IS NULL OR funding_interval_hours=0
         OR next_funding_time IS NULL
      )
    ORDER BY ts DESC
    """
    if limit:
        sql += " LIMIT %s"
        params = (since, limit)
    else:
        params = (since,)
    cur = pg_conn.execute(sql, params)
    cols = [d.name for d in cur.description]
    for row in cur.fetchall():
        yield dict(zip(cols, row))


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill PG funding schedule from SQLite")
    parser.add_argument("--pg-dsn", required=True)
    parser.add_argument("--sqlite-path", default="market_data.db")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--limit", type=int, default=0, help="debug limit rows")
    parser.add_argument("--batch", type=int, default=500)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("backfill_funding")

    since = _utcnow() - timedelta(days=max(1, args.days))
    limit = args.limit or None

    with sqlite3.connect(args.sqlite_path, timeout=30.0) as sqlite_conn, psycopg.connect(
        args.pg_dsn, autocommit=True
    ) as pg_conn:
        updates: List[Tuple[Dict[str, Any], int, datetime]] = []
        scanned = 0
        filled = 0

        for row in iter_missing_rows(pg_conn, since, limit=limit):
            scanned += 1
            ts: datetime = row["ts"]
            perp_nexts: List[datetime] = []
            perp_intervals: List[float] = []
            patch: Dict[str, Any] = {}

            for leg_label in ("a", "b"):
                kind = row.get(f"leg_{leg_label}_kind")
                if kind != "perp":
                    continue
                ex = (row.get(f"leg_{leg_label}_exchange") or "").lower()
                sym = (row.get(f"leg_{leg_label}_symbol") or row.get("symbol") or "").upper()
                interval, next_ft = _get_sqlite_funding(sqlite_conn, ex, sym, ts)
                if next_ft:
                    perp_nexts.append(next_ft)
                    col = f"leg_{leg_label}_next_funding_time"
                    if row.get(col) is None:
                        patch[col] = next_ft
                if interval:
                    perp_intervals.append(interval)

            # top-level fields for multi rows: use nearest perp schedule
            if perp_nexts and row.get("next_funding_time") is None:
                patch["next_funding_time"] = min(perp_nexts)
            if perp_intervals and (row.get("funding_interval_hours") in (None, 0)):
                patch["funding_interval_hours"] = int(round(min(perp_intervals)))

            if patch:
                updates.append((patch, row["id"], ts))

            if len(updates) >= args.batch:
                filled += _flush_updates(pg_conn, updates, logger)
                updates.clear()

        if updates:
            filled += _flush_updates(pg_conn, updates, logger)

        logger.info("scan done. scanned=%s patched_rows=%s", scanned, filled)


def _flush_updates(pg_conn, updates: List[Tuple[Dict[str, Any], int, datetime]], logger) -> int:
    if not updates:
        return 0
    patched = 0
    for patch, rid, ts in updates:
        set_cols = list(patch.keys())
        assigns = ", ".join([f"{c}=%({c})s" for c in set_cols])
        sql = f"UPDATE watchlist.watch_signal_raw SET {assigns} WHERE ts=%(ts)s AND id=%(id)s"
        params = {**patch, "id": rid, "ts": ts}
        pg_conn.execute(sql, params)
        patched += 1
    logger.info("patched %s rows", patched)
    return patched


if __name__ == "__main__":
    main()
