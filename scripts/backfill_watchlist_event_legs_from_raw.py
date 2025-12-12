#!/usr/bin/env python3
"""
Backfill missing leg fields in Postgres watch_signal_event from existing watch_signal_raw.

This targets historical events created before leg columns were populated.

Usage:
  venv/bin/python scripts/backfill_watchlist_event_legs_from_raw.py \
      --pg-dsn "postgresql://..." --window-minutes 360
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import psycopg


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def load_missing_events(pg_conn, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    sql = """
    SELECT id, exchange, symbol, signal_type, start_ts, end_ts
    FROM watchlist.watch_signal_event
    WHERE leg_a_exchange IS NULL OR leg_b_exchange IS NULL
    ORDER BY start_ts ASC
    """
    if limit:
        sql += " LIMIT %s"
        cur = pg_conn.execute(sql, (limit,))
    else:
        cur = pg_conn.execute(sql)
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def find_raw_legs_for_event(pg_conn, symbol: str, signal_type: str, start_ts: datetime, end_ts: Optional[datetime], window: timedelta) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    start = start_ts - timedelta(minutes=5)
    end = (end_ts or (start_ts + window))
    cur = pg_conn.execute(
        """
        SELECT ts,
               leg_a_exchange, leg_a_symbol, leg_a_kind, leg_a_price, leg_a_funding_rate,
               leg_b_exchange, leg_b_symbol, leg_b_kind, leg_b_price, leg_b_funding_rate
        FROM watchlist.watch_signal_raw
        WHERE symbol=%s AND signal_type=%s
          AND ts BETWEEN %s AND %s
          AND leg_a_exchange IS NOT NULL AND leg_b_exchange IS NOT NULL
        ORDER BY ts ASC;
        """,
        (symbol, signal_type, start, end),
    )
    rows = cur.fetchall()
    if not rows:
        return None
    first = rows[0]
    last = rows[-1]
    leg_a = {
        "exchange": first[1],
        "symbol": first[2],
        "kind": first[3],
        "price_first": first[4],
        "price_last": last[4],
        "funding_rate_first": first[5],
        "funding_rate_last": last[5],
    }
    leg_b = {
        "exchange": first[6],
        "symbol": first[7],
        "kind": first[8],
        "price_first": first[9],
        "price_last": last[9],
        "funding_rate_first": first[10],
        "funding_rate_last": last[10],
    }
    return (leg_a, leg_b)


def apply_event_update(pg_conn, event_id: int, leg_a: Dict[str, Any], leg_b: Dict[str, Any]) -> None:
    pg_conn.execute(
        """
        UPDATE watchlist.watch_signal_event
           SET leg_a_exchange=%s,
               leg_a_symbol=%s,
               leg_a_kind=%s,
               leg_a_price_first=%s,
               leg_a_price_last=%s,
               leg_a_funding_rate_first=%s,
               leg_a_funding_rate_last=%s,
               leg_b_exchange=%s,
               leg_b_symbol=%s,
               leg_b_kind=%s,
               leg_b_price_first=%s,
               leg_b_price_last=%s,
               leg_b_funding_rate_first=%s,
               leg_b_funding_rate_last=%s
         WHERE id=%s;
        """,
        (
            leg_a.get("exchange"),
            leg_a.get("symbol"),
            leg_a.get("kind"),
            leg_a.get("price_first"),
            leg_a.get("price_last"),
            leg_a.get("funding_rate_first"),
            leg_a.get("funding_rate_last"),
            leg_b.get("exchange"),
            leg_b.get("symbol"),
            leg_b.get("kind"),
            leg_b.get("price_first"),
            leg_b.get("price_last"),
            leg_b.get("funding_rate_first"),
            leg_b.get("funding_rate_last"),
            event_id,
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill event legs from raw")
    parser.add_argument("--pg-dsn", required=True)
    parser.add_argument("--window-minutes", type=int, default=360, help="fallback window when event end_ts is NULL")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("backfill_event_legs")

    window = timedelta(minutes=max(30, args.window_minutes))
    limit = args.limit or None

    with psycopg.connect(args.pg_dsn, autocommit=True) as pg_conn:
        events = load_missing_events(pg_conn, limit=limit)
        if not events:
            logger.info("no missing-leg events")
            return

        patched = 0
        skipped = 0
        for e in events:
            res = find_raw_legs_for_event(
                pg_conn,
                e["symbol"],
                e["signal_type"],
                e["start_ts"],
                e.get("end_ts"),
                window,
            )
            if not res:
                skipped += 1
                continue
            leg_a, leg_b = res
            apply_event_update(pg_conn, e["id"], leg_a, leg_b)
            patched += 1

        logger.info("done. events=%s patched=%s skipped=%s", len(events), patched, skipped)


if __name__ == "__main__":
    main()
