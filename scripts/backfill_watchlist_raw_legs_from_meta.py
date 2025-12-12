#!/usr/bin/env python3
"""
Backfill leg_a_* / leg_b_* columns in Postgres watch_signal_raw using meta.trigger_details / meta.snapshots.

This targets historical raw rows created before leg columns were populated.

Heuristics:
- Type B: meta.trigger_details.pair/prices/funding -> two perp legs
- Type C: meta.trigger_details.spot_exchange/futures_exchange/spot_price/futures_price/funding -> spot + perp
- Type A: meta.snapshots.binance -> spot + perp on Binance

Usage:
  venv/bin/python scripts/backfill_watchlist_raw_legs_from_meta.py \
      --pg-dsn "postgresql://..." --days 14 --batch 5000
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_dict(val: Any) -> Optional[Dict[str, Any]]:
    if val is None:
        return None
    if isinstance(val, dict):
        return val
    # psycopg may return JSONB as string in some configs
    try:
        import json

        if isinstance(val, (bytes, bytearray)):
            val = val.decode("utf-8", errors="ignore")
        if isinstance(val, str):
            text = val.strip()
            if not text:
                return None
            parsed = json.loads(text)
            return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None
    return None


def _to_float(x: Any) -> Optional[float]:
    try:
        f = float(x)
        return f
    except Exception:
        return None


def iter_rows(pg_conn, since: datetime, limit: Optional[int] = None) -> Iterable[Dict[str, Any]]:
    sql = """
    SELECT id, ts, exchange, symbol, signal_type,
           leg_a_exchange, leg_a_symbol, leg_a_kind, leg_a_price, leg_a_funding_rate,
           leg_b_exchange, leg_b_symbol, leg_b_kind, leg_b_price, leg_b_funding_rate,
           meta->'trigger_details' AS trigger_details,
           meta->'snapshots' AS snapshots
    FROM watchlist.watch_signal_raw
    WHERE ts >= %s
      AND (leg_a_exchange IS NULL OR leg_b_exchange IS NULL)
      AND meta IS NOT NULL
    ORDER BY ts ASC
    """
    if limit:
        sql += " LIMIT %s"
        cur = pg_conn.execute(sql, (since, limit))
    else:
        cur = pg_conn.execute(sql, (since,))
    cols = [d.name for d in cur.description]
    for r in cur.fetchall():
        yield dict(zip(cols, r))


def build_legs(row: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    signal_type = str(row.get("signal_type") or "").strip()
    symbol = str(row.get("symbol") or "").upper()
    trigger_details = _as_dict(row.get("trigger_details")) or {}
    snapshots = _as_dict(row.get("snapshots")) or {}

    if signal_type == "B":
        pair = trigger_details.get("pair") or []
        prices = trigger_details.get("prices") or {}
        funding = trigger_details.get("funding") or {}
        if isinstance(pair, list) and len(pair) == 2:
            ex_a, ex_b = pair[0], pair[1]
            leg_a = {
                "exchange": str(ex_a).lower(),
                "symbol": symbol,
                "kind": "perp",
                "price": _to_float(prices.get(ex_a)),
                "funding_rate": _to_float(funding.get(ex_a)),
            }
            leg_b = {
                "exchange": str(ex_b).lower(),
                "symbol": symbol,
                "kind": "perp",
                "price": _to_float(prices.get(ex_b)),
                "funding_rate": _to_float(funding.get(ex_b)),
            }
            return leg_a, leg_b

    if signal_type == "C":
        spot_ex = trigger_details.get("spot_exchange")
        fut_ex = trigger_details.get("futures_exchange")
        if spot_ex and fut_ex:
            fund_map = trigger_details.get("funding") or {}
            leg_a = {
                "exchange": str(spot_ex).lower(),
                "symbol": symbol,
                "kind": "spot",
                "price": _to_float(trigger_details.get("spot_price")),
                "funding_rate": 0.0,
            }
            leg_b = {
                "exchange": str(fut_ex).lower(),
                "symbol": symbol,
                "kind": "perp",
                "price": _to_float(trigger_details.get("futures_price")),
                "funding_rate": _to_float(fund_map.get(fut_ex)),
            }
            return leg_a, leg_b

    if signal_type == "A":
        snap = snapshots.get("binance") if isinstance(snapshots, dict) else None
        if isinstance(snap, dict):
            spot_price = _to_float(snap.get("spot_price"))
            perp_price = _to_float(snap.get("futures_price"))
            fr = _to_float(snap.get("funding_rate"))
            leg_a = {
                "exchange": "binance",
                "symbol": symbol,
                "kind": "spot",
                "price": spot_price,
                "funding_rate": 0.0,
            }
            leg_b = {
                "exchange": "binance",
                "symbol": symbol,
                "kind": "perp",
                "price": perp_price,
                "funding_rate": fr,
            }
            return leg_a, leg_b

    return None, None


def flush_updates(pg_conn, updates: List[Tuple[int, datetime, Dict[str, Any]]]) -> int:
    patched = 0
    for rid, ts, patch in updates:
        cols = list(patch.keys())
        assigns = ", ".join([f"{c}=%({c})s" for c in cols])
        sql = f"UPDATE watchlist.watch_signal_raw SET {assigns} WHERE ts=%(ts)s AND id=%(id)s"
        params = {**patch, "id": rid, "ts": ts}
        pg_conn.execute(sql, params)
        patched += 1
    return patched


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill raw legs from meta JSON")
    parser.add_argument("--pg-dsn", required=True)
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--batch", type=int, default=5000)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("backfill_raw_legs")

    since = _utcnow() - timedelta(days=max(1, args.days))
    limit = args.limit or None

    with psycopg.connect(args.pg_dsn, autocommit=True) as pg_conn:
        updates: List[Tuple[int, datetime, Dict[str, Any]]] = []
        scanned = 0
        patched = 0
        skipped = 0

        for row in iter_rows(pg_conn, since, limit=limit):
            scanned += 1
            leg_a, leg_b = build_legs(row)
            if not leg_a or not leg_b:
                skipped += 1
                continue

            patch: Dict[str, Any] = {}
            if row.get("leg_a_exchange") is None and leg_a.get("exchange"):
                patch["leg_a_exchange"] = leg_a["exchange"]
            if row.get("leg_a_symbol") is None and leg_a.get("symbol"):
                patch["leg_a_symbol"] = leg_a["symbol"]
            if row.get("leg_a_kind") is None and leg_a.get("kind"):
                patch["leg_a_kind"] = leg_a["kind"]
            if row.get("leg_a_price") is None and leg_a.get("price") is not None:
                patch["leg_a_price"] = leg_a["price"]
            if row.get("leg_a_funding_rate") is None and leg_a.get("funding_rate") is not None:
                patch["leg_a_funding_rate"] = leg_a["funding_rate"]

            if row.get("leg_b_exchange") is None and leg_b.get("exchange"):
                patch["leg_b_exchange"] = leg_b["exchange"]
            if row.get("leg_b_symbol") is None and leg_b.get("symbol"):
                patch["leg_b_symbol"] = leg_b["symbol"]
            if row.get("leg_b_kind") is None and leg_b.get("kind"):
                patch["leg_b_kind"] = leg_b["kind"]
            if row.get("leg_b_price") is None and leg_b.get("price") is not None:
                patch["leg_b_price"] = leg_b["price"]
            if row.get("leg_b_funding_rate") is None and leg_b.get("funding_rate") is not None:
                patch["leg_b_funding_rate"] = leg_b["funding_rate"]

            if patch:
                updates.append((row["id"], row["ts"], patch))

            if len(updates) >= args.batch:
                patched += flush_updates(pg_conn, updates)
                logger.info("patched=%s scanned=%s skipped=%s", patched, scanned, skipped)
                updates.clear()

        if updates:
            patched += flush_updates(pg_conn, updates)

        logger.info("done. scanned=%s patched=%s skipped=%s", scanned, patched, skipped)


if __name__ == "__main__":
    main()

