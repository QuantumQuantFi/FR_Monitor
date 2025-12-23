#!/usr/bin/env python3
"""
Backfill watch_signal_event.features_agg.meta_last.factors_v2 for recent events.

Default behavior:
  - Only fills missing factors_v2 (no overwrite).
  - Only Type B/C.
  - Filters to last N days.

Usage:
  venv/bin/python scripts/backfill_watchlist_series_factors.py --days 30 --batch-size 50
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import watchlist_series_factors as wsf  # noqa: E402


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_dt(val: Any) -> Optional[datetime]:
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    if isinstance(val, str):
        try:
            ts = datetime.fromisoformat(val)
            return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def _load_cfg(args: argparse.Namespace) -> Dict[str, Any]:
    try:
        import config as _cfg  # local import

        return {
            "dsn": args.pg_dsn or _cfg.WATCHLIST_PG_CONFIG.get("dsn"),
            "sqlite_path": args.sqlite_path or _cfg.WATCHLIST_CONFIG.get("db_path", "market_data.db"),
            "metrics_cfg": _cfg.WATCHLIST_METRICS_CONFIG if hasattr(_cfg, "WATCHLIST_METRICS_CONFIG") else {},
        }
    except Exception:
        return {
            "dsn": args.pg_dsn,
            "sqlite_path": args.sqlite_path or "market_data.db",
            "metrics_cfg": {},
        }


def _normalize_features(features_agg: Any) -> Dict[str, Any]:
    if isinstance(features_agg, dict):
        return features_agg
    if isinstance(features_agg, str):
        try:
            return json.loads(features_agg)
        except Exception:
            return {}
    return {}


def _build_leg(exchange: Any, symbol: Any, kind: Any, price_first: Any, price_last: Any, interval_h: Any, next_ft: Any) -> Dict[str, Any]:
    return {
        "exchange": exchange,
        "symbol": symbol,
        "kind": kind or "perp",
        "price": price_first if price_first is not None else price_last,
        "funding_interval_hours": interval_h,
        "next_funding_time": next_ft,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill watchlist factors_v2 for recent events.")
    parser.add_argument("--pg-dsn", default=None, help="Postgres DSN; defaults to config WATCHLIST_PG_CONFIG.dsn")
    parser.add_argument("--sqlite-path", default=None, help="SQLite DB path; defaults to config WATCHLIST_CONFIG.db_path")
    parser.add_argument("--days", type=int, default=30, help="Backfill window in days")
    parser.add_argument("--batch-size", type=int, default=50, help="Rows per batch")
    parser.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between batches")
    parser.add_argument("--max-events", type=int, default=0, help="Optional max rows to process (0 = no limit)")
    parser.add_argument("--start-id", type=int, default=0, help="Start from event id > start-id")
    parser.add_argument("--dry-run", action="store_true", help="Compute but do not write updates")
    parser.add_argument("--cache-entries", type=int, default=64, help="Max in-process series cache entries")
    args = parser.parse_args()

    cfg = _load_cfg(args)
    dsn = cfg.get("dsn")
    sqlite_path = cfg.get("sqlite_path")
    metrics_cfg = cfg.get("metrics_cfg") or {}

    if not dsn:
        raise SystemExit("Missing Postgres DSN (use --pg-dsn or set WATCHLIST_PG_DSN).")

    lookback_min = int(metrics_cfg.get("series_lookback_min", 240))
    tol_sec = int(metrics_cfg.get("series_asof_tol_sec", 90))
    ffill_bars = int(metrics_cfg.get("series_forward_fill_bars", 2))
    min_valid_ratio = float(metrics_cfg.get("series_min_valid_ratio", 0.8))
    max_backfill_bars = int(metrics_cfg.get("series_max_backfill_bars", ffill_bars))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logger = logging.getLogger("backfill_factors_v2")

    since_ts = _utcnow() - timedelta(days=args.days)
    logger.info("Backfill start: since=%s days=%s batch=%s sqlite=%s", since_ts.isoformat(), args.days, args.batch_size, sqlite_path)

    processed = 0
    updated = 0
    last_id = args.start_id

    series_cache = wsf.SeriesCache(max_entries=max(0, args.cache_entries))

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.autocommit = False
        while True:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, start_ts, end_ts,
                           leg_a_exchange, leg_a_symbol, leg_a_kind, leg_a_price_first, leg_a_price_last,
                           leg_a_funding_interval_hours, leg_a_next_funding_time,
                           leg_b_exchange, leg_b_symbol, leg_b_kind, leg_b_price_first, leg_b_price_last,
                           leg_b_funding_interval_hours, leg_b_next_funding_time,
                           features_agg
                    FROM watchlist.watch_signal_event
                    WHERE id > %s
                      AND start_ts >= %s
                      AND signal_type IN ('B','C')
                      AND (features_agg->'meta_last'->'factors_v2') IS NULL
                    ORDER BY id ASC
                    LIMIT %s;
                    """,
                    (last_id, since_ts, args.batch_size),
                )
                rows = cur.fetchall()

            if not rows:
                logger.info("no more rows; processed=%s updated=%s last_id=%s", processed, updated, last_id)
                break

            for row in rows:
                last_id = int(row["id"])
                processed += 1
                if args.max_events and processed > args.max_events:
                    break

                features_agg = _normalize_features(row.get("features_agg"))
                meta_last = features_agg.get("meta_last") if isinstance(features_agg.get("meta_last"), dict) else {}
                if meta_last.get("factors_v2") is not None:
                    continue

                event_ts = _as_dt(row.get("start_ts")) or _as_dt(row.get("end_ts")) or _utcnow()

                leg_a = _build_leg(
                    row.get("leg_a_exchange"),
                    row.get("leg_a_symbol"),
                    row.get("leg_a_kind"),
                    row.get("leg_a_price_first"),
                    row.get("leg_a_price_last"),
                    row.get("leg_a_funding_interval_hours"),
                    row.get("leg_a_next_funding_time"),
                )
                leg_b = _build_leg(
                    row.get("leg_b_exchange"),
                    row.get("leg_b_symbol"),
                    row.get("leg_b_kind"),
                    row.get("leg_b_price_first"),
                    row.get("leg_b_price_last"),
                    row.get("leg_b_funding_interval_hours"),
                    row.get("leg_b_next_funding_time"),
                )

                if not (leg_a.get("exchange") and leg_a.get("symbol") and leg_b.get("exchange") and leg_b.get("symbol")):
                    meta_last["factors_v2_meta"] = {
                        "ok": False,
                        "reason": "missing_leg_identity",
                        "ts": _utcnow().isoformat(),
                    }
                    features_agg["meta_last"] = meta_last
                    if not args.dry_run:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE watchlist.watch_signal_event SET features_agg=%s WHERE id=%s",
                                (Jsonb(features_agg), row["id"]),
                            )
                        conn.commit()
                    continue

                price_a = leg_a.get("price")
                price_b = leg_b.get("price")
                if price_a is None or price_b is None:
                    trigger = meta_last.get("trigger_details") or {}
                    if isinstance(trigger, dict):
                        prices = trigger.get("prices") or {}
                        if isinstance(prices, dict):
                            price_a = price_a if price_a is not None else prices.get(leg_a.get("exchange"))
                            price_b = price_b if price_b is not None else prices.get(leg_b.get("exchange"))

                try:
                    price_a_f = float(price_a) if price_a is not None else None
                    price_b_f = float(price_b) if price_b is not None else None
                except Exception:
                    price_a_f = None
                    price_b_f = None

                if price_a_f is None or price_b_f is None:
                    meta_last["factors_v2_meta"] = {
                        "ok": False,
                        "reason": "missing_leg_price",
                        "ts": _utcnow().isoformat(),
                    }
                    features_agg["meta_last"] = meta_last
                    if not args.dry_run:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE watchlist.watch_signal_event SET features_agg=%s WHERE id=%s",
                                (Jsonb(features_agg), row["id"]),
                            )
                        conn.commit()
                    continue

                if price_a_f == price_b_f:
                    meta_last["factors_v2_meta"] = {
                        "ok": False,
                        "reason": "equal_leg_price",
                        "ts": _utcnow().isoformat(),
                    }
                    features_agg["meta_last"] = meta_last
                    if not args.dry_run:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE watchlist.watch_signal_event SET features_agg=%s WHERE id=%s",
                                (Jsonb(features_agg), row["id"]),
                            )
                        conn.commit()
                    continue

                short_leg, long_leg = (leg_a, leg_b) if price_a_f > price_b_f else (leg_b, leg_a)

                try:
                    factors, meta = wsf.compute_event_series_factors(
                        sqlite_path,
                        event_ts,
                        short_leg,
                        long_leg,
                        lookback_min=lookback_min,
                        tol_sec=tol_sec,
                        ffill_bars=ffill_bars,
                        min_valid_ratio=min_valid_ratio,
                        max_backfill_bars=max_backfill_bars,
                        series_cache=series_cache if args.cache_entries > 0 else None,
                    )
                except Exception as exc:
                    meta_last["factors_v2_meta"] = {
                        "ok": False,
                        "reason": "exception",
                        "ts": _utcnow().isoformat(),
                        "exception": f"{type(exc).__name__}: {exc}",
                    }
                    features_agg["meta_last"] = meta_last
                    if not args.dry_run:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE watchlist.watch_signal_event SET features_agg=%s WHERE id=%s",
                                (Jsonb(features_agg), row["id"]),
                            )
                        conn.commit()
                    continue

                meta_last["factors_v2"] = factors
                meta_last["factors_v2_meta"] = {
                    "ok": True,
                    "ts": _utcnow().isoformat(),
                    **(meta or {}),
                }
                features_agg["meta_last"] = meta_last
                if not args.dry_run:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE watchlist.watch_signal_event SET features_agg=%s WHERE id=%s",
                            (Jsonb(features_agg), row["id"]),
                        )
                    conn.commit()
                updated += 1

            if args.max_events and processed >= args.max_events:
                break

            logger.info("batch done: processed=%s updated=%s last_id=%s", processed, updated, last_id)
            if args.sleep:
                conn.commit()
                time.sleep(args.sleep)

    logger.info("Backfill done: processed=%s updated=%s", processed, updated)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
