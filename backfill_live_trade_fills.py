#!/usr/bin/env python3
"""
Backfill historical fill metrics for watchlist.live_trade_order.

Goal:
- Populate `filled_qty`, `avg_price`, `cum_quote`, `exchange_order_id` (best-effort)
  for historical open/close orders using:
    - stored `order_resp` JSON (preferred),
    - stored `client_order_id` (fallback),
    - exchange order-detail APIs (when needed).

Notes:
- Conservative rate limiting is applied per exchange to reduce ban risk.
- This script is intended for small-to-medium backfills. For very large datasets,
  consider running in batches (via --limit/--max-runtime-seconds).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    import psycopg
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"psycopg not installed in this environment: {exc}") from exc

from config import WATCHLIST_PG_CONFIG
from trading.live_trading_manager import LiveTradingConfig, LiveTradingManager


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _safe_json(val: Any) -> str:
    try:
        return json.dumps(val, ensure_ascii=False)
    except Exception:
        return str(val)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=200000, help="Max rows to process in this run.")
    parser.add_argument("--min-sleep-seconds", type=float, default=0.25, help="Per-exchange min spacing between API calls.")
    parser.add_argument("--max-runtime-seconds", type=float, default=0.0, help="Stop after this many seconds (0=unlimited).")
    parser.add_argument("--dry-run", action="store_true", help="Do not write updates to DB.")
    args = parser.parse_args()

    dsn = str(WATCHLIST_PG_CONFIG.get("dsn") or "")
    if not dsn:
        print("Missing WATCHLIST_PG_CONFIG.dsn", file=sys.stderr)
        return 2

    mgr = LiveTradingManager(LiveTradingConfig(enabled=False, dsn=dsn))

    last_call_at: Dict[str, float] = {}

    def rate_limit(exchange: str) -> None:
        ex = (exchange or "").lower()
        now = time.time()
        last = last_call_at.get(ex, 0.0)
        gap = float(args.min_sleep_seconds)
        if gap > 0 and (now - last) < gap:
            time.sleep(gap - (now - last))
        last_call_at[ex] = time.time()

    started = time.time()
    processed = 0
    updated = 0
    skipped = 0

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            """
            SELECT
              o.id,
              o.signal_id,
              o.action,
              o.exchange,
              o.client_order_id,
              o.exchange_order_id,
              o.order_resp,
              o.filled_qty,
              o.avg_price,
              o.cum_quote,
              s.symbol
            FROM watchlist.live_trade_order o
            JOIN watchlist.live_trade_signal s
              ON s.id = o.signal_id
            WHERE (o.filled_qty IS NULL OR o.avg_price IS NULL OR o.exchange_order_id IS NULL)
              AND o.action IN ('open','close')
            ORDER BY o.created_at ASC
            LIMIT %s;
            """,
            (int(args.limit),),
        ).fetchall()

        for row in rows:
            if args.max_runtime_seconds and (time.time() - started) >= float(args.max_runtime_seconds):
                break
            processed += 1

            order_row_id = int(row[0])
            signal_id = int(row[1])
            action = str(row[2] or "")
            exchange = str(row[3] or "")
            client_order_id = row[4]
            exchange_order_id = row[5]
            order_resp = row[6]
            symbol = str(row[10] or "")

            # Quick skip: no payload and no IDs.
            if not isinstance(order_resp, dict) and not client_order_id and not exchange_order_id:
                skipped += 1
                continue

            # The parser may call exchange order detail APIs; pace it.
            rate_limit(exchange)

            # Prefer stored order_resp; if absent, supply a minimal dict to enable ID lookups by client ID where possible.
            payload = order_resp if isinstance(order_resp, dict) else {}

            fill: Dict[str, Any] = {}
            try:
                fill = mgr._parse_fill_fields(exchange, symbol, payload)  # type: ignore[attr-defined]
            except Exception as exc:
                print(
                    f"[WARN] parse failed order_id={order_row_id} signal_id={signal_id} ex={exchange} action={action}: {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
                fill = {}

            # If still missing an exchange order id, try to derive it from known response shapes.
            if not fill.get("exchange_order_id") and exchange_order_id:
                fill["exchange_order_id"] = str(exchange_order_id)

            # Minimal update set.
            set_filled_qty = fill.get("filled_qty")
            set_avg_price = fill.get("avg_price")
            set_cum_quote = fill.get("cum_quote")
            set_ex_order_id = fill.get("exchange_order_id")
            set_status = fill.get("status")

            if set_filled_qty is None and set_avg_price is None and set_ex_order_id is None and set_cum_quote is None and set_status is None:
                skipped += 1
                continue

            if args.dry_run:
                updated += 1
                continue

            conn.execute(
                """
                UPDATE watchlist.live_trade_order
                   SET filled_qty = COALESCE(filled_qty, %s),
                       avg_price = COALESCE(avg_price, %s),
                       cum_quote = COALESCE(cum_quote, %s),
                       exchange_order_id = COALESCE(exchange_order_id, %s),
                       status = COALESCE(%s, status)
                 WHERE id = %s;
                """,
                (
                    float(set_filled_qty) if set_filled_qty is not None else None,
                    float(set_avg_price) if set_avg_price is not None else None,
                    float(set_cum_quote) if set_cum_quote is not None else None,
                    str(set_ex_order_id) if set_ex_order_id is not None else None,
                    str(set_status) if set_status is not None else None,
                    order_row_id,
                ),
            )
            updated += 1

            if processed % 25 == 0:
                elapsed = time.time() - started
                print(
                    _safe_json(
                        {
                            "ts": _utcnow().isoformat(),
                            "processed": processed,
                            "updated": updated,
                            "skipped": skipped,
                            "elapsed_sec": round(elapsed, 3),
                        }
                    )
                )

    elapsed = time.time() - started
    print(
        _safe_json(
            {
                "ts": _utcnow().isoformat(),
                "processed": processed,
                "updated": updated,
                "skipped": skipped,
                "elapsed_sec": round(elapsed, 3),
                "dry_run": bool(args.dry_run),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

