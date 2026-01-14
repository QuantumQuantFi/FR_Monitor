#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import psycopg
from psycopg.rows import dict_row

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit live trading fee/funding/realized PnL coverage in PG")
    parser.add_argument("--dsn", type=str, default=str(config.WATCHLIST_PG_CONFIG.get("dsn")))
    parser.add_argument("--limit", type=int, default=20, help="Show up to N sample problematic signals")
    args = parser.parse_args()

    with psycopg.connect(args.dsn, autocommit=True, row_factory=dict_row) as conn:
        now = datetime.now(timezone.utc)
        print(f"now_utc={now.isoformat()}")

        row = conn.execute(
            """
            SELECT
              COUNT(*) FILTER (WHERE status='closed') AS n_closed,
              COUNT(*) FILTER (WHERE status='closed' AND funding_pnl_usdt IS NULL) AS n_closed_funding_null
            FROM watchlist.live_trade_signal;
            """
        ).fetchone()
        print("signals", dict(row))

        row = conn.execute(
            """
            SELECT
              COUNT(*) AS n_orders,
              COUNT(*) FILTER (WHERE COALESCE(filled_qty,0) > 0) AS n_filled_orders,
              COUNT(*) FILTER (WHERE COALESCE(filled_qty,0) > 0 AND fee_usdt IS NULL) AS n_filled_fee_null
            FROM watchlist.live_trade_order;
            """
        ).fetchone()
        print("orders", dict(row))

        rows = conn.execute(
            """
            SELECT exchange,
                   COUNT(*) FILTER (WHERE COALESCE(filled_qty,0) > 0) AS n_filled_orders,
                   COUNT(*) FILTER (WHERE COALESCE(filled_qty,0) > 0 AND fee_usdt IS NULL) AS n_filled_fee_null
              FROM watchlist.live_trade_order
             GROUP BY exchange
             ORDER BY n_filled_fee_null DESC, n_filled_orders DESC;
            """
        ).fetchall()
        print("fee_null_by_exchange")
        for r in rows:
            if not isinstance(r, dict):
                continue
            print(r["exchange"], "filled", int(r["n_filled_orders"] or 0), "fee_null", int(r["n_filled_fee_null"] or 0))

        # Closed signals: can we compute NetPnL strictly (needs realized + funding + full fee coverage)?
        row = conn.execute(
            """
            WITH ord_leg AS (
              SELECT
                signal_id,
                action,
                leg,
                SUM(COALESCE(filled_qty,0)) AS filled_sum,
                SUM(COALESCE(cum_quote, CASE WHEN avg_price IS NOT NULL AND filled_qty IS NOT NULL THEN avg_price*filled_qty ELSE 0 END, 0)) AS quote_sum
              FROM watchlist.live_trade_order
              GROUP BY signal_id, action, leg
            ),
            ord AS (
              SELECT
                signal_id,
                SUM(filled_sum) FILTER (WHERE action='open' AND leg='long')  AS olq,
                (SUM(quote_sum) FILTER (WHERE action='open' AND leg='long')  / NULLIF(SUM(filled_sum) FILTER (WHERE action='open' AND leg='long'), 0))  AS olp,
                SUM(filled_sum) FILTER (WHERE action='close' AND leg='long') AS clq,
                (SUM(quote_sum) FILTER (WHERE action='close' AND leg='long') / NULLIF(SUM(filled_sum) FILTER (WHERE action='close' AND leg='long'), 0)) AS clp,
                SUM(filled_sum) FILTER (WHERE action='open' AND leg='short')  AS osq,
                (SUM(quote_sum) FILTER (WHERE action='open' AND leg='short')  / NULLIF(SUM(filled_sum) FILTER (WHERE action='open' AND leg='short'), 0))  AS osp,
                SUM(filled_sum) FILTER (WHERE action='close' AND leg='short') AS csq,
                (SUM(quote_sum) FILTER (WHERE action='close' AND leg='short') / NULLIF(SUM(filled_sum) FILTER (WHERE action='close' AND leg='short'), 0)) AS csp
              FROM ord_leg
              GROUP BY signal_id
            ),
            fee AS (
              SELECT
                signal_id,
                COUNT(*) FILTER (WHERE COALESCE(filled_qty,0) > 0) AS fee_orders,
                COUNT(*) FILTER (WHERE COALESCE(filled_qty,0) > 0 AND fee_usdt IS NOT NULL) AS fee_orders_with_fee
              FROM watchlist.live_trade_order
              GROUP BY signal_id
            )
            SELECT
              COUNT(*) AS n_closed,
              COUNT(*) FILTER (
                WHERE s.funding_pnl_usdt IS NOT NULL
                  AND fee.fee_orders = fee.fee_orders_with_fee
                  AND ord.olq>0 AND ord.clq>0 AND ord.osq>0 AND ord.csq>0
                  AND ord.olp IS NOT NULL AND ord.clp IS NOT NULL AND ord.osp IS NOT NULL AND ord.csp IS NOT NULL
              ) AS n_closed_net_strict
            FROM watchlist.live_trade_signal s
            LEFT JOIN ord ON ord.signal_id=s.id
            LEFT JOIN fee ON fee.signal_id=s.id
            WHERE s.status='closed';
            """
        ).fetchone()
        print("closed_net_strict", dict(row))

        # Samples: fee incomplete on closed signals.
        rows = conn.execute(
            """
            WITH fee AS (
              SELECT
                signal_id,
                COUNT(*) FILTER (WHERE COALESCE(filled_qty,0) > 0) AS fee_orders,
                COUNT(*) FILTER (WHERE COALESCE(filled_qty,0) > 0 AND fee_usdt IS NOT NULL) AS fee_orders_with_fee
              FROM watchlist.live_trade_order
              GROUP BY signal_id
            )
            SELECT
              s.id,
              s.symbol,
              s.leg_long_exchange,
              s.leg_short_exchange,
              s.opened_at,
              s.closed_at,
              fee.fee_orders,
              fee.fee_orders_with_fee
            FROM watchlist.live_trade_signal s
            JOIN fee ON fee.signal_id=s.id
            WHERE s.status='closed'
              AND fee.fee_orders > fee.fee_orders_with_fee
            ORDER BY s.closed_at DESC
            LIMIT %s;
            """,
            (int(args.limit),),
        ).fetchall()
        if rows:
            print("samples_fee_incomplete")
            for r in rows:
                if not isinstance(r, dict):
                    continue
                print(
                    r["id"],
                    r["symbol"],
                    f"{r['leg_long_exchange']}/{r['leg_short_exchange']}",
                    "fee",
                    f"{int(r['fee_orders_with_fee'] or 0)}/{int(r['fee_orders'] or 0)}",
                    "closed_at",
                    r.get("closed_at"),
                )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

