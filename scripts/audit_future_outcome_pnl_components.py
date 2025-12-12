#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import psycopg

# Allow running from repo root; resolve `import config` and `import watchlist_outcome_worker`.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402
from watchlist_outcome_worker import FundingHistoryFetcher, compute_outcome  # noqa: E402
import sqlite3  # noqa: E402  (after sys.path tweak)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(text: str) -> datetime:
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _as_event_row(db_row: Dict[str, Any]) -> Dict[str, Any]:
    # Keys required by compute_outcome():
    keys = [
        "exchange",
        "symbol",
        "signal_type",
        "start_ts",
        "leg_a_exchange",
        "leg_a_symbol",
        "leg_a_kind",
        "leg_a_price_first",
        "leg_a_price_last",
        "leg_a_funding_rate_first",
        "leg_a_funding_rate_last",
        "leg_b_exchange",
        "leg_b_symbol",
        "leg_b_kind",
        "leg_b_price_first",
        "leg_b_price_last",
        "leg_b_funding_rate_first",
        "leg_b_funding_rate_last",
    ]
    return {k: db_row.get(k) for k in keys}


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit future_outcome PnL components by recomputing from SQLite+REST")
    parser.add_argument("--dsn", type=str, default=str(config.WATCHLIST_PG_CONFIG.get("dsn")))
    parser.add_argument("--sqlite-path", type=str, default=str(config.WATCHLIST_CONFIG.get("db_path", "market_data.db")))
    parser.add_argument("--days", type=float, default=3.0)
    parser.add_argument("--horizon-min", type=int, default=240)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--use-rest", action="store_true")
    parser.add_argument("--since", type=str, default="", help="ISO8601, overrides --days")
    parser.add_argument("--tolerance", type=float, default=1e-9)
    args = parser.parse_args()

    now = _utcnow()
    since = _parse_iso(args.since) if args.since else (now - timedelta(days=float(args.days)))
    horizon = int(args.horizon_min)

    sql = """
    SELECT
      o.event_id,
      o.horizon_min,
      o.pnl AS stored_pnl,
      o.spread_change AS stored_spread,
      o.funding_change AS stored_funding,
      o.funding_applied AS stored_funding_applied,
      o.label AS stored_label,
      e.exchange,
      e.symbol,
      e.signal_type,
      e.start_ts,
      e.leg_a_exchange, e.leg_a_symbol, e.leg_a_kind, e.leg_a_price_first, e.leg_a_price_last, e.leg_a_funding_rate_first, e.leg_a_funding_rate_last,
      e.leg_b_exchange, e.leg_b_symbol, e.leg_b_kind, e.leg_b_price_first, e.leg_b_price_last, e.leg_b_funding_rate_first, e.leg_b_funding_rate_last
    FROM watchlist.future_outcome o
    JOIN watchlist.watch_signal_event e ON e.id = o.event_id
    WHERE o.horizon_min = %(h)s
      AND e.start_ts >= %(since)s
      AND o.pnl IS NOT NULL
    ORDER BY random()
    LIMIT %(limit)s;
    """

    fetcher: Optional[FundingHistoryFetcher] = FundingHistoryFetcher() if args.use_rest else None
    mismatches = 0
    checked = 0
    with psycopg.connect(args.dsn) as conn_pg, sqlite3.connect(args.sqlite_path) as conn_sqlite:
        with conn_pg.cursor() as cur:
            cur.execute(sql, {"h": horizon, "since": since, "limit": int(args.limit)})
            cols = [d.name for d in cur.description]
            for row in cur.fetchall():
                db_row = dict(zip(cols, row))
                event_row = _as_event_row(db_row)
                out = compute_outcome(conn_sqlite, event_row, horizon, rest_fetcher=fetcher)
                checked += 1
                if not out:
                    print(f"[skip] event_id={db_row['event_id']} (cannot recompute: missing prices)")
                    continue
                dp = float(out["pnl"]) - float(db_row["stored_pnl"])
                ds = float(out["spread_change"]) - float(db_row["stored_spread"])
                sf0 = db_row["stored_funding"]
                df = None
                if sf0 is None and out.get("funding_change") is None:
                    df = 0.0
                else:
                    df = float(out.get("funding_change") or 0.0) - float(sf0 or 0.0)
                ok = (abs(dp) <= args.tolerance) and (abs(ds) <= args.tolerance) and (abs(df) <= args.tolerance)
                if not ok:
                    mismatches += 1
                    print(
                        "[mismatch]"
                        f" event_id={db_row['event_id']}"
                        f" sig={db_row['signal_type']}"
                        f" horizon={horizon}"
                        f" dp={dp:.6g} ds={ds:.6g} df={df:.6g}"
                    )
                    stored_label = db_row.get("stored_label")
                    if stored_label:
                        try:
                            pos = (stored_label or {}).get("position_rule")
                        except Exception:
                            pos = None
                        if pos:
                            print(f"  stored.position_rule={pos}")
                    new_label = out.get("label") or {}
                    print(f"  new.position_rule={new_label.get('position_rule')}")
                    print(
                        "  stored(pnl/spread/funding)="
                        f"{db_row['stored_pnl']:.6g}/{db_row['stored_spread']:.6g}/{(db_row['stored_funding'] if db_row['stored_funding'] is not None else 'NULL')}"
                    )
                    print(
                        "  new(pnl/spread/funding)="
                        f"{float(out['pnl']):.6g}/{float(out['spread_change']):.6g}/{(float(out['funding_change']) if out.get('funding_change') is not None else 'NULL')}"
                    )
                else:
                    pass

    print(f"checked={checked} mismatches={mismatches} since={since.isoformat()} horizon_min={horizon} use_rest={bool(args.use_rest)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

