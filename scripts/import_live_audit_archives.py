#!/usr/bin/env python3
"""Import local live trading audit artifacts into runtime/live_trading.sqlite3.

This is intentionally conservative: today it seeds the known archived audit via
local_live_store.seed_archive(). Future importers can parse additional report/log
files or exchange API exports and write into the same SQLite schema.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import local_live_store  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Import archived live trading audits into local SQLite store")
    parser.add_argument("--db", default=str(local_live_store.DEFAULT_DB_PATH), help="SQLite DB path")
    parser.add_argument("--json", action="store_true", help="Print machine-readable summary")
    args = parser.parse_args()

    db_path = local_live_store.initialize(args.db)
    summary = local_live_store.summary(args.db)
    payload = {
        "db_path": db_path,
        "summary": summary,
        "signals": local_live_store.list_signals(limit=20, db_path=args.db),
    }
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(f"local live store: {db_path}")
        print(f"signals: {summary.get('strict_live_trade_count')}")
        print(f"closed: {summary.get('closed_count')}")
        print(f"realized_pnl_usdt_sum: {summary.get('realized_pnl_usdt_sum')}")
        print(f"net_pnl_usdt_sum: {summary.get('net_pnl_usdt_sum')} (fee_complete={summary.get('fee_coverage_complete')})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

