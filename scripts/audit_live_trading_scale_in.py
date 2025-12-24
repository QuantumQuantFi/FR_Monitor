#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore[assignment]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_int(val: Any) -> Optional[int]:
    try:
        return int(val)
    except Exception:
        return None


def _as_float(val: Any) -> Optional[float]:
    try:
        return float(val)
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit live trading scale-in state for open trades.")
    ap.add_argument("--limit", type=int, default=50, help="Max rows to show (default: 50).")
    ap.add_argument("--status", type=str, default="open", help="Filter status (default: open).")
    args = ap.parse_args()

    if psycopg is None:
        raise SystemExit("psycopg not available in this environment")

    import os
    import sys

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    import config  # noqa: E402

    dsn = str((getattr(config, "WATCHLIST_PG_CONFIG", {}) or {}).get("dsn") or "")
    if not dsn:
        raise SystemExit("Missing WATCHLIST_PG_CONFIG.dsn")

    status = str(args.status or "").strip().lower()
    limit_n = max(1, int(args.limit or 50))

    with psycopg.connect(dsn, row_factory=psycopg.rows.dict_row) as conn:
        rows = conn.execute(
            """
            SELECT
              id, symbol, status, created_at, opened_at, force_close_at,
              leg_long_exchange, leg_short_exchange,
              entry_spread_metric, entry_spread_pct_actual,
              take_profit_pnl, take_profit_exit_spread_pct,
              last_exit_spread_pct, last_pnl_spread,
              payload
            FROM watchlist.live_trade_signal
            WHERE status=%s
            ORDER BY COALESCE(opened_at, created_at) DESC
            LIMIT %s;
            """,
            (status, limit_n),
        ).fetchall()

    now = _utcnow().isoformat(timespec="seconds")
    print(f"[{now}] live_trade_signal status={status} limit={limit_n}")
    if not rows:
        print("(no rows)")
        return

    for r in rows:
        payload = r.get("payload") if isinstance(r.get("payload"), dict) else {}
        scale_in = payload.get("scale_in") if isinstance(payload, dict) and isinstance(payload.get("scale_in"), dict) else {}
        entries = scale_in.get("entries") if isinstance(scale_in.get("entries"), list) else []

        entries_count = _as_int(scale_in.get("entries_count")) or (len([e for e in entries if isinstance(e, dict)]) if entries else 0)
        next_trigger = _as_float(scale_in.get("next_trigger_spread_pct"))
        entry_first = _as_float(scale_in.get("entry_spread_pct_first"))
        last_scale = scale_in.get("last_scale_in_at")

        print(
            "id={id} sym={sym} pair={lx}/{sx} opened={opened} force={force} "
            "last_exit_spread={les} last_pnl_spread={lps} tp_pnl={tp} "
            "scale_in={cnt} next={nxt} first={fst} last_scale={ls}".format(
                id=int(r.get("id") or 0),
                sym=str(r.get("symbol") or ""),
                lx=str(r.get("leg_long_exchange") or ""),
                sx=str(r.get("leg_short_exchange") or ""),
                opened=str(r.get("opened_at") or r.get("created_at") or ""),
                force=str(r.get("force_close_at") or ""),
                les=str(r.get("last_exit_spread_pct") or ""),
                lps=str(r.get("last_pnl_spread") or ""),
                tp=str(r.get("take_profit_pnl") or ""),
                cnt=entries_count,
                nxt=f"{next_trigger:.6f}" if next_trigger is not None else "",
                fst=f"{entry_first:.6f}" if entry_first is not None else "",
                ls=str(last_scale or ""),
            )
        )


if __name__ == "__main__":
    main()

