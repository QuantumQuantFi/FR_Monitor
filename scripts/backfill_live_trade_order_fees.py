#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg
from psycopg.rows import dict_row

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402
from trading.trade_executor import (  # noqa: E402
    get_bitget_usdt_perp_order_detail,
    get_bybit_execution_list,
    get_hyperliquid_user_fee_rates,
    get_hyperliquid_user_fills_by_time,
    get_okx_swap_order,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _dt_to_ms(dt: datetime) -> int:
    return int(dt.astimezone(timezone.utc).timestamp() * 1000)


def _float_or_none(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
    except Exception:
        return None
    if f != f or f in (float("inf"), float("-inf")):
        return None
    return f


def _okx_fee(symbol: str, ord_id: str, client_order_id: Optional[str]) -> Tuple[Optional[float], Optional[str]]:
    row = get_okx_swap_order(symbol, ord_id=ord_id, client_order_id=client_order_id)
    if not isinstance(row, dict):
        return None, None
    fee = _float_or_none(row.get("fee") or row.get("fillFee"))
    ccy = row.get("feeCcy") or row.get("fillFeeCcy") or "USDT"
    if fee is None:
        return None, None
    return abs(float(fee)), str(ccy).upper()


def _bybit_fee(
    symbol: str,
    order_id: str,
    *,
    start_ms: int,
    end_ms: int,
) -> Tuple[Optional[float], Optional[str]]:
    rows = get_bybit_execution_list(symbol=symbol, order_id=order_id, start_time_ms=start_ms, end_time_ms=end_ms, limit=200)
    total = 0.0
    seen = False
    ccy: Optional[str] = None
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        fee = _float_or_none(r.get("execFee") or r.get("fee") or r.get("commission"))
        if fee is None:
            continue
        total += abs(float(fee))
        seen = True
        ccy0 = r.get("feeCurrency") or r.get("feeCurrency") or r.get("feeTokenId") or r.get("execFeeCurrency")
        if ccy0 and ccy is None:
            ccy = str(ccy0)
    if not seen:
        return None, None
    return float(total), (str(ccy).upper() if ccy else "USDT")


def _hyperliquid_fee(symbol: str, oid: str, *, start_ms: int, end_ms: int) -> Tuple[Optional[float], Optional[str]]:
    fills = get_hyperliquid_user_fills_by_time(start_time_ms=start_ms, end_time_ms=end_ms, aggregate_by_time=True)
    matched: List[Dict[str, Any]] = []
    sym_u = str(symbol or "").upper()
    for f in fills or []:
        if not isinstance(f, dict):
            continue
        if str(f.get("oid") or "") != str(oid):
            continue
        if str(f.get("coin") or "").upper() != sym_u:
            continue
        matched.append(f)
    if not matched:
        return None, None
    rates = get_hyperliquid_user_fee_rates()
    cross_rate = _float_or_none(rates.get("user_cross_rate"))
    add_rate = _float_or_none(rates.get("user_add_rate"))
    if cross_rate is None and add_rate is None:
        return None, None

    total_fee = 0.0
    seen = False
    for f in matched:
        px = _float_or_none(f.get("px"))
        sz = _float_or_none(f.get("sz"))
        if px is None or sz is None or px <= 0 or sz <= 0:
            continue
        notional = float(px) * float(sz)
        crossed = bool(f.get("crossed"))
        rate = cross_rate if crossed else add_rate
        if rate is None:
            continue
        total_fee += abs(float(notional) * float(rate))
        seen = True
    if not seen:
        return None, None
    return float(total_fee), "USDC"


def _bitget_fee(symbol: str, ord_id: str, client_order_id: Optional[str]) -> Tuple[Optional[float], Optional[str]]:
    row = get_bitget_usdt_perp_order_detail(symbol, order_id=ord_id, client_order_id=client_order_id)
    if not isinstance(row, dict):
        return None, None
    fee = _float_or_none(row.get("fee"))
    ccy = row.get("feeCoin") or row.get("marginCoin") or "USDT"
    if fee is None:
        return None, None
    return abs(float(fee)), str(ccy).upper()


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill missing fee_usdt on watchlist.live_trade_order (best-effort)")
    parser.add_argument("--dsn", type=str, default=str(config.WATCHLIST_PG_CONFIG.get("dsn")))
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--since-hours", type=float, default=24 * 7)
    parser.add_argument("--exchange", type=str, default="", help="Filter: okx|bybit|hyperliquid|bitget")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    since = _utcnow() - timedelta(hours=float(args.since_hours))
    exch = str(args.exchange or "").lower().strip()

    sql = """
    SELECT
      o.id,
      o.signal_id,
      s.symbol,
      o.exchange,
      o.market_type,
      o.action,
      o.leg,
      o.created_at,
      o.filled_qty,
      o.avg_price,
      o.cum_quote,
      o.exchange_order_id,
      o.client_order_id
    FROM watchlist.live_trade_order o
    JOIN watchlist.live_trade_signal s ON s.id=o.signal_id
    WHERE o.fee_usdt IS NULL
      AND COALESCE(o.filled_qty, 0) > 0
      AND o.created_at >= %(since)s
    ORDER BY o.created_at DESC
    LIMIT %(limit)s;
    """

    updated = 0
    checked = 0
    with psycopg.connect(args.dsn, autocommit=True, row_factory=dict_row) as conn:
        rows = conn.execute(sql, {"since": since, "limit": int(args.limit)}).fetchall()
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            checked += 1
            ex = str(r.get("exchange") or "").lower().strip()
            if exch and ex != exch:
                continue
            sym = str(r.get("symbol") or "").upper()
            oid = str(r.get("exchange_order_id") or "").strip()
            clid = str(r.get("client_order_id") or "").strip() or None
            created_at = r.get("created_at")
            if not isinstance(created_at, datetime):
                continue
            start_ms = _dt_to_ms(created_at - timedelta(minutes=30))
            end_ms = _dt_to_ms(created_at + timedelta(minutes=30))

            fee_usdt: Optional[float] = None
            fee_ccy: Optional[str] = None
            try:
                if ex == "okx" and oid:
                    fee_usdt, fee_ccy = _okx_fee(sym, oid, clid)
                elif ex == "bybit" and oid:
                    fee_usdt, fee_ccy = _bybit_fee(sym, oid, start_ms=start_ms, end_ms=end_ms)
                elif ex == "hyperliquid" and oid:
                    fee_usdt, fee_ccy = _hyperliquid_fee(sym, oid, start_ms=start_ms, end_ms=end_ms)
                elif ex == "bitget" and oid:
                    fee_usdt, fee_ccy = _bitget_fee(sym, oid, clid)
            except Exception:
                fee_usdt, fee_ccy = None, None

            if fee_usdt is None:
                continue

            if not args.dry_run:
                conn.execute(
                    """
                    UPDATE watchlist.live_trade_order
                       SET fee_usdt=%s,
                           fee_currency=%s
                     WHERE id=%s AND fee_usdt IS NULL;
                    """,
                    (float(fee_usdt), str(fee_ccy) if fee_ccy else None, int(r["id"])),
                )
            updated += 1
            if updated % 20 == 0:
                time.sleep(0.15)

    print(f"checked={checked} updated={updated} since={since.isoformat()} dry_run={bool(args.dry_run)} exchange={exch or 'ALL'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
