"""
Bitget fill reliability diagnostic (live).

Purpose
- Validate whether avg_price / filled_qty we backfill from Bitget order detail is reliable.
- Compare:
  1) orderbook sweep price (expected market impact for given notional)
  2) actual filled avgPrice/filledQty from Bitget order detail endpoint

Usage
- python test_bitget_fill_reliability.py --symbol ETH --notional 20
"""

from __future__ import annotations

import argparse
import json
import time
from typing import Any, Dict, Optional, Tuple

from orderbook_utils import fetch_orderbook_prices
from trading.trade_executor import (
    TradeExecutionError,
    get_bitget_usdt_perp_order_detail,
    place_bitget_usdt_perp_market_order,
    derive_bitget_usdt_perp_size_from_usdt,
    set_bitget_usdt_perp_leverage,
)


def _float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def _extract_ids(resp: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    data = resp.get("data") if isinstance(resp, dict) else None
    if isinstance(data, dict):
        oid = data.get("orderId") or data.get("order_id")
        cl = data.get("clientOid") or data.get("client_oid")
        return (str(oid) if oid else None, str(cl) if cl else None)
    return (None, None)


def _detail_summary(detail: Dict[str, Any]) -> Dict[str, Any]:
    # Field names vary across Bitget versions; keep raw + a best-effort mapping.
    out: Dict[str, Any] = {"raw": detail}
    out["priceAvg"] = detail.get("priceAvg") or detail.get("avgPrice") or detail.get("avgPx")
    out["filledQty"] = (
        detail.get("filledQty")
        or detail.get("filledSize")
        or detail.get("baseVolume")
        or detail.get("size")
        or detail.get("baseQty")
    )
    out["quoteVolume"] = detail.get("quoteVolume") or detail.get("quoteVol")
    out["state"] = detail.get("state") or detail.get("status")
    try:
        avg = _float(out.get("priceAvg"))
        q = _float(out.get("filledQty"))
        qq = _float(out.get("quoteVolume"))
        if (avg is None or avg <= 0) and qq and q and q > 0:
            out["priceAvg_derived"] = qq / q
    except Exception:
        pass
    return out


def _calc_qty_for_notional(exchange: str, symbol: str, notional: float, side: str) -> Tuple[float, Dict[str, Any]]:
    ob = fetch_orderbook_prices(exchange, symbol, "perp", notional=float(notional)) or {}
    if ob.get("error"):
        raise TradeExecutionError(f"orderbook error: {ob.get('error')}")
    px = ob.get("buy") if side == "buy" else ob.get("sell")
    if not px:
        raise TradeExecutionError("orderbook missing buy/sell")
    qty = float(notional) / float(px)
    return qty, ob


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="ETH", help="Base symbol, e.g. ETH")
    ap.add_argument("--notional", type=float, default=20.0, help="USDT notional for open leg")
    ap.add_argument("--sleep", type=float, default=0.6, help="Sleep seconds between requests")
    args = ap.parse_args()

    symbol = str(args.symbol).upper().strip()
    notional = float(args.notional)
    sleep_s = float(args.sleep)

    print(f"[bitget] diagnostic symbol={symbol} notional={notional}")

    # Ensure leverage=1 for safety (best-effort; may no-op if already set).
    try:
        set_bitget_usdt_perp_leverage(symbol, leverage=1)
    except Exception as exc:
        print("[warn] set leverage failed:", exc)

    # 1) Compute expected buy sweep price and size.
    # Use Bitget's min size filter to avoid "below minimum" rejects.
    try:
        size_str = derive_bitget_usdt_perp_size_from_usdt(symbol, notional)
        qty_open = float(size_str)
    except Exception:
        qty_open, _ = _calc_qty_for_notional("bitget", symbol, notional, "buy")
    _, ob_buy = _calc_qty_for_notional("bitget", symbol, notional, "buy")
    print("[orderbook] buy_px_sweep=", ob_buy.get("buy"), "qty_req=", qty_open)

    client_open = f"diag-open-{symbol}-{int(time.time())}"
    open_resp = place_bitget_usdt_perp_market_order(
        symbol,
        side="buy",
        size=qty_open,
        reduce_only=False,
        trade_side="open",
        client_order_id=client_open,
    )
    oid_open, cl_open = _extract_ids(open_resp)
    print("[open] resp_ids=", {"orderId": oid_open, "clientOid": cl_open})
    time.sleep(sleep_s)

    detail_open = get_bitget_usdt_perp_order_detail(symbol, order_id=oid_open, client_order_id=cl_open)
    s_open = _detail_summary(detail_open)
    avg_open = _float(s_open.get("priceAvg"))
    fill_open = _float(s_open.get("filledQty"))
    quote_open = _float(s_open.get("quoteVolume"))
    print("[open] detail=", json.dumps({k: s_open.get(k) for k in ("priceAvg", "filledQty", "quoteVolume", "state")}, ensure_ascii=False))

    # Prefer filled size from detail if available; otherwise use requested.
    qty_close = float(fill_open) if fill_open and fill_open > 0 else float(qty_open)

    # 2) Compute expected sell sweep price for close qty.
    ob_sell = fetch_orderbook_prices("bitget", symbol, "perp", notional=float(notional)) or {}
    print("[orderbook] sell_px_sweep=", ob_sell.get("sell"), "qty_close=", qty_close)

    # Bitget hedge-mode close semantics can be non-intuitive; follow the same mapping as LiveTradingManager:
    # primary (buy, close, posSide=long) then fallback.
    client_close = f"diag-close-{symbol}-{int(time.time())}"
    try:
        close_resp = place_bitget_usdt_perp_market_order(
            symbol,
            side="buy",
            size=qty_close,
            reduce_only=True,
            trade_side="close",
            pos_side="long",
            client_order_id=client_close,
        )
    except Exception:
        close_resp = place_bitget_usdt_perp_market_order(
            symbol,
            side="sell",
            size=qty_close,
            reduce_only=True,
            trade_side="close",
            pos_side="long",
            client_order_id=client_close,
        )
    oid_close, cl_close = _extract_ids(close_resp)
    print("[close] resp_ids=", {"orderId": oid_close, "clientOid": cl_close})
    time.sleep(sleep_s)

    detail_close = get_bitget_usdt_perp_order_detail(symbol, order_id=oid_close, client_order_id=cl_close)
    s_close = _detail_summary(detail_close)
    avg_close = _float(s_close.get("priceAvg"))
    fill_close = _float(s_close.get("filledQty"))
    quote_close = _float(s_close.get("quoteVolume"))
    print("[close] detail=", json.dumps({k: s_close.get(k) for k in ("priceAvg", "filledQty", "quoteVolume", "state")}, ensure_ascii=False))

    def _pct_diff(a: Optional[float], b: Optional[float]) -> Optional[float]:
        if a is None or b is None or b == 0:
            return None
        return (a - b) / b

    exp_buy = _float(ob_buy.get("buy"))
    exp_sell = _float(ob_sell.get("sell"))
    print("[compare] avg_open vs ob_buy:", {"avg_open": avg_open, "ob_buy": exp_buy, "diff": _pct_diff(avg_open, exp_buy)})
    print("[compare] avg_close vs ob_sell:", {"avg_close": avg_close, "ob_sell": exp_sell, "diff": _pct_diff(avg_close, exp_sell)})

    # Sanity: derive notional from avg*filled when quoteVolume missing.
    if quote_open is None and avg_open and fill_open:
        quote_open = avg_open * fill_open
    if quote_close is None and avg_close and fill_close:
        quote_close = avg_close * fill_close

    print("[notional] open_quote=", quote_open, "close_quote=", quote_close)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
