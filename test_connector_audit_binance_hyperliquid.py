#!/usr/bin/env python3
"""
Binance + Hyperliquid connector audit (public/private/trade smoke tests).

Usage
  Public-only (safe):
    ./venv/bin/python test_connector_audit_binance_hyperliquid.py --symbol ETH --notional 20

  Include private account endpoints (still no trades):
    ./venv/bin/python test_connector_audit_binance_hyperliquid.py --private

  Execute a tiny live trade (opens then closes immediately; requires credentials):
    ./venv/bin/python test_connector_audit_binance_hyperliquid.py --private --trade --confirm-live-trade

Notes
  - This script is intentionally verbose and records raw response snippets to help debugging.
  - It will NOT place orders unless both --trade and --confirm-live-trade are provided.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from orderbook_utils import fetch_orderbook_prices
from rest_collectors import fetch_binance, fetch_hyperliquid, get_hyperliquid_funding_map
from trading.trade_executor import (
    TradeExecutionError,
    get_binance_funding_fee_income,
    get_binance_perp_account,
    get_binance_perp_order,
    get_binance_perp_positions,
    get_binance_perp_price,
    get_binance_perp_usdt_balance,
    get_hyperliquid_balance_summary,
    get_hyperliquid_perp_positions,
    get_hyperliquid_user_funding_history,
    place_binance_perp_market_order,
    place_hyperliquid_perp_market_order,
    set_binance_perp_leverage,
)


PUBLIC_ENDPOINTS = {
    "binance": {
        "ticker_bulk": "GET https://fapi.binance.com/fapi/v1/ticker/24hr (via rest_collectors.fetch_binance)",
        "funding_info": "GET https://fapi.binance.com/fapi/v1/fundingInfo (interval hint cache)",
        "depth": "GET https://fapi.binance.com/fapi/v1/depth (via orderbook_utils)",
        "price": "GET https://fapi.binance.com/fapi/v1/ticker/price (via trading.trade_executor.get_binance_perp_price)",
    },
    "hyperliquid": {
        "meta_and_ctxs": "POST https://api.hyperliquid.xyz/info {type=metaAndAssetCtxs} (via rest_collectors.fetch_hyperliquid)",
        "l2_book": "POST https://api.hyperliquid.xyz/info {type=l2Book, coin=...} (via orderbook_utils)",
        "funding_map": "POST https://api.hyperliquid.xyz/info {type=metaAndAssetCtxs} (cached via get_hyperliquid_funding_map)",
    },
}

PRIVATE_ENDPOINTS = {
    "binance": {
        "account": "GET /fapi/v2/account (via get_binance_perp_account)",
        "positions": "GET /fapi/v2/positionRisk (via get_binance_perp_positions)",
        "balance_usdt": "GET /fapi/v2/account (via get_binance_perp_usdt_balance)",
        "order_query": "GET /fapi/v1/order (via get_binance_perp_order)",
        "income": "GET /fapi/v1/income (via get_binance_funding_fee_income)",
        "set_leverage": "POST /fapi/v1/leverage (via set_binance_perp_leverage)",
        "place_market": "POST /fapi/v1/order type=MARKET (via place_binance_perp_market_order)",
    },
    "hyperliquid": {
        "user_state": "SDK Info.user_state (HTTP /info under the hood)",
        "positions": "SDK Info.user_state (via get_hyperliquid_perp_positions)",
        "balance": "SDK Info.user_state (via get_hyperliquid_balance_summary)",
        "funding_history": "SDK Info.user_funding_history (via get_hyperliquid_user_funding_history)",
        "place_market": "SDK Exchange.market_open / Exchange.order reduceOnly (via place_hyperliquid_perp_market_order)",
    },
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _truncate(value: Any, limit: int = 2000) -> Any:
    try:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        text = str(value)
    if len(text) <= limit:
        return value
    return (text[:limit] + "...<truncated>")


@dataclass
class CheckResult:
    name: str
    ok: bool
    seconds: float
    result: Optional[Any] = None
    error: Optional[str] = None
    traceback: Optional[str] = None


def _run_check(name: str, fn: Callable[[], Any]) -> CheckResult:
    started = time.time()
    try:
        out = fn()
        return CheckResult(name=name, ok=True, seconds=time.time() - started, result=_truncate(out))
    except Exception as exc:
        return CheckResult(
            name=name,
            ok=False,
            seconds=time.time() - started,
            error=str(exc),
            traceback=traceback.format_exc(limit=8),
        )


def _assert_has_key(mapping: Any, key: str) -> None:
    if not isinstance(mapping, dict) or key not in mapping:
        raise AssertionError(f"missing key: {key}")


def _public_binance_snapshot(symbol: str) -> Dict[str, Any]:
    data = fetch_binance()
    _assert_has_key(data, symbol.upper())
    snap = data[symbol.upper()].get("futures") or {}
    if not snap:
        raise AssertionError("binance snapshot missing futures payload")
    return {"symbol": symbol.upper(), "futures": snap}


def _public_hyperliquid_snapshot(symbol: str) -> Dict[str, Any]:
    data = fetch_hyperliquid()
    _assert_has_key(data, symbol.upper())
    snap = data[symbol.upper()].get("futures") or {}
    if not snap:
        raise AssertionError("hyperliquid snapshot missing futures payload")
    return {"symbol": symbol.upper(), "futures": snap}


def _public_orderbook(exchange: str, symbol: str, notional: float) -> Dict[str, Any]:
    ob = fetch_orderbook_prices(exchange, symbol, "perp", notional=float(notional)) or {}
    if ob.get("error"):
        raise AssertionError(f"{exchange} orderbook error={ob.get('error')} meta={ob.get('meta')}")
    return ob


def _binance_is_hedge_mode(account: Dict[str, Any]) -> bool:
    try:
        return bool(account.get("dualSidePosition"))
    except Exception:
        return False


def _binance_flat_positions(positions: Any, symbol: str) -> bool:
    pair = f"{symbol.upper()}USDT"
    if not isinstance(positions, list):
        return True
    for pos in positions:
        if not isinstance(pos, dict):
            continue
        if str(pos.get("symbol") or "").upper() != pair:
            continue
        try:
            amt = float(pos.get("positionAmt") or 0.0)
        except Exception:
            amt = 0.0
        if abs(amt) > 1e-12:
            return False
    return True


def _hyperliquid_flat_positions(positions: Any, symbol: str) -> bool:
    sym = symbol.upper()
    if not isinstance(positions, list):
        return True
    for item in positions:
        position = item.get("position") if isinstance(item, dict) else None
        if not isinstance(position, dict):
            continue
        if (position.get("coin") or "").upper() != sym:
            continue
        try:
            szi = float(position.get("szi") or 0.0)
        except Exception:
            szi = 0.0
        if abs(szi) > 1e-12:
            return False
    return True


def _trade_binance(symbol: str, notional: float) -> Dict[str, Any]:
    price = float(get_binance_perp_price(symbol))
    qty = float(notional) / price
    account = get_binance_perp_account()
    hedge_mode = _binance_is_hedge_mode(account)

    # Use 1x leverage to match live trading defaults.
    set_binance_perp_leverage(symbol, 1)

    client_base = f"audit-bn-{symbol}-{int(time.time())}"
    open_kwargs: Dict[str, Any] = {"client_order_id": client_base + "-O"}
    close_kwargs: Dict[str, Any] = {"client_order_id": client_base + "-C", "reduce_only": True}
    if hedge_mode:
        open_kwargs["position_side"] = "LONG"
        close_kwargs["position_side"] = "LONG"

    open_resp = place_binance_perp_market_order(symbol, "BUY", qty, **open_kwargs)
    order_id = open_resp.get("orderId")
    open_query = None
    if order_id is not None:
        open_query = get_binance_perp_order(symbol, order_id=order_id)

    # Small delay to avoid querying too quickly / allow position to register
    time.sleep(1.5)

    close_resp = place_binance_perp_market_order(symbol, "SELL", qty, **close_kwargs)
    close_order_id = close_resp.get("orderId")
    close_query = None
    if close_order_id is not None:
        close_query = get_binance_perp_order(symbol, order_id=close_order_id)

    # Verify positions are flat
    positions = get_binance_perp_positions(symbol=symbol)
    flat = _binance_flat_positions(positions, symbol)
    return {
        "price": price,
        "qty": qty,
        "hedge_mode": hedge_mode,
        "open": {"resp": open_resp, "order": open_query},
        "close": {"resp": close_resp, "order": close_query},
        "positions_flat": flat,
    }


def _trade_hyperliquid(symbol: str, notional: float) -> Dict[str, Any]:
    ob = fetch_orderbook_prices("hyperliquid", symbol, "perp", notional=float(notional)) or {}
    if ob.get("error") or not ob.get("buy"):
        raise TradeExecutionError(f"hyperliquid orderbook unavailable: {ob.get('error')}")
    size = float(notional) / float(ob["buy"])
    client_base = f"audit-hl-{symbol}-{int(time.time())}"
    open_resp = place_hyperliquid_perp_market_order(symbol, "long", size, client_order_id=client_base + "-O")
    time.sleep(2.0)
    close_resp = place_hyperliquid_perp_market_order(
        symbol, "sell", size, reduce_only=True, client_order_id=client_base + "-C"
    )
    positions = get_hyperliquid_perp_positions()
    flat = _hyperliquid_flat_positions(positions, symbol)
    return {"size": size, "open": open_resp, "close": close_resp, "positions_flat": flat}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="ETH")
    parser.add_argument("--notional", type=float, default=20.0)
    parser.add_argument(
        "--exchanges",
        default="binance,hyperliquid",
        help="comma-separated subset: binance,hyperliquid (default: both)",
    )
    parser.add_argument("--private", action="store_true", help="also test private endpoints (no trades)")
    parser.add_argument("--trade", action="store_true", help="place a tiny open+close market order on each venue")
    parser.add_argument(
        "--confirm-live-trade",
        action="store_true",
        help="required together with --trade; acts as a safety latch",
    )
    args = parser.parse_args()

    symbol = (args.symbol or "ETH").upper()
    notional = float(args.notional or 20.0)
    enabled = {x.strip().lower() for x in str(args.exchanges or "").split(",") if x.strip()}
    if not enabled:
        enabled = {"binance", "hyperliquid"}

    if args.trade and not args.confirm_live_trade:
        raise SystemExit("Refusing to trade: provide both --trade and --confirm-live-trade")

    results: Dict[str, Any] = {
        "ts": _utc_now_iso(),
        "symbol": symbol,
        "notional": notional,
        "public_endpoints": PUBLIC_ENDPOINTS,
        "private_endpoints": PRIVATE_ENDPOINTS if args.private else {},
        "checks": [],
    }

    checks = []

    # Public: snapshots and orderbooks (read-only)
    if "binance" in enabled:
        checks.append(_run_check("public.binance.snapshot", lambda: _public_binance_snapshot(symbol)))
        checks.append(_run_check("public.binance.orderbook", lambda: _public_orderbook("binance", symbol, notional)))
        checks.append(_run_check("public.binance.price", lambda: {"price": get_binance_perp_price(symbol)}))

    if "hyperliquid" in enabled:
        checks.append(_run_check("public.hyperliquid.snapshot", lambda: _public_hyperliquid_snapshot(symbol)))
        checks.append(
            _run_check("public.hyperliquid.orderbook", lambda: _public_orderbook("hyperliquid", symbol, notional))
        )
        checks.append(_run_check("public.hyperliquid.funding_map", lambda: get_hyperliquid_funding_map()))

    if args.private:
        # Binance private
        if "binance" in enabled:
            checks.append(_run_check("private.binance.balance_usdt", lambda: get_binance_perp_usdt_balance()))
            checks.append(_run_check("private.binance.positions", lambda: get_binance_perp_positions(symbol=symbol)))
            checks.append(_run_check("private.binance.account", lambda: get_binance_perp_account()))
            checks.append(
                _run_check(
                    "private.binance.income_24h",
                    lambda: get_binance_funding_fee_income(
                        start_time_ms=int((time.time() - 24 * 3600) * 1000), end_time_ms=int(time.time() * 1000)
                    ),
                )
            )

        # Hyperliquid private (SDK)
        if "hyperliquid" in enabled:
            checks.append(_run_check("private.hyperliquid.balance", lambda: get_hyperliquid_balance_summary()))
            checks.append(_run_check("private.hyperliquid.positions", lambda: get_hyperliquid_perp_positions()))
            checks.append(
                _run_check(
                    "private.hyperliquid.funding_history_24h",
                    lambda: get_hyperliquid_user_funding_history(
                        start_time_ms=int((time.time() - 24 * 3600) * 1000), end_time_ms=int(time.time() * 1000)
                    ),
                )
            )

    if args.trade:
        # Trades are ALWAYS opt-in; each venue opens+closes immediately.
        if "binance" in enabled:
            checks.append(_run_check("trade.binance.open_close", lambda: _trade_binance(symbol, notional)))
        if "hyperliquid" in enabled:
            checks.append(_run_check("trade.hyperliquid.open_close", lambda: _trade_hyperliquid(symbol, notional)))

    results["checks"] = [asdict(c) for c in checks]

    ok_count = sum(1 for c in checks if c.ok)
    print(f"[connector_audit] symbol={symbol} notional={notional} ok={ok_count}/{len(checks)} ts={results['ts']}")
    for c in checks:
        status = "OK" if c.ok else "FAIL"
        print(f"- {status:4s} {c.name} ({c.seconds:.2f}s)")
        if not c.ok:
            print(f"  error: {c.error}")

    os.makedirs("reports", exist_ok=True)
    out_path = os.path.join("reports", f"connector_audit_{symbol.lower()}_{int(time.time())}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("saved:", out_path)


if __name__ == "__main__":
    main()
