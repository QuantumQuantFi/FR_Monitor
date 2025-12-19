#!/usr/bin/env python3
"""
Lighter connector audit (public/private/trade smoke tests).

Usage
  Public-only:
    ./venv/bin/python test_connector_audit_lighter.py --symbol ETH --notional 20

  Private (no trades):
    ./venv/bin/python test_connector_audit_lighter.py --private

  Live trade (opens then closes immediately):
    ./venv/bin/python test_connector_audit_lighter.py --private --trade --confirm-live-trade --symbol ETH --notional 20

Notes
  - Lighter uses USDC collateral; `--notional` is treated as ~USDC notional.
  - This script will NOT place orders unless both --trade and --confirm-live-trade are provided.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import traceback
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from orderbook_utils import fetch_orderbook_prices
from trading.trade_executor import (
    TradeExecutionError,
    get_lighter_balance_summary,
    get_lighter_funding_rates_map,
    get_lighter_market_meta,
    get_lighter_orderbook_orders,
    get_lighter_recent_trades,
    place_lighter_perp_market_order,
)

try:
    from lighter.signer_client import SignerClient  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    SignerClient = None  # type: ignore[assignment]


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


def _market_id(symbol: str) -> int:
    meta = get_lighter_market_meta(symbol)
    mid = meta.get("market_id")
    if mid is None:
        raise TradeExecutionError(f"market_id missing for {symbol}: {meta}")
    return int(mid)


def _check_client() -> Dict[str, Any]:
    if SignerClient is None:
        raise TradeExecutionError("lighter SDK not installed")
    import config

    cp = getattr(config, "config_private", None)
    if cp is None:
        raise TradeExecutionError("config_private.py missing")
    base_url = "https://mainnet.zklighter.elliot.ai"

    import asyncio

    async def _run():
        client = SignerClient(
            base_url,
            getattr(cp, "LIGHTER_PRIVATE_KEY"),
            int(getattr(cp, "LIGHTER_KEY_INDEX")),
            int(getattr(cp, "LIGHTER_ACCOUNT_INDEX")),
        )
        try:
            err = client.check_client()
            return {"ok": err is None, "error": err}
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                await close()

    return asyncio.run(_run())


def _trade_open_close(symbol: str, notional: float) -> Dict[str, Any]:
    ob = fetch_orderbook_prices("lighter", symbol, "perp", notional=float(notional)) or {}
    if ob.get("error"):
        raise TradeExecutionError(f"orderbook error: {ob.get('error')} meta={ob.get('meta')}")
    px_buy = float(ob["buy"])
    size = float(notional) / px_buy
    client_base = f"audit-lighter-{symbol}-{int(time.time())}"

    open_resp = place_lighter_perp_market_order(symbol, "long", size, client_order_id=client_base + "1")
    time.sleep(2.0)
    close_resp = place_lighter_perp_market_order(
        symbol, "sell", size, reduce_only=True, client_order_id=client_base + "2"
    )

    bal = get_lighter_balance_summary()
    raw = bal.get("raw_account") or {}
    positions = raw.get("positions") or []
    remaining = []
    for pos in positions:
        if not isinstance(pos, dict):
            continue
        if str(pos.get("symbol") or "").upper() != symbol.upper():
            continue
        try:
            qty = float(pos.get("position") or 0.0)
        except Exception:
            qty = 0.0
        if abs(qty) > 1e-10:
            remaining.append(pos)

    return {
        "notional": notional,
        "size": size,
        "open": open_resp,
        "close": close_resp,
        "positions_remaining": remaining,
    }


def main() -> None:
    logging.getLogger().setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="ETH")
    parser.add_argument("--notional", type=float, default=20.0)
    parser.add_argument("--private", action="store_true")
    parser.add_argument("--trade", action="store_true")
    parser.add_argument("--confirm-live-trade", action="store_true")
    args = parser.parse_args()

    symbol = (args.symbol or "ETH").upper()
    notional = float(args.notional or 20.0)

    if args.trade and not args.confirm_live_trade:
        raise SystemExit("Refusing to trade: provide both --trade and --confirm-live-trade")

    checks = []
    results: Dict[str, Any] = {"ts": _utc_now_iso(), "symbol": symbol, "notional": notional, "checks": []}

    checks.append(_run_check("public.market_meta", lambda: get_lighter_market_meta(symbol)))
    checks.append(_run_check("public.funding_rates", lambda: get_lighter_funding_rates_map()))
    checks.append(_run_check("public.orderbook_orders", lambda: get_lighter_orderbook_orders(_market_id(symbol), limit=20)))
    checks.append(_run_check("public.recent_trades", lambda: get_lighter_recent_trades(_market_id(symbol), limit=20)))
    checks.append(_run_check("public.orderbook_sweep_prices", lambda: fetch_orderbook_prices("lighter", symbol, "perp", notional=notional)))

    if args.private:
        checks.append(_run_check("private.balance_summary", lambda: get_lighter_balance_summary()))
        checks.append(_run_check("private.check_client", _check_client))

    if args.trade:
        checks.append(_run_check("trade.open_close", lambda: _trade_open_close(symbol, notional)))

    results["checks"] = [asdict(c) for c in checks]

    ok_count = sum(1 for c in checks if c.ok)
    print(f"[lighter_audit] symbol={symbol} notional={notional} ok={ok_count}/{len(checks)} ts={results['ts']}")
    for c in checks:
        status = "OK" if c.ok else "FAIL"
        print(f"- {status:4s} {c.name} ({c.seconds:.2f}s)")
        if not c.ok:
            print(f"  error: {c.error}")

    os.makedirs("reports", exist_ok=True)
    out_path = os.path.join("reports", f"connector_audit_lighter_{symbol.lower()}_{int(time.time())}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    print("saved:", out_path)


if __name__ == "__main__":
    main()
