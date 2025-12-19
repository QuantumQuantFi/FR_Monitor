#!/usr/bin/env python3
"""
Smoke-test Lighter integration through LiveTradingManager primitives.

This validates:
  - execute_perp_market_order(exchange="lighter") works
  - LiveTradingManager._get_exchange_position_size("lighter") works
  - LiveTradingManager._place_close_order(exchange="lighter") works

It WILL place real orders. Use with a small notional.

Run:
  ./venv/bin/python test_lighter_live_trading_smoke.py --symbol ETH --notional 20 --confirm-live-trade
"""

from __future__ import annotations

import argparse
import time

from orderbook_utils import fetch_orderbook_prices
from config import WATCHLIST_PG_CONFIG
from trading.live_trading_manager import LiveTradingConfig, LiveTradingManager
from trading.trade_executor import TradeExecutionError, execute_perp_market_order


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="ETH")
    parser.add_argument("--notional", type=float, default=20.0)
    parser.add_argument("--confirm-live-trade", action="store_true")
    args = parser.parse_args()

    if not args.confirm_live_trade:
        raise SystemExit("Refusing to trade: pass --confirm-live-trade")

    symbol = (args.symbol or "ETH").upper()
    notional = float(args.notional or 20.0)

    ob = fetch_orderbook_prices("lighter", symbol, "perp", notional=notional) or {}
    if ob.get("error"):
        raise SystemExit(f"lighter orderbook error: {ob.get('error')} meta={ob.get('meta')}")
    buy_px = float(ob["buy"])
    size = notional / buy_px

    manager = LiveTradingManager(
        LiveTradingConfig(
            enabled=False,
            dsn=str(WATCHLIST_PG_CONFIG["dsn"]),
            allowed_exchanges=("lighter",),
            per_leg_notional_usdt=notional,
        )
    )

    before = manager._get_exchange_position_size("lighter", symbol) or 0.0
    if abs(float(before)) > 1e-9:
        raise SystemExit(f"ABORT: existing lighter position detected: {before} {symbol}")

    client_base = f"smoke-lt-{int(time.time())}"
    print(f"placing OPEN long lighter {symbol} notional~{notional} size={size} client={client_base}-O")
    open_order = execute_perp_market_order(
        "lighter",
        symbol,
        size,
        side="long",
        order_kwargs={"client_order_id": client_base + "1"},
    )
    print("open resp:", open_order)

    time.sleep(2.0)
    after = manager._get_exchange_position_size("lighter", symbol) or 0.0
    print("position after open:", after)
    if float(after) <= 0:
        raise SystemExit(f"OPEN failed: position not detected after open ({after})")

    print(f"placing CLOSE long->sell lighter {symbol} qty={abs(after)} client={client_base}-C")
    close_order = manager._place_close_order(
        exchange="lighter",
        symbol=symbol,
        position_leg="long",
        quantity=abs(float(after)),
        client_order_id=client_base + "2",
    )
    print("close resp:", close_order)

    time.sleep(2.0)
    final = manager._get_exchange_position_size("lighter", symbol) or 0.0
    print("position after close:", final)
    if abs(float(final)) > 1e-9:
        raise SystemExit(f"CLOSE failed: residual position remains: {final}")

    print("OK: lighter live trading smoke passed.")


if __name__ == "__main__":
    try:
        main()
    except TradeExecutionError as exc:
        raise SystemExit(f"TradeExecutionError: {exc}") from exc
