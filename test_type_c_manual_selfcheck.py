#!/usr/bin/env python3
import time
from typing import Any, Dict, Optional, Tuple

from orderbook_utils import fetch_orderbook_prices
from trading.live_trading_manager import LiveTradingConfig, LiveTradingManager
from trading.trade_executor import (
    TradeExecutionError,
    execute_perp_market_order,
    execute_spot_market_order,
    get_binance_perp_price,
)


EXCHANGES = ("okx", "binance", "bybit", "bitget")
SYMBOL = "ETH"
NOTIONAL_USDT = 50.0
SELL_BUFFER = 0.995


def _parse_fill(mgr: LiveTradingManager, exchange: str, order: Any, market_type: str) -> Dict[str, Any]:
    try:
        return mgr._parse_fill_fields(exchange, SYMBOL, order, market_type=market_type)
    except Exception:
        return {}


def _spot_balance(mgr: LiveTradingManager, exchange: str, asset: str) -> float:
    try:
        size = mgr._get_exchange_position_size(exchange, asset, market_type="spot")
    except Exception:
        size = None
    return float(size or 0.0)


def _perp_position(mgr: LiveTradingManager, exchange: str) -> float:
    try:
        size = mgr._get_exchange_position_size(exchange, SYMBOL, market_type="perp")
    except Exception:
        size = None
    return float(size or 0.0)


def _close_spot(mgr: LiveTradingManager, exchange: str, qty_hint: float) -> Optional[Dict[str, Any]]:
    bal = _spot_balance(mgr, exchange, SYMBOL)
    if bal <= 0:
        return None
    qty = min(bal, qty_hint) * SELL_BUFFER
    if qty <= 0:
        return None
    return execute_spot_market_order(
        exchange,
        SYMBOL,
        side="sell",
        quantity=qty,
        order_kwargs={"client_order_id": f"selfcheck-{exchange}-spot-sell-{int(time.time())}"},
    )


def _close_perp(mgr: LiveTradingManager, exchange: str, qty_hint: float) -> Optional[Dict[str, Any]]:
    pos = _perp_position(mgr, exchange)
    if abs(pos) <= 1e-9:
        return None
    qty = abs(pos)
    if qty_hint > 0:
        qty = min(qty, qty_hint)
    return mgr._place_close_order(
        exchange=exchange,
        symbol=SYMBOL,
        position_leg="short" if pos < 0 else "long",
        quantity=qty,
        client_order_id=f"selfcheck-{exchange}-perp-close-{int(time.time())}",
        market_type="perp",
    )


def _run_exchange(mgr: LiveTradingManager, exchange: str) -> Tuple[bool, str]:
    usdt_bal = _spot_balance(mgr, exchange, "USDT")
    if usdt_bal < NOTIONAL_USDT:
        return False, f"insufficient USDT balance ({usdt_bal:.2f})"

    try:
        mgr._maybe_set_leverage_1x(SYMBOL, exchange)
    except Exception:
        pass

    ob = fetch_orderbook_prices(exchange, SYMBOL, "perp", notional=NOTIONAL_USDT) or {}
    perp_px = None
    if not ob.get("error") and ob.get("sell"):
        try:
            perp_px = float(ob["sell"])
        except Exception:
            perp_px = None
    if perp_px is None and exchange == "binance":
        try:
            perp_px = float(get_binance_perp_price(SYMBOL))
        except Exception:
            perp_px = None
    if perp_px is None:
        return False, f"perp orderbook unavailable: {ob.get('error')}"
    perp_qty = NOTIONAL_USDT / perp_px if perp_px > 0 else 0.0
    if perp_qty <= 0:
        return False, "invalid perp qty"

    spot_order = None
    perp_order = None
    spot_qty = 0.0

    try:
        spot_order = execute_spot_market_order(
            exchange,
            SYMBOL,
            side="buy",
            quote_quantity=NOTIONAL_USDT,
            order_kwargs={"client_order_id": f"selfcheck-{exchange}-spot-buy-{int(time.time())}"},
        )
        spot_fill = _parse_fill(mgr, exchange, spot_order, "spot")
        spot_qty = float(spot_fill.get("filled_qty") or 0.0)

        perp_order = execute_perp_market_order(
            exchange,
            SYMBOL,
            perp_qty,
            side="short",
            order_kwargs={"client_order_id": f"selfcheck-{exchange}-perp-short-{int(time.time())}"},
        )
        _ = _parse_fill(mgr, exchange, perp_order, "perp")
    except TradeExecutionError as exc:
        # Rollback any partial leg.
        if perp_order:
            try:
                _close_perp(mgr, exchange, perp_qty)
            except Exception:
                pass
        if spot_qty > 0:
            try:
                _close_spot(mgr, exchange, spot_qty)
            except Exception:
                pass
        return False, f"open failed: {exc}"

    time.sleep(0.6)

    perp_close = None
    spot_close = None
    try:
        perp_close = _close_perp(mgr, exchange, perp_qty)
    except Exception as exc:
        return False, f"perp close failed: {exc}"

    try:
        spot_close = _close_spot(mgr, exchange, spot_qty)
    except Exception as exc:
        return False, f"spot close failed: {exc}"

    return True, "ok"


def main() -> None:
    mgr = LiveTradingManager(LiveTradingConfig(enabled=False, dsn=""))
    for ex in EXCHANGES:
        print(f"== {ex} ==")
        ok, msg = _run_exchange(mgr, ex)
        print(f"{ex}: {msg}")


if __name__ == "__main__":
    main()
