#!/usr/bin/env python3
import time
from typing import Any, Optional, Tuple

from trading.live_trading_manager import LiveTradingConfig, LiveTradingManager
from trading.trade_executor import TradeExecutionError, execute_spot_market_order


TARGET_SYMBOL = "ETH"
QUOTE_CCY = "USDT"
NOTIONAL_USDT = 50.0
EXCHANGES: Tuple[str, ...] = ("okx", "bybit", "bitget", "binance")


def _choose_exchange(mgr: LiveTradingManager) -> Optional[str]:
    for ex in EXCHANGES:
        try:
            bal = mgr._get_exchange_position_size(ex, QUOTE_CCY, market_type="spot")
        except Exception:
            bal = None
        if bal is None:
            continue
        if float(bal) >= NOTIONAL_USDT:
            return ex
    return None


def _parse_fill(mgr: LiveTradingManager, exchange: str, order: Any) -> dict:
    try:
        return mgr._parse_fill_fields(exchange, TARGET_SYMBOL, order, market_type="spot")
    except Exception:
        return {}


def _spot_balance(mgr: LiveTradingManager, exchange: str, asset: str) -> float:
    try:
        bal = mgr._get_exchange_position_size(exchange, asset, market_type="spot")
    except Exception:
        bal = None
    return float(bal or 0.0)


def main() -> None:
    mgr = LiveTradingManager(LiveTradingConfig(enabled=False, dsn=""))
    ex = _choose_exchange(mgr)
    if not ex:
        print("No exchange with sufficient USDT balance found.")
        return

    ts = int(time.time())
    pre_base = _spot_balance(mgr, ex, TARGET_SYMBOL)
    buy_client_id = f"spot-test-buy-{ex}-{ts}"
    print(f"Using {ex} for spot test. Buying {NOTIONAL_USDT} {QUOTE_CCY} of {TARGET_SYMBOL}...")
    buy_order = execute_spot_market_order(
        ex,
        TARGET_SYMBOL,
        side="buy",
        quote_quantity=NOTIONAL_USDT,
        order_kwargs={"client_order_id": buy_client_id},
    )
    buy_fill = _parse_fill(mgr, ex, buy_order)
    filled_qty = float(buy_fill.get("filled_qty") or 0.0)
    avg_price = buy_fill.get("avg_price")
    print(f"Buy filled_qty={filled_qty:.8f} avg_price={avg_price}")

    if filled_qty <= 0:
        print("Buy did not fill; aborting sell.")
        return

    time.sleep(0.5)
    post_base = _spot_balance(mgr, ex, TARGET_SYMBOL)
    delta_base = max(0.0, float(post_base) - float(pre_base))
    sell_base = min(float(filled_qty), float(delta_base) if delta_base > 0 else float(filled_qty))
    sell_qty = sell_base * 0.995
    if sell_qty <= 0:
        print("Sell size is too small after fee buffer; aborting sell.")
        return
    sell_client_id = f"spot-test-sell-{ex}-{ts}"
    print(f"Selling {sell_qty:.8f} {TARGET_SYMBOL} on {ex}...")
    try:
        sell_order = execute_spot_market_order(
            ex,
            TARGET_SYMBOL,
            side="sell",
            quantity=sell_qty,
            order_kwargs={"client_order_id": sell_client_id},
        )
    except TradeExecutionError as exc:
        print(f"Sell failed: {exc}")
        return
    sell_fill = _parse_fill(mgr, ex, sell_order)
    sell_avg = sell_fill.get("avg_price")
    sell_filled = sell_fill.get("filled_qty")
    print(f"Sell filled_qty={sell_filled} avg_price={sell_avg}")


if __name__ == "__main__":
    main()
