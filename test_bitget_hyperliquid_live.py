import math
import os
import time
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row

from config import WATCHLIST_PG_CONFIG, config_private
from orderbook_utils import fetch_orderbook_prices
from trading.live_trading_manager import LiveTradingConfig, LiveTradingManager
from trading.trade_executor import (
    TradeExecutionError,
    get_bitget_usdt_perp_positions,
    get_hyperliquid_perp_positions,
)


SYMBOL = "ETH"
NOTIONAL_USDT = 50.0


def _utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _has_bitget_position(symbol: str) -> bool:
    positions = get_bitget_usdt_perp_positions(symbol=symbol)
    for pos in positions:
        for key in ("total", "available", "openQty", "pos", "holdVol", "size"):
            if key not in pos:
                continue
            try:
                val = float(pos.get(key) or 0)
            except Exception:
                continue
            if abs(val) > 1e-12:
                return True
    return False


def _has_hyperliquid_position(symbol: str) -> bool:
    positions = get_hyperliquid_perp_positions()
    sym = symbol.upper()
    for item in positions:
        position = item.get("position") if isinstance(item, dict) else None
        if not isinstance(position, dict):
            continue
        if (position.get("coin") or "").upper() != sym:
            continue
        try:
            szi = float(position.get("szi") or 0)
        except Exception:
            szi = 0.0
        if abs(szi) > 1e-12:
            return True
    return False


def _has_hyperliquid_credentials() -> bool:
    pk = (os.getenv("HYPERLIQUID_PRIVATE_KEY") or "").strip()
    if pk:
        return True
    if config_private is None:
        return False
    return bool((getattr(config_private, "HYPERLIQUID_PRIVATE_KEY", "") or "").strip())


def main() -> None:
    if not _has_hyperliquid_credentials():
        raise SystemExit(
            "Missing Hyperliquid credential: set env HYPERLIQUID_PRIVATE_KEY or add it to config_private.py."
        )

    for ex in ("bitget", "hyperliquid"):
        ob = fetch_orderbook_prices(ex, SYMBOL, "perp", notional=NOTIONAL_USDT) or {}
        if ob.get("error"):
            raise SystemExit(f"Orderbook unavailable on {ex}: {ob.get('error')}")

    if _has_bitget_position(SYMBOL):
        raise SystemExit(f"ABORT: existing {SYMBOL} position detected on bitget; flatten first.")
    if _has_hyperliquid_position(SYMBOL):
        raise SystemExit(f"ABORT: existing {SYMBOL} position detected on hyperliquid; flatten first.")

    ob_bitget = fetch_orderbook_prices("bitget", SYMBOL, "perp", notional=NOTIONAL_USDT) or {}
    ob_hl = fetch_orderbook_prices("hyperliquid", SYMBOL, "perp", notional=NOTIONAL_USDT) or {}
    bitget_buy, bitget_sell = float(ob_bitget["buy"]), float(ob_bitget["sell"])
    hl_buy, hl_sell = float(ob_hl["buy"]), float(ob_hl["sell"])

    metric_long_bitget_short_hl = math.log(hl_sell / bitget_buy)
    metric_long_hl_short_bitget = math.log(bitget_sell / hl_buy)

    if metric_long_bitget_short_hl >= metric_long_hl_short_bitget:
        long_ex, short_ex = "bitget", "hyperliquid"
    else:
        long_ex, short_ex = "hyperliquid", "bitget"

    manager = LiveTradingManager(
        LiveTradingConfig(
            enabled=True,
            dsn=str(WATCHLIST_PG_CONFIG["dsn"]),
            allowed_exchanges=("bitget", "hyperliquid"),
            per_leg_notional_usdt=NOTIONAL_USDT,
            max_concurrent_trades=10,
        )
    )
    manager.ensure_schema()

    event_id = int(time.time() * 1000)
    client_base = f"sim{event_id}"
    payload = {
        "event": {"id": event_id, "symbol": SYMBOL, "signal_type": "B", "start_ts": _utc_iso(datetime.now(timezone.utc))},
        "debug": {
            "mode": "manual_simulation",
            "picked_exchanges": ["bitget", "hyperliquid"],
            "chosen_direction": {"long": long_ex, "short": short_ex},
            "orderbook_entry": {"bitget": ob_bitget, "hyperliquid": ob_hl},
            "notional_usdt": NOTIONAL_USDT,
        },
    }

    with psycopg.connect(str(WATCHLIST_PG_CONFIG["dsn"]), row_factory=dict_row, autocommit=True) as conn:
        signal_id = manager._insert_signal(
            conn,
            event={"id": event_id, "symbol": SYMBOL, "signal_type": "B"},
            leg_long_exchange=long_ex,
            leg_short_exchange=short_ex,
            pnl_hat=0.02,
            win_prob=0.99,
            pnl_hat_ob=0.02,
            win_prob_ob=0.99,
            payload=payload,
            status="opening",
            reason="manual_simulation",
            client_order_id_base=client_base,
        )
        if not signal_id:
            raise SystemExit("insert live_trade_signal failed")

        try:
            manager._open_trade(
                conn,
                int(signal_id),
                SYMBOL,
                long_ex,
                short_ex,
                client_base,
                pnl_hat_ob=0.02,
            )
            manager._update_signal_status(conn, int(signal_id), "open")
        except TradeExecutionError as exc:
            manager._record_error(
                conn,
                signal_id=int(signal_id),
                stage="manual_sim_open",
                error_type="TradeExecutionError",
                message=str(exc),
                context={"symbol": SYMBOL, "long_ex": long_ex, "short_ex": short_ex},
            )
            manager._update_signal_status(conn, int(signal_id), "failed", reason=f"manual_sim_open: {exc}")
            raise

    time.sleep(10)
    with psycopg.connect(str(WATCHLIST_PG_CONFIG["dsn"]), row_factory=dict_row, autocommit=True) as conn:
        conn.execute(
            "UPDATE watchlist.live_trade_signal SET force_close_at = now() - interval '1 second', updated_at=now() WHERE id=%s;",
            (int(signal_id),),
        )

    manager.monitor_open_trades_once()

    if _has_bitget_position(SYMBOL):
        raise SystemExit("bitget position still open after close attempt")
    if _has_hyperliquid_position(SYMBOL):
        raise SystemExit("hyperliquid position still open after close attempt")

    print(f"OK: bitget+hyperliquid live flow finished. signal_id={signal_id} long={long_ex} short={short_ex}")


if __name__ == "__main__":
    main()
