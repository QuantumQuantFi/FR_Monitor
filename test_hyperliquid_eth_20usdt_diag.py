import time
from datetime import datetime, timezone

from hyperliquid.info import Info

from orderbook_utils import fetch_orderbook_prices
from trading.trade_executor import _resolve_hyperliquid_credentials, place_hyperliquid_perp_market_order


SYMBOL = "ETH"
NOTIONAL_USDT = 20.0


def _now_ms() -> int:
    return int(time.time() * 1000)


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_oid(resp):
    try:
        statuses = resp.get("response", {}).get("data", {}).get("statuses", [])
        if statuses and isinstance(statuses[0], dict):
            filled = statuses[0].get("filled")
            if isinstance(filled, dict):
                return filled.get("oid")
    except Exception:
        return None
    return None


def main() -> None:
    creds = _resolve_hyperliquid_credentials(None, None, base_url=None)
    addr = creds.address
    info = Info(base_url=creds.base_url, skip_ws=True, timeout=10)

    state = info.user_state(addr)
    account_value = float((state.get("marginSummary") or {}).get("accountValue") or 0.0)
    print("hyperliquid base_url:", creds.base_url)
    print("hyperliquid user_address:", addr)
    print("hyperliquid account_value:", account_value)
    if account_value <= 0:
        print(
            "WARNING: account_value=0.0; if this private key is an API wallet (agent), set "
            "HYPERLIQUID_USER_ADDRESS/HYPERLIQUID_WALLET_ADDRESS to the main trading address."
        )

    ob = fetch_orderbook_prices("hyperliquid", SYMBOL, "perp", notional=NOTIONAL_USDT) or {}
    if ob.get("error"):
        raise SystemExit(f"orderbook error: {ob.get('error')}")
    size = NOTIONAL_USDT / float(ob["buy"])
    client = f"diag-{SYMBOL}-{int(time.time())}"

    print("entry orderbook:", ob)
    print("placing OPEN long", SYMBOL, "notional", NOTIONAL_USDT, "size", size, "client", client, "ts", _utc_iso())
    resp_open = place_hyperliquid_perp_market_order(SYMBOL, "long", size, client_order_id=client)
    oid_open = _extract_oid(resp_open)
    print("open resp:", resp_open)
    print("open oid:", oid_open)

    end = _now_ms()
    fills = info.user_fills_by_time(addr, end - 10 * 60 * 1000, end)
    print("fills(last10m) after open:", len(fills) if isinstance(fills, list) else fills)
    if oid_open:
        print("orderStatus(open):", info.query_order_by_oid(addr, int(oid_open)))

    print("sleep 3s...")
    time.sleep(3)

    print("placing CLOSE (reduce-only) sell", SYMBOL, "size", size, "client", client + "-C", "ts", _utc_iso())
    resp_close = place_hyperliquid_perp_market_order(SYMBOL, "sell", size, reduce_only=True, client_order_id=client + "-C")
    oid_close = _extract_oid(resp_close)
    print("close resp:", resp_close)
    print("close oid:", oid_close)

    end = _now_ms()
    fills = info.user_fills_by_time(addr, end - 10 * 60 * 1000, end)
    print("fills(last10m) after close:", len(fills) if isinstance(fills, list) else fills)
    if oid_close:
        print("orderStatus(close):", info.query_order_by_oid(addr, int(oid_close)))


if __name__ == "__main__":
    main()

