import math
from typing import Any, Dict, List, Optional, Tuple

import requests
import config

HYPERLIQUID_API_BASE_URL = "https://api.hyperliquid.xyz"
LIGHTER_BASE_URL = config.LIGHTER_REST_BASE_URL.rstrip("/")

DEFAULT_SWEEP_NOTIONAL = 100.0  # USD notional to simulate across orderbooks
REQUEST_TIMEOUT = 4
BITGET_USDT_PRODUCT_TYPE = "USDT-FUTURES"


def _safe_float(val: Any) -> Optional[float]:
    try:
        f = float(val)
        if math.isfinite(f):
            return f
    except (TypeError, ValueError):
        return None
    return None


def _sweep_avg(levels: List[Tuple[float, float]], notional: float, side: str) -> Optional[float]:
    """
    levels: list of (price, size) sorted best-first.
    side: 'buy' uses asks, 'sell' uses bids.
    """
    remaining = notional
    if not levels or notional <= 0:
        return None
    total_cost = 0.0
    total_qty = 0.0
    for price, size in levels:
        if price <= 0 or size <= 0:
            continue
        level_notional = price * size
        take = min(level_notional, remaining)
        qty = take / price
        total_cost += qty * price
        total_qty += qty
        remaining -= take
        if remaining <= 0:
            break
    if total_qty == 0:
        return None
    return total_cost / total_qty


def _sweep_avg_qty(levels: List[Tuple[float, float]], quantity: float, side: str) -> Optional[float]:
    """
    levels: list of (price, size) sorted best-first.
    quantity: base asset quantity to simulate.
    side: 'buy' uses asks, 'sell' uses bids.
    """
    remaining = quantity
    if not levels or quantity <= 0:
        return None
    total_cost = 0.0
    total_qty = 0.0
    for price, size in levels:
        if price <= 0 or size <= 0:
            continue
        take_qty = min(size, remaining)
        total_cost += take_qty * price
        total_qty += take_qty
        remaining -= take_qty
        if remaining <= 0:
            break
    if total_qty == 0:
        return None
    return total_cost / total_qty


def _req_json(url: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {"url": url, "params": params}
    try:
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        meta["status_code"] = int(resp.status_code)
        meta["headers"] = {
            k.lower(): v
            for k, v in resp.headers.items()
            if k.lower() in {"retry-after", "x-mbx-used-weight-1m", "x-mbx-used-weight", "x-ratelimit-remaining"}
        }
        if resp.status_code == 200:
            return resp.json(), meta
        try:
            meta["body_json"] = resp.json()
        except Exception:
            meta["body_text"] = (resp.text or "")[:300]
        return None, meta
    except Exception as exc:
        meta["error"] = str(exc)
        return None, meta


def _build_symbol(exchange: str, symbol: str, market_type: str) -> Optional[str]:
    base = symbol.upper()
    if exchange == 'binance':
        return f"{base}USDT"
    if exchange == 'okx':
        return f"{base}-USDT-SWAP" if market_type == 'perp' else f"{base}-USDT"
    if exchange == 'bybit':
        return f"{base}USDT"
    if exchange == 'bitget':
        return f"{base}USDT"
    if exchange == 'hyperliquid':
        return base
    if exchange == 'lighter':
        return base
    return None


def _fetch_raw_orderbook(
    exchange: str, symbol: str, market_type: str
) -> Tuple[Optional[Dict[str, List[Tuple[float, float]]]], Optional[str], Dict[str, Any]]:
    ex = exchange.lower()
    inst = _build_symbol(ex, symbol, market_type)
    meta: Dict[str, Any] = {"exchange": ex, "symbol": symbol, "inst": inst, "market_type": market_type}
    if not inst and ex != 'hyperliquid':
        return None, "unsupported_exchange", meta
    if ex == 'binance':
        path = "https://fapi.binance.com/fapi/v1/depth" if market_type == 'perp' else "https://api.binance.com/api/v3/depth"
        data, http_meta = _req_json(path, params={'symbol': inst, 'limit': 50})
        meta.update({"http": http_meta})
        if not isinstance(data, dict):
            err = "rate_limited" if http_meta.get("status_code") == 429 else "no_data"
            return None, err, meta
        bids = [(_safe_float(p), _safe_float(q)) for p, q in data.get('bids', [])]
        asks = [(_safe_float(p), _safe_float(q)) for p, q in data.get('asks', [])]
    elif ex == 'okx':
        data, http_meta = _req_json("https://www.okx.com/api/v5/market/books", params={'instId': inst, 'sz': 200})
        meta.update({"http": http_meta})
        if not isinstance(data, dict) or not data.get('data'):
            err = "rate_limited" if http_meta.get("status_code") == 429 else "no_data"
            return None, err, meta
        book = data['data'][0] if isinstance(data['data'][0], dict) else None
        if book is None:
            return None, "no_data", meta
        # OKX swaps return sizes in contracts; convert to base-asset qty via ctVal.
        size_multiplier = 1.0
        if market_type == 'perp':
            try:
                from trading import trade_executor as _te  # local import to avoid cycles
                filters = _te._get_okx_instrument_filters(inst, base_url="https://www.okx.com", inst_type="SWAP")
                size_multiplier = float(getattr(filters, "contract_value", 1.0) or 1.0)
                if size_multiplier <= 0:
                    size_multiplier = 1.0
            except Exception:
                size_multiplier = 1.0

        bids = [
            (_safe_float(p), (_safe_float(sz) * size_multiplier if _safe_float(sz) is not None else None))
            for p, sz, *_ in book.get('bids', [])
        ]
        asks = [
            (_safe_float(p), (_safe_float(sz) * size_multiplier if _safe_float(sz) is not None else None))
            for p, sz, *_ in book.get('asks', [])
        ]
    elif ex == 'bybit':
        category = 'linear' if market_type == 'perp' else 'spot'
        data, http_meta = _req_json(
            "https://api.bybit.com/v5/market/orderbook", params={'category': category, 'symbol': inst, 'limit': 50}
        )
        meta.update({"http": http_meta})
        if not isinstance(data, dict):
            err = "rate_limited" if http_meta.get("status_code") == 429 else "no_data"
            return None, err, meta
        result = data.get('result') or {}
        bids = [(_safe_float(p), _safe_float(sz)) for p, sz, *_ in result.get('b', [])]
        asks = [(_safe_float(p), _safe_float(sz)) for p, sz, *_ in result.get('a', [])]
    elif ex == 'bitget':
        if market_type == 'perp':
            data, http_meta = _req_json(
                "https://api.bitget.com/api/v2/mix/market/merge-depth",
                # 与交易/持仓接口使用的 productType 保持一致，避免拿到不同市场的深度导致监控/验算失真。
                params={'symbol': inst, 'limit': 50, 'productType': BITGET_USDT_PRODUCT_TYPE},
            )
        else:
            data, http_meta = _req_json(
                "https://api.bitget.com/api/v2/spot/market/merge-depth",
                params={'symbol': inst, 'limit': 50},
            )
        meta.update({"http": http_meta})
        if not isinstance(data, dict):
            err = "rate_limited" if http_meta.get("status_code") == 429 else "no_data"
            return None, err, meta
        book = data.get('data') or {}
        bids = [(_safe_float(p), _safe_float(sz)) for p, sz in book.get('bids', [])]
        asks = [(_safe_float(p), _safe_float(sz)) for p, sz in book.get('asks', [])]
    elif ex == 'lighter':
        # Lighter REST 未直接提供档位深度，这里回退到 exchangeStats 的最近成交价作为近似中价
        try:
            resp = requests.get(f"{LIGHTER_BASE_URL}/exchangeStats", timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                meta.update({"http": {"status_code": int(resp.status_code), "body_text": (resp.text or "")[:300]}})
                return None, f"status_{resp.status_code}", meta
            data = resp.json()
        except Exception:
            meta.update({"http": {"error": "http_error"}})
            return None, "http_error", meta
        sym_upper = symbol.upper()
        stats = None
        if isinstance(data, dict):
            for entry in data.get('order_book_stats', []):
                if (entry.get('symbol') or '').upper() == sym_upper:
                    stats = entry
                    break
        if not stats:
            return None, "no_data", meta
        price = _safe_float(stats.get('last_trade_price') or stats.get('mark_price'))
        if not price:
            return None, "no_data", meta
        # 用单档估计买卖均价，标记误差在前端显示 error=None 以参与计算
        bids = [(price, 1.0)]
        asks = [(price, 1.0)]
    elif ex == 'hyperliquid':
        payload = {'type': 'l2Book', 'coin': symbol.upper()}
        try:
            resp = requests.post(f"{HYPERLIQUID_API_BASE_URL}/info", json=payload, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                meta.update({"http": {"status_code": int(resp.status_code), "body_text": (resp.text or "")[:300]}})
                return None, f"status_{resp.status_code}", meta
            data = resp.json()
        except Exception:
            meta.update({"http": {"error": "http_error"}})
            return None, "http_error", meta
        levels = data.get('levels') if isinstance(data, dict) else None
        if not levels or not isinstance(levels, list) or len(levels) < 2:
            return None, "no_data", meta
        raw_bids, raw_asks = levels[0], levels[1]
        bids = [(_safe_float(entry.get('px') if isinstance(entry, dict) else entry[0]),
                 _safe_float(entry.get('sz') if isinstance(entry, dict) else entry[1]))
                for entry in raw_bids]
        asks = [(_safe_float(entry.get('px') if isinstance(entry, dict) else entry[0]),
                 _safe_float(entry.get('sz') if isinstance(entry, dict) else entry[1]))
                for entry in raw_asks]
    else:
        return None, "unsupported_exchange", meta

    bids_clean = [(p, sz) for p, sz in bids if p and sz]
    asks_clean = [(p, sz) for p, sz in asks if p and sz]
    if not bids_clean and not asks_clean:
        return None, "empty_orderbook", meta
    return {'bids': bids_clean, 'asks': asks_clean}, None, meta


def fetch_orderbook_prices(exchange: str, symbol: str, market_type: str, *, notional: float = DEFAULT_SWEEP_NOTIONAL) -> Optional[Dict[str, Any]]:
    raw, err, meta = _fetch_raw_orderbook(exchange, symbol, market_type)
    if not raw:
        return {'error': err or 'no_data', 'meta': meta}
    asks = raw.get('asks') or []
    bids = raw.get('bids') or []
    buy_avg = _sweep_avg(asks, notional, 'buy')
    sell_avg = _sweep_avg(bids, notional, 'sell')
    raw_summary = {
        'bids_n': len(bids),
        'asks_n': len(asks),
        'best_bid': bids[0] if bids else None,
        'best_ask': asks[0] if asks else None,
        'bids_head': bids[:5],
        'asks_head': asks[:5],
        'notional': float(notional),
    }
    if buy_avg is None or sell_avg is None:
        return {'buy': buy_avg, 'sell': sell_avg, 'mid': None, 'error': 'missing_buy_or_sell', 'meta': meta, 'raw_summary': raw_summary}
    mid = None
    if buy_avg is not None and sell_avg is not None:
        mid = (buy_avg + sell_avg) / 2
    elif buy_avg is not None:
        mid = buy_avg
    elif sell_avg is not None:
        mid = sell_avg
    return {
        'buy': buy_avg,
        'sell': sell_avg,
        'mid': mid,
        'error': None,
        'meta': meta,
        'raw_summary': raw_summary,
    }


def fetch_orderbook_prices_for_quantity(
    exchange: str, symbol: str, market_type: str, *, quantity: float
) -> Optional[Dict[str, Any]]:
    raw, err, meta = _fetch_raw_orderbook(exchange, symbol, market_type)
    if not raw:
        return {'error': err or 'no_data', 'meta': meta}
    asks = raw.get('asks') or []
    bids = raw.get('bids') or []
    buy_avg = _sweep_avg_qty(asks, float(quantity), 'buy')
    sell_avg = _sweep_avg_qty(bids, float(quantity), 'sell')
    raw_summary = {
        'bids_n': len(bids),
        'asks_n': len(asks),
        'best_bid': bids[0] if bids else None,
        'best_ask': asks[0] if asks else None,
        'bids_head': bids[:5],
        'asks_head': asks[:5],
        'quantity': float(quantity),
    }
    if buy_avg is None or sell_avg is None:
        return {'buy': buy_avg, 'sell': sell_avg, 'mid': None, 'error': 'missing_buy_or_sell', 'meta': meta, 'raw_summary': raw_summary}
    mid = None
    if buy_avg is not None and sell_avg is not None:
        mid = (buy_avg + sell_avg) / 2
    elif buy_avg is not None:
        mid = buy_avg
    elif sell_avg is not None:
        mid = sell_avg
    return {
        'buy': buy_avg,
        'sell': sell_avg,
        'mid': mid,
        'error': None,
        'meta': meta,
        'raw_summary': raw_summary,
    }


def compute_orderbook_spread(legs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    legs: [{'exchange':..., 'market_type':..., 'price': {'buy':..., 'sell':..., 'mid':...}}]
    返回 forward（高买 / 低卖）与 reverse（高卖 / 低买）价差。
    """
    # Use executable sweep prices (buy=asks, sell=bids). Do NOT rely on mid ordering:
    # a venue can have a slightly lower mid but a better bid/ask, and arbitrage direction must
    # be evaluated using bid-vs-ask pairs.
    usable = [leg for leg in legs if isinstance(leg.get("price"), dict)]
    if len(usable) < 2:
        return None

    forward_spread = None
    forward_high = None
    forward_low = None
    buy_legs = [l for l in usable if l["price"].get("buy") is not None]
    sell_legs = [l for l in usable if l["price"].get("sell") is not None]
    if buy_legs and sell_legs:
        forward_high = max(buy_legs, key=lambda l: float(l["price"]["buy"]))
        forward_low = min(sell_legs, key=lambda l: float(l["price"]["sell"]))
    if forward_high and forward_low and forward_high["price"].get("buy") and forward_low["price"].get("sell"):
        base = min(forward_high["price"]["buy"], forward_low["price"]["sell"])
        if base:
            forward_spread = (forward_high["price"]["buy"] - forward_low["price"]["sell"]) / base

    reverse_spread = None
    reverse_high = None
    reverse_low = None
    if sell_legs and buy_legs:
        reverse_high = max(sell_legs, key=lambda l: float(l["price"]["sell"]))
        reverse_low = min(buy_legs, key=lambda l: float(l["price"]["buy"]))
    if reverse_high and reverse_low and reverse_high["price"].get("sell") and reverse_low["price"].get("buy"):
        base = min(reverse_high["price"]["sell"], reverse_low["price"]["buy"])
        if base:
            reverse_spread = (reverse_high["price"]["sell"] - reverse_low["price"]["buy"]) / base

    return {
        'legs': usable,
        'forward': {
            'high': forward_high,
            'low': forward_low,
            'spread': forward_spread,
        },
        'reverse': {
            'high': reverse_high,
            'low': reverse_low,
            'spread': reverse_spread,
        },
    }
