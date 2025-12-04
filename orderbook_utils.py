import math
from typing import Any, Dict, List, Optional, Tuple

import requests

HYPERLIQUID_API_BASE_URL = "https://api.hyperliquid.xyz"

DEFAULT_SWEEP_NOTIONAL = 100.0  # USD notional to simulate across orderbooks
REQUEST_TIMEOUT = 4


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


def _req_json(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    try:
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        return None
    return None


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
    return None


def _fetch_raw_orderbook(exchange: str, symbol: str, market_type: str) -> Tuple[Optional[Dict[str, List[Tuple[float, float]]]], Optional[str]]:
    ex = exchange.lower()
    inst = _build_symbol(ex, symbol, market_type)
    if not inst and ex != 'hyperliquid':
        return None, "unsupported_exchange"
    if ex == 'binance':
        path = "https://fapi.binance.com/fapi/v1/depth" if market_type == 'perp' else "https://api.binance.com/api/v3/depth"
        data = _req_json(path, params={'symbol': inst, 'limit': 50})
        if not isinstance(data, dict):
            return None, "no_data"
        bids = [(_safe_float(p), _safe_float(q)) for p, q in data.get('bids', [])]
        asks = [(_safe_float(p), _safe_float(q)) for p, q in data.get('asks', [])]
    elif ex == 'okx':
        data = _req_json("https://www.okx.com/api/v5/market/books", params={'instId': inst, 'sz': 200})
        if not isinstance(data, dict) or not data.get('data'):
            return None, "no_data"
        book = data['data'][0] if isinstance(data['data'][0], dict) else None
        if book is None:
            return None, "no_data"
        bids = [(_safe_float(p), _safe_float(sz)) for p, sz, *_ in book.get('bids', [])]
        asks = [(_safe_float(p), _safe_float(sz)) for p, sz, *_ in book.get('asks', [])]
    elif ex == 'bybit':
        category = 'linear' if market_type == 'perp' else 'spot'
        data = _req_json("https://api.bybit.com/v5/market/orderbook", params={'category': category, 'symbol': inst, 'limit': 50})
        if not isinstance(data, dict):
            return None, "no_data"
        result = data.get('result') or {}
        bids = [(_safe_float(p), _safe_float(sz)) for p, sz, *_ in result.get('b', [])]
        asks = [(_safe_float(p), _safe_float(sz)) for p, sz, *_ in result.get('a', [])]
    elif ex == 'bitget':
        if market_type == 'perp':
            data = _req_json(
                "https://api.bitget.com/api/v2/mix/market/merge-depth",
                params={'symbol': inst, 'limit': 50, 'productType': 'usdt-futures'},
            )
        else:
            data = _req_json(
                "https://api.bitget.com/api/v2/spot/market/merge-depth",
                params={'symbol': inst, 'limit': 50},
            )
        if not isinstance(data, dict):
            return None, "no_data"
        book = data.get('data') or {}
        bids = [(_safe_float(p), _safe_float(sz)) for p, sz in book.get('bids', [])]
        asks = [(_safe_float(p), _safe_float(sz)) for p, sz in book.get('asks', [])]
    elif ex == 'hyperliquid':
        payload = {'type': 'l2Book', 'coin': symbol.upper()}
        try:
            resp = requests.post(f"{HYPERLIQUID_API_BASE_URL}/info", json=payload, timeout=REQUEST_TIMEOUT)
            if resp.status_code != 200:
                return None, f"status_{resp.status_code}"
            data = resp.json()
        except Exception:
            return None, "http_error"
        levels = data.get('levels') if isinstance(data, dict) else None
        if not levels or not isinstance(levels, list) or len(levels) < 2:
            return None, "no_data"
        raw_bids, raw_asks = levels[0], levels[1]
        bids = [(_safe_float(entry.get('px') if isinstance(entry, dict) else entry[0]),
                 _safe_float(entry.get('sz') if isinstance(entry, dict) else entry[1]))
                for entry in raw_bids]
        asks = [(_safe_float(entry.get('px') if isinstance(entry, dict) else entry[0]),
                 _safe_float(entry.get('sz') if isinstance(entry, dict) else entry[1]))
                for entry in raw_asks]
    else:
        return None, "unsupported_exchange"

    bids_clean = [(p, sz) for p, sz in bids if p and sz]
    asks_clean = [(p, sz) for p, sz in asks if p and sz]
    if not bids_clean and not asks_clean:
        return None, "empty_orderbook"
    return {'bids': bids_clean, 'asks': asks_clean}, None


def fetch_orderbook_prices(exchange: str, symbol: str, market_type: str, *, notional: float = DEFAULT_SWEEP_NOTIONAL) -> Optional[Dict[str, Any]]:
    raw, err = _fetch_raw_orderbook(exchange, symbol, market_type)
    if not raw:
        return {'error': err or 'no_data'}
    asks = raw.get('asks') or []
    bids = raw.get('bids') or []
    buy_avg = _sweep_avg(asks, notional, 'buy')
    sell_avg = _sweep_avg(bids, notional, 'sell')
    if buy_avg is None and sell_avg is None:
        return {'error': 'insufficient_depth'}
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
    }


def compute_orderbook_spread(legs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    legs: [{'exchange':..., 'market_type':..., 'price': {'buy':..., 'sell':..., 'mid':...}}]
    返回 forward（高买 / 低卖）与 reverse（高卖 / 低买）价差。
    """
    usable = [leg for leg in legs if leg.get('price') and leg['price'].get('mid') is not None]
    if len(usable) < 2:
        return None
    high = max(usable, key=lambda l: l['price']['mid'])
    low = min(usable, key=lambda l: l['price']['mid'])

    forward_spread = None
    if high['price'].get('buy') and low['price'].get('sell'):
        base = min(high['price']['buy'], low['price']['sell'])
        if base:
            forward_spread = (high['price']['buy'] - low['price']['sell']) / base

    reverse_spread = None
    if high['price'].get('sell') and low['price'].get('buy'):
        base = min(high['price']['sell'], low['price']['buy'])
        if base:
            reverse_spread = (high['price']['sell'] - low['price']['buy']) / base

    return {
        'legs': usable,
        'forward': {
            'high': high,
            'low': low,
            'spread': forward_spread,
        },
        'reverse': {
            'high': high,
            'low': low,
            'spread': reverse_spread,
        },
    }
