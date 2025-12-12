"""
REST snapshot collectors for exchanges.
Fetches spot/futures tickers in bulk and returns normalized maps.
Uses lightweight requests with timeouts; avoids asyncio for simplicity.
"""

from collections import deque
from datetime import datetime, timedelta, timezone
import threading
from typing import Any, Dict, List, Optional
import time

import requests

try:  # Optional dependency for GRVT REST integration
    from pysdk.grvt_ccxt import GrvtCcxt
    from pysdk.grvt_ccxt_env import GrvtEnv
except Exception:  # pragma: no cover - dependency optional
    GrvtCcxt = None
    GrvtEnv = None

from config import (
    CURRENT_SUPPORTED_SYMBOLS,
    GRVT_API_KEY,
    GRVT_API_SECRET,
    GRVT_ENVIRONMENT,
    GRVT_REST_SYMBOLS_PER_CALL,
    GRVT_TRADING_ACCOUNT_ID,
    HYPERLIQUID_API_BASE_URL,
    HYPERLIQUID_FUNDING_REFRESH_SECONDS,
    LIGHTER_MARKET_REFRESH_SECONDS,
    LIGHTER_REST_BASE_URL,
    REST_CONNECTION_CONFIG,
)
from binance_contract_filter import binance_filter
from bitget_symbol_filter import bitget_filter
from precision_utils import normalize_funding_rate
from funding_utils import (
    derive_funding_interval_hours,
    derive_interval_hours_from_times,
    normalize_next_funding_time,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _req_json(url: str) -> Any:
    timeout = REST_CONNECTION_CONFIG.get('timeout', 10)
    headers = {'User-Agent': REST_CONNECTION_CONFIG.get('user_agent', 'CrossExchange-Arb/1.0')}
    retry = max(0, int(REST_CONNECTION_CONFIG.get('retry', 0)))

    for attempt in range(retry + 1):
        try:
            resp = requests.get(url, timeout=timeout, headers=headers)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            if attempt >= retry:
                raise
        # small backoff
        time.sleep(0.2)
    return None


def _rest_headers() -> Dict[str, str]:
    return {
        'User-Agent': REST_CONNECTION_CONFIG.get('user_agent', 'CrossExchange-Arb/1.0'),
        'Accept': 'application/json'
    }


def _safe_float(value) -> Optional[float]:
    if value in (None, ''):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ms_to_iso(value: Optional[Any]) -> Optional[str]:
    try:
        if value in (None, '', 0):
            return None
        return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _next_utc_boundary_iso(step_hours: int, now: Optional[datetime] = None) -> str:
    step_hours = int(step_hours)
    if step_hours <= 0:
        step_hours = 1
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    floored = now.replace(minute=0, second=0, microsecond=0)
    base_hour = (floored.hour // step_hours) * step_hours
    base = floored.replace(hour=base_hour)
    candidate = base + timedelta(hours=step_hours)
    if candidate <= now:
        candidate = candidate + timedelta(hours=step_hours)
    return candidate.isoformat()


# -------------------------------
# Binance funding interval cache
# -------------------------------
_BINANCE_FUNDING_INTERVALS: Dict[str, float] = {}
_BINANCE_FUNDING_INTERVALS_LOCK = threading.Lock()
_BINANCE_FUNDING_INTERVALS_LAST_REFRESH = 0.0
_BINANCE_FUNDING_INTERVALS_TTL_SECONDS = 3600.0


def _get_binance_funding_interval_map(force_refresh: bool = False) -> Dict[str, float]:
    global _BINANCE_FUNDING_INTERVALS_LAST_REFRESH, _BINANCE_FUNDING_INTERVALS
    now = time.time()
    with _BINANCE_FUNDING_INTERVALS_LOCK:
        has_cache = bool(_BINANCE_FUNDING_INTERVALS)
        last_refresh = _BINANCE_FUNDING_INTERVALS_LAST_REFRESH
    if not force_refresh and has_cache and (now - last_refresh) < _BINANCE_FUNDING_INTERVALS_TTL_SECONDS:
        with _BINANCE_FUNDING_INTERVALS_LOCK:
            return dict(_BINANCE_FUNDING_INTERVALS)

    try:
        payload = _req_json('https://fapi.binance.com/fapi/v1/fundingInfo')
    except Exception as exc:
        print(f"Binance fundingInfo 获取失败: {exc}")
        with _BINANCE_FUNDING_INTERVALS_LOCK:
            return dict(_BINANCE_FUNDING_INTERVALS)

    intervals: Dict[str, float] = {}
    if isinstance(payload, list):
        for item in payload:
            sym = (item.get('symbol') or '').upper()
            if not sym.endswith('USDT'):
                continue
            base = sym[:-4]
            interval = derive_funding_interval_hours('binance', item.get('fundingIntervalHours'), fallback=False)
            if interval:
                intervals[base] = float(interval)

    if intervals:
        with _BINANCE_FUNDING_INTERVALS_LOCK:
            _BINANCE_FUNDING_INTERVALS = intervals
            _BINANCE_FUNDING_INTERVALS_LAST_REFRESH = now
            return dict(_BINANCE_FUNDING_INTERVALS)

    with _BINANCE_FUNDING_INTERVALS_LOCK:
        return dict(_BINANCE_FUNDING_INTERVALS)


# -------------------------------
# Hyperliquid helpers
# 说明：Hyperliquid 的公共 REST API 通过 POST `/info`
# （type=allMids/meta/metaAndAssetCtxs）返回 JSON，
# 这里封装了常用调用与缓存逻辑，供 fetch_hyperliquid 使用。
# -------------------------------
_HYPERLIQUID_BASES: List[str] = []
_HYPERLIQUID_BASES_LOCK = threading.Lock()
_HYPERLIQUID_FUNDING_CACHE: Dict[str, Dict[str, Any]] = {}
_HYPERLIQUID_FUNDING_LOCK = threading.Lock()
_HYPERLIQUID_LAST_FUNDING_REFRESH = 0.0


def _hyperliquid_base_url() -> str:
    return HYPERLIQUID_API_BASE_URL.rstrip('/')


def _hyperliquid_post(payload: Dict[str, Any]) -> Optional[Any]:
    url = f"{_hyperliquid_base_url()}/info"
    headers = _rest_headers()
    headers['Content-Type'] = 'application/json'
    timeout = REST_CONNECTION_CONFIG.get('timeout', 10)
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        print(f"Hyperliquid API {payload.get('type')} 调用失败: {resp.status_code} {resp.text[:120]}")
    except Exception as exc:
        print(f"Hyperliquid API请求异常({payload.get('type')}): {exc}")
    return None


def get_hyperliquid_supported_bases(force_refresh: bool = False) -> List[str]:
    """返回Hyperliquid当前支持的永续标的，缓存以减少频繁请求。"""
    global _HYPERLIQUID_BASES
    with _HYPERLIQUID_BASES_LOCK:
        if _HYPERLIQUID_BASES and not force_refresh:
            return _HYPERLIQUID_BASES.copy()

    payload = _hyperliquid_post({'type': 'meta'})
    bases: List[str] = []
    if isinstance(payload, dict):
        for entry in payload.get('universe', []):
            base = (entry.get('name') or '').upper()
            if not base or entry.get('isDelisted'):
                continue
            bases.append(base)

    with _HYPERLIQUID_BASES_LOCK:
        if bases:
            _HYPERLIQUID_BASES = sorted(set(bases))
        return _HYPERLIQUID_BASES.copy()


def _refresh_hyperliquid_funding(force: bool = False):
    global _HYPERLIQUID_LAST_FUNDING_REFRESH, _HYPERLIQUID_FUNDING_CACHE
    now = time.time()
    if not force and _HYPERLIQUID_FUNDING_CACHE:
        if (now - _HYPERLIQUID_LAST_FUNDING_REFRESH) < HYPERLIQUID_FUNDING_REFRESH_SECONDS:
            return

    payload = _hyperliquid_post({'type': 'metaAndAssetCtxs'})
    if not isinstance(payload, list) or len(payload) < 2:
        return

    universe = payload[0].get('universe', []) if isinstance(payload[0], dict) else []
    ctxs = payload[1] if isinstance(payload[1], list) else []
    if not universe or not ctxs:
        return

    ctx_map: Dict[str, Dict[str, Any]] = {}
    timestamp = _now_iso()
    for meta_entry, ctx in zip(universe, ctxs):
        base = (meta_entry.get('name') or '').upper()
        if not base or meta_entry.get('isDelisted'):
            continue
        info: Dict[str, Any] = {'timestamp': timestamp}
        funding = _safe_float(ctx.get('funding'))
        if funding is not None:
            info['funding'] = funding
        mark_px = _safe_float(ctx.get('midPx') or ctx.get('markPx') or ctx.get('oraclePx'))
        if mark_px is not None:
            info['markPx'] = mark_px
        ctx_map[base] = info

    if ctx_map:
        with _HYPERLIQUID_FUNDING_LOCK:
            _HYPERLIQUID_FUNDING_CACHE = ctx_map
            _HYPERLIQUID_LAST_FUNDING_REFRESH = now


def get_hyperliquid_funding_map(force_refresh: bool = False) -> Dict[str, Dict[str, Any]]:
    """返回最近一次 funding/markPx 快照。"""
    with _HYPERLIQUID_FUNDING_LOCK:
        has_cache = bool(_HYPERLIQUID_FUNDING_CACHE)
    if force_refresh or not has_cache:
        _refresh_hyperliquid_funding(force_refresh)
    with _HYPERLIQUID_FUNDING_LOCK:
        return dict(_HYPERLIQUID_FUNDING_CACHE)


# -------------------------------
# Lighter market metadata cache
# -------------------------------
_LIGHTER_MARKET_CACHE: Dict[str, Dict[str, Any]] = {}
_LIGHTER_MARKET_LOOKUP: Dict[int, str] = {}
_LIGHTER_MARKET_LOCK = threading.Lock()
_LIGHTER_LAST_REFRESH = 0.0


# -------------------------------
# Bitget funding schedule cache
# -------------------------------
_BITGET_FUNDING_CACHE: Dict[str, Dict[str, Any]] = {}
_BITGET_FUNDING_LOCK = threading.Lock()
_BITGET_SYMBOL_QUEUE: deque[str] = deque()
_BITGET_QUEUE_LOCK = threading.Lock()


# -------------------------------
# OKX funding schedule cache
# -------------------------------
_OKX_FUNDING_CACHE: Dict[str, Dict[str, Any]] = {}
_OKX_FUNDING_LOCK = threading.Lock()
_OKX_SYMBOL_QUEUE: deque[str] = deque()
_OKX_QUEUE_LOCK = threading.Lock()


def _ensure_bitget_symbol_queue(symbols_or_bases: List[str]):
    """Initialize Bitget funding schedule queue from either bases or XXXUSDT symbols."""
    bases: List[str] = []
    for sym in symbols_or_bases or []:
        sym = (sym or '').upper()
        if not sym:
            continue
        if sym.endswith('USDT'):
            bases.append(sym[:-4])
        else:
            bases.append(sym)
    if not bases:
        return

    with _BITGET_QUEUE_LOCK:
        existing = set(_BITGET_SYMBOL_QUEUE)
        for base in bases:
            if base not in existing:
                _BITGET_SYMBOL_QUEUE.append(base)
                existing.add(base)


def _bitget_fetch_schedule(base: str) -> Optional[Dict[str, Any]]:
    base = (base or '').upper()
    if not base:
        return None
    symbol = f"{base}USDT"
    url = (
        "https://api.bitget.com/api/v2/mix/market/current-fund-rate"
        f"?productType=USDT-FUTURES&symbol={symbol}"
    )
    try:
        payload = _req_json(url)
    except Exception:
        return None
    if not isinstance(payload, dict) or payload.get('code') != '00000':
        return None
    data = payload.get('data') or []
    if not data or not isinstance(data, list):
        return None
    entry = data[0] if isinstance(data[0], dict) else None
    if not entry:
        return None

    schedule: Dict[str, Any] = {'timestamp': _now_iso()}
    next_ft = normalize_next_funding_time(entry.get('nextUpdate'))
    if next_ft:
        schedule['next_funding_time'] = next_ft
    interval = derive_funding_interval_hours('bitget', entry.get('fundingRateInterval'), fallback=False)
    if interval:
        schedule['funding_interval_hours'] = float(interval)
    return schedule if len(schedule) > 1 else None


def _get_bitget_funding_schedule_map(
    symbols_or_bases: Optional[List[str]] = None, *, batch_size: int = 32
) -> Dict[str, Dict[str, Any]]:
    """Return cached funding schedule, refreshing a small batch each call."""
    if symbols_or_bases:
        _ensure_bitget_symbol_queue(symbols_or_bases)

    refreshed: Dict[str, Dict[str, Any]] = {}
    with _BITGET_QUEUE_LOCK:
        for _ in range(min(int(batch_size), len(_BITGET_SYMBOL_QUEUE))):
            base = _BITGET_SYMBOL_QUEUE.popleft()
            _BITGET_SYMBOL_QUEUE.append(base)
            schedule = _bitget_fetch_schedule(base)
            if schedule:
                refreshed[base] = schedule

    if refreshed:
        with _BITGET_FUNDING_LOCK:
            _BITGET_FUNDING_CACHE.update(refreshed)

    with _BITGET_FUNDING_LOCK:
        return dict(_BITGET_FUNDING_CACHE)


def _ensure_okx_symbol_queue(bases: List[str]):
    bases = [(b or '').upper() for b in (bases or []) if (b or '').strip()]
    if not bases:
        return
    with _OKX_QUEUE_LOCK:
        existing = set(_OKX_SYMBOL_QUEUE)
        for base in bases:
            if base not in existing:
                _OKX_SYMBOL_QUEUE.append(base)
                existing.add(base)


def _okx_fetch_schedule(base: str) -> Optional[Dict[str, Any]]:
    base = (base or '').upper()
    if not base:
        return None
    inst_id = f"{base}-USDT-SWAP"
    url = f"https://www.okx.com/api/v5/public/funding-rate?instId={inst_id}"
    try:
        payload = _req_json(url)
    except Exception:
        return None
    if not isinstance(payload, dict) or payload.get('code') != '0':
        return None
    data = payload.get('data') or []
    if not data or not isinstance(data, list) or not isinstance(data[0], dict):
        return None
    entry = data[0]

    schedule: Dict[str, Any] = {'timestamp': _now_iso()}
    next_ft = normalize_next_funding_time(entry.get('nextFundingTime'))
    if next_ft:
        schedule['next_funding_time'] = next_ft
    interval = derive_interval_hours_from_times(entry.get('fundingTime'), entry.get('nextFundingTime'))
    if interval:
        schedule['funding_interval_hours'] = float(interval)
    return schedule if len(schedule) > 1 else None


def _get_okx_funding_schedule_map(bases: Optional[List[str]] = None, *, batch_size: int = 16) -> Dict[str, Dict[str, Any]]:
    if bases:
        _ensure_okx_symbol_queue(bases)

    refreshed: Dict[str, Dict[str, Any]] = {}
    with _OKX_QUEUE_LOCK:
        for _ in range(min(int(batch_size), len(_OKX_SYMBOL_QUEUE))):
            base = _OKX_SYMBOL_QUEUE.popleft()
            _OKX_SYMBOL_QUEUE.append(base)
            schedule = _okx_fetch_schedule(base)
            if schedule:
                refreshed[base] = schedule

    if refreshed:
        with _OKX_FUNDING_LOCK:
            _OKX_FUNDING_CACHE.update(refreshed)

    with _OKX_FUNDING_LOCK:
        return dict(_OKX_FUNDING_CACHE)


def _lighter_base_url() -> str:
    return LIGHTER_REST_BASE_URL.rstrip('/')


def _lighter_api(path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    url = f"{_lighter_base_url()}/{path.lstrip('/')}"
    try:
        resp = requests.get(
            url,
            params=params,
            headers=_rest_headers(),
            timeout=REST_CONNECTION_CONFIG.get('timeout', 10),
        )
        if resp.status_code == 200:
            return resp.json()
        print(f"Lighter API调用失败 {resp.status_code}: {resp.text[:120]}")
    except Exception as exc:
        print(f"Lighter API请求错误: {exc}")
    return None


def _refresh_lighter_markets(force: bool = False):
    global _LIGHTER_LAST_REFRESH, _LIGHTER_MARKET_CACHE, _LIGHTER_MARKET_LOOKUP
    now = time.time()
    if not force and _LIGHTER_MARKET_CACHE and (now - _LIGHTER_LAST_REFRESH) < LIGHTER_MARKET_REFRESH_SECONDS:
        return

    payload = _lighter_api('orderBooks')
    if not payload or 'order_books' not in payload:
        return

    markets_by_symbol: Dict[str, Dict[str, Any]] = {}
    lookup: Dict[int, str] = {}
    for entry in payload.get('order_books', []):
        symbol = (entry.get('symbol') or '').upper()
        market_id = entry.get('market_id')
        status = entry.get('status', '').lower()
        if not symbol or market_id is None or status not in ('active', ''):
            continue
        markets_by_symbol[symbol] = entry
        try:
            lookup[int(market_id)] = symbol
        except (TypeError, ValueError):
            continue

    if markets_by_symbol:
        _LIGHTER_MARKET_CACHE = markets_by_symbol
        _LIGHTER_MARKET_LOOKUP = lookup
        _LIGHTER_LAST_REFRESH = now


def get_lighter_market_info(force_refresh: bool = False) -> Dict[str, Dict[str, Any]]:
    with _LIGHTER_MARKET_LOCK:
        _refresh_lighter_markets(force_refresh)
        return dict(_LIGHTER_MARKET_CACHE)


def get_lighter_market_lookup(force_refresh: bool = False) -> Dict[int, str]:
    with _LIGHTER_MARKET_LOCK:
        _refresh_lighter_markets(force_refresh)
        return dict(_LIGHTER_MARKET_LOOKUP)


def get_lighter_supported_bases(force_refresh: bool = False) -> List[str]:
    info = get_lighter_market_info(force_refresh)
    return sorted(info.keys())


def _bybit_observed_to_coin(observed_base: str) -> str:
    """Bybit返回的基础符号映射为统一币种名。
    目标：确保与 NEIRO 相关的品种归并到 NEIROETH。
    例如：NEIROETH -> NEIROETH；NEIRO -> NEIROETH；其它保持不变。
    """
    alias_to_coin = {
        'NEIRO': 'NEIROETH',
        'NEIROETH': 'NEIROETH',
        'APP': 'APPbybit',
        'APPbybit': 'APPbybit',
        'APPBYBIT': 'APPbybit',
    }
    return alias_to_coin.get(observed_base, observed_base)


def fetch_binance() -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Fetch Binance spot/futures 24h tickers and normalize to base->type maps.
    过滤指数合约和清算中合约，避免污染币种价格比较."""
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    ts = _now_iso()
    interval_map = _get_binance_funding_interval_map()
    premium_meta: Dict[str, Dict[str, Any]] = {}

    try:
        premium_payload = _req_json('https://fapi.binance.com/fapi/v1/premiumIndex')
        if isinstance(premium_payload, list):
            for item in premium_payload:
                sym = item.get('symbol', '')
                if not sym.endswith('USDT'):
                    continue
                base = sym[:-4]
                extras: Dict[str, Any] = {}
                funding = normalize_funding_rate(item.get('lastFundingRate'))
                if funding is not None:
                    extras['funding_rate'] = funding
                next_ft = normalize_next_funding_time(item.get('nextFundingTime'))
                if next_ft:
                    extras['next_funding_time'] = next_ft
                if extras:
                    premium_meta[base] = extras
    except Exception as exc:
        print(f"Binance资金费率REST获取失败: {exc}")
    
    # 获取有效的符号列表（失败或为空则退化为不过滤）
    valid_symbols = binance_filter.get_valid_symbols()
    has_spot_filter = bool(valid_symbols.get('spot'))
    has_fut_filter = bool(valid_symbols.get('futures'))
    
    try:
        data = _req_json('https://api.binance.com/api/v3/ticker/24hr')
        if isinstance(data, list):
            for item in data:
                sym = item.get('symbol', '')
                if sym.endswith('USDT') and (not has_spot_filter or sym in valid_symbols['spot']):
                    base = sym[:-4]
                    price = float(item.get('lastPrice') or item.get('c') or 0)  # lastPrice preferred
                    if price:
                        out.setdefault(base, {}).setdefault('spot', {})
                        out[base]['spot'] = {'price': price, 'timestamp': ts, 'symbol': sym}
    except Exception as e:
        print(f"Binance现货REST获取失败: {e}")

    try:
        data = _req_json('https://fapi.binance.com/fapi/v1/ticker/24hr')
        if isinstance(data, list):
            filtered_count = 0
            for item in data:
                sym = item.get('symbol', '')
                if sym.endswith('USDT'):
                    if (not has_fut_filter) or (sym in valid_symbols['futures']):
                        # 有效的期货合约
                        base = sym[:-4]
                        price = float(item.get('lastPrice') or item.get('c') or 0)
                        if price:
                            out.setdefault(base, {}).setdefault('futures', {})
                            futures_snapshot = {
                                'price': price,
                                'timestamp': ts,
                                'symbol': sym
                            }
                            extras = premium_meta.get(base)
                            if extras:
                                futures_snapshot.update(extras)
                            interval = interval_map.get(base)
                            if interval:
                                futures_snapshot['funding_interval_hours'] = float(interval)
                            out[base]['futures'] = futures_snapshot
                    elif has_fut_filter and sym in valid_symbols['futures_invalid']:
                        # 过滤的无效合约（指数/清算中）
                        filtered_count += 1
            
            if filtered_count > 0:
                print(f"Binance期货REST已过滤 {filtered_count} 个指数/清算合约")
                
    except Exception as e:
        print(f"Binance期货REST获取失败: {e}")

    # 补充仅有资金费率数据但暂未获取到价格的标的
    for base, extras in premium_meta.items():
        entry = out.setdefault(base, {}).setdefault('futures', {})
        if 'timestamp' not in entry:
            entry['timestamp'] = ts
        entry.update(extras)
        interval = interval_map.get(base)
        if interval and 'funding_interval_hours' not in entry:
            entry['funding_interval_hours'] = float(interval)
        
    return out


def fetch_okx() -> Dict[str, Dict[str, Dict[str, Any]]]:
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    ts = _now_iso()
    allowed = set(CURRENT_SUPPORTED_SYMBOLS or [])
    try:
        data = _req_json('https://www.okx.com/api/v5/market/tickers?instType=SPOT')
        if isinstance(data, dict) and data.get('code') == '0':
            for item in data.get('data', []):
                inst_id = item.get('instId', '')  # e.g., BTC-USDT
                if inst_id.endswith('-USDT'):
                    base = inst_id.split('-')[0]
                    if allowed and base not in allowed:
                        continue
                    price = float(item.get('last') or 0)
                    if price:
                        out.setdefault(base, {}).setdefault('spot', {})
                        out[base]['spot'] = {'price': price, 'timestamp': ts, 'symbol': inst_id}
    except Exception:
        pass

    try:
        data = _req_json('https://www.okx.com/api/v5/market/tickers?instType=SWAP')
        if isinstance(data, dict) and data.get('code') == '0':
            bases_seen: List[str] = []
            for item in data.get('data', []):
                inst_id = item.get('instId', '')  # e.g., BTC-USDT-SWAP
                if inst_id.endswith('-USDT-SWAP'):
                    base = inst_id.split('-')[0]
                    if allowed and base not in allowed:
                        continue
                    price = float(item.get('last') or 0)
                    if price:
                        bases_seen.append(base)
                        out.setdefault(base, {}).setdefault('futures', {})
                        snapshot = {'price': price, 'timestamp': ts, 'symbol': inst_id}
                        out[base]['futures'] = snapshot
            if bases_seen:
                schedule_map = _get_okx_funding_schedule_map(bases_seen, batch_size=24)
                for base in bases_seen:
                    schedule = schedule_map.get(base)
                    if not schedule:
                        continue
                    snap = out.get(base, {}).get('futures')
                    if not isinstance(snap, dict):
                        continue
                    if schedule.get('next_funding_time'):
                        snap['next_funding_time'] = schedule['next_funding_time']
                    if schedule.get('funding_interval_hours'):
                        snap['funding_interval_hours'] = schedule['funding_interval_hours']
                # 对本轮出现的 base 做少量补齐，避免 queue 未覆盖导致短期缺失
                missing = [
                    b for b in bases_seen
                    if isinstance(out.get(b, {}).get('futures'), dict)
                    and (
                        'next_funding_time' not in out[b]['futures']
                        or 'funding_interval_hours' not in out[b]['futures']
                    )
                ]
                for base in missing[:6]:
                    schedule = _okx_fetch_schedule(base)
                    if not schedule:
                        continue
                    snap = out.get(base, {}).get('futures')
                    if not isinstance(snap, dict):
                        continue
                    if schedule.get('next_funding_time'):
                        snap['next_funding_time'] = schedule['next_funding_time']
                    if schedule.get('funding_interval_hours'):
                        snap['funding_interval_hours'] = schedule['funding_interval_hours']
                    with _OKX_FUNDING_LOCK:
                        _OKX_FUNDING_CACHE[base] = schedule
    except Exception:
        pass
    return out


def fetch_bybit() -> Dict[str, Dict[str, Dict[str, Any]]]:
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    ts = _now_iso()
    try:
        data = _req_json('https://api.bybit.com/v5/market/tickers?category=spot')
        if isinstance(data, dict) and data.get('retCode') == 0:
            for item in data.get('result', {}).get('list', []):
                sym = item.get('symbol', '')
                if sym.endswith('USDT'):
                    base = sym[:-4]
                    coin = _bybit_observed_to_coin(base)
                    price = float(item.get('lastPrice') or 0)
                    if price:
                        out.setdefault(coin, {}).setdefault('spot', {})
                        out[coin]['spot'] = {'price': price, 'timestamp': ts, 'symbol': sym}
    except Exception:
        pass

    try:
        data = _req_json('https://api.bybit.com/v5/market/tickers?category=linear')
        if isinstance(data, dict) and data.get('retCode') == 0:
            for item in data.get('result', {}).get('list', []):
                sym = item.get('symbol', '')
                if sym.endswith('USDT'):
                    base = sym[:-4]
                    coin = _bybit_observed_to_coin(base)
                    price = float(item.get('lastPrice') or 0)
                    if price:
                        out.setdefault(coin, {}).setdefault('futures', {})

                        # 捕获Bybit资金费率，REST返回数据即为权威值
                        raw_funding = item.get('fundingRate')
                        funding_rate_value = normalize_funding_rate(raw_funding)

                        futures_snapshot = {
                            'price': price,
                            'timestamp': ts,
                            'symbol': sym
                        }

                        if funding_rate_value is not None:
                            futures_snapshot['funding_rate'] = funding_rate_value

                        next_funding_time = normalize_next_funding_time(item.get('nextFundingTime'))
                        if next_funding_time:
                            futures_snapshot['next_funding_time'] = next_funding_time

                        interval = derive_funding_interval_hours('bybit', item.get('fundingIntervalHour'))
                        if interval:
                            futures_snapshot['funding_interval_hours'] = interval

                        out[coin]['futures'] = futures_snapshot
    except Exception:
        pass
    return out


# GRVT helper：通过官方 grvt-pysdk 访问 REST/WS
# SDK 会负责签名与 IP 白名单校验，我们在这里缓存 client 并轮询 ticker。
_GRVT_CLIENT_LOCK = threading.Lock()
_GRVT_CLIENT: Optional["GrvtCcxt"] = None
_GRVT_SYMBOL_QUEUE: deque[str] = deque()
_GRVT_BASE_SYMBOLS: List[str] = []


def _get_grvt_env_enum():
    if GrvtEnv is None:
        return None
    env = (GRVT_ENVIRONMENT or 'prod').lower()
    mapping = {
        'prod': GrvtEnv.PROD,
        'testnet': GrvtEnv.TESTNET,
        'staging': GrvtEnv.STAGING,
        'dev': GrvtEnv.DEV,
    }
    return mapping.get(env, GrvtEnv.PROD)


def _get_grvt_client():
    global _GRVT_CLIENT
    if GrvtCcxt is None:
        return None
    if not GRVT_API_KEY:
        return None
    env_enum = _get_grvt_env_enum()
    if env_enum is None:
        return None
    with _GRVT_CLIENT_LOCK:
        if _GRVT_CLIENT is None:
            params = {'api_key': GRVT_API_KEY}
            if GRVT_TRADING_ACCOUNT_ID:
                params['trading_account_id'] = GRVT_TRADING_ACCOUNT_ID
            if GRVT_API_SECRET:
                params['private_key'] = GRVT_API_SECRET
            try:
                _GRVT_CLIENT = GrvtCcxt(env=env_enum, parameters=params)
            except Exception as exc:
                print(f"GRVT REST客户端初始化失败: {exc}")
                return None
    return _GRVT_CLIENT


def get_grvt_supported_bases(refresh: bool = False) -> List[str]:
    """
    Retrieve active GRVT USDT perpetual bases via the official SDK.
    Results are cached in-memory unless refresh=True.
    """
    global _GRVT_BASE_SYMBOLS
    if _GRVT_BASE_SYMBOLS and not refresh:
        return _GRVT_BASE_SYMBOLS.copy()

    client = _get_grvt_client()
    if client is None:
        return []

    try:
        markets = client.fetch_markets()
    except Exception as exc:
        print(f"获取GRVT市场列表失败: {exc}")
        return _GRVT_BASE_SYMBOLS.copy()

    bases: List[str] = []
    for market in markets or []:
        quote = (market.get('quote') or '').upper()
        kind = (market.get('kind') or '').upper()
        base = (market.get('base') or '').upper()
        if not base or quote != 'USDT':
            continue
        if kind not in ('PERPETUAL', 'SWAP'):
            continue
        if base not in bases:
            bases.append(base)

    if bases:
        _GRVT_BASE_SYMBOLS = bases
    return _GRVT_BASE_SYMBOLS.copy()


def _ensure_grvt_symbol_queue():
    if _GRVT_SYMBOL_QUEUE:
        return
    symbols = get_grvt_supported_bases() or CURRENT_SUPPORTED_SYMBOLS or []
    if not symbols:
        return
    for symbol in symbols:
        _GRVT_SYMBOL_QUEUE.append(symbol)


def _next_grvt_batch(limit: int) -> List[str]:
    if not _GRVT_SYMBOL_QUEUE:
        return []
    limit = max(1, limit)
    batch: List[str] = []
    for _ in range(min(limit, len(_GRVT_SYMBOL_QUEUE))):
        sym = _GRVT_SYMBOL_QUEUE.popleft()
        batch.append(sym)
        _GRVT_SYMBOL_QUEUE.append(sym)
    return batch


def _build_grvt_instrument(symbol: str, market_type: str = 'futures') -> str:
    base = symbol.upper()
    if market_type == 'spot':
        return f"{base}_USDT"
    return f"{base}_USDT_Perp"


def _decode_grvt_price(value) -> Optional[float]:
    if value in (None, ''):
        return None
    try:
        # textual float already scaled (e.g., "0.0123")
        if isinstance(value, str) and '.' in value:
            return float(value)
        return float(value) / 1e9
    except (TypeError, ValueError):
        return None


def _decode_grvt_funding(value) -> Optional[float]:
    if value in (None, ''):
        return None
    try:
        funding = float(value)
    except (TypeError, ValueError):
        return None
    if abs(funding) > 1:
        funding = funding / 1e8
    return funding


def fetch_grvt() -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Fetch GRVT tickers via authenticated REST calls using the official SDK.
    SDK 会向 `market-data.<env>.grvt.io` 发起 `full/v1/ticker` 请求，
    因此必须提前配置 API Key/白名单；这里按批次轮询避免触发限频。
    """
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    client = _get_grvt_client()
    if client is None:
        return out

    _ensure_grvt_symbol_queue()
    if not _GRVT_SYMBOL_QUEUE:
        return out

    ts = _now_iso()
    symbols = _next_grvt_batch(GRVT_REST_SYMBOLS_PER_CALL)
    for symbol in symbols:
        instrument = _build_grvt_instrument(symbol)
        try:
            ticker = client.fetch_ticker(instrument)
        except Exception as exc:
            print(f"GRVT REST获取 {instrument} 失败: {exc}")
            continue
        if not ticker:
            continue

        price = (
            _decode_grvt_price(ticker.get('mark_price'))
            or _decode_grvt_price(ticker.get('last_price'))
        )
        if not price:
            continue

        snapshot: Dict[str, Any] = {
            'price': price,
            'timestamp': ts,
            'symbol': instrument
        }
        interval = derive_funding_interval_hours('grvt')
        if interval:
            snapshot['funding_interval_hours'] = interval
        funding = _decode_grvt_funding(
            ticker.get('funding_rate_8h_curr')
            or ticker.get('funding_rate_curr')
            or ticker.get('funding_rate')
        )
        normalized_funding = normalize_funding_rate(funding, assume_percent=True)
        if normalized_funding is not None:
            snapshot['funding_rate'] = normalized_funding

        next_ft = normalize_next_funding_time(
            ticker.get('next_funding_time') or ticker.get('nextFundingTime')
        )
        if next_ft:
            snapshot['next_funding_time'] = next_ft
        else:
            snapshot['next_funding_time'] = _next_utc_boundary_iso(int(interval or 8))

        out.setdefault(symbol, {})['futures'] = snapshot

    return out


def fetch_hyperliquid() -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Fetch Hyperliquid perp mid prices and funding data via /info."""
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    payload = _hyperliquid_post({'type': 'allMids'})
    if not isinstance(payload, dict):
        return out

    allowed = set(get_hyperliquid_supported_bases()) or set(CURRENT_SUPPORTED_SYMBOLS or [])
    funding_map = get_hyperliquid_funding_map()
    ts = _now_iso()

    for base, raw_price in payload.items():
        base_upper = (base or '').upper()
        if not base_upper or '/' in base_upper or base_upper.startswith('@'):
            continue
        if allowed and base_upper not in allowed:
            continue

        price = _safe_float(raw_price)
        if price is None:
            continue

        snapshot: Dict[str, Any] = {
            'price': price,
            'timestamp': ts,
            'symbol': f"{base_upper}-PERP"
        }
        interval = derive_funding_interval_hours('hyperliquid')
        if interval:
            snapshot['funding_interval_hours'] = interval
        snapshot['next_funding_time'] = _next_utc_boundary_iso(int(interval or 1))

        ctx = funding_map.get(base_upper)
        if ctx:
            funding = ctx.get('funding')
            normalized_funding = normalize_funding_rate(funding)
            if normalized_funding is not None:
                snapshot['funding_rate'] = normalized_funding
            mark_px = ctx.get('markPx')
            if mark_px is not None:
                snapshot['mark_price'] = mark_px
            ctx_ts = ctx.get('timestamp')
            if ctx_ts:
                snapshot['context_timestamp'] = ctx_ts

        out.setdefault(base_upper, {})['futures'] = snapshot

    return out


def fetch_lighter() -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Fetch Lighter exchange statistics (futures only)."""
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    payload = _lighter_api('exchangeStats')
    if not payload or payload.get('code') != 200:
        return out

    ts = _now_iso()
    market_info = get_lighter_market_info()
    for entry in payload.get('order_book_stats', []):
        symbol = (entry.get('symbol') or '').upper()
        if not symbol:
            continue
        price = _safe_float(entry.get('last_trade_price') or entry.get('mark_price'))
        if price is None:
            continue

        snapshot: Dict[str, Any] = {
            'price': price,
            'timestamp': ts,
            'symbol': f"{symbol}-PERP"
        }

        mark_price = _safe_float(entry.get('mark_price'))
        if mark_price is not None:
            snapshot['mark_price'] = mark_price

        index_price = _safe_float(entry.get('index_price'))
        if index_price is not None:
            snapshot['index_price'] = index_price

        funding = _safe_float(entry.get('current_funding_rate') or entry.get('funding_rate'))
        normalized_funding = normalize_funding_rate(funding, assume_percent=True)
        if normalized_funding is not None:
            snapshot['funding_rate'] = normalized_funding

        funding_ts = normalize_next_funding_time(
            entry.get('funding_timestamp') or entry.get('next_funding_time')
        )
        if funding_ts:
            snapshot['next_funding_time'] = funding_ts

        interval = derive_funding_interval_hours('lighter')
        if interval:
            snapshot['funding_interval_hours'] = interval
        snapshot['next_funding_time'] = _next_utc_boundary_iso(int(interval or 1))

        market_meta = market_info.get(symbol)
        if market_meta:
            snapshot['market_id'] = market_meta.get('market_id')

        out.setdefault(symbol, {}).setdefault('futures', snapshot)

    return out


def fetch_bitget() -> Dict[str, Dict[str, Dict[str, Any]]]:
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    ts = _now_iso()
    
    # 获取有效的符号列表（失败或为空则退化为不过滤）
    valid_symbols = bitget_filter.get_valid_symbols()
    has_spot_filter = bool(valid_symbols.get('spot'))
    has_fut_filter = bool(valid_symbols.get('futures'))
    
    try:
        data = _req_json('https://api.bitget.com/api/v2/spot/market/tickers')
        if isinstance(data, dict) and data.get('code') == '00000':
            filtered_count = 0
            for item in data.get('data', []):
                sym = item.get('symbol', '')  # e.g., BTCUSDT
                if sym.endswith('USDT'):
                    if (not has_spot_filter) or (sym in valid_symbols['spot']):
                        base = sym[:-4]
                        price = float(item.get('lastPr') or 0)
                        if price:
                            out.setdefault(base, {}).setdefault('spot', {})
                            out[base]['spot'] = {'price': price, 'timestamp': ts, 'symbol': sym}
                    elif has_spot_filter:
                        # 被过滤的符号
                        filtered_count += 1
            
            if filtered_count > 0:
                print(f"Bitget现货REST已过滤 {filtered_count} 个暂停/异常币种")
                
    except Exception:
        pass

    try:
        data = _req_json('https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES')
        if isinstance(data, dict) and data.get('code') == '00000':
            filtered_count = 0
            bases_seen: List[str] = []
            for item in data.get('data', []):
                sym = item.get('symbol', '')  # e.g., BTCUSDT
                if sym.endswith('USDT'):
                    if (not has_fut_filter) or (sym in valid_symbols['futures']):
                        base = sym[:-4]
                        price = float(item.get('lastPr') or 0)
                        if price:
                            bases_seen.append(base)
                            out.setdefault(base, {}).setdefault('futures', {})
                            snapshot = {'price': price, 'timestamp': ts, 'symbol': sym}
                            out[base]['futures'] = snapshot
                    elif has_fut_filter:
                        filtered_count += 1
            
            if filtered_count > 0:
                print(f"Bitget期货REST已过滤 {filtered_count} 个异常币种")
            if bases_seen:
                schedule_map = _get_bitget_funding_schedule_map(bases_seen, batch_size=32)
                for base in bases_seen:
                    schedule = schedule_map.get(base)
                    if not schedule:
                        continue
                    snap = out.get(base, {}).get('futures')
                    if not isinstance(snap, dict):
                        continue
                    if schedule.get('next_funding_time'):
                        snap['next_funding_time'] = schedule['next_funding_time']
                    if schedule.get('funding_interval_hours'):
                        snap['funding_interval_hours'] = schedule['funding_interval_hours']
                # 对本轮出现的 base 做少量补齐，避免 queue 未覆盖导致短期缺失
                missing = [
                    b for b in bases_seen
                    if isinstance(out.get(b, {}).get('futures'), dict)
                    and (
                        'next_funding_time' not in out[b]['futures']
                        or 'funding_interval_hours' not in out[b]['futures']
                    )
                ]
                for base in missing[:8]:
                    schedule = _bitget_fetch_schedule(base)
                    if not schedule:
                        continue
                    snap = out.get(base, {}).get('futures')
                    if not isinstance(snap, dict):
                        continue
                    if schedule.get('next_funding_time'):
                        snap['next_funding_time'] = schedule['next_funding_time']
                    if schedule.get('funding_interval_hours'):
                        snap['funding_interval_hours'] = schedule['funding_interval_hours']
                    with _BITGET_FUNDING_LOCK:
                        _BITGET_FUNDING_CACHE[base] = schedule
                
    except Exception:
        pass
    return out


def fetch_all_exchanges() -> Dict[str, Dict[str, Dict[str, Dict[str, Any]]]]:
    """
    Return structure: {exchange: {base_symbol: {'spot': {...}, 'futures': {...}}}}
    Only fields present in snapshot are included; funding_rate is not provided here.
    """
    results: Dict[str, Dict[str, Dict[str, Dict[str, Any]]]] = {}

    results['binance'] = fetch_binance()
    time.sleep(REST_CONNECTION_CONFIG.get('stagger_ms', 200) / 1000.0)
    results['okx'] = fetch_okx()
    time.sleep(REST_CONNECTION_CONFIG.get('stagger_ms', 200) / 1000.0)
    results['bybit'] = fetch_bybit()
    time.sleep(REST_CONNECTION_CONFIG.get('stagger_ms', 200) / 1000.0)
    results['bitget'] = fetch_bitget()
    time.sleep(REST_CONNECTION_CONFIG.get('stagger_ms', 200) / 1000.0)
    results['grvt'] = fetch_grvt()
    time.sleep(REST_CONNECTION_CONFIG.get('stagger_ms', 200) / 1000.0)
    results['lighter'] = fetch_lighter()
    time.sleep(REST_CONNECTION_CONFIG.get('stagger_ms', 200) / 1000.0)
    results['hyperliquid'] = fetch_hyperliquid()

    return results
