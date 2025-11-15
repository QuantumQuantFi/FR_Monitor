"""
REST snapshot collectors for exchanges.
Fetches spot/futures tickers in bulk and returns normalized maps.
Uses lightweight requests with timeouts; avoids asyncio for simplicity.
"""

from collections import deque
from datetime import datetime, timezone
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
    REST_CONNECTION_CONFIG,
)
from binance_contract_filter import binance_filter
from bitget_symbol_filter import bitget_filter


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
                            # funding not present here; keep if already set by WS
                            out[base]['futures'] = {
                                'price': price,
                                'timestamp': ts,
                                'symbol': sym
                            }
                    elif has_fut_filter and sym in valid_symbols['futures_invalid']:
                        # 过滤的无效合约（指数/清算中）
                        filtered_count += 1
            
            if filtered_count > 0:
                print(f"Binance期货REST已过滤 {filtered_count} 个指数/清算合约")
                
    except Exception as e:
        print(f"Binance期货REST获取失败: {e}")
        
    return out


def fetch_okx() -> Dict[str, Dict[str, Dict[str, Any]]]:
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    ts = _now_iso()
    try:
        data = _req_json('https://www.okx.com/api/v5/market/tickers?instType=SPOT')
        if isinstance(data, dict) and data.get('code') == '0':
            for item in data.get('data', []):
                inst_id = item.get('instId', '')  # e.g., BTC-USDT
                if inst_id.endswith('-USDT'):
                    base = inst_id.split('-')[0]
                    price = float(item.get('last') or 0)
                    if price:
                        out.setdefault(base, {}).setdefault('spot', {})
                        out[base]['spot'] = {'price': price, 'timestamp': ts, 'symbol': inst_id}
    except Exception:
        pass

    try:
        data = _req_json('https://www.okx.com/api/v5/market/tickers?instType=SWAP')
        if isinstance(data, dict) and data.get('code') == '0':
            for item in data.get('data', []):
                inst_id = item.get('instId', '')  # e.g., BTC-USDT-SWAP
                if inst_id.endswith('-USDT-SWAP'):
                    base = inst_id.split('-')[0]
                    price = float(item.get('last') or 0)
                    if price:
                        out.setdefault(base, {}).setdefault('futures', {})
                        out[base]['futures'] = {'price': price, 'timestamp': ts, 'symbol': inst_id}
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
                        funding_rate_value = None
                        raw_funding = item.get('fundingRate')
                        if raw_funding not in (None, ''):
                            try:
                                funding_rate_value = float(raw_funding)
                            except (TypeError, ValueError):
                                funding_rate_value = None

                        futures_snapshot = {
                            'price': price,
                            'timestamp': ts,
                            'symbol': sym
                        }

                        if funding_rate_value is not None:
                            futures_snapshot['funding_rate'] = funding_rate_value

                        next_funding_time = item.get('nextFundingTime')
                        if next_funding_time:
                            futures_snapshot['next_funding_time'] = next_funding_time

                        out[coin]['futures'] = futures_snapshot
    except Exception:
        pass
    return out


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
    Rotates through supported symbols to avoid rate limits.
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
        funding = _decode_grvt_funding(
            ticker.get('funding_rate_8h_curr')
            or ticker.get('funding_rate_curr')
            or ticker.get('funding_rate')
        )
        if funding is not None:
            snapshot['funding_rate'] = funding

        out.setdefault(symbol, {})['futures'] = snapshot

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
            for item in data.get('data', []):
                sym = item.get('symbol', '')  # e.g., BTCUSDT
                if sym.endswith('USDT'):
                    if (not has_fut_filter) or (sym in valid_symbols['futures']):
                        base = sym[:-4]
                        price = float(item.get('lastPr') or 0)
                        if price:
                            out.setdefault(base, {}).setdefault('futures', {})
                            out[base]['futures'] = {'price': price, 'timestamp': ts, 'symbol': sym}
                    elif has_fut_filter:
                        filtered_count += 1
            
            if filtered_count > 0:
                print(f"Bitget期货REST已过滤 {filtered_count} 个异常币种")
                
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

    return results
