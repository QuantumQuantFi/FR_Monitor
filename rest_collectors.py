"""
REST snapshot collectors for exchanges.
Fetches spot/futures tickers in bulk and returns normalized maps.
Uses lightweight requests with timeouts; avoids asyncio for simplicity.
"""

from typing import Dict, Any, List
from datetime import datetime, timezone
import time
import requests

from config import REST_CONNECTION_CONFIG
from binance_contract_filter import binance_filter
from bitget_symbol_filter import bitget_filter


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _req_json(url: str) -> Any:
    timeout = REST_CONNECTION_CONFIG.get('timeout', 10)
    headers = {'User-Agent': REST_CONNECTION_CONFIG.get('user_agent', 'WLFI-Monitor/1.0')}
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
                        out[coin]['futures'] = {'price': price, 'timestamp': ts, 'symbol': sym}
    except Exception:
        pass
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
