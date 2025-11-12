"""
Bitget 符号过滤器
- 缓存交易所信息，过滤暂停交易(halt)和其他非正常状态的币种
- 避免暂停交易的币种污染价格比较
"""

import time
import requests
from typing import Dict, Set


class _BitgetSymbolFilter:
    def __init__(self):
        self._cache_ttl = 1800  # 30分钟
        self._last_refresh = 0.0
        self._spot_valid: Set[str] = set()       # e.g., BTCUSDT
        self._futures_valid: Set[str] = set()    # e.g., BTCUSDT
        
    def _maybe_refresh(self):
        now = time.time()
        if now - self._last_refresh < self._cache_ttl:
            return
        try:
            self._refresh_symbols()
        except Exception:
            # 网络或解析失败，不更新
            pass
        self._last_refresh = now
    
    def _refresh_symbols(self):
        # 获取现货交易对信息
        url = 'https://api.bitget.com/api/v2/spot/public/symbols'
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        spot_valid: Set[str] = set()
        if data.get('code') == '00000':
            for item in data.get('data', []):
                symbol = item.get('symbol', '')
                status = item.get('status', '')
                quote_coin = item.get('quoteCoin', '')
                
                # 只接受正常交易状态的USDT对
                if (symbol.endswith('USDT') and 
                    quote_coin == 'USDT' and 
                    status == 'online'):  # online表示正常交易
                    spot_valid.add(symbol)
                elif status == 'halt' and symbol.endswith('USDT'):
                    print(f"Bitget过滤暂停交易现货: {symbol} (状态: {status})")
        
        self._spot_valid = spot_valid
        
        # 获取期货交易对信息（如果需要的话）
        try:
            url = 'https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES'
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            
            futures_valid: Set[str] = set()
            if data.get('code') == '00000':
                for item in data.get('data', []):
                    symbol = item.get('symbol', '')
                    status = item.get('symbolStatus', '')  # Bitget期货使用symbolStatus字段
                    
                    if (symbol.endswith('USDT') and 
                        status == 'normal'):  # normal表示正常交易
                        futures_valid.add(symbol)
                    elif status != 'normal' and symbol.endswith('USDT'):
                        print(f"Bitget过滤非正常期货: {symbol} (状态: {status})")
            
            self._futures_valid = futures_valid
        except Exception:
            # 期货信息获取失败，保持空集合
            self._futures_valid = set()
    
    def get_valid_symbols(self) -> Dict[str, Set[str]]:
        self._maybe_refresh()
        return {
            'spot': set(self._spot_valid),
            'futures': set(self._futures_valid),
        }


# 全局实例
bitget_filter = _BitgetSymbolFilter()