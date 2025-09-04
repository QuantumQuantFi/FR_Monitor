"""
Binance 合约过滤器
 - 缓存 exchangeInfo，过滤指数合约(INDEX)与非TRADING(如 SETTLING)合约
 - 返回可用于 REST 快照过滤的有效符号集合

注意：
 - 依赖网络请求（运行时环境需可访问 Binance API）
 - 异常时回退为空过滤（不筛掉任何符号，避免影响主流程）
"""

from __future__ import annotations

import time
from typing import Dict, Set
import requests


class _BinanceContractFilter:
    def __init__(self):
        self._cache_ttl = 1800  # 30分钟
        self._last_refresh = 0.0
        self._spot_valid: Set[str] = set()       # e.g., BTCUSDT
        self._futures_valid: Set[str] = set()    # e.g., BTCUSDT
        self._futures_invalid: Set[str] = set()  # 指数/清算中 合约

        # 额外的静态排除（兜底）
        self._denylist_bases = {
            'DEFI', 'BTCDOM', 'ALL', 'BLUEBIRD'
        }

    def _maybe_refresh(self):
        now = time.time()
        if now - self._last_refresh < self._cache_ttl:
            return
        try:
            self._refresh_spot()
        except Exception:
            # 网络或解析失败，不更新
            pass
        try:
            self._refresh_futures()
        except Exception:
            pass
        self._last_refresh = now

    def _refresh_spot(self):
        url = 'https://api.binance.com/api/v3/exchangeInfo'
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        spot_valid: Set[str] = set()
        for s in data.get('symbols', []):
            if s.get('status') == 'TRADING' and s.get('quoteAsset') == 'USDT':
                sym = f"{s.get('baseAsset','')}{s.get('quoteAsset','')}"
                if len(sym) > 4 and sym.endswith('USDT'):
                    # 兜底排除指数别名（若存在于现货列表中）
                    base = sym[:-4]
                    if base not in self._denylist_bases:
                        spot_valid.add(sym)
        self._spot_valid = spot_valid

    def _refresh_futures(self):
        url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        futures_valid: Set[str] = set()
        futures_invalid: Set[str] = set()
        for s in data.get('symbols', []):
            status = s.get('status')  # TRADING, SETTLING 等
            quote = s.get('quoteAsset')
            ctype = s.get('contractType')  # PERPETUAL/…
            sym = s.get('symbol', '')
            base = s.get('baseAsset', '')

            # 尝试通过字段识别指数合约（不同版本字段名可能不同）
            underlying_type = (s.get('underlyingType') or s.get('underlyingSubType') or '')
            if isinstance(underlying_type, list):
                underlying_type = ','.join(underlying_type)
            underlying_type = str(underlying_type).upper()

            is_index_like = ('INDEX' in underlying_type) or (base in self._denylist_bases)

            if quote == 'USDT' and ctype == 'PERPETUAL':
                if status == 'TRADING' and not is_index_like:
                    futures_valid.add(sym)
                else:
                    futures_invalid.add(sym)

        self._futures_valid = futures_valid
        self._futures_invalid = futures_invalid

    def get_valid_symbols(self) -> Dict[str, Set[str]]:
        self._maybe_refresh()
        # 返回副本，避免外部修改
        return {
            'spot': set(self._spot_valid),
            'futures': set(self._futures_valid),
            'futures_invalid': set(self._futures_invalid),
        }


binance_filter = _BinanceContractFilter()

