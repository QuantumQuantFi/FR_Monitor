#!/usr/bin/env python3
"""
Market Information Module
动态获取各交易所支持的现货和期货市场列表
"""

import requests
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional
import asyncio
import aiohttp
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MarketInfoCollector:
    def __init__(self, cache_duration_hours: int = 1):
        """
        初始化市场信息收集器
        :param cache_duration_hours: 缓存持续时间（小时）
        """
        self.cache_duration = timedelta(hours=cache_duration_hours)
        self.cache = {}
        self.last_update = {}
        
        # 交易所API端点
        self.api_endpoints = {
            'binance': {
                'spot': 'https://api.binance.com/api/v3/exchangeInfo',
                'futures': 'https://fapi.binance.com/fapi/v1/exchangeInfo'
            },
            'okx': {
                'instruments': 'https://www.okx.com/api/v5/public/instruments'
            },
            'bybit': {
                'spot': 'https://api.bybit.com/v5/market/instruments-info',
                'linear': 'https://api.bybit.com/v5/market/instruments-info'
            },
            'bitget': {
                'spot': 'https://api.bitget.com/api/v2/spot/public/symbols',
                'futures': 'https://api.bitget.com/api/v2/mix/market/contracts'
            }
        }
        
        # 最小要求（过滤条件）
        self.min_exchanges_support = 2  # 最少需要几个交易所支持
        self.min_volume_threshold = 0   # 最小24小时交易量（USDT）
        
    def _is_cache_valid(self, key: str) -> bool:
        """检查缓存是否有效"""
        if key not in self.last_update:
            return False
        return datetime.now() - self.last_update[key] < self.cache_duration
    
    def _update_cache(self, key: str, data):
        """更新缓存"""
        self.cache[key] = data
        self.last_update[key] = datetime.now()
    
    async def get_binance_markets(self) -> Dict[str, Set[str]]:
        """获取Binance支持的市场"""
        markets = {'spot': set(), 'futures': set()}
        
        try:
            # 现货市场
            async with aiohttp.ClientSession() as session:
                async with session.get(self.api_endpoints['binance']['spot'], timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        for symbol_info in data.get('symbols', []):
                            if (symbol_info.get('status') == 'TRADING' and 
                                symbol_info.get('quoteAsset') == 'USDT'):
                                base_asset = symbol_info.get('baseAsset')
                                if base_asset:
                                    markets['spot'].add(base_asset)
                
                # 期货市场
                async with session.get(self.api_endpoints['binance']['futures'], timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        for symbol_info in data.get('symbols', []):
                            if (symbol_info.get('status') == 'TRADING' and 
                                symbol_info.get('quoteAsset') == 'USDT' and
                                symbol_info.get('contractType') == 'PERPETUAL'):
                                # 过滤指数类合约（如 DEFI、BTCDOM、ALL 等）
                                underlying_type = symbol_info.get('underlyingType') or symbol_info.get('underlyingSubType') or ''
                                if isinstance(underlying_type, list):
                                    underlying_type = ','.join(underlying_type)
                                if isinstance(underlying_type, str) and 'INDEX' in underlying_type.upper():
                                    continue
                                base_asset = symbol_info.get('baseAsset')
                                if base_asset and base_asset not in {'DEFI', 'BTCDOM', 'ALL', 'BLUEBIRD'}:
                                    markets['futures'].add(base_asset)
        
        except Exception as e:
            logger.error(f"获取Binance市场信息失败: {e}")
        
        logger.info(f"Binance: 现货 {len(markets['spot'])} 个, 期货 {len(markets['futures'])} 个")
        return markets
    
    async def get_okx_markets(self) -> Dict[str, Set[str]]:
        """获取OKX支持的市场"""
        markets = {'spot': set(), 'futures': set()}
        
        try:
            async with aiohttp.ClientSession() as session:
                # 获取所有交易工具
                params = {'instType': 'SPOT'}
                async with session.get(self.api_endpoints['okx']['instruments'], 
                                     params=params, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('code') == '0':
                            for instrument in data.get('data', []):
                                inst_id = instrument.get('instId', '')
                                if inst_id.endswith('-USDT') and instrument.get('state') == 'live':
                                    base_asset = inst_id.split('-')[0]
                                    markets['spot'].add(base_asset)
                
                # 获取永续合约
                params = {'instType': 'SWAP'}
                async with session.get(self.api_endpoints['okx']['instruments'], 
                                     params=params, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('code') == '0':
                            for instrument in data.get('data', []):
                                inst_id = instrument.get('instId', '')
                                if (inst_id.endswith('-USDT-SWAP') and 
                                    instrument.get('state') == 'live'):
                                    base_asset = inst_id.split('-')[0]
                                    markets['futures'].add(base_asset)
        
        except Exception as e:
            logger.error(f"获取OKX市场信息失败: {e}")
        
        logger.info(f"OKX: 现货 {len(markets['spot'])} 个, 期货 {len(markets['futures'])} 个")
        return markets
    
    async def get_bybit_markets(self) -> Dict[str, Set[str]]:
        """获取Bybit支持的市场"""
        markets = {'spot': set(), 'futures': set()}
        
        try:
            async with aiohttp.ClientSession() as session:
                # 现货市场
                params = {'category': 'spot'}
                async with session.get(self.api_endpoints['bybit']['spot'], 
                                     params=params, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('retCode') == 0:
                            for instrument in data.get('result', {}).get('list', []):
                                symbol = instrument.get('symbol', '')
                                if (symbol.endswith('USDT') and 
                                    instrument.get('status') == 'Trading'):
                                    base_asset = symbol[:-4]  # 移除USDT后缀
                                    markets['spot'].add(base_asset)
                
                # 永续合约市场
                params = {'category': 'linear'}
                async with session.get(self.api_endpoints['bybit']['linear'], 
                                     params=params, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('retCode') == 0:
                            for instrument in data.get('result', {}).get('list', []):
                                symbol = instrument.get('symbol', '')
                                if (symbol.endswith('USDT') and 
                                    instrument.get('status') == 'Trading' and
                                    instrument.get('contractType') == 'LinearPerpetual'):
                                    base_asset = symbol[:-4]  # 移除USDT后缀
                                    markets['futures'].add(base_asset)
        
        except Exception as e:
            logger.error(f"获取Bybit市场信息失败: {e}")
        
        logger.info(f"Bybit: 现货 {len(markets['spot'])} 个, 期货 {len(markets['futures'])} 个")
        return markets
    
    async def get_bitget_markets(self) -> Dict[str, Set[str]]:
        """获取Bitget支持的市场"""
        markets = {'spot': set(), 'futures': set()}
        
        try:
            async with aiohttp.ClientSession() as session:
                # 现货市场
                async with session.get(self.api_endpoints['bitget']['spot'], timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('code') == '00000':
                            for symbol_info in data.get('data', []):
                                symbol = symbol_info.get('symbol', '')
                                if (symbol.endswith('USDT') and 
                                    symbol_info.get('status') == 'online'):
                                    base_asset = symbol[:-4]  # 移除USDT后缀
                                    markets['spot'].add(base_asset)
                
                # 期货市场
                params = {'productType': 'USDT-FUTURES'}
                async with session.get(self.api_endpoints['bitget']['futures'], 
                                     params=params, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('code') == '00000':
                            for contract in data.get('data', []):
                                symbol = contract.get('symbol', '')
                                if (symbol.endswith('USDT') and 
                                    contract.get('symbolStatus') == 'normal'):
                                    base_asset = symbol[:-4]  # 移除USDT后缀
                                    markets['futures'].add(base_asset)
        
        except Exception as e:
            logger.error(f"获取Bitget市场信息失败: {e}")
        
        logger.info(f"Bitget: 现货 {len(markets['spot'])} 个, 期货 {len(markets['futures'])} 个")
        return markets
    
    async def collect_all_markets(self, force_refresh: bool = False) -> Dict[str, Dict[str, Set[str]]]:
        """收集所有交易所的市场信息"""
        cache_key = 'all_markets'
        
        if not force_refresh and self._is_cache_valid(cache_key):
            return self.cache[cache_key]
        
        logger.info("开始收集所有交易所市场信息...")
        
        # 并行获取所有交易所的市场信息
        tasks = [
            self.get_binance_markets(),
            self.get_okx_markets(),
            self.get_bybit_markets(),
            self.get_bitget_markets()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_markets = {}
        exchange_names = ['binance', 'okx', 'bybit', 'bitget']
        
        for i, result in enumerate(results):
            exchange = exchange_names[i]
            if isinstance(result, Exception):
                logger.error(f"{exchange} 市场信息获取失败: {result}")
                all_markets[exchange] = {'spot': set(), 'futures': set()}
            else:
                all_markets[exchange] = result
        
        self._update_cache(cache_key, all_markets)
        return all_markets
    
    async def get_exchange_specific_symbols(self, force_refresh: bool = False) -> Dict[str, Dict[str, List[str]]]:
        """获取各交易所特定的币种列表，用于WebSocket订阅"""
        cache_key = 'exchange_symbols'
        
        if not force_refresh and self._is_cache_valid(cache_key):
            return self.cache[cache_key]
        
        logger.info("开始收集各交易所特定币种列表...")
        
        all_markets = await self.collect_all_markets(force_refresh)
        
        # 转换为列表格式，便于WebSocket订阅使用
        exchange_symbols = {}
        for exchange, markets in all_markets.items():
            exchange_symbols[exchange] = {
                'spot': sorted(list(markets['spot'])),
                'futures': sorted(list(markets['futures']))
            }
            
            logger.info(f"{exchange.upper()}: 现货 {len(exchange_symbols[exchange]['spot'])} 个, "
                       f"期货 {len(exchange_symbols[exchange]['futures'])} 个")
        
        self._update_cache(cache_key, exchange_symbols)
        return exchange_symbols
    
    def merge_and_filter_symbols(self, all_markets: Dict[str, Dict[str, Set[str]]]) -> Dict[str, Dict]:
        """合并和过滤币种，返回推荐的监控币种列表"""
        # 统计每个币种在多少个交易所可用
        symbol_stats = {}
        
        # 收集所有唯一币种
        all_symbols = set()
        for exchange_data in all_markets.values():
            all_symbols.update(exchange_data['spot'])
            all_symbols.update(exchange_data['futures'])
        
        # 统计每个币种的可用性
        for symbol in all_symbols:
            stats = {
                'exchanges_with_spot': [],
                'exchanges_with_futures': [],
                'total_exchanges': 0,
                'completeness_score': 0
            }
            
            for exchange, markets in all_markets.items():
                has_spot = symbol in markets['spot']
                has_futures = symbol in markets['futures']
                
                if has_spot:
                    stats['exchanges_with_spot'].append(exchange)
                if has_futures:
                    stats['exchanges_with_futures'].append(exchange)
                if has_spot or has_futures:
                    stats['total_exchanges'] += 1
            
            # 计算完整性评分 (0-100)
            spot_coverage = len(stats['exchanges_with_spot'])
            futures_coverage = len(stats['exchanges_with_futures'])
            stats['completeness_score'] = (spot_coverage + futures_coverage) / 8 * 100
            
            symbol_stats[symbol] = stats
        
        return symbol_stats
    
    def get_recommended_symbols(self, all_markets: Dict[str, Dict[str, Set[str]]], 
                              min_exchanges: int = None, 
                              min_completeness: int = 25) -> List[str]:
        """获取推荐监控的币种列表"""
        if min_exchanges is None:
            min_exchanges = self.min_exchanges_support
        
        symbol_stats = self.merge_and_filter_symbols(all_markets)
        
        # 过滤符合条件的币种
        recommended = []
        for symbol, stats in symbol_stats.items():
            if (stats['total_exchanges'] >= min_exchanges and 
                stats['completeness_score'] >= min_completeness):
                recommended.append(symbol)
        
        # 按完整性评分排序
        recommended.sort(key=lambda s: symbol_stats[s]['completeness_score'], reverse=True)
        
        return recommended
    
    def get_symbol_coverage_report(self, all_markets: Dict[str, Dict[str, Set[str]]]) -> Dict:
        """生成币种覆盖度报告"""
        symbol_stats = self.merge_and_filter_symbols(all_markets)
        
        report = {
            'total_unique_symbols': len(symbol_stats),
            'high_coverage_symbols': [],  # >= 75%
            'medium_coverage_symbols': [],  # 25-75%
            'low_coverage_symbols': [],   # < 25%
            'exchange_summary': {
                'binance': {'spot': 0, 'futures': 0},
                'okx': {'spot': 0, 'futures': 0},
                'bybit': {'spot': 0, 'futures': 0},
                'bitget': {'spot': 0, 'futures': 0}
            }
        }
        
        # 按完整性分类
        for symbol, stats in symbol_stats.items():
            score = stats['completeness_score']
            symbol_info = {
                'symbol': symbol,
                'completeness_score': round(score, 1),
                'spot_exchanges': len(stats['exchanges_with_spot']),
                'futures_exchanges': len(stats['exchanges_with_futures']),
                'total_exchanges': stats['total_exchanges']
            }
            
            if score >= 75:
                report['high_coverage_symbols'].append(symbol_info)
            elif score >= 25:
                report['medium_coverage_symbols'].append(symbol_info)
            else:
                report['low_coverage_symbols'].append(symbol_info)
        
        # 交易所统计
        for exchange, markets in all_markets.items():
            if exchange in report['exchange_summary']:
                report['exchange_summary'][exchange] = {
                    'spot': len(markets['spot']),
                    'futures': len(markets['futures'])
                }
        
        return report
    
    async def get_dynamic_symbol_list(self, force_refresh: bool = False) -> List[str]:
        """获取动态币种列表（推荐用于WebSocket监控的币种）"""
        all_markets = await self.collect_all_markets(force_refresh)
        recommended_symbols = self.get_recommended_symbols(all_markets)
        
        logger.info(f"推荐监控币种数量: {len(recommended_symbols)}")
        return recommended_symbols


# 异步函数封装，用于同步调用
def get_dynamic_symbols(force_refresh: bool = False) -> List[str]:
    """同步接口：获取动态币种列表"""
    collector = MarketInfoCollector()
    return asyncio.run(collector.get_dynamic_symbol_list(force_refresh))

def get_market_report(force_refresh: bool = False) -> Dict:
    """同步接口：获取市场覆盖度报告"""
    async def _get_report():
        collector = MarketInfoCollector()
        all_markets = await collector.collect_all_markets(force_refresh)
        return collector.get_symbol_coverage_report(all_markets)
    
    return asyncio.run(_get_report())

def get_exchange_symbols(force_refresh: bool = False) -> Dict[str, Dict[str, List[str]]]:
    """同步接口：获取各交易所特定的币种列表"""
    collector = MarketInfoCollector()
    return asyncio.run(collector.get_exchange_specific_symbols(force_refresh))


# 命令行测试
if __name__ == "__main__":
    import sys
    
    async def main():
        collector = MarketInfoCollector()
        
        print("🚀 开始收集交易所市场信息...")
        all_markets = await collector.collect_all_markets()
        
        print("\n📊 生成覆盖度报告...")
        report = collector.get_symbol_coverage_report(all_markets)
        
        print(f"\n=== 市场覆盖度报告 ===")
        print(f"总币种数量: {report['total_unique_symbols']}")
        print(f"高覆盖度币种 (≥75%): {len(report['high_coverage_symbols'])}")
        print(f"中覆盖度币种 (25-75%): {len(report['medium_coverage_symbols'])}")
        print(f"低覆盖度币种 (<25%): {len(report['low_coverage_symbols'])}")
        
        print(f"\n=== 交易所市场统计 ===")
        for exchange, stats in report['exchange_summary'].items():
            print(f"{exchange.upper()}: 现货 {stats['spot']}, 期货 {stats['futures']}")
        
        print(f"\n=== 推荐监控币种 (前50个) ===")
        recommended = collector.get_recommended_symbols(all_markets)
        for i, symbol in enumerate(recommended[:50], 1):
            stats = collector.merge_and_filter_symbols(all_markets)[symbol]
            print(f"{i:2d}. {symbol:8s} - 完整性: {stats['completeness_score']:5.1f}% "
                  f"(现货: {len(stats['exchanges_with_spot'])}/4, "
                  f"期货: {len(stats['exchanges_with_futures'])}/4)")
        
        # 保存结果到文件
        with open('market_analysis.json', 'w', encoding='utf-8') as f:
            json.dump({
                'report': report,
                'recommended_symbols': recommended,
                'timestamp': datetime.now().isoformat()
            }, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n✅ 分析结果已保存到 market_analysis.json")
    
    if __name__ == "__main__":
        asyncio.run(main())
