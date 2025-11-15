#!/usr/bin/env python3
"""
测试四个交易所的 REST API 快照功能
检查是否可以通过单个API调用获取所有币种的价格快照
"""

import asyncio
import aiohttp
import json
import time
from typing import Dict, List, Optional

from rest_collectors import fetch_grvt, get_grvt_supported_bases


class ExchangeAPITester:
    def __init__(self):
        self.session = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def test_binance(self) -> Dict:
        """测试 Binance REST API 快照功能"""
        results = {
            'exchange': 'Binance',
            'spot': {'success': False, 'count': 0, 'api': '', 'sample': None},
            'futures': {'success': False, 'count': 0, 'api': '', 'sample': None}
        }
        
        # 测试现货 ticker
        try:
            spot_api = 'https://api.binance.com/api/v3/ticker/24hr'
            async with self.session.get(spot_api) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    results['spot'] = {
                        'success': True,
                        'count': len(data),
                        'api': spot_api,
                        'sample': data[0] if data else None
                    }
        except Exception as e:
            print(f"Binance 现货 API 错误: {e}")

        # 测试期货 ticker
        try:
            futures_api = 'https://fapi.binance.com/fapi/v1/ticker/24hr'
            async with self.session.get(futures_api) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    results['futures'] = {
                        'success': True,
                        'count': len(data),
                        'api': futures_api,
                        'sample': data[0] if data else None
                    }
        except Exception as e:
            print(f"Binance 期货 API 错误: {e}")
            
        return results

    async def test_okx(self) -> Dict:
        """测试 OKX REST API 快照功能"""
        results = {
            'exchange': 'OKX',
            'spot': {'success': False, 'count': 0, 'api': '', 'sample': None},
            'futures': {'success': False, 'count': 0, 'api': '', 'sample': None}
        }
        
        # 测试现货 tickers
        try:
            spot_api = 'https://www.okx.com/api/v5/market/tickers?instType=SPOT'
            async with self.session.get(spot_api) as resp:
                if resp.status == 200:
                    response = await resp.json()
                    if response.get('code') == '0':
                        data = response.get('data', [])
                        results['spot'] = {
                            'success': True,
                            'count': len(data),
                            'api': spot_api,
                            'sample': data[0] if data else None
                        }
        except Exception as e:
            print(f"OKX 现货 API 错误: {e}")

        # 测试永续合约 tickers
        try:
            futures_api = 'https://www.okx.com/api/v5/market/tickers?instType=SWAP'
            async with self.session.get(futures_api) as resp:
                if resp.status == 200:
                    response = await resp.json()
                    if response.get('code') == '0':
                        data = response.get('data', [])
                        results['futures'] = {
                            'success': True,
                            'count': len(data),
                            'api': futures_api,
                            'sample': data[0] if data else None
                        }
        except Exception as e:
            print(f"OKX 期货 API 错误: {e}")
            
        return results

    async def test_bybit(self) -> Dict:
        """测试 Bybit REST API 快照功能"""
        results = {
            'exchange': 'Bybit',
            'spot': {'success': False, 'count': 0, 'api': '', 'sample': None},
            'futures': {'success': False, 'count': 0, 'api': '', 'sample': None}
        }
        
        # 测试现货 tickers
        try:
            spot_api = 'https://api.bybit.com/v5/market/tickers?category=spot'
            async with self.session.get(spot_api) as resp:
                if resp.status == 200:
                    response = await resp.json()
                    if response.get('retCode') == 0:
                        data = response.get('result', {}).get('list', [])
                        results['spot'] = {
                            'success': True,
                            'count': len(data),
                            'api': spot_api,
                            'sample': data[0] if data else None
                        }
        except Exception as e:
            print(f"Bybit 现货 API 错误: {e}")

        # 测试线性永续合约 tickers
        try:
            futures_api = 'https://api.bybit.com/v5/market/tickers?category=linear'
            async with self.session.get(futures_api) as resp:
                if resp.status == 200:
                    response = await resp.json()
                    if response.get('retCode') == 0:
                        data = response.get('result', {}).get('list', [])
                        results['futures'] = {
                            'success': True,
                            'count': len(data),
                            'api': futures_api,
                            'sample': data[0] if data else None
                        }
        except Exception as e:
            print(f"Bybit 期货 API 错误: {e}")
            
        return results

    async def test_bitget(self) -> Dict:
        """测试 Bitget REST API 快照功能"""
        results = {
            'exchange': 'Bitget',
            'spot': {'success': False, 'count': 0, 'api': '', 'sample': None},
            'futures': {'success': False, 'count': 0, 'api': '', 'sample': None}
        }
        
        # 测试现货 tickers
        try:
            spot_api = 'https://api.bitget.com/api/v2/spot/market/tickers'
            async with self.session.get(spot_api) as resp:
                if resp.status == 200:
                    response = await resp.json()
                    if response.get('code') == '00000':
                        data = response.get('data', [])
                        results['spot'] = {
                            'success': True,
                            'count': len(data),
                            'api': spot_api,
                            'sample': data[0] if data else None
                        }
        except Exception as e:
            print(f"Bitget 现货 API 错误: {e}")

        # 测试合约 tickers
        try:
            futures_api = 'https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES'
            async with self.session.get(futures_api) as resp:
                if resp.status == 200:
                    response = await resp.json()
                    if response.get('code') == '00000':
                        data = response.get('data', [])
                        results['futures'] = {
                            'success': True,
                            'count': len(data),
                            'api': futures_api,
                            'sample': data[0] if data else None
                        }
        except Exception as e:
            print(f"Bitget 期货 API 错误: {e}")
            
        return results

    async def test_grvt(self) -> Dict:
        """测试 GRVT REST API（通过官方SDK封装）"""
        results = {
            'exchange': 'GRVT',
            'spot': {'success': False, 'count': 0, 'api': '', 'sample': None},
            'futures': {'success': False, 'count': 0, 'api': 'fetch_grvt()', 'sample': None}
        }

        bases = get_grvt_supported_bases()
        snapshot = fetch_grvt()
        if snapshot:
            results['futures'] = {
                'success': True,
                'count': len(snapshot),
                'api': f"SDK full/v1/ticker ({len(bases)} 支持品种)",
                'sample': next(iter(snapshot.values())) if snapshot else None
            }
        else:
            results['futures']['api'] = "fetch_grvt() (无返回)"
        return results

    async def run_all_tests(self):
        """运行所有交易所的API测试"""
        print("🚀 开始测试五个交易所的 REST API 快照功能\n")
        print("=" * 80)
        
        # 并发测试所有交易所
        tasks = [
            self.test_binance(),
            self.test_okx(),
            self.test_bybit(),
            self.test_bitget(),
            self.test_grvt()
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 汇总结果
        print(f"{'交易所':<10} {'现货API':<8} {'现货数量':<8} {'期货API':<8} {'期货数量':<8}")
        print("-" * 80)
        
        summary = {
            'total_tested': len(tasks),
            'exchanges_with_spot_api': 0,
            'exchanges_with_futures_api': 0,
            'total_spot_symbols': 0,
            'total_futures_symbols': 0
        }
        
        for result in results:
            if isinstance(result, Exception):
                print(f"测试错误: {result}")
                continue
                
            exchange = result['exchange']
            spot_success = "✅" if result['spot']['success'] else "❌"
            spot_count = result['spot']['count'] if result['spot']['success'] else 0
            futures_success = "✅" if result['futures']['success'] else "❌"
            futures_count = result['futures']['count'] if result['futures']['success'] else 0
            
            print(f"{exchange:<10} {spot_success:<8} {spot_count:<8} {futures_success:<8} {futures_count:<8}")
            
            # 更新统计
            if result['spot']['success']:
                summary['exchanges_with_spot_api'] += 1
                summary['total_spot_symbols'] += spot_count
            if result['futures']['success']:
                summary['exchanges_with_futures_api'] += 1
                summary['total_futures_symbols'] += futures_count
                
        print("-" * 80)
        print(f"📊 测试总结:")
        print(f"   支持现货快照API的交易所: {summary['exchanges_with_spot_api']}/{summary['total_tested']}")
        print(f"   支持期货快照API的交易所: {summary['exchanges_with_futures_api']}/{summary['total_tested']}")
        print(f"   现货币种总数: {summary['total_spot_symbols']}")
        print(f"   期货币种总数: {summary['total_futures_symbols']}")
        
        # 显示详细API信息和示例数据
        print("\n" + "=" * 80)
        print("📋 详细API信息和示例数据:")
        print("=" * 80)
        
        for result in results:
            if isinstance(result, Exception):
                continue
                
            print(f"\n🏢 {result['exchange']}")
            print("-" * 40)
            
            # 现货API信息
            spot = result['spot']
            if spot['success']:
                print(f"✅ 现货API: {spot['api']}")
                print(f"   币种数量: {spot['count']}")
                if spot['sample']:
                    print(f"   示例数据: {json.dumps(spot['sample'], indent=2)[:200]}...")
            else:
                print(f"❌ 现货API: 测试失败")
                
            # 期货API信息  
            futures = result['futures']
            if futures['success']:
                print(f"✅ 期货API: {futures['api']}")
                print(f"   币种数量: {futures['count']}")
                if futures['sample']:
                    print(f"   示例数据: {json.dumps(futures['sample'], indent=2)[:200]}...")
            else:
                print(f"❌ 期货API: 测试失败")


async def main():
    async with ExchangeAPITester() as tester:
        await tester.run_all_tests()


if __name__ == "__main__":
    asyncio.run(main())
