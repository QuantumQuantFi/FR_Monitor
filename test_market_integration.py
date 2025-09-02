#!/usr/bin/env python3
"""
Market Integration Test
测试市场信息获取和动态币种列表功能
"""

import asyncio
import time
from market_info import MarketInfoCollector
from config import get_supported_symbols, CURRENT_SUPPORTED_SYMBOLS

async def test_market_integration():
    """测试市场信息集成功能"""
    print("🚀 开始测试市场信息集成功能...")
    
    # 1. 测试配置系统
    print("\n=== 测试配置系统 ===")
    print(f"当前支持币种数量: {len(CURRENT_SUPPORTED_SYMBOLS)}")
    print(f"前10个币种: {CURRENT_SUPPORTED_SYMBOLS[:10]}")
    
    dynamic_symbols = get_supported_symbols()
    print(f"动态获取币种数量: {len(dynamic_symbols)}")
    
    # 2. 测试市场信息收集器
    print("\n=== 测试市场信息收集器 ===")
    collector = MarketInfoCollector()
    
    print("正在收集市场信息...")
    all_markets = await collector.collect_all_markets()
    
    print(f"\n各交易所市场统计:")
    for exchange, markets in all_markets.items():
        spot_count = len(markets['spot'])
        futures_count = len(markets['futures'])
        print(f"  {exchange.upper()}: 现货 {spot_count}, 期货 {futures_count}")
    
    # 3. 测试币种推荐
    print("\n=== 测试币种推荐 ===")
    recommended = collector.get_recommended_symbols(all_markets, min_exchanges=2, min_completeness=25)
    print(f"推荐监控币种数量: {len(recommended)}")
    print(f"前20个推荐币种: {recommended[:20]}")
    
    # 4. 测试覆盖度分析
    print("\n=== 测试覆盖度分析 ===")
    symbol_stats = collector.merge_and_filter_symbols(all_markets)
    
    high_coverage = [s for s, stats in symbol_stats.items() if stats['completeness_score'] >= 75]
    medium_coverage = [s for s, stats in symbol_stats.items() if 25 <= stats['completeness_score'] < 75]
    low_coverage = [s for s, stats in symbol_stats.items() if stats['completeness_score'] < 25]
    
    print(f"高覆盖度币种 (≥75%): {len(high_coverage)}")
    print(f"中覆盖度币种 (25-75%): {len(medium_coverage)}")
    print(f"低覆盖度币种 (<25%): {len(low_coverage)}")
    
    # 5. 显示特定币种详情
    print("\n=== 特定币种分析 ===")
    test_symbols = ['BTC', 'ETH', 'WLFI', 'LINK']
    for symbol in test_symbols:
        if symbol in symbol_stats:
            stats = symbol_stats[symbol]
            print(f"{symbol}: 完整性 {stats['completeness_score']:.1f}% "
                  f"(现货: {len(stats['exchanges_with_spot'])}/4, "
                  f"期货: {len(stats['exchanges_with_futures'])}/4)")
    
    # 6. 测试缓存功能
    print("\n=== 测试缓存功能 ===")
    start_time = time.time()
    cached_symbols = await collector.get_dynamic_symbol_list(force_refresh=False)
    cache_time = time.time() - start_time
    print(f"缓存获取耗时: {cache_time:.2f}秒, 币种数: {len(cached_symbols)}")
    
    print("\n✅ 市场信息集成测试完成!")
    
    return {
        'total_symbols': len(all_markets.get('binance', {}).get('spot', set()) | 
                            all_markets.get('okx', {}).get('spot', set()) |
                            all_markets.get('bybit', {}).get('spot', set()) |
                            all_markets.get('bitget', {}).get('spot', set())),
        'recommended_count': len(recommended),
        'high_coverage_count': len(high_coverage),
        'exchanges_tested': len(all_markets)
    }

if __name__ == "__main__":
    # 运行测试
    result = asyncio.run(test_market_integration())
    
    print(f"\n=== 测试结果摘要 ===")
    print(f"总计发现币种: {result['total_symbols']}")
    print(f"推荐监控币种: {result['recommended_count']}")
    print(f"高覆盖度币种: {result['high_coverage_count']}")
    print(f"成功测试交易所: {result['exchanges_tested']}/4")