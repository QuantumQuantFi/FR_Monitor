#!/usr/bin/env python3
"""
Exchange Market Availability Monitor
检测各交易所对每个币种的市场支持情况
"""

import json
import time
from datetime import datetime
from exchange_connectors import ExchangeDataCollector
from config import SUPPORTED_SYMBOLS

class ExchangeAvailabilityMonitor:
    def __init__(self):
        self.data_collector = ExchangeDataCollector()
        self.availability_data = {}
        self.last_check_time = None
        
    def start_monitoring(self):
        """启动数据收集"""
        self.data_collector.start_all_connections()
        print("交易所可用性监控已启动...")
        
    def check_availability(self):
        """检查各交易所对每个币种的支持情况"""
        self.last_check_time = datetime.now()
        
        # 获取所有数据
        all_data = self.data_collector.get_all_data()
        
        availability = {}
        
        for symbol in SUPPORTED_SYMBOLS:
            availability[symbol] = {}
            
            for exchange in ['okx', 'binance', 'bybit', 'bitget']:
                exchange_data = {
                    'spot_available': False,
                    'futures_available': False,
                    'funding_rate_available': False,
                    'last_update': None
                }
                
                if symbol in all_data[exchange]:
                    symbol_data = all_data[exchange][symbol]
                    
                    # 检查现货
                    if symbol_data['spot'] and symbol_data['spot'].get('price', 0) > 0:
                        exchange_data['spot_available'] = True
                        exchange_data['last_update'] = symbol_data['spot'].get('timestamp')
                    
                    # 检查期货
                    if symbol_data['futures'] and symbol_data['futures'].get('price', 0) > 0:
                        exchange_data['futures_available'] = True
                        exchange_data['last_update'] = symbol_data['futures'].get('timestamp')
                        
                    # 检查资金费率
                    if symbol_data['futures'] and symbol_data['futures'].get('funding_rate') is not None:
                        exchange_data['funding_rate_available'] = True
                
                availability[symbol][exchange] = exchange_data
        
        self.availability_data = availability
        return availability
    
    def generate_summary(self):
        """生成汇总报告"""
        if not self.availability_data:
            return "暂无可用数据"
            
        summary = {
            'total_symbols': len(SUPPORTED_SYMBOLS),
            'exchange_coverage': {},
            'symbol_coverage': {},
            'last_check': self.last_check_time.isoformat()
        }
        
        # 按交易所统计
        for exchange in ['okx', 'binance', 'bybit', 'bitget']:
            exchange_stats = {
                'spot_count': 0,
                'futures_count': 0,
                'funding_rate_count': 0,
                'coverage_percent': 0
            }
            
            for symbol in SUPPORTED_SYMBOLS:
                data = self.availability_data[symbol][exchange]
                if data['spot_available']:
                    exchange_stats['spot_count'] += 1
                if data['futures_available']:
                    exchange_stats['futures_count'] += 1
                if data['funding_rate_available']:
                    exchange_stats['funding_rate_count'] += 1
            
            exchange_stats['coverage_percent'] = round(
                (exchange_stats['spot_count'] + exchange_stats['futures_count']) / 
                (len(SUPPORTED_SYMBOLS) * 2) * 100, 2
            )
            
            summary['exchange_coverage'][exchange] = exchange_stats
        
        # 按币种统计
        for symbol in SUPPORTED_SYMBOLS:
            symbol_stats = {
                'exchanges_with_spot': 0,
                'exchanges_with_futures': 0,
                'exchanges_with_funding': 0,
                'completeness_score': 0
            }
            
            for exchange in ['okx', 'binance', 'bybit', 'bitget']:
                data = self.availability_data[symbol][exchange]
                if data['spot_available']:
                    symbol_stats['exchanges_with_spot'] += 1
                if data['futures_available']:
                    symbol_stats['exchanges_with_futures'] += 1
                if data['funding_rate_available']:
                    symbol_stats['exchanges_with_funding'] += 1
            
            # 完整性评分 (0-100)
            symbol_stats['completeness_score'] = round(
                (symbol_stats['exchanges_with_spot'] + symbol_stats['exchanges_with_futures']) / 8 * 100, 2
            )
            
            summary['symbol_coverage'][symbol] = symbol_stats
        
        return summary
    
    def get_problematic_symbols(self, min_completeness=50):
        """获取完整性较低的币种"""
        if not self.availability_data:
            return []
            
        problematic = []
        summary = self.generate_summary()
        
        for symbol, stats in summary['symbol_coverage'].items():
            if stats['completeness_score'] < min_completeness:
                problematic.append({
                    'symbol': symbol,
                    'completeness_score': stats['completeness_score'],
                    'spot_coverage': stats['exchanges_with_spot'],
                    'futures_coverage': stats['exchanges_with_futures']
                })
        
        return sorted(problematic, key=lambda x: x['completeness_score'])
    
    def get_exchange_gaps(self):
        """获取各交易所的市场缺口"""
        if not self.availability_data:
            return {}
            
        gaps = {}
        
        for exchange in ['okx', 'binance', 'bybit', 'bitget']:
            exchange_gaps = {
                'missing_spot': [],
                'missing_futures': [],
                'missing_funding': []
            }
            
            for symbol in SUPPORTED_SYMBOLS:
                data = self.availability_data[symbol][exchange]
                
                if not data['spot_available']:
                    exchange_gaps['missing_spot'].append(symbol)
                if not data['futures_available']:
                    exchange_gaps['missing_futures'].append(symbol)
                if not data['funding_rate_available']:
                    exchange_gaps['missing_funding'].append(symbol)
            
            gaps[exchange] = exchange_gaps
        
        return gaps

# 示例使用
if __name__ == "__main__":
    monitor = ExchangeAvailabilityMonitor()
    monitor.start_monitoring()
    
    # 等待数据收集
    time.sleep(10)
    
    # 检查可用性
    availability = monitor.check_availability()
    
    # 生成报告
    summary = monitor.generate_summary()
    
    print("=== 交易所市场覆盖情况 ===")
    for exchange, stats in summary['exchange_coverage'].items():
        print(f"{exchange.upper()}:")
        print(f"  现货: {stats['spot_count']}/{len(SUPPORTED_SYMBOLS)}")
        print(f"  期货: {stats['futures_count']}/{len(SUPPORTED_SYMBOLS)}")
        print(f"  资金费率: {stats['funding_rate_count']}/{len(SUPPORTED_SYMBOLS)}")
        print(f"  覆盖度: {stats['coverage_percent']}%")
        print()
    
    # 显示问题币种
    problematic = monitor.get_problematic_symbols()
    if problematic:
        print("=== 完整性较低的币种 ===")
        for symbol_info in problematic:
            print(f"{symbol_info['symbol']}: {symbol_info['completeness_score']}% "
                  f"(现货:{symbol_info['spot_coverage']}/4, 期货:{symbol_info['futures_coverage']}/4)")
    
    # 显示GRT具体情况
    print("\n=== GRT 详细情况 ===")
    grt_data = availability['GRT']
    for exchange, data in grt_data.items():
        print(f"{exchange.upper()}: 现货={data['spot_available']}, "
              f"期货={data['futures_available']}, 资金费率={data['funding_rate_available']}")