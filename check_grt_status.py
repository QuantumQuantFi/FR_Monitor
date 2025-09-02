#!/usr/bin/env python3
"""
GRT 数据状态检查工具
检查 GRT 在各交易所的可用性状态
"""

import json
import time
from exchange_connectors import ExchangeDataCollector

# 创建数据收集器实例
data_collector = ExchangeDataCollector()

def check_grt_status():
    """检查 GRT 在各交易所的状态"""
    print("=== GRT 数据状态检查 ===")
    
    # 获取所有数据
    all_data = data_collector.get_all_data()
    
    exchanges = ['okx', 'binance', 'bybit', 'bitget']
    
    for exchange in exchanges:
        print(f"\n{exchange.upper()}:")
        
        if 'GRT' in all_data[exchange]:
            grt_data = all_data[exchange]['GRT']
            
            # 检查现货
            spot_price = grt_data['spot'].get('price', 0)
            spot_available = spot_price > 0
            print(f"  现货: {'✅' if spot_available else '❌'} {spot_price}")
            
            # 检查期货
            futures_price = grt_data['futures'].get('price', 0)
            futures_available = futures_price > 0
            print(f"  期货: {'✅' if futures_available else '❌'} {futures_price}")
            
            # 检查资金费率
            funding_rate = grt_data['futures'].get('funding_rate', None)
            funding_available = funding_rate is not None
            print(f"  资金费率: {'✅' if funding_available else '❌'} {funding_rate}")
            
        else:
            print("  ❌ GRT 数据不存在")

if __name__ == "__main__":
    # 启动数据收集
    data_collector.start_all_connections()
    
    # 等待数据收集
    print("等待数据收集...")
    time.sleep(8)
    
    # 检查状态
    check_grt_status()
    
    # 停止连接
    data_collector.stop_all_connections()