#!/usr/bin/env python3
"""
验证配置是否正确
"""

from config import SUPPORTED_SYMBOLS

def verify_config():
    """验证配置"""
    print("=" * 50)
    print("配置验证报告")
    print("=" * 50)
    
    # 检查币种数量
    total_symbols = len(SUPPORTED_SYMBOLS)
    print(f"总币种数量: {total_symbols}")
    
    # 检查重复币种
    unique_symbols = set(SUPPORTED_SYMBOLS)
    if len(unique_symbols) != total_symbols:
        print("❌ 存在重复币种")
    else:
        print("✅ 无重复币种")
    
    # 分类统计
    categories = {
        '主流币种': 30,
        'DeFi热门': 25, 
        'Layer1/Layer2': 25,
        'GameFi/NFT': 20,
        '基础设施': 20,
        '新兴热门': 30,
        '特殊关注': 10
    }
    
    expected_total = sum(categories.values())
    if total_symbols == expected_total:
        print(f"✅ 币种分类数量正确: {total_symbols}")
    else:
        print(f"❌ 币种数量不匹配: 预期 {expected_total}, 实际 {total_symbols}")
    
    # 检查特定币种是否存在
    important_symbols = ['BTC', 'ETH', 'WLFI', 'LINK']
    missing = []
    for symbol in important_symbols:
        if symbol not in SUPPORTED_SYMBOLS:
            missing.append(symbol)
    
    if missing:
        print(f"❌ 缺失重要币种: {missing}")
    else:
        print("✅ 所有重要币种都存在")
    
    # 显示前10个和后10个币种
    print(f"\n前10个币种: {SUPPORTED_SYMBOLS[:10]}")
    print(f"后10个币种: {SUPPORTED_SYMBOLS[-10:]}")
    
    print("\n" + "=" * 50)
    print("WebSocket容量估算")
    print("=" * 50)
    
    # 各交易所容量估算
    exchange_capacity = {
        'Binance': min(1024, total_symbols * 2),  # 每个币种2个stream
        'OKX': min(480 // 3, total_symbols),      # 每个币种3个请求
        'Bybit': min(2000, total_symbols),        # 永续合约限制
        'Bitget': min(1000 // 2, total_symbols)   # 每个币种2个channel
    }
    
    for exchange, capacity in exchange_capacity.items():
        status = "✅" if capacity >= total_symbols else "⚠️ "
        print(f"{status} {exchange}: 支持 {capacity} 个币种")
    
    # 总体评估
    if all(capacity >= total_symbols for capacity in exchange_capacity.values()):
        print(f"\n🎉 完美! 所有交易所都支持 {total_symbols} 个币种")
    else:
        print(f"\nℹ️  建议: 可以考虑减少到 {min(exchange_capacity.values())} 个币种以获得最佳兼容性")

if __name__ == "__main__":
    verify_config()