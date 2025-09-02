# 配置文件

import os
import json
from datetime import datetime
from typing import List, Optional

# WebSocket连接URLs
EXCHANGE_WEBSOCKETS = {
    'okx': {
        'public': 'wss://ws.okx.com:8443/ws/v5/public',
    },
    'binance': {
        'spot': 'wss://stream.binance.com:9443/ws/',
        'futures': 'wss://fstream.binance.com/ws/'
    },
    'bybit': {
        'spot': 'wss://stream.bybit.com/v5/public/spot',
        'linear': 'wss://stream.bybit.com/v5/public/linear'
    },
    'bitget': {
        'public': 'wss://ws.bitget.com/v2/ws/public'
    }
}

# 默认监听币种
DEFAULT_SYMBOL = 'LINK'

# 支持的币种列表 - 优化后可同时监听200+个币种的资金费率
SUPPORTED_SYMBOLS = [
    # 主流币种 (30个)
    'BTC', 'ETH', 'BNB', 'XRP', 'ADA', 'DOGE', 'SOL', 'TRX', 'DOT', 'MATIC',
    'AVAX', 'LTC', 'SHIB', 'WBTC', 'LEO', 'UNI', 'ATOM', 'ETC', 'LINK', 'XMR',
    'BCH', 'XLM', 'NEAR', 'APT', 'VET', 'ICP', 'FIL', 'HBAR', 'QNT', 'ALGO',
    
    # DeFi热门 (25个)
    'AAVE', 'COMP', 'SUSHI', 'CRV', 'YFI', '1INCH', 'MKR', 'SNX', 'REN', 'BAL',
    'UMA', 'ZRX', 'KNC', 'BAND', 'NMR', 'LRC', 'OMG', 'REP', 'MLN', 'PNT',
    'RSR', 'RLC', 'ANT', 'NEXO', 'CVC',
    
    # Layer1/Layer2 (25个)
    'FTM', 'ROSE', 'EGLD', 'KLAY', 'FLOW', 'ONE', 'CELO', 'HNT', 'IOTA', 'QTUM',
    'ZIL', 'ICX', 'NANO', 'WAVES', 'DASH', 'ZEC', 'XTZ', 'ONT', 'VTHO', 'RVN',
    'SC', 'DGB', 'LSK', 'STEEM', 'ARK',
    
    # GameFi/NFT (20个)
    'MANA', 'SAND', 'AXS', 'ENJ', 'CHZ', 'GALA', 'ILV', 'YGG', 'MAGIC', 'APE',
    'IMX', 'RNDR', 'HIGH', 'TVK', 'SLP', 'ALICE', 'DG', 'GHST', 'WAXP', 'SFP',
    
    # 基础设施 (19个)
    'GRT', 'LPT', 'BAT', 'STORJ', 'OCEAN', 'ANKR', 'FET', 'AGIX', 'NKN', 'HOT',
    'DENT', 'KEY', 'DATA', 'BLZ', 'REQ', 'POWR', 'SUB', 'MTL', 'SALT',
    
    # 新兴热门 (30个)
    'AR', 'RUNE', 'KSM', 'DYDX', 'PERP', 'ENS', 'LDO', 'FXS', 'CVX', 'BICO',
    'JASMY', 'C98', 'GTC', 'BTRST', 'RAD', 'API3', 'CTSI', 'AUCTION', 'BADGER', 'BOND',
    'DPX', 'FIDA', 'GOG', 'HFT', 'IQ', 'JUP', 'LCX', 'LOKA', 'METIS', 'MNGO',
    
    # 特殊关注 (2个)
    'WLFI'  # 项目重点关注
]

# 数据刷新间隔（秒） - 降低频率避免数据过多
DATA_REFRESH_INTERVAL = 0.5  # 500ms

# WebSocket数据推送频率控制（秒） 
WS_UPDATE_INTERVAL = 0.1  # 100ms WebSocket数据更新间隔

# 市场信息缓存时间（小时）
MARKET_INFO_CACHE_HOURS = 1

# 动态币种筛选参数
MARKET_FILTER_CONFIG = {
    'min_exchanges_support': 2,        # 最少需要几个交易所支持
    'min_completeness_score': 25,      # 最低完整性评分 (0-100)
    'max_symbols': 200,                # 最大监控币种数量
    'priority_symbols': ['WLFI', 'BTC', 'ETH', 'BNB'],  # 优先级币种（始终包含）
    'exclude_symbols': [],             # 排除币种列表
}

# 动态币种列表缓存文件
DYNAMIC_SYMBOLS_CACHE_FILE = 'dynamic_symbols_cache.json'

def get_supported_symbols() -> List[str]:
    """
    获取支持的币种列表
    优先使用动态获取的列表，如果失败则使用静态备用列表
    """
    try:
        # 尝试从缓存文件读取动态币种列表
        if os.path.exists(DYNAMIC_SYMBOLS_CACHE_FILE):
            with open(DYNAMIC_SYMBOLS_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
                
                # 检查缓存是否过期（1小时）
                cache_time = datetime.fromisoformat(cache_data.get('timestamp', ''))
                hours_passed = (datetime.now() - cache_time).total_seconds() / 3600
                
                if hours_passed < MARKET_INFO_CACHE_HOURS:
                    symbols = cache_data.get('symbols', [])
                    if symbols:
                        print(f"使用缓存的动态币种列表: {len(symbols)} 个币种")
                        return symbols
        
        # 缓存过期或不存在，使用静态备用列表
        print(f"使用静态备用币种列表: {len(SUPPORTED_SYMBOLS)} 个币种")
        return SUPPORTED_SYMBOLS.copy()
        
    except Exception as e:
        print(f"获取动态币种列表失败: {e}, 使用静态备用列表")
        return SUPPORTED_SYMBOLS.copy()

def save_dynamic_symbols(symbols: List[str]) -> None:
    """保存动态币种列表到缓存文件"""
    try:
        cache_data = {
            'symbols': symbols,
            'timestamp': datetime.now().isoformat(),
            'total_count': len(symbols)
        }
        
        with open(DYNAMIC_SYMBOLS_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)
        
        print(f"动态币种列表已保存: {len(symbols)} 个币种")
        
    except Exception as e:
        print(f"保存动态币种列表失败: {e}")

def update_supported_symbols_async():
    """
    异步更新支持的币种列表
    这个函数将在后台线程中运行，避免阻塞主程序启动
    """
    try:
        # 延迟导入，避免循环依赖
        from market_info import get_dynamic_symbols
        
        print("开始异步更新币种列表...")
        dynamic_symbols = get_dynamic_symbols(force_refresh=True)
        
        if dynamic_symbols:
            # 确保优先级币种始终包含
            final_symbols = list(MARKET_FILTER_CONFIG['priority_symbols'])
            
            # 添加动态获取的币种，去重
            for symbol in dynamic_symbols:
                if symbol not in final_symbols and symbol not in MARKET_FILTER_CONFIG['exclude_symbols']:
                    final_symbols.append(symbol)
            
            # 限制最大数量
            max_symbols = MARKET_FILTER_CONFIG['max_symbols']
            if len(final_symbols) > max_symbols:
                final_symbols = final_symbols[:max_symbols]
            
            save_dynamic_symbols(final_symbols)
            return final_symbols
        else:
            print("动态币种获取失败，保持使用静态列表")
            return SUPPORTED_SYMBOLS.copy()
            
    except Exception as e:
        print(f"异步更新币种列表失败: {e}")
        return SUPPORTED_SYMBOLS.copy()

# 初始化时获取币种列表（使用缓存或静态列表）
CURRENT_SUPPORTED_SYMBOLS = get_supported_symbols()