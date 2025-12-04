# 配置文件

import os
import json
import socket
from datetime import datetime
from typing import List, Optional

try:
    import config_private  # local secrets; attributes managed per account
except ImportError:  # pragma: no cover - secrets file optional
    config_private = None


def _get_private(attr_name: str, env_name: Optional[str] = None, default: Optional[str] = None):
    """Fetch secrets from config_private first, then environment variables."""
    if config_private and hasattr(config_private, attr_name):
        return getattr(config_private, attr_name)
    return os.getenv(env_name or attr_name, default)


def _is_truthy(value: Optional[str]) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on')


# Global IPv4 enforcement (helps exchanges that whitelist IPv4 only)
FORCE_IPV4_CONNECTIONS = _is_truthy(
    _get_private('FORCE_IPV4', 'FORCE_IPV4', _get_private('GRVT_FORCE_IPV4', 'GRVT_FORCE_IPV4', '1'))
)
if FORCE_IPV4_CONNECTIONS:
    _ORIGINAL_GETADDRINFO = socket.getaddrinfo

    def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        if family == 0:
            family = socket.AF_INET
        return _ORIGINAL_GETADDRINFO(host, port, family, type, proto, flags)

    socket.getaddrinfo = _ipv4_only_getaddrinfo


# GRVT-specific configuration (optional)
def _build_grvt_domain(prefix: str, env_name: str, scheme: str = 'https') -> str:
    env_name = (env_name or 'prod').lower()
    if env_name == 'prod':
        host = f"{prefix}.grvt.io"
    elif env_name == 'testnet':
        host = f"{prefix}.testnet.grvt.io"
    elif env_name in ('staging', 'dev'):
        host = f"{prefix}.{env_name}.gravitymarkets.io"
    else:  # fallback to prod
        host = f"{prefix}.grvt.io"
    return f"{scheme}://{host}"


GRVT_ENVIRONMENT = _get_private('GRVT_ENVIRONMENT', 'GRVT_ENVIRONMENT', 'prod').lower()
_GRVT_MD_HTTP = _build_grvt_domain('market-data', GRVT_ENVIRONMENT, 'https')
_GRVT_MD_WS = _build_grvt_domain('market-data', GRVT_ENVIRONMENT, 'wss') + '/ws'

GRVT_WS_PUBLIC_URL = _get_private('GRVT_WS_PUBLIC_URL', 'GRVT_WS_PUBLIC_URL', _GRVT_MD_WS)
GRVT_REST_BASE_URL = _get_private('GRVT_REST_BASE_URL', 'GRVT_REST_BASE_URL', _GRVT_MD_HTTP)
GRVT_API_KEY = _get_private('GRVT_API_KEY', 'GRVT_API_KEY', '')
GRVT_API_SECRET = (
    _get_private('GRVT_SECRET_KEY', 'GRVT_SECRET_KEY', '')
    or _get_private('GRVT_PRIVATE_KEY', 'GRVT_PRIVATE_KEY', '')
)
GRVT_TRADING_ACCOUNT_ID = _get_private('GRVT_TRADING_ACCOUNT_ID', 'GRVT_TRADING_ACCOUNT_ID', '')
GRVT_WS_RATE_MS = float(_get_private('GRVT_WS_RATE_MS', 'GRVT_WS_RATE_MS', '500'))
GRVT_REST_SYMBOLS_PER_CALL = int(
    float(_get_private('GRVT_REST_SYMBOLS_PER_CALL', 'GRVT_REST_SYMBOLS_PER_CALL', '25'))
)

# Lighter (DEX) configuration
LIGHTER_WS_PUBLIC_URL = _get_private(
    'LIGHTER_WS_PUBLIC_URL',
    'LIGHTER_WS_PUBLIC_URL',
    'wss://mainnet.zklighter.elliot.ai/stream'
)
LIGHTER_REST_BASE_URL = _get_private(
    'LIGHTER_REST_BASE_URL',
    'LIGHTER_REST_BASE_URL',
    'https://mainnet.zklighter.elliot.ai/api/v1'
)
LIGHTER_MARKET_REFRESH_SECONDS = int(
    float(_get_private('LIGHTER_MARKET_REFRESH_SECONDS', 'LIGHTER_MARKET_REFRESH_SECONDS', '900'))
)

# Hyperliquid configuration
# 说明：Hyperliquid 公共行情通过 REST `/info` 与 WebSocket `allMids` 提供，
# 下面的配置用来描述 REST 与 WS 端点，方便在不同环境（主网/测试网）之间切换。
HYPERLIQUID_API_BASE_URL = _get_private(
    'HYPERLIQUID_API_BASE_URL',
    'HYPERLIQUID_API_BASE_URL',
    'https://api.hyperliquid.xyz'
)
HYPERLIQUID_WS_PUBLIC_URL = _get_private(
    'HYPERLIQUID_WS_PUBLIC_URL',
    'HYPERLIQUID_WS_PUBLIC_URL',
    'wss://api.hyperliquid.xyz/ws'
)
HYPERLIQUID_FUNDING_REFRESH_SECONDS = int(
    float(_get_private('HYPERLIQUID_FUNDING_REFRESH_SECONDS', 'HYPERLIQUID_FUNDING_REFRESH_SECONDS', '60'))
)

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
    },
    'grvt': {
        'public': GRVT_WS_PUBLIC_URL
    },
    'lighter': {
        'public': LIGHTER_WS_PUBLIC_URL
    },
    'hyperliquid': {
        'public': HYPERLIQUID_WS_PUBLIC_URL
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
    
    # 新兴热门 (31个)
    'AR', 'RUNE', 'KSM', 'DYDX', 'PERP', 'ENS', 'LDO', 'FXS', 'CVX', 'BICO',
    'JASMY', 'C98', 'GTC', 'BTRST', 'RAD', 'API3', 'CTSI', 'AUCTION', 'BADGER', 'BOND',
    'DPX', 'FIDA', 'GOG', 'HFT', 'IQ', 'JUP', 'LCX', 'LOKA', 'METIS', 'MNGO', 'WLFI',
    
    # 策略重点 (10个)
    'ARB', 'OP', 'GMX', 'PYTH', 'TIA', 'SUI', 'ONDO', 'PENDLE', 'JTO', 'NTRN'
]

# 数据刷新间隔（秒） - 优化频率减少资源消耗
DATA_REFRESH_INTERVAL = 2.0  # 2秒，减少主循环频率

# WebSocket数据推送频率控制（秒） - 同一币种数据更新间隔
WS_UPDATE_INTERVAL = 3.0  # 3秒，同一币种数据更新间隔

# 市场信息缓存时间（小时）
MARKET_INFO_CACHE_HOURS = 1

# WebSocket连接优化配置
WS_CONNECTION_CONFIG = {
    'max_reconnect_attempts': 50,      # 最大重连次数（增加到50次，基本不会放弃重连）
    'base_reconnect_delay': 5,         # 基础重连延迟（5秒，更快重连）
    'max_reconnect_delay': 120,        # 最大重连延迟（2分钟，缩短最大延迟）
    'exponential_backoff': True,       # 启用指数退避
    'connection_timeout': 20,          # 连接超时（20秒，更快检测失败）
    'ping_interval': 30,               # ping间隔（30秒，更频繁心跳）
    'ping_timeout': 10,                # ping超时（10秒，更快检测断线）
}

# REST补充轮询配置
REST_ENABLED = True                 # 默认开启；可按需关闭
REST_UPDATE_INTERVAL = 2.0          # REST 快照轮询间隔（秒）
REST_CONNECTION_CONFIG = {
    'timeout': 10,                  # 单次请求超时（秒）
    'retry': 1,                     # 失败重试次数（轻量）
    'stagger_ms': 200,              # 交易所之间的错峰延迟（毫秒）
    'user_agent': 'CrossExchange-Arb/1.0',
}

# REST合并策略
REST_MERGE_POLICY = {
    'prefer_ws_secs': 0.2,          # 若现有数据在N秒内更新，优先保留WS数据
}

# 动态关注列表（watchlist）配置
WATCHLIST_CONFIG = {
    # 资金费率阈值：0.003 = 0.3%
    'funding_abs_threshold': float(_get_private('WATCHLIST_FUNDING_THRESHOLD', 'WATCHLIST_FUNDING_THRESHOLD', '0.003')),
    # 滑动窗口（小时）
    'lookback_hours': float(_get_private('WATCHLIST_LOOKBACK_HOURS', 'WATCHLIST_LOOKBACK_HOURS', '2')),
    # 刷新频率（秒），用于扫描符合条件的交易对
    'refresh_seconds': float(_get_private('WATCHLIST_REFRESH_SECONDS', 'WATCHLIST_REFRESH_SECONDS', '30')),
    # Type B：跨交易所永续价差
    'type_b_spread_threshold': float(_get_private('WATCHLIST_TYPEB_SPREAD', 'WATCHLIST_TYPEB_SPREAD', '0.01')),  # 1%
    'type_b_funding_min': float(_get_private('WATCHLIST_TYPEB_FUNDING_MIN', 'WATCHLIST_TYPEB_FUNDING_MIN', '-0.001')),  # -0.1%
    'type_b_funding_max': float(_get_private('WATCHLIST_TYPEB_FUNDING_MAX', 'WATCHLIST_TYPEB_FUNDING_MAX', '0.001')),   # +0.1%
    # Type C：现货低于永续
    'type_c_spread_threshold': float(_get_private('WATCHLIST_TYPEC_SPREAD', 'WATCHLIST_TYPEC_SPREAD', '0.01')),  # 1%
    'type_c_funding_min': float(_get_private('WATCHLIST_TYPEC_FUNDING_MIN', 'WATCHLIST_TYPEC_FUNDING_MIN', '-0.001')),
}

# Watchlist 价差指标配置（仅计算，不做交易决策）
WATCHLIST_METRICS_CONFIG = {
    # 价差相对值 = (spot - futures) / spot
    'window_minutes': 60,                # 用于均值/方差/基线的滚动窗口
    'slope_minutes': 3,                  # 最近斜率的窗口（分钟）
    'midline_minutes': 15,               # 中线计算窗口
    'range_hours_short': 1,
    'range_hours_long': 12,
    'spread_abs_baseline': 0.05,         # 价差绝对阈值（5%）
    'volatility_threshold': 0.005,       # 价差波动率阈值（0.5%）
    'range_threshold_short': 0.01,       # 1h 区间阈值（1%）
    'range_threshold_long': 0.015,       # 12h 区间阈值（默认 1.5%）
    'crossing_min_count': 2,             # 穿越中线次数阈值（3h窗口）
    'drift_ratio_max': 0.3,              # Drift/RANGE 上限
    'take_profit_multiplier': 1.2,       # 止盈临界值 = mean_long + multiplier * std_long
    'stop_loss_buffer': 0.005,           # 止损缓冲（0.5%）
    'funding_exit_minutes': 5,           # 靠近资金费时间的提前平仓窗口
}

# 内存优化配置
MEMORY_OPTIMIZATION_CONFIG = {
    'max_historical_records': 120,     # 内存中的历史记录数（约6分钟窗口）
    'memory_cleanup_interval': 300,    # 内存清理间隔（5分钟）
    'batch_size': 50,                  # 批处理大小
    'data_compression': True,          # 启用数据压缩
}

# 数据库存储节流间隔（秒）
DB_WRITE_INTERVAL_SECONDS = 60

# 动态币种筛选参数
MARKET_FILTER_CONFIG = {
    # 为了“全量监听USDT币种”，放宽过滤门槛并提高上限
    'min_exchanges_support': 1,        # 至少任一交易所有现货或合约即可
    'min_completeness_score': 0,       # 不限制覆盖度评分
    'max_symbols': 5000,               # 提高上限，允许全量USDT币种
    'priority_symbols': ['BTC', 'ETH', 'BNB', 'ARB', 'SOL'],  # 关键币种优先纳入
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
