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
DATA_REFRESH_INTERVAL = 15.0  # 进一步放缓主循环，减少CPU占用避免接口阻塞

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
REST_UPDATE_INTERVAL = 8.0          # REST 快照轮询间隔（秒，降低 CPU 占用）
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
    # 订单簿缓存刷新频率（秒），用于前端展示“买高卖低/卖高买低”等扫单口径数据
    # 注意：不要与 refresh_seconds 绑定；refresh_seconds 可能被调得很慢（如 10min），会导致前端订单簿长期为空。
    'orderbook_refresh_seconds': float(
        _get_private('WATCHLIST_ORDERBOOK_REFRESH_SECONDS', 'WATCHLIST_ORDERBOOK_REFRESH_SECONDS', '10')
    ),
    # Type B：跨交易所永续价差
    'type_b_spread_threshold': float(_get_private('WATCHLIST_TYPEB_SPREAD', 'WATCHLIST_TYPEB_SPREAD', '0.01')),  # 1%
    'type_b_funding_min': float(_get_private('WATCHLIST_TYPEB_FUNDING_MIN', 'WATCHLIST_TYPEB_FUNDING_MIN', '-0.001')),  # -0.1%
    'type_b_funding_max': float(_get_private('WATCHLIST_TYPEB_FUNDING_MAX', 'WATCHLIST_TYPEB_FUNDING_MAX', '0.001')),   # +0.1%
    # Type B 资金费过滤模式：
    # - "range": 要求两腿 funding_rate 同时落在 [min, max]（旧行为，默认）
    # - "net_cost": 允许 funding_rate 绝对值很大，但要求按「高价做空/低价做多」方向计算的净资金费“亏损”不超过阈值
    #   亏损定义：net_funding_over_horizon < 0 时，abs(net_funding_over_horizon) <= max_loss
    'type_b_funding_filter_mode': str(_get_private('WATCHLIST_TYPEB_FUNDING_FILTER_MODE', 'WATCHLIST_TYPEB_FUNDING_FILTER_MODE', 'net_cost')).strip().lower(),
    'type_b_funding_net_cost_max': float(_get_private('WATCHLIST_TYPEB_FUNDING_NET_COST_MAX', 'WATCHLIST_TYPEB_FUNDING_NET_COST_MAX', '0.001')),  # 0.1%
    'type_b_funding_net_cost_horizon_hours': float(_get_private('WATCHLIST_TYPEB_FUNDING_NET_COST_HORIZON_H', 'WATCHLIST_TYPEB_FUNDING_NET_COST_HORIZON_H', '8')),
    # Type C：现货低于永续
    'type_c_spread_threshold': float(_get_private('WATCHLIST_TYPEC_SPREAD', 'WATCHLIST_TYPEC_SPREAD', '0.01')),  # 1%
    'type_c_funding_min': float(_get_private('WATCHLIST_TYPEC_FUNDING_MIN', 'WATCHLIST_TYPEC_FUNDING_MIN', '-0.001')),
}

# 8010 订单簿聚合服务（open-trading Monitor/V2）：FR_Monitor 侧只做代理/触发信号，不做重复持久化。
MONITOR_8010_CONFIG = {
    'base_url': str(_get_private('MONITOR_8010_URL', 'MONITOR_8010_URL', 'http://127.0.0.1:8010')).strip(),
    'enabled': _is_truthy(_get_private('MONITOR_8010_ENABLED', 'MONITOR_8010_ENABLED', '1')),
    'timeout_seconds': float(_get_private('MONITOR_8010_TIMEOUT_SEC', 'MONITOR_8010_TIMEOUT_SEC', '3')),
    # FR_Monitor -> 8010 watchlist/add 默认参数（best-effort）
    'default_quote': str(_get_private('MONITOR_8010_DEFAULT_QUOTE', 'MONITOR_8010_DEFAULT_QUOTE', 'USDC')).strip().upper(),
    'watchlist_ttl_seconds': int(float(_get_private('MONITOR_8010_WL_TTL', 'MONITOR_8010_WL_TTL', '86400'))),
    'watchlist_source': str(_get_private('MONITOR_8010_WL_SOURCE', 'MONITOR_8010_WL_SOURCE', 'FR_Monitor')).strip(),
    # 去抖：同一 (symbol,exchanges) 最快多久发一次 add（避免每分钟 raw 都打 8010）
    'watchlist_signal_min_interval_sec': float(
        _get_private('MONITOR_8010_WL_MIN_INTERVAL_SEC', 'MONITOR_8010_WL_MIN_INTERVAL_SEC', '600')
    ),
    # raw 触发时希望 8010 订阅哪些交易所：
    # - 默认（推荐）：订阅 8010 /health 返回的“全量可用交易所”（以便前端/其他消费者拿到全量 BBO）
    # - 可显式指定 CSV：MONITOR_8010_WL_EXCHANGES=binance,okx,bybit,...
    # - 也可显式写 all/*：MONITOR_8010_WL_EXCHANGES=all
    'watchlist_exchanges': str(_get_private('MONITOR_8010_WL_EXCHANGES', 'MONITOR_8010_WL_EXCHANGES', '')).strip(),
    # 单次 add 的交易所上限（<=0 表示不限制；若 MONITOR_8010_WL_EXCHANGES=all，则默认不应用该限制）
    'watchlist_max_exchanges_per_symbol': int(float(_get_private(
        'MONITOR_8010_WL_MAX_EXCH', 'MONITOR_8010_WL_MAX_EXCH', '0'
    ))),
}

# Watchlist PG 写入配置（双写开关）
WATCHLIST_PG_CONFIG = {
    'enabled': _is_truthy(_get_private('WATCHLIST_PG_ENABLED', 'WATCHLIST_PG_ENABLED', '1')),
    'dsn': _get_private(
        'WATCHLIST_PG_DSN',
        'WATCHLIST_PG_DSN',
        'postgresql://wl_writer:wl_writer_A3f9xB2@127.0.0.1:5432/watchlist'
    ),
    'batch_size': int(float(_get_private('WATCHLIST_PG_BATCH', 'WATCHLIST_PG_BATCH', '500'))),
    'flush_seconds': float(_get_private('WATCHLIST_PG_FLUSH_SEC', 'WATCHLIST_PG_FLUSH_SEC', '5')),
    # 连续触发分钟数：1 可以显著降低 event 生成延迟（利于捕捉秒级价差），但会增加 event 数量与后续订单簿验算压力。
    'consecutive_required': int(float(_get_private('WATCHLIST_PG_CONSEC', 'WATCHLIST_PG_CONSEC', '1'))),
    'cooldown_minutes': int(float(_get_private('WATCHLIST_PG_COOLDOWN_MIN', 'WATCHLIST_PG_COOLDOWN_MIN', '3'))),
    # 默认开启事件合并；如需关闭可设置 WATCHLIST_PG_EVENT_MERGE=0
    'enable_event_merge': _is_truthy(_get_private('WATCHLIST_PG_EVENT_MERGE', 'WATCHLIST_PG_EVENT_MERGE', '1')),
    # 写入 event 时是否同步做订单簿验算：会显著增加写入延迟（网络 I/O），默认开启（但内部有阈值预筛选），live trading 仍会下单前二次复核。
    'orderbook_validation_on_write': _is_truthy(
        _get_private('WATCHLIST_PG_OB_VALIDATION', 'WATCHLIST_PG_OB_VALIDATION', '1')
    ),
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
    # v2 因子序列（event→series→factors）参数，仅 1min K
    'series_lookback_min': 21600,        # T2 因子含 3/7/15day，默认取 15d 覆盖
    'series_asof_tol_sec': 90,           # asof 最近邻容忍
    'series_forward_fill_bars': 2,       # 允许最多 forward-fill 的 bar 数
    'series_min_valid_ratio': 0.8,       # 允许窗口内最多 20% 缺失
    'series_max_backfill_bars': 2,       # 计算 ret 时允许回补的最大 bar
    'series_cache_entries': int(_get_private('WATCHLIST_SERIES_CACHE_ENTRIES', 'WATCHLIST_SERIES_CACHE_ENTRIES', '64')),
}

# Watchlist v2 推断配置（Ridge + Logistic）
WATCHLIST_V2_PRED_CONFIG = {
    'enabled': _is_truthy(_get_private('WATCHLIST_V2_ENABLED', 'WATCHLIST_V2_ENABLED', '1')),
    'model_path_240': _get_private(
        'WATCHLIST_V2_MODEL_240',
        'WATCHLIST_V2_MODEL_240',
        'reports/v2_ridge_logistic_model_h240.json',
    ),
    'model_path_1440': _get_private(
        'WATCHLIST_V2_MODEL_1440',
        'WATCHLIST_V2_MODEL_1440',
        'reports/v2_ridge_logistic_model_h1440.json',
    ),
    'max_missing_ratio': float(_get_private('WATCHLIST_V2_MAX_MISSING', 'WATCHLIST_V2_MAX_MISSING', '0.2')),
}

# Live trading (Phase 1: Type B only, perp-perp)
LIVE_TRADING_CONFIG = {
    # 默认开启自动实盘；如需关闭请设置 LIVE_TRADING_ENABLED=0
    'enabled': _is_truthy(_get_private('LIVE_TRADING_ENABLED', 'LIVE_TRADING_ENABLED', '1')),
    'horizon_min': int(float(_get_private('LIVE_TRADING_HORIZON_MIN', 'LIVE_TRADING_HORIZON_MIN', '240'))),
    # 入场阈值：同时满足 pnl_hat 与 win_prob（默认 240min）
    'pnl_threshold': float(_get_private('LIVE_TRADING_PNL_THRESHOLD', 'LIVE_TRADING_PNL_THRESHOLD', '0.0085')),
    'win_prob_threshold': float(_get_private('LIVE_TRADING_WIN_PROB_THRESHOLD', 'LIVE_TRADING_WIN_PROB_THRESHOLD', '0.85')),
    # v2 预测触发阈值（v2 的 win_prob 分布整体低于 v1；默认适度放宽以确保能触发）
    'v2_enabled': _is_truthy(_get_private('LIVE_TRADING_V2_ENABLED', 'LIVE_TRADING_V2_ENABLED', '1')),
    # v2 阈值：可按 horizon 独立配置；默认值以“每天触发量可控 + 不至于长期 0 v2 开仓”为目标。
    'v2_pnl_threshold_240': float(_get_private('LIVE_TRADING_V2_PNL_240', 'LIVE_TRADING_V2_PNL_240', '0.0080')),
    'v2_win_prob_threshold_240': float(_get_private('LIVE_TRADING_V2_PROB_240', 'LIVE_TRADING_V2_PROB_240', '0.82')),
    'v2_pnl_threshold_1440': float(_get_private('LIVE_TRADING_V2_PNL_1440', 'LIVE_TRADING_V2_PNL_1440', '0.0080')),
    'v2_win_prob_threshold_1440': float(_get_private('LIVE_TRADING_V2_PROB_1440', 'LIVE_TRADING_V2_PROB_1440', '0.81')),
    'max_concurrent_trades': int(float(_get_private('LIVE_TRADING_MAX_CONCURRENT', 'LIVE_TRADING_MAX_CONCURRENT', '10'))),
    # 扫描周期：建议使用 watchlist → kick 的事件驱动作为 fast-path，
    # 定时扫描作为兜底即可，避免频繁订单簿复核导致交易所 ban。
    'scan_interval_seconds': float(_get_private('LIVE_TRADING_SCAN_SEC', 'LIVE_TRADING_SCAN_SEC', '10')),
    # 以 kick 为主：watchlist 写入 event 后立即唤醒 live trading；同时保留 scan_interval_seconds 作为兜底。
    'kick_driven': _is_truthy(_get_private('LIVE_TRADING_KICK_DRIVEN', 'LIVE_TRADING_KICK_DRIVEN', '1')),
    'monitor_interval_seconds': float(_get_private('LIVE_TRADING_MONITOR_SEC', 'LIVE_TRADING_MONITOR_SEC', '60')),
    'take_profit_ratio': float(_get_private('LIVE_TRADING_TP_RATIO', 'LIVE_TRADING_TP_RATIO', '0.7')),
    # Optional guard for Type B: require both legs' current funding rates to satisfy
    # abs(funding_rate) <= max_abs_funding before opening a trade.
    # Default disabled (0) because watchlist already filters funding and we want maximum entry speed.
    'max_abs_funding': float(_get_private('LIVE_TRADING_MAX_ABS_FUNDING', 'LIVE_TRADING_MAX_ABS_FUNDING', '0')),
    # 订单簿快速复核：在“决定开仓/止盈平仓”前做多次短间隔验算，避免瞬时价差假信号。
    # 注意：每次验算会同时请求两家交易所的订单簿（REST），请结合限频调整。
    # 临时策略：仅保留 i0（单次）复核，降低 skipped（后续可再改回 3 次复核）。
    'orderbook_confirm_samples': int(float(_get_private('LIVE_TRADING_OB_CONFIRM_SAMPLES', 'LIVE_TRADING_OB_CONFIRM_SAMPLES', '1'))),
    'orderbook_confirm_sleep_seconds': float(_get_private('LIVE_TRADING_OB_CONFIRM_SLEEP_SEC', 'LIVE_TRADING_OB_CONFIRM_SLEEP_SEC', '0.7')),
    # 强制平仓：开仓后最大持仓天数（默认 7 天）
    'max_hold_days': int(float(_get_private('LIVE_TRADING_MAX_HOLD_DAYS', 'LIVE_TRADING_MAX_HOLD_DAYS', '7'))),
    # 止损：仓位盈亏 + 已收取资金费率亏损超过阈值（百分比，小数）
    'stop_loss_total_pnl_pct': float(
        _get_private('LIVE_TRADING_STOP_LOSS_TOTAL_PCT', 'LIVE_TRADING_STOP_LOSS_TOTAL_PCT', '0.01')
    ),
    # 止损：当前资金费率折算到 1h 后的净亏损超过阈值（百分比，小数）
    'stop_loss_funding_per_hour_pct': float(
        _get_private(
            'LIVE_TRADING_STOP_LOSS_FUNDING_PER_HOUR_PCT',
            'LIVE_TRADING_STOP_LOSS_FUNDING_PER_HOUR_PCT',
            '0.003',
        )
    ),
    'close_retry_cooldown_seconds': float(
        _get_private('LIVE_TRADING_CLOSE_RETRY_COOLDOWN_SEC', 'LIVE_TRADING_CLOSE_RETRY_COOLDOWN_SEC', '120')
    ),
    # Hyperliquid reduce-only close uses IOC limit internally; too small slippage may fail to cross the spread.
    'hyperliquid_close_slippage': float(
        _get_private('LIVE_TRADING_HL_CLOSE_SLIPPAGE', 'LIVE_TRADING_HL_CLOSE_SLIPPAGE', '0.03')
    ),
    # 价差扩大补救加仓（scale-in rescue）：默认开启。
    'scale_in_enabled': _is_truthy(_get_private('LIVE_TRADING_SCALE_IN_ENABLED', 'LIVE_TRADING_SCALE_IN_ENABLED', '1')),
    'scale_in_max_entries': int(float(_get_private('LIVE_TRADING_SCALE_IN_MAX_ENTRIES', 'LIVE_TRADING_SCALE_IN_MAX_ENTRIES', '4'))),
    'scale_in_trigger_mult': float(_get_private('LIVE_TRADING_SCALE_IN_TRIGGER_MULT', 'LIVE_TRADING_SCALE_IN_TRIGGER_MULT', '1.5')),
    'scale_in_min_interval_minutes': int(
        float(_get_private('LIVE_TRADING_SCALE_IN_MIN_INTERVAL_MIN', 'LIVE_TRADING_SCALE_IN_MIN_INTERVAL_MIN', '30'))
    ),
    'scale_in_signal_max_age_minutes': int(
        float(_get_private('LIVE_TRADING_SCALE_IN_SIGNAL_MAX_AGE_MIN', 'LIVE_TRADING_SCALE_IN_SIGNAL_MAX_AGE_MIN', '30'))
    ),
    # Safety cap: max total notional per leg per symbol across entries.
    # 留空/0 表示默认使用 max_entries * per_leg_notional_usdt。
    'scale_in_max_total_notional_usdt': float(
        _get_private('LIVE_TRADING_SCALE_IN_MAX_TOTAL_NOTIONAL', 'LIVE_TRADING_SCALE_IN_MAX_TOTAL_NOTIONAL', '0')
    ),
    # 只处理“最近”的 event，避免对历史候选重复做订单簿复核。
    'event_lookback_minutes': int(float(_get_private('LIVE_TRADING_LOOKBACK_MIN', 'LIVE_TRADING_LOOKBACK_MIN', '3'))),
    # 注意：不同交易所/合约对存在最小下单名义金额/最小下单数量；默认提升到 50U 以降低拒单概率。
    'per_leg_notional_usdt': float(_get_private('LIVE_TRADING_PER_LEG_USDT', 'LIVE_TRADING_PER_LEG_USDT', '50')),
    # 限制自动实盘允许使用的交易所（逗号分隔）；避免因为某个交易所接口变更/余额不足导致全局失败。
    'allowed_exchanges': _get_private(
        'LIVE_TRADING_ALLOWED_EXCHANGES',
        'LIVE_TRADING_ALLOWED_EXCHANGES',
        'binance,bybit,okx,bitget,hyperliquid,lighter,grvt',
    ),
    'candidate_limit': int(float(_get_private('LIVE_TRADING_CANDIDATE_LIMIT', 'LIVE_TRADING_CANDIDATE_LIMIT', '50'))),
    'per_symbol_top_k': int(float(_get_private('LIVE_TRADING_PER_SYMBOL_TOPK', 'LIVE_TRADING_PER_SYMBOL_TOPK', '3'))),
    # 每次扫描最多评估多少个币种（每个币种至少会请求 2 次订单簿：两家交易所各一次）。
    # 用于控制对 Binance/OKX 等 REST 深度接口的压力，避免触发 418 ban。
    'max_symbols_per_scan': int(float(_get_private('LIVE_TRADING_MAX_SYMBOLS_PER_SCAN', 'LIVE_TRADING_MAX_SYMBOLS_PER_SCAN', '8'))),
    # watchlist 写入 event 时的订单簿验算：每个 symbol 最多抓取 Top-K 个交易所的订单簿（一次/交易所），并在内存里评估所有 pair。
    'watchlist_event_topk_exchanges': int(
        float(_get_private('LIVE_TRADING_WATCHLIST_EVENT_TOPK_EXCH', 'LIVE_TRADING_WATCHLIST_EVENT_TOPK_EXCH', '5'))
    ),
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
