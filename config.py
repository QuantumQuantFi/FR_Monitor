# 配置文件

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

# 数据刷新间隔（秒）
DATA_REFRESH_INTERVAL = 1