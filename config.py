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

# 支持的币种列表
SUPPORTED_SYMBOLS = ['LINK', 'WLFI', 'BTC', 'ETH']

# 数据刷新间隔（秒）
DATA_REFRESH_INTERVAL = 1