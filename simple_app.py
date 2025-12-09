from flask import Flask, render_template, jsonify, request
import json
import threading
import time
import gc
import sqlite3
import logging
from logging.handlers import RotatingFileHandler
import psutil
import os
import builtins
import sys
import decimal
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, wait
from exchange_connectors import ExchangeDataCollector
from arbitrage import ArbitrageMonitor
from config import (
    DATA_REFRESH_INTERVAL,
    CURRENT_SUPPORTED_SYMBOLS,
    MEMORY_OPTIMIZATION_CONFIG,
    WS_UPDATE_INTERVAL,
    WS_CONNECTION_CONFIG,
    DB_WRITE_INTERVAL_SECONDS,
    WATCHLIST_CONFIG,
    WATCHLIST_PG_CONFIG,
)
from database import PriceDatabase
from market_info import get_dynamic_symbols, get_market_report
from exchange_details import fetch_exchange_details
from trading.trade_executor import (
    TradeExecutionError,
    execute_dual_perp_market_order,
    execute_perp_market_batch,
    get_supported_trading_backends,
    get_bybit_linear_positions,
    get_bitget_usdt_perp_positions,
)
from watchlist_manager import WatchlistManager
from watchlist_metrics import (
    compute_series_with_signals,
    compute_metrics_for_entries,
    compute_series_for_entries,
)
from orderbook_utils import (
    fetch_orderbook_prices,
    compute_orderbook_spread,
    DEFAULT_SWEEP_NOTIONAL,
)
# optional PG browse
try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - optional dependency
    psycopg = None
    dict_row = None

LOG_DIR = os.environ.get("SIMPLE_APP_LOG_DIR", os.path.join("logs", "simple_app"))
LOG_FILE_NAME = "simple_app.log"
LOG_MAX_BYTES = 25 * 1024 * 1024  # 25MB per file, 4 files total <=100MB
LOG_BACKUP_COUNT = 3  # plus the active file -> 4*25MB = 100MB cap
_LOGGING_CONFIGURED = False
EXCHANGE_DISPLAY_ORDER = ['binance', 'okx', 'bybit', 'bitget', 'grvt', 'lighter', 'hyperliquid']

ORDERBOOK_CACHE: Dict[tuple, Dict[str, Any]] = {}
ORDERBOOK_CACHE_TTL = 60  # seconds，拉深度频率不必过高
ORDERBOOK_FETCH_TIMEOUT = 4.0
ORDERBOOK_CACHE_LOCK = threading.Lock()
# 订单簿抓取线程池减小并发，降低 CPU 占用
ORDERBOOK_EXECUTOR = ThreadPoolExecutor(max_workers=8)

# watchlist 订单簿/跨所价差的后台缓存，避免在请求路径阻塞
WATCHLIST_ORDERBOOK_SNAPSHOT = {
    'orderbook': {},
    'cross_spreads': {},
    'timestamp': None,
    'error': None,
    'stale_reason': 'not_ready',
}
WATCHLIST_METRICS_SNAPSHOT = {
    'metrics': {},
    'symbols': [],
    'timestamp': None,
    'error': None,
    'stale_reason': 'not_ready',
}

CHART_INTERVAL_OPTIONS = [
    ('1min', '1分钟'),
    ('5min', '5分钟'),
    ('15min', '15分钟'),
    ('30min', '30分钟'),
    ('1h', '1小时'),
    ('4h', '4小时'),
    ('1day', '1天'),
]
CHART_INTERVAL_MINUTES = {
    '1min': 1,
    '5min': 5,
    '15min': 15,
    '30min': 30,
    '1h': 60,
    '4h': 240,
    '1day': 1440,
}
DEFAULT_CHART_INTERVAL = '5min'
CHART_INTERVAL_LABEL_MAP = {value: label for value, label in CHART_INTERVAL_OPTIONS}
CHART_CACHE_TTL_SECONDS = 60
CHART_CACHE_MAX_ENTRIES = 64
_chart_cache = {}
_chart_cache_lock = threading.Lock()


def _get_chart_cache_entry(cache_key):
    """Fetch cached chart payload if it is still fresh."""
    now = time.time()
    with _chart_cache_lock:
        entry = _chart_cache.get(cache_key)
        if not entry:
            return None
        if now - entry['timestamp'] > CHART_CACHE_TTL_SECONDS:
            _chart_cache.pop(cache_key, None)
            return None
        return {'timestamp': entry['timestamp'], 'payload': entry['payload']}


def _set_chart_cache_entry(cache_key, payload):
    """Store chart payload with eviction of the oldest item if needed."""
    now = time.time()
    with _chart_cache_lock:
        if len(_chart_cache) >= CHART_CACHE_MAX_ENTRIES:
            oldest_key = min(_chart_cache.items(), key=lambda item: item[1]['timestamp'])[0]
            _chart_cache.pop(oldest_key, None)
        _chart_cache[cache_key] = {'timestamp': now, 'payload': payload}


def configure_logging() -> None:
    """Configure application-wide rotating file logging and capture stdout prints."""
    global _LOGGING_CONFIGURED
    if _LOGGING_CONFIGURED:
        return

    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, LOG_FILE_NAME)

    handler = RotatingFileHandler(
        log_path,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    formatter = logging.Formatter(
        "%(asctime)s %(name)s [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.WARNING)  # keep stdout noise low to avoid huge service logs
    stream_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    # 记录到文件的默认等级调低，减少锁争用
    root_logger.setLevel(logging.WARNING)
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.addHandler(stream_handler)

    flask_logger = logging.getLogger("flask.app")
    flask_logger.handlers.clear()
    flask_logger.propagate = True

    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    original_print = builtins.print

    def logging_print(*args, **kwargs):
        target = kwargs.get("file", sys.stdout)
        if target not in (sys.stdout, sys.stderr, None):
            return original_print(*args, **kwargs)

        sep = kwargs.get("sep", " ")
        end = kwargs.get("end", "\n")
        message = sep.join(str(arg) for arg in args)
        if end and end != "\n":
            message = f"{message}{end}"

        logger = logging.getLogger("simple_app.stdout")
        if message:
            logger.info(message)
        else:
            logger.info("")

    builtins.print = logging_print
    _LOGGING_CONFIGURED = True


configure_logging()

# 自定义JSON编码器以保持数值精度，避免科学计数法
class PrecisionJSONEncoder(json.JSONEncoder):
    @staticmethod
    def _format_fraction(value_str: str, minimum_fraction: int) -> str:
        """Ensure fraction part keeps at least minimum_fraction digits."""
        if '.' not in value_str:
            return value_str
        integer, fraction = value_str.split('.')
        trimmed = fraction.rstrip('0')
        if len(trimmed) < minimum_fraction:
            trimmed = (fraction[:minimum_fraction]).ljust(minimum_fraction, '0')
        return f"{integer}.{trimmed}"

    def _format_float(self, value: float):
        if value == 0:
            return 0
        abs_value = abs(value)
        if abs_value < 0.001:
            formatted = f"{value:.12f}"
            return self._format_fraction(formatted, 6)
        if abs_value < 1:
            formatted = f"{value:.8f}"
            return self._format_fraction(formatted, 4)
        return value

    def _format_funding_value(self, value):
        if value in (None, ''):
            return value
        if value in (0, '0'):
            return '0'
        if isinstance(value, str):
            return value
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return value
        formatted = f"{numeric:.12f}".rstrip('0').rstrip('.')
        return formatted or '0'

    def _process_value(self, key, value):
        """处理单个值，特别关注资金费率字段的精度"""
        if key == 'funding_rate':
            return self._format_funding_value(value)
        if key == 'funding_rates' and isinstance(value, list):
            return [self._format_funding_value(item) for item in value]
        if isinstance(value, float) and value != 0:
            return self._format_float(value)
        return value
    
    def _process_object(self, obj):
        """递归处理对象，保持数值精度"""
        if isinstance(obj, dict):
            return {key: self._process_object(self._process_value(key, value)) 
                   for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._process_object(item) for item in obj]
        elif isinstance(obj, float):
            return self._format_float(obj)
        return obj
    
    def encode(self, obj):
        processed_obj = self._process_object(obj)
        return super().encode(processed_obj)
    
    def iterencode(self, obj, _one_shot=False):
        processed_obj = self._process_object(obj)
        return super().iterencode(processed_obj, _one_shot)

app = Flask(__name__)

# 配置Flask应用使用自定义JSON编码器
app.json_encoder = PrecisionJSONEncoder

# 自定义高精度JSON响应函数
def precision_jsonify(*args, **kwargs):
    """使用自定义编码器的jsonify，保持数值精度"""
    from flask import Response
    if args and kwargs:
        raise TypeError('jsonify() takes either *args or **kwargs, not both')
    if args:
        data = args[0] if len(args) == 1 else args
    else:
        data = kwargs
    
    json_string = json.dumps(data, cls=PrecisionJSONEncoder, ensure_ascii=False, separators=(',', ':'))
    return Response(json_string, mimetype='application/json')


PERPETUAL_MARKET_KEYS = {"perpetual", "futures"}
DEFAULT_QUANT = Decimal("0.00000001")


def _safe_number(value):
    try:
        return float(value)
    except (TypeError, ValueError, decimal.InvalidOperation):
        return 0.0


def _parse_db_timestamp(value):
    """将数据库中的时间字符串转换为 datetime 对象"""
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    text = text.replace('T', ' ')
    if text.endswith('Z'):
        text = text[:-1]
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        try:
            base = text.split('.')[0]
            return datetime.strptime(base, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None


def _floor_timestamp(dt, interval_minutes):
    """将时间戳向下取整到指定分钟粒度"""
    if not interval_minutes:
        return dt.replace(second=0, microsecond=0)
    day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    minutes_since_day_start = dt.hour * 60 + dt.minute
    bucket_minutes = (minutes_since_day_start // interval_minutes) * interval_minutes
    return day_start + timedelta(minutes=bucket_minutes)


def resample_price_rows(rows, interval):
    """将1分钟K线数据聚合为更高粒度"""
    if interval == '1min':
        return rows
    interval_minutes = CHART_INTERVAL_MINUTES.get(interval)
    if not interval_minutes:
        return rows
    buckets = {}
    for row in rows:
        ts = _parse_db_timestamp(row.get('timestamp'))
        if not ts:
            continue
        exchange = row.get('exchange')
        symbol = row.get('symbol')
        bucket_start = _floor_timestamp(ts, interval_minutes)
        key = (exchange, bucket_start)
        data_points = row.get('data_points') or 1
        spot_high = _safe_number(row.get('spot_price_high'))
        spot_low = _safe_number(row.get('spot_price_low'))
        futures_high = _safe_number(row.get('futures_price_high'))
        futures_low = _safe_number(row.get('futures_price_low'))
        entry = buckets.get(key)
        if not entry:
            entry = {
                'symbol': symbol,
                'exchange': exchange,
                'timestamp': bucket_start,
                'first_ts': ts,
                'last_ts': ts,
                'spot_price_open': _safe_number(row.get('spot_price_open')),
                'spot_price_close': _safe_number(row.get('spot_price_close')),
                'spot_price_high': spot_high,
                'spot_price_low': spot_low if spot_low > 0 else 0.0,
                'futures_price_open': _safe_number(row.get('futures_price_open')),
                'futures_price_close': _safe_number(row.get('futures_price_close')),
                'futures_price_high': futures_high,
                'futures_price_low': futures_low if futures_low > 0 else 0.0,
                'funding_weighted_sum': _safe_number(row.get('funding_rate_avg')) * data_points,
                'premium_weighted_sum': _safe_number(row.get('premium_percent_avg')) * data_points,
                'volume_weighted_sum': _safe_number(row.get('volume_24h_avg')) * data_points,
                'total_points': data_points,
                'funding_interval_hours': row.get('funding_interval_hours'),
                'next_funding_time': row.get('next_funding_time'),
            }
            buckets[key] = entry
        else:
            entry['total_points'] += data_points
            entry['funding_weighted_sum'] += _safe_number(row.get('funding_rate_avg')) * data_points
            entry['premium_weighted_sum'] += _safe_number(row.get('premium_percent_avg')) * data_points
            entry['volume_weighted_sum'] += _safe_number(row.get('volume_24h_avg')) * data_points
            entry['spot_price_high'] = max(entry['spot_price_high'], spot_high)
            entry['futures_price_high'] = max(entry['futures_price_high'], futures_high)
            if spot_low > 0:
                if entry['spot_price_low'] <= 0:
                    entry['spot_price_low'] = spot_low
                else:
                    entry['spot_price_low'] = min(entry['spot_price_low'], spot_low)
            if futures_low > 0:
                if entry['futures_price_low'] <= 0:
                    entry['futures_price_low'] = futures_low
                else:
                    entry['futures_price_low'] = min(entry['futures_price_low'], futures_low)
            if ts < entry['first_ts']:
                entry['first_ts'] = ts
                entry['spot_price_open'] = _safe_number(row.get('spot_price_open'))
                entry['futures_price_open'] = _safe_number(row.get('futures_price_open'))
            if ts > entry['last_ts']:
                entry['last_ts'] = ts
                entry['spot_price_close'] = _safe_number(row.get('spot_price_close'))
                entry['futures_price_close'] = _safe_number(row.get('futures_price_close'))
                entry['funding_interval_hours'] = row.get('funding_interval_hours')
                entry['next_funding_time'] = row.get('next_funding_time')

    result = []
    for entry in buckets.values():
        total_points = entry['total_points'] or 1
        timestamp_dt = entry['timestamp']
        timestamp_str = timestamp_dt.strftime('%Y-%m-%d %H:%M:%S')
        result.append({
            'timestamp': timestamp_str,
            'symbol': entry['symbol'],
            'exchange': entry['exchange'],
            'spot_price_open': entry['spot_price_open'],
            'spot_price_high': entry['spot_price_high'],
            'spot_price_low': entry['spot_price_low'],
            'spot_price_close': entry['spot_price_close'],
            'futures_price_open': entry['futures_price_open'],
            'futures_price_high': entry['futures_price_high'],
            'futures_price_low': entry['futures_price_low'],
            'futures_price_close': entry['futures_price_close'],
            'funding_rate_avg': entry['funding_weighted_sum'] / total_points if total_points else 0.0,
            'premium_percent_avg': entry['premium_weighted_sum'] / total_points if total_points else 0.0,
            'volume_24h_avg': entry['volume_weighted_sum'] / total_points if total_points else 0.0,
            'data_points': total_points,
            'funding_interval_hours': entry.get('funding_interval_hours'),
            'next_funding_time': entry.get('next_funding_time'),
            '_timestamp_dt': timestamp_dt,
        })
    result.sort(key=lambda item: item['_timestamp_dt'], reverse=True)
    for entry in result:
        entry.pop('_timestamp_dt', None)
    return result


class TradeRequestValidationError(Exception):
    """Raised when incoming trade instructions fail validation."""


def _decimal_to_str(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _coerce_positive_decimal(value: Any, field: str) -> Decimal:
    try:
        numeric = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise TradeRequestValidationError(f"{field} 必须是有效的正数") from None
    if numeric <= 0:
        raise TradeRequestValidationError(f"{field} 必须是有效的正数")
    return numeric


def _resolve_market_key(market_type: str) -> str:
    key = (market_type or "").lower()
    if key in PERPETUAL_MARKET_KEYS:
        return "futures"
    if key == "spot":
        return "spot"
    raise TradeRequestValidationError(f"不支持的市场类型: {market_type}")


def _lookup_recent_price(symbol: str, exchange: str, market_key: str) -> Optional[Decimal]:
    exchange_key = (exchange or "").lower()
    if not exchange_key:
        return None

    symbol_candidates = {symbol}
    try:
        normalized = data_collector.normalize_symbol_for_exchange(exchange_key, symbol)
        symbol_candidates.add(normalized)
    except Exception:
        pass

    data_bucket = data_collector.data.get(exchange_key, {}) if hasattr(data_collector, "data") else {}
    for candidate in symbol_candidates:
        entry = data_bucket.get(candidate)
        if not entry:
            continue
        market_entry = entry.get(market_key, {}) if isinstance(entry, dict) else {}
        price = market_entry.get("price")
        try:
            price_decimal = Decimal(str(price))
        except (InvalidOperation, TypeError, ValueError):
            continue
        if price_decimal > 0:
            return price_decimal

    for candidate in symbol_candidates:
        rows = db.get_latest_prices(candidate)
        for row in rows:
            row_exchange = (row.get("exchange") or "").lower()
            if row_exchange != exchange_key:
                continue
            field = "futures_price" if market_key == "futures" else "spot_price"
            price = row.get(field)
            try:
                price_decimal = Decimal(str(price))
            except (InvalidOperation, TypeError, ValueError):
                continue
            if price_decimal > 0:
                return price_decimal

    return None


def _convert_notional_to_quantity(
    symbol: str,
    exchange: str,
    market_type: str,
    notional: Any,
):
    """Return the derived (quantity, reference price, notional) tuple."""
    market_key = _resolve_market_key(market_type)
    price = _lookup_recent_price(symbol, exchange, market_key)
    if price is None:
        raise TradeRequestValidationError(
            f"无法获取 {exchange.upper()} {symbol} 最新价格，请稍后再试"
        )

    notional_dec = _coerce_positive_decimal(notional, "交易金额(USDT)")
    with decimal.localcontext() as ctx:  # type: ignore[attr-defined]
        ctx.prec = 28
        quantity = (notional_dec / price).quantize(DEFAULT_QUANT, rounding=ROUND_DOWN)

    if quantity <= 0:
        raise TradeRequestValidationError("换算后的下单数量过小，请提高USDT金额")

    return quantity, price, notional_dec


# 交易动作配置字典 - 定义6种交易操作的标准行为
# 包括：开多、开空、平多（部分）、平空（部分）、平多全部、平空全部
TRADE_ACTION_CONFIG: Dict[str, Dict[str, Any]] = {
    "open_long": {  # 开多仓
        "side": "long",              # 交易方向：做多
        "reduce_only": None,         # 不限制为只平仓
        "requires_notional": True,   # 需要提供USDT金额
        "close_all": False,          # 不是全部平仓操作
        "target_position": None,     # 不针对现有持仓
    },
    "open_short": {  # 开空仓
        "side": "short",             # 交易方向：做空
        "reduce_only": None,         # 不限制为只平仓
        "requires_notional": True,   # 需要提供USDT金额
        "close_all": False,          # 不是全部平仓操作
        "target_position": None,     # 不针对现有持仓
    },
    "close_long": {  # 平多仓（部分平仓）
        "side": "short",             # 交易方向：做空以平多
        "reduce_only": True,         # 只平仓，不开新仓
        "requires_notional": True,   # 需要提供USDT金额（指定平仓数量）
        "close_all": False,          # 部分平仓
        "target_position": "long",   # 目标平仓的持仓类型：多仓
    },
    "close_short": {  # 平空仓（部分平仓）
        "side": "long",              # 交易方向：做多以平空
        "reduce_only": True,         # 只平仓，不开新仓
        "requires_notional": True,   # 需要提供USDT金额（指定平仓数量）
        "close_all": False,          # 部分平仓
        "target_position": "short",  # 目标平仓的持仓类型：空仓
    },
    "close_long_all": {  # 平多全部（一键平仓）
        "side": "short",             # 交易方向：做空以平多
        "reduce_only": True,         # 只平仓，不开新仓
        "requires_notional": False,  # 不需要USDT金额（自动查询持仓）
        "close_all": True,           # 全部平仓标志
        "target_position": "long",   # 目标平仓的持仓类型：多仓
    },
    "close_short_all": {  # 平空全部（一键平仓）
        "side": "long",              # 交易方向：做多以平空
        "reduce_only": True,         # 只平仓，不开新仓
        "requires_notional": False,  # 不需要USDT金额（自动查询持仓）
        "close_all": True,           # 全部平仓标志
        "target_position": "short",  # 目标平仓的持仓类型：空仓
    },
}


def _resolve_close_all_quantity(
    symbol: str,
    exchange_key: str,
    target_position: str,
    order_context: Dict[str, Any],
    *,
    market_type: str,
) -> Decimal:
    """
    自动查询持仓数量用于全部平仓操作

    功能：
    - 支持 Bybit Linear 和 Bitget USDT Perp 自动查询持仓
    - 根据 target_position (long/short) 确定需要平仓的持仓方向
    - 返回精确的持仓数量，避免手动输入错误

    参数：
    - symbol: 交易标的（如 BTC）
    - exchange_key: 交易所标识（bybit/bitget）
    - target_position: 目标持仓类型（long=多仓, short=空仓）
    - order_context: 订单上下文（包含API凭证等）
    - market_type: 市场类型（perpetual/futures）

    返回：
    - Decimal: 需要平仓的精确数量

    异常：
    - TradeRequestValidationError: 持仓为空或不支持的交易所
    """

    exchange_norm = exchange_key.lower()
    target_norm = (target_position or "").lower()

    if target_norm not in {"long", "short"}:
        raise TradeRequestValidationError("无法识别需平仓的持仓方向，请检查指令配置")

    if exchange_norm == "bybit":
        category = str(order_context.get("category") or "linear").lower()
        recv_window = int(order_context.get("recv_window") or 5000)
        base_url = order_context.get("base_url") or "https://api.bybit.com"

        try:
            positions = get_bybit_linear_positions(
                symbol=symbol,
                category=category,
                recv_window=recv_window,
                api_key=order_context.get("api_key"),
                secret_key=order_context.get("secret_key"),
                base_url=base_url,
            )
        except TradeExecutionError as exc:
            raise TradeRequestValidationError(
                f"无法查询 Bybit 持仓用于全部平仓: {exc}"
            ) from exc

        desired_side = "buy" if target_norm == "long" else "sell"
        total = Decimal("0")
        for position in positions:
            side = (position.get("side") or "").lower()
            if side != desired_side:
                continue
            size_raw = position.get("size") or position.get("qty")
            if size_raw in (None, "", 0, "0"):
                continue
            try:
                size_dec = Decimal(str(size_raw))
            except (InvalidOperation, TypeError):
                continue
            if size_dec > 0:
                total += size_dec

        if total <= 0:
            raise TradeRequestValidationError("当前 Bybit 持仓为空，无需执行全部平仓指令")

        return total.quantize(DEFAULT_QUANT, rounding=ROUND_DOWN)

    if exchange_norm == "bitget":
        margin_coin = order_context.get("margin_coin") or "USDT"
        base_url = order_context.get("base_url") or "https://api.bitget.com"
        product_type = order_context.get("product_type") or "umcbl"

        try:
            positions = get_bitget_usdt_perp_positions(
                symbol=symbol,
                margin_coin=margin_coin,
                product_type=product_type,
                api_key=order_context.get("api_key"),
                secret_key=order_context.get("secret_key"),
                passphrase=order_context.get("passphrase"),
                base_url=base_url,
            )
        except TradeExecutionError as exc:
            raise TradeRequestValidationError(
                f"无法查询 Bitget 持仓用于全部平仓: {exc}"
            ) from exc

        desired_side = "long" if target_norm == "long" else "short"
        total = Decimal("0")
        for position in positions:
            hold_side = (
                position.get("holdSide")
                or position.get("hold_side")
                or position.get("side")
                or ""
            ).lower()
            if hold_side != desired_side:
                continue

            size_candidates = [
                position.get("total"),
                position.get("totalSize"),
                position.get("available"),
                position.get("availableSize"),
                position.get("size"),
                position.get("holdAmount"),
            ]

            for value in size_candidates:
                if value in (None, "", 0, "0"):
                    continue
                try:
                    size_dec = Decimal(str(value))
                except (InvalidOperation, TypeError):
                    continue
                if size_dec > 0:
                    total += size_dec
                    break

        if total <= 0:
            raise TradeRequestValidationError("当前 Bitget 持仓为空，无需执行全部平仓指令")

        return total.quantize(DEFAULT_QUANT, rounding=ROUND_DOWN)

    raise TradeRequestValidationError(
        f"{exchange_key.upper()} 暂未支持“全部平仓”快捷操作，请改为手动输入USDT金额"
    )


def _execute_multi_leg_trade(
    symbol: str,
    market_type: str,
    legs_payload: Any,
    base_order_kwargs: dict,
    base_client_order_id: Optional[str],
):
    """
    执行多腿交易（支持同时在多个交易所下单）

    功能：
    - 批量处理多条交易指令（legs）
    - 自动识别交易动作类型（开仓/平仓/全部平仓）
    - 支持 USDT 金额自动转换为交易数量
    - 支持全部平仓自动查询持仓数量
    - 统一处理 client_order_id 分配

    参数：
    - symbol: 交易标的（如 BTC）
    - market_type: 市场类型（perpetual/futures/spot）
    - legs_payload: 交易腿列表，每条包含 exchange、action、notional 等
    - base_order_kwargs: 基础订单参数（如 API 凭证）
    - base_client_order_id: 基础客户订单ID（可选）

    返回：
    - List[Dict]: 每条交易腿的执行结果，包含订单详情

    异常：
    - TradeRequestValidationError: 参数验证失败
    - TradeExecutionError: 订单执行失败
    """
    if not isinstance(legs_payload, list) or not legs_payload:
        raise TradeRequestValidationError("请至少配置一条交易腿")

    sanitized_base_kwargs = {
        key: value for key, value in (base_order_kwargs or {}).items() if key != "client_order_id"
    }
    prepared_legs = []
    leg_details = []

    for index, leg in enumerate(legs_payload, start=1):
        if not isinstance(leg, dict):
            raise TradeRequestValidationError(f"第{index}条指令格式错误")

        exchange = leg.get("exchange")
        if not exchange:
            raise TradeRequestValidationError(f"第{index}条指令缺少交易所")
        exchange_key = (exchange or "").lower()

        leg_kwargs = dict(sanitized_base_kwargs)
        leg_specific_kwargs = leg.get("order_kwargs")
        if isinstance(leg_specific_kwargs, dict):
            leg_kwargs.update(leg_specific_kwargs)

        raw_action = leg.get("action")
        action_key = str(raw_action).lower() if raw_action is not None else ""
        action_config = TRADE_ACTION_CONFIG.get(action_key)
        close_all_flag = bool(leg.get("close_all"))
        requires_notional = False
        reduce_only_flag: Optional[bool] = None
        target_position: Optional[str] = None

        if action_config:
            side = action_config["side"]
            reduce_only_flag = action_config.get("reduce_only")
            requires_notional = bool(action_config.get("requires_notional"))
            if action_config.get("close_all"):
                close_all_flag = True
            target_position = action_config.get("target_position")
        else:
            side = leg.get("side")

        if side is None:
            raise TradeRequestValidationError(f"第{index}条指令缺少方向")

        side_key = str(side).lower()
        if side_key not in {"long", "short"}:
            raise TradeRequestValidationError(f"第{index}条指令方向不支持: {side}")

        leg_market = (leg.get("market_type") or market_type or "").lower()
        try:
            market_key = _resolve_market_key(leg_market)
        except TradeRequestValidationError as exc:
            raise TradeRequestValidationError(f"第{index}条指令市场类型不支持: {leg_market}") from exc

        if reduce_only_flag is True:
            leg_kwargs["reduce_only"] = True
        elif reduce_only_flag is False:
            leg_kwargs["reduce_only"] = False

        specific_client_id = leg.get("client_order_id")
        if specific_client_id:
            leg_kwargs["client_order_id"] = str(specific_client_id)
        elif base_client_order_id:
            base_id = str(base_client_order_id)
            trimmed = base_id[-20:] if len(base_id) > 20 else base_id
            leg_kwargs["client_order_id"] = f"{trimmed}-{index:02d}"

        if close_all_flag and not target_position:
            target_position = "long" if side_key == "short" else "short"

        quantity_dec: Optional[Decimal] = None
        price_dec: Optional[Decimal] = None
        notional_dec: Optional[Decimal] = None

        if close_all_flag:
            quantity_dec = _resolve_close_all_quantity(
                symbol,
                exchange_key,
                target_position or ("long" if side_key == "short" else "short"),
                leg_kwargs,
                market_type=leg_market,
            )
        elif leg.get("quantity") is not None:
            quantity_dec = _coerce_positive_decimal(leg.get("quantity"), "下单数量").quantize(
                DEFAULT_QUANT, rounding=ROUND_DOWN
            )
        else:
            notional_value = leg.get("notional")
            if notional_value is None or (isinstance(notional_value, str) and not notional_value.strip()):
                notional_value = leg.get("notional_usdt")
            if notional_value is None or (isinstance(notional_value, str) and not notional_value.strip()):
                if requires_notional or action_config:
                    raise TradeRequestValidationError(f"第{index}条指令缺少USDT金额")
                raise TradeRequestValidationError(f"第{index}条指令缺少USDT金额")
            quantity_dec, price_dec, notional_dec = _convert_notional_to_quantity(
                symbol,
                exchange_key,
                leg_market,
                notional_value,
            )

        if quantity_dec is None:
            raise TradeRequestValidationError(f"第{index}条指令无法确定下单数量")
        if quantity_dec <= 0:
            raise TradeRequestValidationError(f"第{index}条指令换算后的下单数量过小，请提高USDT金额")

        prepared_legs.append(
            {
                "exchange": exchange_key,
                "side": side_key,
                "quantity": quantity_dec,
                "order_kwargs": leg_kwargs,
                "action": action_key or None,
                "close_all": close_all_flag,
            }
        )

        if price_dec is None:
            try:
                price_dec = _lookup_recent_price(symbol, exchange_key, market_key)
                if price_dec is not None and notional_dec is None:
                    with decimal.localcontext() as ctx:  # type: ignore[attr-defined]
                        ctx.prec = 28
                        notional_dec = (quantity_dec * price_dec).quantize(
                            DEFAULT_QUANT, rounding=ROUND_DOWN
                        )
            except TradeRequestValidationError:
                price_dec = None
            except Exception:
                price_dec = None

        leg_details.append(
            {
                "exchange": exchange_key,
                "side": side_key,
                "market_type": leg_market,
                "notional_usdt": _decimal_to_str(notional_dec) if isinstance(notional_dec, Decimal) else None,
                "reference_price": _decimal_to_str(price_dec) if isinstance(price_dec, Decimal) else None,
                "base_quantity": _decimal_to_str(quantity_dec),
                "client_order_id": leg_kwargs.get("client_order_id"),
                "action": action_key or None,
                "close_all": close_all_flag,
                "reduce_only": leg_kwargs.get("reduce_only"),
                "target_position": target_position,
            }
        )

    execution_results = execute_perp_market_batch(symbol, prepared_legs)
    combined = []
    for detail, execution in zip(leg_details, execution_results):
        combined.append(
            {
                **detail,
                "exchange": execution.get("exchange", detail["exchange"]),
                "side": execution.get("side", detail["side"]),
                "normalized_quantity": execution.get("quantity"),
                "order": execution.get("order"),
            }
        )

    return combined

# 全局数据收集器
data_collector = ExchangeDataCollector()

# 数据库实例
db = PriceDatabase()

# 价差套利监控器
arbitrage_monitor = ArbitrageMonitor(
    spread_threshold=0.6,
    sample_window=3,
    sample_interval_seconds=8.0,
    cooldown_seconds=30.0,
)

# Binance 动态关注列表管理器
watchlist_manager = WatchlistManager(WATCHLIST_CONFIG, WATCHLIST_PG_CONFIG)

# 启动时尝试用数据库过去窗口内的资金费率预热，避免重启后空窗口
try:
    watchlist_manager.preload_from_database(db.db_path)
except Exception as exc:
    logging.getLogger('watchlist').warning("watchlist preload at startup failed: %s", exc)

# 优化的内存数据结构 - 减少内存占用
class MemoryDataManager:
    def __init__(self):
        self.max_records = MEMORY_OPTIMIZATION_CONFIG['max_historical_records']
        self.cleanup_interval = MEMORY_OPTIMIZATION_CONFIG['memory_cleanup_interval']
        self.last_cleanup = datetime.now()
        self.data = {}
        self._init_data_structure()
    
    def _init_data_structure(self):
        """初始化数据结构"""
        for symbol in CURRENT_SUPPORTED_SYMBOLS:
            self.data[symbol] = {
                'okx': [],
                'binance': [],
                'bybit': [],
                'bitget': []
            }
    
    def add_record(self, symbol, exchange, record):
        """添加记录并控制内存使用"""
        if symbol not in self.data:
            self.data[symbol] = {
                'okx': [],
                'binance': [],
                'bybit': [],
                'bitget': []
            }
        
        exchange_data = self.data[symbol].get(exchange, [])
        exchange_data.append(record)
        
        # 控制内存使用 - 保持记录数不超过限制
        if len(exchange_data) > self.max_records:
            # 移除最旧的记录
            exchange_data.pop(0)
        
        self.data[symbol][exchange] = exchange_data
    
    def get_data(self, symbol=None):
        """获取数据"""
        if symbol:
            return self.data.get(symbol, {})
        return self.data
    
    def cleanup_memory(self):
        """定期内存清理"""
        current_time = datetime.now()
        if (current_time - self.last_cleanup).seconds >= self.cleanup_interval:
            print("执行内存清理...")
            # 清理空的数据结构
            empty_symbols = []
            for symbol, symbol_data in self.data.items():
                if all(len(exchange_data) == 0 for exchange_data in symbol_data.values()):
                    empty_symbols.append(symbol)
            
            for symbol in empty_symbols:
                del self.data[symbol]
            
            if empty_symbols:
                print(f"清理了 {len(empty_symbols)} 个空的币种数据")
            
            self.last_cleanup = current_time

# 使用优化的内存管理器
memory_manager = MemoryDataManager()


def now_utc_iso() -> str:
    """Return current time as ISO 8601 string in UTC with timezone info.
    Example: '2024-09-03T07:00:00.123456+00:00'
    """
    return datetime.now(timezone.utc).isoformat()


def _orderbook_cache_key(exchange: str, market_type: str, symbol: str) -> tuple:
    return (exchange.lower(), market_type, symbol.upper())


def _prefetch_orderbooks(leg_keys: List[tuple], sweep_notional: float) -> Dict[tuple, Dict[str, Any]]:
    """并发拉取订单簿，带 15s 缓存，避免阻塞 /api/watchlist。"""
    now = time.time()
    ready: Dict[tuple, Dict[str, Any]] = {}
    to_fetch: List[tuple] = []
    with ORDERBOOK_CACHE_LOCK:
        for key in set(leg_keys):
            cached = ORDERBOOK_CACHE.get(key)
            if cached and now - cached.get('ts', 0) < ORDERBOOK_CACHE_TTL:
                ready[key] = cached.get('val')
            else:
                to_fetch.append(key)
    futures: Dict[Any, tuple] = {}
    for key in to_fetch:
        exch, market_type, sym = key
        try:
            fut = ORDERBOOK_EXECUTOR.submit(
                fetch_orderbook_prices,
                exch,
                sym,
                market_type,
                notional=sweep_notional,
            )
            futures[fut] = key
        except Exception:
            continue
    if futures:
        done, pending = wait(list(futures.keys()), timeout=ORDERBOOK_FETCH_TIMEOUT)
        for fut in done:
            key = futures.get(fut)
            try:
                val = fut.result()
            except Exception as exc:
                val = {'error': str(exc)}
            if key:
                ready[key] = val
                with ORDERBOOK_CACHE_LOCK:
                    ORDERBOOK_CACHE[key] = {'ts': time.time(), 'val': val}
        for fut in pending:
            key = futures.get(fut)
            if key:
                val = {'error': 'timeout'}
                ready[key] = val
                with ORDERBOOK_CACHE_LOCK:
                    ORDERBOOK_CACHE[key] = {'ts': time.time(), 'val': val}
    return ready


def _entry_legs_for_orderbook(entry: Dict[str, Any]) -> List[Dict[str, str]]:
    etype = entry.get('entry_type')
    trigger = entry.get('trigger_details') or {}
    symbol = entry.get('symbol')
    if not symbol:
        return []
    if etype == 'A':
        return [
            {'exchange': 'binance', 'market_type': 'spot'},
            {'exchange': 'binance', 'market_type': 'perp'},
        ]
    if etype == 'B':
        pair = trigger.get('pair') or []
        if len(pair) == 2:
            return [
                {'exchange': pair[0], 'market_type': 'perp'},
                {'exchange': pair[1], 'market_type': 'perp'},
            ]
    if etype == 'C':
        spot_ex = trigger.get('spot_exchange')
        fut_ex = trigger.get('futures_exchange')
        if spot_ex and fut_ex:
            return [
                {'exchange': spot_ex, 'market_type': 'spot'},
                {'exchange': fut_ex, 'market_type': 'perp'},
            ]
    return []


def build_orderbook_annotation(entries: List[Dict[str, Any]], sweep_notional: float = DEFAULT_SWEEP_NOTIONAL) -> Dict[str, Any]:
    """
    对 watchlist 条目补充订单簿扫单价格（模拟 sweep 100 USDT），并计算双向价差：
    - forward: 高侧用买单簿（asks）价格，低侧用卖单簿（bids）
    - reverse: 高侧用卖单簿（bids），低侧用买单簿（asks）
    """
    out: Dict[str, Any] = {}
    leg_keys: List[tuple] = []
    for entry in entries:
        symbol = entry.get('symbol')
        if not symbol:
            continue
        for leg in _entry_legs_for_orderbook(entry):
            leg_keys.append(_orderbook_cache_key(leg['exchange'], leg['market_type'], symbol))
    prices = _prefetch_orderbooks(leg_keys, sweep_notional)

    for entry in entries:
        symbol = entry.get('symbol')
        if not symbol:
            continue
        legs_meta = _entry_legs_for_orderbook(entry)
        legs: List[Dict[str, Any]] = []
        for leg in legs_meta:
            key = _orderbook_cache_key(leg['exchange'], leg['market_type'], symbol)
            price_info = prices.get(key) or {'error': 'no_data'}
            legs.append({
                'exchange': leg['exchange'],
                'market_type': leg['market_type'],
                'price': price_info,
                'error': price_info.get('error') if isinstance(price_info, dict) else None,
            })
        valid_legs = [l for l in legs if isinstance(l.get('price'), dict) and not l['price'].get('error')]
        spread = compute_orderbook_spread(valid_legs) if len(valid_legs) >= 2 else None
        if spread:
            out[symbol] = {
                'legs': legs,
                'forward': spread.get('forward'),
                'reverse': spread.get('reverse'),
            }
        else:
            out[symbol] = {'legs': legs, 'forward': None, 'reverse': None}
    return out


def build_cross_spread_matrix(entries: List[Dict[str, Any]], all_data: Dict[str, Any], sweep_notional: float = DEFAULT_SWEEP_NOTIONAL) -> Dict[str, Any]:
    """
    基于订单簿（扫 100 USDT）计算跨交易所 FF/SF 差价矩阵。
    - FF：永续 vs 永续
    - SF：现货 vs 永续
    方向按 compute_orderbook_spread 的 forward(买高卖低)/reverse(卖高买低) 计算。
    """
    result: Dict[str, Any] = {}
    leg_keys: List[tuple] = []
    for entry in entries:
        sym = entry.get('symbol')
        if not sym:
            continue
        for exch, sym_map in all_data.items():
            payload = sym_map.get(sym, {})
            if payload.get('futures'):
                leg_keys.append(_orderbook_cache_key(exch, 'perp', sym))
            if payload.get('spot'):
                leg_keys.append(_orderbook_cache_key(exch, 'spot', sym))
    prices = _prefetch_orderbooks(leg_keys, sweep_notional)

    for entry in entries:
        sym = entry.get('symbol')
        if not sym:
            continue
        legs: List[Dict[str, Any]] = []
        for exch, sym_map in all_data.items():
            payload = sym_map.get(sym, {})
            if payload.get('futures'):
                legs.append({'exchange': exch, 'market_type': 'perp'})
            if payload.get('spot'):
                legs.append({'exchange': exch, 'market_type': 'spot'})
        priced_legs: List[Dict[str, Any]] = []
        for leg in legs:
            key = _orderbook_cache_key(leg['exchange'], leg['market_type'], sym)
            price_info = prices.get(key) or {'error': 'no_data'}
            priced_legs.append({
                'exchange': leg['exchange'],
                'market_type': leg['market_type'],
                'price': price_info,
                'error': price_info.get('error') if isinstance(price_info, dict) else 'no_data',
            })
        perps = [l for l in priced_legs if l['market_type'] == 'perp']
        spots = [l for l in priced_legs if l['market_type'] == 'spot']
        pairs: List[Dict[str, Any]] = []

        def _add_pair(a: Dict[str, Any], b: Dict[str, Any], ptype: str):
            spread = compute_orderbook_spread([a, b])
            if not spread:
                return
            forward = spread.get('forward', {})
            reverse = spread.get('reverse', {})
            f_val = forward.get('spread')
            r_val = reverse.get('spread')
            if f_val is None and r_val is None:
                return
            pairs.append({
                'type': ptype,
                'exchange_a': a['exchange'],
                'exchange_b': b['exchange'],
                'forward': f_val,
                'reverse': r_val,
            })

        for i in range(len(perps)):
            for j in range(i + 1, len(perps)):
                if perps[i].get('price') and not perps[i]['price'].get('error') and perps[j].get('price') and not perps[j]['price'].get('error'):
                    _add_pair(perps[i], perps[j], 'FF')
        for spot in spots:
            if not spot.get('price') or spot['price'].get('error'):
                continue
            for perp in perps:
                if perp.get('price') and not perp['price'].get('error'):
                    _add_pair(spot, perp, 'SF')

        pairs_sorted = sorted(
            pairs,
            key=lambda p: max(abs(p['forward'] or 0), abs(p['reverse'] or 0)),
            reverse=True
        )
        result[sym] = {
            'legs': priced_legs,
            'pairs': pairs_sorted,
        }
    return result

def refresh_watchlist_orderbook_cache():
    """后台刷新 watchlist 订单簿/跨所差价缓存，避免在请求路径阻塞。"""
    logger = logging.getLogger('watchlist')
    min_interval = 10.0  # 不要太频繁
    while True:
        start = time.time()
        try:
            snapshot = watchlist_manager.snapshot()
            active_entries = [e for e in snapshot.get('entries', []) if e.get('status') == 'active']
            if not active_entries:
                WATCHLIST_ORDERBOOK_SNAPSHOT.update({
                    'orderbook': {},
                    'cross_spreads': {},
                    'timestamp': now_utc_iso(),
                    'error': None,
                    'stale_reason': 'no_active_entries',
                })
            else:
                all_data = data_collector.get_all_data()
                orderbook = build_orderbook_annotation(active_entries)
                cross_spreads = build_cross_spread_matrix(active_entries, all_data)
                WATCHLIST_ORDERBOOK_SNAPSHOT.update({
                    'orderbook': orderbook,
                    'cross_spreads': cross_spreads,
                    'timestamp': now_utc_iso(),
                    'error': None,
                    'stale_reason': None,
                })
        except Exception as exc:
            WATCHLIST_ORDERBOOK_SNAPSHOT.update({
                'orderbook': WATCHLIST_ORDERBOOK_SNAPSHOT.get('orderbook') or {},
                'cross_spreads': WATCHLIST_ORDERBOOK_SNAPSHOT.get('cross_spreads') or {},
                'timestamp': now_utc_iso(),
                'error': str(exc),
                'stale_reason': 'exception',
            })
            logger.warning("watchlist orderbook cache refresh failed: %s", exc)
        # 将休眠时间与 watchlist 刷新周期对齐，但保留最小间隔
        elapsed = time.time() - start
        sleep_seconds = max(min_interval, watchlist_manager.refresh_seconds - elapsed)
        time.sleep(sleep_seconds)

def refresh_watchlist_metrics_cache():
    """后台刷新 watchlist 价差指标缓存，避免接口长时间阻塞导致前端 502。"""
    logger = logging.getLogger('watchlist')
    min_interval = 60.0  # 指标不需要太高频
    while True:
        start = time.time()
        try:
            snapshot = watchlist_manager.snapshot()
            active_entries = [e for e in snapshot.get('entries', []) if e.get('status') == 'active']
            if not active_entries:
                WATCHLIST_METRICS_SNAPSHOT.update({
                    'metrics': {},
                    'symbols': [],
                    'timestamp': now_utc_iso(),
                    'error': None,
                    'stale_reason': 'no_active_entries',
                })
            else:
                metrics = compute_metrics_for_entries(db.db_path, active_entries)
                WATCHLIST_METRICS_SNAPSHOT.update({
                    'metrics': metrics,
                    'symbols': [e.get('symbol') for e in active_entries if e.get('symbol')],
                    'timestamp': now_utc_iso(),
                    'error': None,
                    'stale_reason': None,
                })
        except Exception as exc:
            WATCHLIST_METRICS_SNAPSHOT.update({
                'metrics': WATCHLIST_METRICS_SNAPSHOT.get('metrics') or {},
                'symbols': WATCHLIST_METRICS_SNAPSHOT.get('symbols') or [],
                'timestamp': now_utc_iso(),
                'error': str(exc),
                'stale_reason': 'exception',
            })
            logger.warning("watchlist metrics cache refresh failed: %s", exc)
        elapsed = time.time() - start
        sleep_seconds = max(min_interval, watchlist_manager.refresh_seconds - elapsed)
        time.sleep(sleep_seconds)

def background_data_collection():
    """优化的后台数据收集 - 减少磁盘写入频率"""
    last_maintenance = datetime.now()
    maintenance_interval = 60    # 1分钟执行一次维护任务（聚合与清理）
    
    # 批处理缓冲区与写入节流
    batch_buffer = []
    last_db_write = {}
    batch_size = MEMORY_OPTIMIZATION_CONFIG['batch_size']

    while True:
        try:
            # 获取所有数据（包含所有币种）
            all_data = data_collector.get_all_data()
            observed_at = datetime.now(timezone.utc)
            timestamp = observed_at.isoformat()
            
            # 为每个币种保存历史数据 - 使用动态支持列表，兼容LINEA等新币
            for symbol in data_collector.supported_symbols:
                premium_data = data_collector.calculate_premium(symbol)
                futures_prices = {}
                
                for exchange in all_data:
                    if symbol in all_data[exchange]:
                        symbol_data = all_data[exchange][symbol]
                        if symbol_data['spot'] or symbol_data['futures']:
                            # 保存到优化的内存管理器
                            historical_entry = {
                                'timestamp': timestamp,
                                'spot_price': symbol_data['spot'].get('price', 0),
                                'futures_price': symbol_data['futures'].get('price', 0),
                                'funding_rate': symbol_data['futures'].get('funding_rate', 0),
                                'funding_interval_hours': symbol_data['futures'].get('funding_interval_hours'),
                                'next_funding_time': symbol_data['futures'].get('next_funding_time'),
                                'premium': premium_data.get(exchange, {}).get('premium_percent', 0)
                            }
                            
                            # 使用内存管理器添加记录
                            memory_manager.add_record(symbol, exchange, historical_entry)
                            
                            futures_price = symbol_data['futures'].get('price') if symbol_data['futures'] else None
                            if isinstance(futures_price, (int, float)) and futures_price > 0:
                                futures_prices[exchange] = futures_price

                            # 批量保存到数据库以减少磁盘IO
                            key = (symbol, exchange)
                            last_write = last_db_write.get(key)
                            if not last_write or (observed_at - last_write).total_seconds() >= DB_WRITE_INTERVAL_SECONDS:
                                batch_buffer.append({
                                    'symbol': symbol,
                                    'exchange': exchange,
                                    'symbol_data': symbol_data,
                                    'premium_data': premium_data
                                })
                                last_db_write[key] = observed_at

                            # 当批处理缓冲区满时，批量写入数据库
                            if len(batch_buffer) >= batch_size:
                                try:
                                    for batch_item in batch_buffer:
                                        db.save_price_data(
                                            batch_item['symbol'],
                                            batch_item['exchange'],
                                            batch_item['symbol_data'],
                                            batch_item['premium_data']
                                        )
                                    batch_buffer.clear()
                                    print(f"批量写入数据库完成: {batch_size} 条记录")
                                except Exception as e:
                                    print(f"批量写入数据库失败: {e}")
                                    batch_buffer.clear()  # 清空缓冲区避免重复尝试

                # 更新轻量套利监控
                if futures_prices:
                    signals = arbitrage_monitor.update(symbol, futures_prices, observed_at=observed_at)
                    for signal in signals:
                        print(
                            "套利信号:",
                            signal.symbol,
                            f"{signal.short_exchange}->{signal.long_exchange}",
                            f"spread={signal.spread_percent:.2f}%",
                        )
            
            # 定期维护任务（每5分钟执行一次）
            current_time = datetime.now()
            if (current_time - last_maintenance).seconds >= maintenance_interval:
                try:
                    print("开始定期维护任务...")
                    
                    # 写入剩余的批处理数据
                    if batch_buffer:
                        print(f"写入剩余批处理数据: {len(batch_buffer)} 条记录")
                        for batch_item in batch_buffer:
                            db.save_price_data(
                                batch_item['symbol'],
                                batch_item['exchange'],
                                batch_item['symbol_data'],
                                batch_item['premium_data']
                            )
                        batch_buffer.clear()
                    
                    # 内存清理
                    memory_manager.cleanup_memory()
                    
                    # 强制垃圾回收
                    collected = gc.collect()
                    if collected > 0:
                        print(f"垃圾回收: 清理了 {collected} 个对象")
                    
                    # 资源监控
                    try:
                        process = psutil.Process(os.getpid())
                        memory_info = process.memory_info()
                        cpu_percent = process.cpu_percent()
                        print(f"资源使用: 内存 {memory_info.rss / 1024 / 1024:.1f}MB, CPU {cpu_percent:.1f}%")
                    except:
                        pass  # 忽略监控错误
                    
                    # 数据库维护（降低频率）
                    db.aggregate_to_1min()
                    db.cleanup_old_data()
                    
                    last_maintenance = current_time
                    print("定期维护任务完成")
                    
                except Exception as e:
                    print(f"定期维护错误: {e}")
            
            # 仅显示当前币种的更新日志
            current_symbol = data_collector.current_symbol
            active_exchanges = 0
            for exchange in all_data:
                if current_symbol in all_data[exchange] and (all_data[exchange][current_symbol]['spot'] or all_data[exchange][current_symbol]['futures']):
                    active_exchanges += 1
            
            if active_exchanges > 0:
                print(f"数据更新: {current_symbol} - {active_exchanges}个交易所活跃 - {timestamp}")
            
        except Exception as e:
            print(f"数据收集错误: {e}")
        
        time.sleep(DATA_REFRESH_INTERVAL)


def watchlist_refresh_loop():
    """周期性刷新 Binance 关注列表，避免对所有币种全量计算。"""
    logger = logging.getLogger('watchlist')
    preloaded = False
    while True:
        try:
            if not preloaded:
                try:
                    watchlist_manager.preload_from_database(db.db_path)
                finally:
                    preloaded = True
            all_data = data_collector.get_all_data()
            exchange_symbols = getattr(data_collector, 'exchange_symbols', {})
            watchlist_manager.refresh(all_data, exchange_symbols, WATCHLIST_ORDERBOOK_SNAPSHOT)
        except Exception as exc:
            logger.warning("watchlist refresh failed: %s", exc)
        time.sleep(watchlist_manager.refresh_seconds)


def cold_start_watchlist_from_db(limit_minutes: int = 1) -> None:
    """
    用最近 1 分钟数据库行情做一次冷启动扫描，避免等待实时流全连上。
    仅用于快速填充 Type B/C 所需的跨所价格与资金费率。
    """
    logger = logging.getLogger('watchlist')
    try:
        conn = sqlite3.connect(db.db_path, timeout=10.0)
        cur = conn.cursor()
        cur.execute("SELECT MAX(timestamp) FROM price_data_1min")
        latest = cur.fetchone()[0]
        if not latest:
            logger.info("cold start skipped: no recent rows in price_data_1min")
            return
        # 检查是否有 funding_rate 列，兼容旧版 SQLite
        cur.execute("PRAGMA table_info(price_data_1min)")
        cols = [row[1] for row in cur.fetchall()]
        has_funding = "funding_rate" in cols
        select_sql = """
            SELECT symbol, exchange, spot_price_close, futures_price_close, {funding}, timestamp
            FROM price_data_1min
            WHERE timestamp >= datetime(?, ?)
        """.format(funding="funding_rate" if has_funding else "NULL AS funding_rate")
        cur.execute(select_sql, (latest, f"-{limit_minutes} minutes"))
        rows = cur.fetchall()
        if not rows:
            logger.info("cold start skipped: no rows within last minute")
            return

        all_data: Dict[str, Dict[str, Any]] = {}
        exchange_symbols: Dict[str, Dict[str, List[str]]] = {}
        for symbol, exch, spot_price, fut_price, fr, ts_raw in rows:
            if exch not in all_data:
                all_data[exch] = {}
                exchange_symbols[exch] = {'spot': [], 'futures': []}
            if symbol not in all_data[exch]:
                all_data[exch][symbol] = {'spot': {}, 'futures': {}}
            if spot_price and spot_price > 0:
                all_data[exch][symbol]['spot'] = {'price': float(spot_price), 'timestamp': ts_raw}
                exchange_symbols[exch]['spot'].append(symbol)
            if fut_price and fut_price > 0:
                all_data[exch][symbol]['futures'] = {
                    'price': float(fut_price),
                    'last_price': float(fut_price),
                    'funding_rate': fr,
                    'timestamp': ts_raw,
                }
                exchange_symbols[exch]['futures'].append(symbol)

        watchlist_manager.refresh(all_data, exchange_symbols, WATCHLIST_ORDERBOOK_SNAPSHOT)
        logger.info("cold start watchlist completed with %s rows (latest ts %s)", len(rows), latest)
    except Exception as exc:
        logger.warning("cold start watchlist failed: %s", exc)

@app.route('/')
def index():
    """主页 - 增强版按币种聚合展示所有可用币种"""
    return render_template('enhanced_aggregated.html', 
                         symbols=data_collector.supported_symbols,
                         exchange_order=EXCHANGE_DISPLAY_ORDER)

@app.route('/exchanges')
def exchanges_view():
    """按交易所展示页面"""
    return render_template('simple_index.html', 
                         symbols=data_collector.supported_symbols,
                         current_symbol=data_collector.current_symbol,
                         exchange_order=EXCHANGE_DISPLAY_ORDER)

@app.route('/aggregated')
def aggregated_index():
    """聚合页面（兼容路由）- 使用增强版聚合视图"""
    return render_template('enhanced_aggregated.html', 
                         symbols=data_collector.supported_symbols,
                         exchange_order=EXCHANGE_DISPLAY_ORDER)

@app.route('/charts')
def charts():
    """图表页面"""
    # 获取URL参数中的symbol，如果有的话就切换到该symbol
    requested_symbol = request.args.get('symbol')
    # 使用动态支持列表进行校验
    if requested_symbol and requested_symbol.upper() in data_collector.supported_symbols:
        # 切换到请求的symbol
        data_collector.set_symbol(requested_symbol)
        current_symbol = requested_symbol.upper()
    else:
        current_symbol = data_collector.current_symbol
    
    return render_template('chart_index.html',
                         # 提供动态支持的币种列表，保证前端按钮与实际支持一致
                         symbols=data_collector.supported_symbols,
                         current_symbol=current_symbol,
                         exchange_order=EXCHANGE_DISPLAY_ORDER,
                         chart_intervals=CHART_INTERVAL_OPTIONS,
                         default_chart_interval=DEFAULT_CHART_INTERVAL,
                         chart_interval_label_map=CHART_INTERVAL_LABEL_MAP)


@app.route('/watchlist')
def watchlist_view():
    """Binance 资金费率驱动的关注列表页面"""
    return render_template('watchlist.html')


@app.route('/watchlist/charts')
def watchlist_charts():
    """关注列表的独立图表页面"""
    return render_template('watchlist_charts.html')


@app.route('/api/data')
def get_current_data():
    """获取当前显示币种的实时数据API"""
    current_symbol = data_collector.current_symbol
    symbol_data = data_collector.get_symbol_data(current_symbol)
    premium_data = data_collector.calculate_premium(current_symbol)
    
    return precision_jsonify({
        'realtime_data': symbol_data,
        'premium_data': premium_data,
        'symbol': current_symbol,
        'exchange_order': EXCHANGE_DISPLAY_ORDER,
        'timestamp': now_utc_iso()
    })

@app.route('/api/data/all')
def get_all_data():
    """获取所有币种的实时数据API"""
    all_data = data_collector.get_all_data()
    all_premiums = data_collector.calculate_all_premiums()
    
    return precision_jsonify({
        'all_realtime_data': all_data,
        'all_premium_data': all_premiums,
        # 返回动态支持列表
        'supported_symbols': data_collector.supported_symbols,
        'current_symbol': data_collector.current_symbol,
        'exchange_order': EXCHANGE_DISPLAY_ORDER,
        'timestamp': now_utc_iso()
    })


@app.route('/api/watchlist')
def get_watchlist():
    """获取基于资金费率的Binance关注列表"""
    payload = watchlist_manager.snapshot()
    try:
        entries = payload.get('entries', [])
        active_entries = [e for e in entries if e.get('status') == 'active']
        all_data = data_collector.get_all_data()
        # 使用后台缓存的订单簿/跨所差价，避免请求路径阻塞
        ob_snapshot = WATCHLIST_ORDERBOOK_SNAPSHOT.copy()
        payload['orderbook'] = ob_snapshot.get('orderbook') or {}
        payload['cross_spreads'] = ob_snapshot.get('cross_spreads') or {}
        payload['orderbook_ts'] = ob_snapshot.get('timestamp')
        payload['orderbook_stale_reason'] = ob_snapshot.get('stale_reason')
        if ob_snapshot.get('error'):
            payload['orderbook_error'] = ob_snapshot.get('error')

        # 补充交易所视图：按符号列出各所期现价格、资金费率，以及跨所永续最佳价差
        def _to_float(val):
            try:
                f = float(val)
                return f if f == f and f != float('inf') and f != float('-inf') else None
            except Exception:
                return None

        overview: Dict[str, Any] = {}
        for entry in active_entries:
            sym = entry.get('symbol')
            if not sym:
                continue
            rows = []
            high = None
            low = None
            for exch, sym_map in all_data.items():
                payload_sym = sym_map.get(sym, {})
                spot = payload_sym.get('spot') or {}
                fut = payload_sym.get('futures') or {}
                spot_price = _to_float(spot.get('price'))
                futures_price = _to_float(fut.get('price') or fut.get('last_price'))
                funding_rate = _to_float(fut.get('funding_rate'))
                interval = _to_float(fut.get('funding_interval_hours'))
                next_ft = fut.get('next_funding_time')
                if futures_price is not None:
                    if high is None or futures_price > high['price']:
                        high = {'exchange': exch, 'price': futures_price, 'funding_rate': funding_rate}
                    if low is None or futures_price < low['price']:
                        low = {'exchange': exch, 'price': futures_price, 'funding_rate': funding_rate}
                row = {
                    'exchange': exch,
                    'spot_price': spot_price,
                    'futures_price': futures_price,
                    'funding_rate': funding_rate,
                    'funding_interval_hours': interval,
                    'next_funding_time': next_ft,
                }
                rows.append(row)
            best = None
            if high and low and high['price'] and low['price']:
                base = min(high['price'], low['price'])
                if base:
                    best = {
                        'high_exchange': high['exchange'],
                        'low_exchange': low['exchange'],
                        'high_price': high['price'],
                        'low_price': low['price'],
                        'spread': (high['price'] - low['price']) / base,
                        'high_funding': high.get('funding_rate'),
                        'low_funding': low.get('funding_rate'),
                    }
            overview[sym] = {'rows': rows, 'best_perp_spread': best}
        payload['exchange_overview'] = overview

        # 更完整的交易所指标（资金费上限/下限、OI、24h 额等），直接调用公开 REST
        try:
            symbols = [e.get('symbol') for e in active_entries if e.get('symbol')]
            if symbols:
                payload['exchange_details'] = fetch_exchange_details(symbols)
        except Exception as exc:
            payload['exchange_details_error'] = str(exc)
    except Exception as exc:
        payload['orderbook_error'] = str(exc)
    return precision_jsonify(payload)


@app.route('/api/watchlist/refresh', methods=['POST'])
def trigger_watchlist_refresh():
    """
    手动触发一次 watchlist 计算，立即使用当前内存行情进行扫描。
    """
    try:
        all_data = data_collector.get_all_data()
        exchange_symbols = getattr(data_collector, 'exchange_symbols', {})
        watchlist_manager.refresh(all_data, exchange_symbols, WATCHLIST_ORDERBOOK_SNAPSHOT)
        return jsonify({'message': 'ok', 'timestamp': now_utc_iso(), 'summary': watchlist_manager.snapshot().get('summary')})
    except Exception as exc:
        return jsonify({'error': str(exc), 'timestamp': now_utc_iso()}), 500


@app.route('/watchlist/db')
def watchlist_db_view():
    """
    简易的 watchlist PG 浏览页面，随用随查。
    - 支持 raw/event/outcome 三张核心表。
    - 默认每页 10 条，可通过 ?page=2&limit=20 翻页。
    """
    if psycopg is None:
        return render_template('watchlist_db_view.html', error='psycopg 未安装，无法连接 PG')

    table_key = request.args.get('table', 'raw')
    page = max(1, int(request.args.get('page', 1) or 1))
    limit = int(request.args.get('limit', 10) or 10)
    limit = min(max(limit, 1), 200)
    offset = (page - 1) * limit

    table_map = {
        'raw': ('watchlist.watch_signal_raw', 'ts'),
        'event': ('watchlist.watch_signal_event', 'start_ts'),
        'outcome': ('watchlist.future_outcome', 'event_id'),
    }
    if table_key not in table_map:
        table_key = 'raw'
    table_name, order_col = table_map[table_key]

    def _shorten(val: Any, max_len: int = 200) -> str:
        if val is None:
            return ''
        try:
            if isinstance(val, (dict, list)):
                text = json.dumps(val, ensure_ascii=False)
            elif isinstance(val, (datetime,)):
                text = val.isoformat()
            else:
                text = str(val)
        except Exception:
            text = str(val)
        if len(text) > max_len:
            return text[:max_len] + ' …'
        return text

    rows: List[Dict[str, Any]] = []
    total_count: Optional[int] = None
    error: Optional[str] = None
    try:
        conn_kwargs: Dict[str, Any] = {"autocommit": True}
        if dict_row:
            conn_kwargs["row_factory"] = dict_row
        with psycopg.connect(WATCHLIST_PG_CONFIG['dsn'], **conn_kwargs) as conn:
            try:
                total_row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {table_name}").fetchone()
                total_count = total_row['cnt'] if total_row else None
            except Exception:
                total_count = None
            order_sql = f"ORDER BY {order_col} DESC"
            if table_key == 'outcome':
                order_sql = "ORDER BY event_id DESC, horizon_min ASC"
            cur = conn.execute(
                f"SELECT * FROM {table_name} {order_sql} LIMIT %s OFFSET %s",
                (limit, offset),
            )
            fetched = cur.fetchall()
            if dict_row:
                for r in fetched:
                    rows.append({k: _shorten(v) for k, v in r.items()})
            else:
                cols = [d.name for d in cur.description] if cur.description else []
                for r in fetched:
                    rows.append({col: _shorten(val) for col, val in zip(cols, r)})
    except Exception as exc:
        error = str(exc)

    columns = list(rows[0].keys()) if rows else []
    has_next = len(rows) == limit and (total_count is None or offset + len(rows) < total_count)
    return render_template(
        'watchlist_db_view.html',
        table_key=table_key,
        rows=rows,
        columns=columns,
        page=page,
        limit=limit,
        has_next=has_next,
        total_count=total_count,
        error=error,
    )

@app.route('/api/watchlist/metrics')
def get_watchlist_metrics():
    """
    仅计算 watchlist active 符号的价差指标，用于监控/展示，不触发任何交易动作。
    """
    snap = WATCHLIST_METRICS_SNAPSHOT.copy()
    payload = {
        'symbols': snap.get('symbols') or [],
        'metrics': snap.get('metrics') or {},
        'timestamp': snap.get('timestamp') or now_utc_iso(),
        'stale_reason': snap.get('stale_reason'),
        'error': snap.get('error'),
    }
    status_code = 200 if not snap.get('error') else 500
    if not snap.get('metrics') and not snap.get('error'):
        payload['message'] = snap.get('stale_reason') or 'metrics cache not ready'
    return jsonify(payload), status_code

@app.route('/api/watchlist/series')
def get_watchlist_series():
    """
    返回 active 符号的 6h 价差序列及信号点，仅用于前端可视化。
    """
    try:
        snapshot = watchlist_manager.snapshot()
        active_entries = [entry for entry in snapshot.get('entries', []) if entry.get('status') == 'active']
        active_symbols = [entry['symbol'] for entry in active_entries]
        if not active_entries:
            return jsonify({'series': {}, 'symbols': [], 'timestamp': now_utc_iso()})
        series = compute_series_for_entries(db.db_path, active_entries)
        return jsonify({'series': series, 'symbols': active_symbols, 'timestamp': now_utc_iso()})
    except Exception as exc:
        return jsonify({'error': str(exc), 'timestamp': now_utc_iso()}), 500


@app.route('/api/history/<symbol>')
def get_historical_data(symbol):
    """获取内存中的历史数据API (用于实时图表)"""
    symbol = symbol.upper()
    data = memory_manager.get_data(symbol)
    if data:
        return jsonify(data)
    return jsonify({})

@app.route('/api/history/<symbol>/database')
def get_database_historical_data(symbol):
    """获取数据库中的历史数据API"""
    symbol = symbol.upper()
    # 使用动态支持列表
    if symbol not in data_collector.supported_symbols:
        return jsonify({'error': 'Unsupported symbol'}), 400
    
    # 获取查询参数
    hours = request.args.get('hours', 24, type=int)  # 默认24小时
    exchange = request.args.get('exchange', None)    # 特定交易所，默认所有
    interval = request.args.get('interval', DEFAULT_CHART_INTERVAL)
    interval = (interval or DEFAULT_CHART_INTERVAL).lower()
    
    # 限制查询范围以提高性能
    hours = min(hours, 720)  # 最多30天
    
    supported_intervals = set(CHART_INTERVAL_MINUTES.keys()) | {'raw'}
    if interval not in supported_intervals:
        return jsonify({'error': 'Unsupported interval'}), 400
    
    db_interval = '1min' if interval in CHART_INTERVAL_MINUTES else interval
    
    try:
        data = db.get_historical_data(symbol, exchange, hours, db_interval)
        if interval in CHART_INTERVAL_MINUTES and interval != '1min':
            data = resample_price_rows(data, interval)
        return jsonify({
            'symbol': symbol,
            'exchange': exchange,
            'hours': hours,
            'interval': interval,
            'data': data,
            'count': len(data)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/trade/options')
def get_trade_options():
    """Expose supported trading exchanges for the front-end controls."""

    options = get_supported_trading_backends()
    return precision_jsonify({
        'market_types': options,
        'timestamp': now_utc_iso()
    })


@app.route('/api/trade/dual', methods=['POST'])
def execute_dual_trade():
    """Handle hedged market order requests from the charts page."""

    payload = request.get_json(silent=True) or {}
    symbol = (payload.get('symbol') or '').upper()
    market_type = (payload.get('market_type') or 'perpetual').lower()
    legs_payload = payload.get('legs')
    order_kwargs_payload = payload.get('order_kwargs') or {}
    client_order_id = payload.get('client_order_id')

    if not symbol:
        return precision_jsonify({'error': 'symbol is required'}), 400
    if symbol not in data_collector.supported_symbols:
        return precision_jsonify({'error': f'unsupported symbol: {symbol}'}), 400
    if market_type not in PERPETUAL_MARKET_KEYS:
        if market_type == 'spot':
            return precision_jsonify({'error': 'spot trading is not implemented yet'}), 400
        return precision_jsonify({'error': f'unsupported market_type: {market_type}'}), 400
    if not isinstance(order_kwargs_payload, dict):
        return precision_jsonify({'error': 'order_kwargs must be an object'}), 400

    base_order_kwargs = dict(order_kwargs_payload)
    base_client_order_id = client_order_id or base_order_kwargs.get('client_order_id')

    if legs_payload:
        try:
            legs_result = _execute_multi_leg_trade(
                symbol,
                market_type,
                legs_payload,
                base_order_kwargs,
                base_client_order_id,
            )
        except TradeRequestValidationError as exc:
            return precision_jsonify({'error': str(exc)}), 400
        except TradeExecutionError as exc:
            logging.getLogger('trading').warning(
                'Multi-leg order submission failed on %s (%s): %s',
                symbol,
                market_type,
                exc,
            )
            return precision_jsonify({'error': str(exc)}), 400

        return precision_jsonify({
            'market_type': market_type,
            'symbol': symbol,
            'legs': legs_result,
            'timestamp': now_utc_iso()
        })

    exchange = payload.get('exchange')
    quantity = payload.get('quantity')
    if not exchange:
        return precision_jsonify({'error': 'exchange is required'}), 400
    if quantity is None:
        return precision_jsonify({'error': 'quantity is required'}), 400

    try:
        orders = execute_dual_perp_market_order(
            exchange,
            symbol,
            quantity,
            order_kwargs=base_order_kwargs,
        )
    except TradeExecutionError as exc:
        logging.getLogger('trading').warning(
            'Dual order submission failed on %s/%s (%s): %s',
            exchange,
            symbol,
            market_type,
            exc,
        )
        return precision_jsonify({'error': str(exc)}), 400

    return precision_jsonify({
        'exchange': exchange,
        'market_type': market_type,
        'symbol': symbol,
        'orders': orders,
        'timestamp': now_utc_iso()
    })

@app.route('/api/chart/<symbol>')
def get_chart_data(symbol):
    """获取图表数据API - 优化格式用于Chart.js"""
    symbol = symbol.upper()
    # 使用动态支持列表
    if symbol not in data_collector.supported_symbols:
        return jsonify({'error': 'Unsupported symbol'}), 400
    
    # 获取查询参数
    hours = request.args.get('hours', 6, type=int)  # 默认6小时用于图表
    exchange = request.args.get('exchange', None)
    interval = request.args.get('interval', DEFAULT_CHART_INTERVAL)
    interval = (interval or DEFAULT_CHART_INTERVAL).lower()
    
    # 限制查询范围
    hours = min(hours, 720)  # 最多30天
    
    supported_intervals = set(CHART_INTERVAL_MINUTES.keys()) | {'raw'}
    if interval not in supported_intervals:
        return jsonify({'error': 'Unsupported interval'}), 400
    
    db_interval = '1min' if interval in CHART_INTERVAL_MINUTES else interval
    
    cache_key = (symbol, exchange or '', interval, hours)
    cached_entry = _get_chart_cache_entry(cache_key)
    raw_data = None
    last_error = None
    retry_attempts = 3
    backoff_seconds = 0.4
    
    for attempt in range(retry_attempts):
        try:
            raw_data = db.get_historical_data(symbol, exchange, hours, db_interval, raise_on_error=True)
            if interval in CHART_INTERVAL_MINUTES and interval != '1min':
                raw_data = resample_price_rows(raw_data, interval)
            break
        except sqlite3.OperationalError as exc:
            last_error = exc
            if 'locked' in str(exc).lower():
                time.sleep(backoff_seconds * (attempt + 1))
                continue
            break
        except Exception as exc:
            last_error = exc
            break

    if raw_data is None:
        if cached_entry:
            payload = dict(cached_entry['payload'])
            payload['from_cache'] = True
            payload['cache_age_seconds'] = round(time.time() - cached_entry['timestamp'], 1)
            return precision_jsonify(payload)
        if last_error:
            return jsonify({'error': 'database temporarily unavailable', 'details': str(last_error)}), 503
        return jsonify({'error': 'no chart data available'}), 404

    try:
        def to_utc_iso(ts: str) -> str:
            """Ensure timestamp string is UTC ISO8601.
            If no timezone offset is present, treat as UTC and append 'Z'.
            Also normalize space to 'T'.
            """
            if not isinstance(ts, str):
                return ts
            has_offset = ts.endswith('Z') or ('+' in ts[-6:] or '-' in ts[-6:])
            base = ts.replace(' ', 'T')
            return base if has_offset else (base + 'Z')
        
        # 按交易所组织数据
        chart_data = {}
        for row in raw_data:
            exchange_name = row['exchange']
            if exchange_name not in chart_data:
                chart_data[exchange_name] = {
                    'labels': [],
                    'spot_prices': [],
                    'futures_prices': [],
                    'funding_rates': [],
                    'premiums': [],
                    'funding_intervals': [],
                    'next_funding_times': []
                }
            
            # 使用时间戳作为标签
            timestamp = row['timestamp']
            chart_data[exchange_name]['labels'].append(to_utc_iso(timestamp))
            
            # 添加价格数据，优先使用聚合字段，缺失时回退到原始字段
            spot_close = row.get('spot_price_close')
            if spot_close is None:
                spot_close = row.get('spot_price')
            futures_close = row.get('futures_price_close')
            if futures_close is None:
                futures_close = row.get('futures_price')
            funding_value = row.get('funding_rate_avg')
            if funding_value is None:
                funding_value = row.get('funding_rate')
            premium_value = row.get('premium_percent_avg')
            if premium_value is None:
                premium_value = row.get('premium_percent')

            chart_data[exchange_name]['spot_prices'].append(spot_close or 0)
            chart_data[exchange_name]['futures_prices'].append(futures_close or 0)
            chart_data[exchange_name]['funding_rates'].append(funding_value or 0)
            chart_data[exchange_name]['premiums'].append(premium_value or 0)
            chart_data[exchange_name]['funding_intervals'].append(row.get('funding_interval_hours'))
            chart_data[exchange_name]['next_funding_times'].append(row.get('next_funding_time'))
        
        response_payload = {
            'symbol': symbol,
            'hours': hours,
            'interval': interval,
            'chart_data': chart_data,
            'timestamp': now_utc_iso()
        }
        _set_chart_cache_entry(cache_key, response_payload)
        return precision_jsonify(response_payload)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/latest/<symbol>')
def get_latest_prices(symbol):
    """获取最新价格数据API"""
    symbol = symbol.upper()
    # 使用动态支持列表
    if symbol not in data_collector.supported_symbols:
        return jsonify({'error': 'Unsupported symbol'}), 400
    
    try:
        latest_data = db.get_latest_prices(symbol)
        return jsonify({
            'symbol': symbol,
            'latest_data': latest_data,
            'timestamp': now_utc_iso()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/switch_symbol', methods=['POST'])
def switch_symbol():
    """切换币种API"""
    new_symbol = request.json.get('symbol', '').upper()
    if new_symbol in data_collector.supported_symbols:
        data_collector.set_symbol(new_symbol)
        return jsonify({'status': 'success', 'symbol': new_symbol})
    return jsonify({'status': 'error', 'message': 'Unsupported symbol'})

@app.route('/api/aggregated_data')
def get_aggregated_data():
    """获取聚合数据API - 按币种聚合所有交易所数据"""
    try:
        all_data = data_collector.get_all_data()
        
        # 重组数据结构：从 交易所->币种 改为 币种->交易所
        aggregated = {}
        for exchange, symbol_map in all_data.items():
            for raw_symbol, exchange_data in symbol_map.items():
                # 统一通过 normalize_symbol_for_exchange() 应用硬编码别名，避免前端比较到不同标的
                # 如需新增别名，仅需在 exchange_connectors.ExchangeDataCollector.symbol_overrides 中维护即可。
                normalized_symbol = data_collector.normalize_symbol_for_exchange(exchange, raw_symbol)
                if normalized_symbol not in aggregated:
                    aggregated[normalized_symbol] = {}
                aggregated[normalized_symbol][exchange] = exchange_data
        
        return jsonify({
            'aggregated_data': aggregated,
            'supported_symbols': data_collector.supported_symbols,
            'timestamp': now_utc_iso()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/arbitrage/signals')
def get_arbitrage_signals():
    """获取最近的跨交易所套利信号"""
    return precision_jsonify({
        'signals': arbitrage_monitor.get_recent_signals(),
        'timestamp': now_utc_iso()
    })

@app.route('/api/markets')
def get_markets_info():
    """获取市场信息 - 支持的币种及其交易所覆盖情况"""
    try:
        # 获取市场报告（如果缓存有效则使用缓存）
        report = get_market_report(force_refresh=False)
        
        return jsonify({
            'market_report': report,
            'current_symbols': data_collector.supported_symbols,
            'timestamp': now_utc_iso()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/coverage')
def get_coverage_stats():
    """获取市场覆盖度统计"""
    try:
        all_data = data_collector.get_all_data()
        
        # 计算覆盖度统计
        stats = {
            'total_symbols': len(data_collector.supported_symbols),
            'exchange_stats': {},
            'symbol_stats': {},
            'quality_metrics': {}
        }
        
        # 按交易所统计
        for exchange in ['okx', 'binance', 'bybit', 'bitget']:
            spot_count = sum(1 for symbol in data_collector.supported_symbols 
                           if all_data.get(exchange, {}).get(symbol, {}).get('spot', {}).get('price', 0) > 0)
            futures_count = sum(1 for symbol in data_collector.supported_symbols 
                              if all_data.get(exchange, {}).get(symbol, {}).get('futures', {}).get('price', 0) > 0)
            
            stats['exchange_stats'][exchange] = {
                'spot_symbols': spot_count,
                'futures_symbols': futures_count,
                'coverage_percent': round((spot_count + futures_count) / (len(data_collector.supported_symbols) * 2) * 100, 2)
            }
        
        return jsonify({
            'coverage_stats': stats,
            'timestamp': now_utc_iso()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/database/stats')
def database_stats():
    """获取数据库统计信息API"""
    try:
        import sqlite3
        import os
        
        stats = {}
        
        # 数据库文件大小
        if os.path.exists(db.db_path):
            stats['database_size_mb'] = round(os.path.getsize(db.db_path) / 1024 / 1024, 2)
        else:
            stats['database_size_mb'] = 0
        
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.cursor()
            
            # 原始数据记录数
            cursor.execute('SELECT COUNT(*) FROM price_data')
            stats['raw_records'] = cursor.fetchone()[0]
            
            # 1分钟聚合数据记录数
            cursor.execute('SELECT COUNT(*) FROM price_data_1min')
            stats['aggregated_records'] = cursor.fetchone()[0]
            
            # 最早和最晚的数据时间
            cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM price_data')
            raw_range = cursor.fetchone()
            stats['raw_data_range'] = {
                'earliest': raw_range[0],
                'latest': raw_range[1]
            }
            
            cursor.execute('SELECT MIN(timestamp), MAX(timestamp) FROM price_data_1min')
            agg_range = cursor.fetchone()
            stats['aggregated_data_range'] = {
                'earliest': agg_range[0],
                'latest': agg_range[1]
            }
            
            # 按交易所和币种统计数据
            cursor.execute('''
                SELECT symbol, exchange, COUNT(*) as count
                FROM price_data
                WHERE timestamp >= datetime('now', '-24 hours')
                GROUP BY symbol, exchange
                ORDER BY symbol, exchange
            ''')
            stats['recent_data_by_symbol_exchange'] = [
                {'symbol': row[0], 'exchange': row[1], 'count': row[2]}
                for row in cursor.fetchall()
            ]
        
        return jsonify({
            'status': 'success',
            'stats': stats,
            'timestamp': now_utc_iso()
        })
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/database/maintenance', methods=['POST'])
def manual_maintenance():
    """手动触发数据库维护API"""
    try:
        print("手动触发数据库维护...")
        
        # 数据聚合
        db.aggregate_to_1min()
        
        # 数据清理
        db.cleanup_old_data(force=True)
        
        print("手动数据库维护完成")
        
        return jsonify({
            'status': 'success', 
            'message': '数据库维护任务已完成',
            'timestamp': now_utc_iso()
        })
        
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/system/status')
def get_system_status():
    """获取系统状态监控API"""
    try:
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        
        # 获取内存管理器状态
        memory_stats = {}
        for symbol, symbol_data in memory_manager.data.items():
            total_records = sum(len(exchange_data) for exchange_data in symbol_data.values())
            if total_records > 0:
                memory_stats[symbol] = total_records
        
        status = {
            'system': {
                'memory_usage_mb': round(memory_info.rss / 1024 / 1024, 1),
                'cpu_percent': process.cpu_percent(),
                'threads_count': process.num_threads(),
                'connections_count': len(data_collector.ws_connections),
            },
            'data': {
                'total_symbols': len(CURRENT_SUPPORTED_SYMBOLS),
                'active_memory_symbols': len(memory_stats),
                'memory_records_per_symbol': memory_stats,
                'max_records_per_exchange': MEMORY_OPTIMIZATION_CONFIG['max_historical_records']
            },
            'connections': {
                'websocket_status': {
                    name: 'connected' if ws else 'disconnected'
                    for name, ws in data_collector.ws_connections.items()
                },
                'reconnect_attempts': data_collector.reconnect_attempts
            },
            'rest': {
                'enabled': getattr(data_collector, 'rest_enabled', False),
                'update_interval': getattr(data_collector, 'rest_update_interval', None),
                'last_sync': getattr(data_collector, 'rest_last_sync', {})
            },
            'timestamp': now_utc_iso()
        }
        
        return jsonify(status)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("🚀 启动跨交易所期现套利监控系统...")
    
    # 显示优化配置信息
    print(f"📊 监控币种数量: {len(CURRENT_SUPPORTED_SYMBOLS)}")
    print(f"⚡ 数据更新间隔: {DATA_REFRESH_INTERVAL}秒")
    print(f"🔄 WebSocket数据间隔: {WS_UPDATE_INTERVAL}秒") 
    print(f"💾 内存最大记录数: {MEMORY_OPTIMIZATION_CONFIG['max_historical_records']}")
    print(f"🔧 最大重连次数: {WS_CONNECTION_CONFIG['max_reconnect_attempts']}")
    print(f"🕒 重连基础延迟: {WS_CONNECTION_CONFIG['base_reconnect_delay']}秒")
    
    try:
        # 显示系统资源
        process = psutil.Process(os.getpid())
        print(f"🖥️  初始内存使用: {process.memory_info().rss / 1024 / 1024:.1f}MB")
    except:
        pass
    
    print("📡 启动数据收集...")
    # 启动数据收集
    data_collector.start_all_connections()

    # 用最近 1 分钟数据库数据做一次冷启动，避免跨所列表长时间为空
    try:
        cold_start_watchlist_from_db(limit_minutes=1)
    except Exception as exc:
        logging.getLogger('watchlist').warning("cold start at bootstrap failed: %s", exc)
    
    print("🔄 启动后台数据处理...")
    # 启动后台数据收集线程
    background_thread = threading.Thread(target=background_data_collection, daemon=True)
    background_thread.start()

    print("📑 启动Watchlist订单簿缓存线程...")
    watchlist_ob_thread = threading.Thread(target=refresh_watchlist_orderbook_cache, daemon=True)
    watchlist_ob_thread.start()

    print("📈 启动Watchlist指标缓存线程...")
    watchlist_metrics_thread = threading.Thread(target=refresh_watchlist_metrics_cache, daemon=True)
    watchlist_metrics_thread.start()

    print("👀 启动Binance关注列表刷新线程...")
    watchlist_thread = threading.Thread(target=watchlist_refresh_loop, daemon=True)
    watchlist_thread.start()
    
    print("🌐 启动Web服务器...")
    print("📊 系统状态监控: http://localhost:4002/api/system/status")
    
    # 启动Flask应用
    app.run(debug=False, host='0.0.0.0', port=4002, threaded=True)
