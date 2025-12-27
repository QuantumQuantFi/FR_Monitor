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
import re
import builtins
import sys
import decimal
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from functools import lru_cache
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
    REST_CONNECTION_CONFIG,
    DB_WRITE_INTERVAL_SECONDS,
    WATCHLIST_CONFIG,
    WATCHLIST_PG_CONFIG,
    LIVE_TRADING_CONFIG,
    MONITOR_8010_CONFIG,
)
from database import PriceDatabase
from market_info import get_dynamic_symbols, get_market_report
from exchange_details import fetch_exchange_details
from trading.trade_executor import (
    TradeExecutionError,
    execute_dual_perp_market_order,
    execute_perp_market_batch,
    get_supported_trading_backends,
    get_binance_perp_account,
    get_binance_perp_positions,
    get_binance_perp_usdt_balance,
    get_binance_funding_fee_income,
    derive_binance_perp_qty_from_usdt,
    get_okx_swap_positions,
    get_okx_swap_contract_value,
    get_okx_account_balance,
    get_okx_funding_fee_bills,
    derive_okx_swap_size_from_usdt,
    get_bybit_linear_positions,
    get_bybit_wallet_balance,
    get_bybit_funding_fee_transactions,
    derive_bybit_linear_qty_from_usdt,
    get_bitget_usdt_perp_positions,
    get_bitget_usdt_balance,
    get_bitget_funding_fee_bills,
    derive_bitget_usdt_perp_size_from_usdt,
    get_hyperliquid_perp_positions,
    get_hyperliquid_balance_summary,
    get_hyperliquid_user_funding_history,
    get_lighter_balance_summary,
    get_grvt_balance_summary,
    get_grvt_perp_positions,
    get_grvt_funding_payment_history,
)
from trading.live_trading_manager import LiveTradingConfig, LiveTradingManager
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
from funding_utils import (
    derive_funding_interval_hours,
    derive_interval_hours_from_times,
    normalize_next_funding_time,
)
from rest_collectors import get_hyperliquid_funding_map
import requests
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
RUNTIME_DIR = os.environ.get("SIMPLE_APP_RUNTIME_DIR", os.path.join("runtime", "simple_app"))
os.makedirs(RUNTIME_DIR, exist_ok=True)
DAILY_DB_MAINTENANCE_HOUR_UTC = 4  # 04:00 UTC 低峰期执行 WAL checkpoint + VACUUM
DAILY_DB_MAINTENANCE_STATE = os.path.join(RUNTIME_DIR, "daily_db_maintenance.json")
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
    'monitor8010_bbo': {},
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

WATCHLIST_BOOTSTRAP_LOCK = threading.Lock()
WATCHLIST_BOOTSTRAP_LAST_TS = 0.0
WATCHLIST_BOOTSTRAP_MIN_INTERVAL_SEC = 15.0

WATCHLIST_BBO_PG_READY = False
WATCHLIST_BBO_PG_LOCK = threading.Lock()

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
DEFAULT_CHART_INTERVAL = '15min'
CHART_INTERVAL_LABEL_MAP = {value: label for value, label in CHART_INTERVAL_OPTIONS}
CHART_CACHE_TTL_SECONDS = 900  # 15 分钟缓存，便于大窗口重复请求直接命中
CHART_CACHE_MAX_ENTRIES = 64
CHART_MAX_HOURS = 168  # 图表查询最大时间范围（小时），避免拉取超大窗口导致超时
CHART_PREWARM_ITEMS = [
    {'symbol': 'AIA', 'hours': 168, 'interval': '15min'},
]
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


def prewarm_chart_cache(items=None):
    """在启动时预热部分大窗口图表，避免首个请求超时。"""
    items = items or CHART_PREWARM_ITEMS
    logger = logging.getLogger('charts')
    for item in items:
        symbol = (item or {}).get('symbol')
        if not symbol:
            continue
        hours = (item or {}).get('hours', 168)
        interval = (item or {}).get('interval', DEFAULT_CHART_INTERVAL)
        try:
            with app.test_request_context(f"/api/chart/{symbol}?hours={hours}&interval={interval}"):
                # get_chart_data 可能返回 (json, status_code)，用 make_response 统一成 Response 便于读 status_code
                resp = app.make_response(get_chart_data(symbol))
                status_code = getattr(resp, "status_code", None)
                if status_code and status_code >= 400:
                    logger.warning("chart prewarm failed symbol=%s hours=%s interval=%s status=%s", symbol, hours, interval, status_code)
                else:
                    print(f"chart prewarm done: {symbol} hours={hours} interval={interval}")
                    logger.info("chart prewarm done symbol=%s hours=%s interval=%s", symbol, hours, interval)
        except Exception as exc:
            print(f"chart prewarm error {symbol} hours={hours} interval={interval}: {exc}")
            logger.warning("chart prewarm error symbol=%s hours=%s interval=%s err=%s", symbol, hours, interval, exc)


def _load_daily_maintenance_state() -> dict:
    try:
        with open(DAILY_DB_MAINTENANCE_STATE, "r", encoding="utf-8") as fp:
            return json.load(fp)
    except Exception:
        return {}


def _mark_daily_db_maintenance(now_utc: datetime) -> None:
    state = {"last_run_date": now_utc.date().isoformat()}
    try:
        with open(DAILY_DB_MAINTENANCE_STATE, "w", encoding="utf-8") as fp:
            json.dump(state, fp)
    except Exception:
        pass


def _should_run_daily_db_maintenance(now_utc: datetime) -> bool:
    """Return True if we are within the daily maintenance window and not already run."""
    if now_utc.hour != DAILY_DB_MAINTENANCE_HOUR_UTC:
        return False
    state = _load_daily_maintenance_state()
    last_run = state.get("last_run_date")
    return last_run != now_utc.date().isoformat()


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
    exchange_key = (exchange or "").lower()
    quantity: Optional[Decimal] = None
    if market_key == "futures":
        try:
            if exchange_key == "binance":
                qty_str = derive_binance_perp_qty_from_usdt(
                    symbol, float(notional_dec), price=float(price)
                )
                quantity = Decimal(str(qty_str))
            elif exchange_key == "okx":
                size_str = derive_okx_swap_size_from_usdt(
                    symbol, float(notional_dec), price=float(price)
                )
                ct_val = get_okx_swap_contract_value(symbol)
                quantity = (Decimal(str(size_str)) * Decimal(str(ct_val))).quantize(
                    DEFAULT_QUANT, rounding=ROUND_DOWN
                )
            elif exchange_key == "bybit":
                qty_str = derive_bybit_linear_qty_from_usdt(
                    symbol, float(notional_dec), price=float(price)
                )
                quantity = Decimal(str(qty_str))
            elif exchange_key == "bitget":
                size_str = derive_bitget_usdt_perp_size_from_usdt(
                    symbol, float(notional_dec), price=float(price)
                )
                quantity = Decimal(str(size_str))
        except TradeExecutionError as exc:
            raise TradeRequestValidationError(str(exc)) from exc

    if quantity is None:
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

    if exchange_norm == "binance":
        recv_window = int(order_context.get("recv_window") or 5000)
        base_url = order_context.get("base_url") or "https://fapi.binance.com"

        pair = symbol.upper()
        if not pair.endswith("USDT"):
            pair = f"{pair}USDT"

        try:
            positions = get_binance_perp_positions(
                symbol=pair,
                recv_window=recv_window,
                api_key=order_context.get("api_key"),
                secret_key=order_context.get("secret_key"),
                base_url=base_url,
            )
        except TradeExecutionError as exc:
            raise TradeRequestValidationError(f"无法查询 Binance 持仓用于全部平仓: {exc}") from exc

        total = Decimal("0")
        for position in positions:
            if not isinstance(position, dict):
                continue
            if str(position.get("symbol") or "").upper() != pair:
                continue
            pos_side = str(position.get("positionSide") or "BOTH").upper()
            amt_raw = position.get("positionAmt")
            if amt_raw in (None, "", 0, "0"):
                continue
            try:
                amt = Decimal(str(amt_raw))
            except (InvalidOperation, TypeError):
                continue

            if pos_side in {"LONG"} and target_norm == "long" and amt > 0:
                total += amt
            elif pos_side in {"SHORT"} and target_norm == "short":
                if amt < 0:
                    total += (-amt)
                elif amt > 0:
                    total += amt
            elif pos_side == "BOTH":
                if target_norm == "long" and amt > 0:
                    total += amt
                elif target_norm == "short" and amt < 0:
                    total += (-amt)

        if total <= 0:
            raise TradeRequestValidationError("当前 Binance 持仓为空，无需执行全部平仓指令")

        return total.quantize(DEFAULT_QUANT, rounding=ROUND_DOWN)

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

        # Bybit hedge-mode requires explicit positionIdx:
        # - positionIdx=1 for LONG position
        # - positionIdx=2 for SHORT position
        # For reduce-only (close) actions, select by target_position, not by order direction.
        if exchange_key == "bybit" and "position_idx" not in leg_kwargs and target_position in {"long", "short"}:
            leg_kwargs["position_idx"] = 1 if target_position == "long" else 2
        # Binance hedge-mode needs positionSide for reduce-only closes to target the correct leg.
        if exchange_key == "binance" and "position_side" not in leg_kwargs and target_position in {"long", "short"}:
            leg_kwargs["position_side"] = "LONG" if target_position == "long" else "SHORT"

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

# 启动时尝试用数据库过去窗口内的资金费率预热，避免重启后空窗口。
# 注意：SQLite 可能很大（例如 market_data.db），同步预热会阻塞 Flask 启动；因此用后台线程执行。
def _watchlist_preload_worker() -> None:
    try:
        watchlist_manager.preload_from_database(db.db_path)
    except Exception as exc:
        logging.getLogger('watchlist').warning("watchlist preload at startup failed: %s", exc)


if str(os.getenv("WATCHLIST_PRELOAD_ON_STARTUP", "1")).strip().lower() not in {"0", "false", "no", "off"}:
    threading.Thread(target=_watchlist_preload_worker, name="watchlist-preload", daemon=True).start()

# Live trading manager (Phase 1: Type B only). Default disabled via LIVE_TRADING_CONFIG.
live_trading_manager = LiveTradingManager(
    LiveTradingConfig(
        enabled=bool(LIVE_TRADING_CONFIG.get('enabled')),
        dsn=str(WATCHLIST_PG_CONFIG.get('dsn')),
        allowed_exchanges=tuple(
            x.strip().lower()
            for x in str(LIVE_TRADING_CONFIG.get('allowed_exchanges') or '').split(',')
            if x.strip()
        ) or ("binance", "bybit"),
        horizon_min=int(LIVE_TRADING_CONFIG.get('horizon_min', 240)),
        pnl_threshold=float(LIVE_TRADING_CONFIG.get('pnl_threshold', 0.012)),
        win_prob_threshold=float(LIVE_TRADING_CONFIG.get('win_prob_threshold', 0.93)),
        v2_enabled=bool(LIVE_TRADING_CONFIG.get('v2_enabled', True)),
        v2_pnl_threshold_240=float(LIVE_TRADING_CONFIG.get('v2_pnl_threshold_240', 0.012)),
        v2_win_prob_threshold_240=float(LIVE_TRADING_CONFIG.get('v2_win_prob_threshold_240', 0.92)),
        v2_pnl_threshold_1440=float(LIVE_TRADING_CONFIG.get('v2_pnl_threshold_1440', 0.014)),
        v2_win_prob_threshold_1440=float(LIVE_TRADING_CONFIG.get('v2_win_prob_threshold_1440', 0.93)),
        max_concurrent_trades=int(LIVE_TRADING_CONFIG.get('max_concurrent_trades', 10)),
        scan_interval_seconds=float(LIVE_TRADING_CONFIG.get('scan_interval_seconds', 20.0)),
        monitor_interval_seconds=float(LIVE_TRADING_CONFIG.get('monitor_interval_seconds', 60.0)),
        take_profit_ratio=float(LIVE_TRADING_CONFIG.get('take_profit_ratio', 0.8)),
        orderbook_confirm_samples=int(LIVE_TRADING_CONFIG.get('orderbook_confirm_samples', 3)),
        orderbook_confirm_sleep_seconds=float(LIVE_TRADING_CONFIG.get('orderbook_confirm_sleep_seconds', 0.7)),
        max_hold_days=int(LIVE_TRADING_CONFIG.get('max_hold_days', 7)),
        stop_loss_total_pnl_pct=float(LIVE_TRADING_CONFIG.get('stop_loss_total_pnl_pct', 0.01)),
        stop_loss_funding_per_hour_pct=float(LIVE_TRADING_CONFIG.get('stop_loss_funding_per_hour_pct', 0.003)),
        max_abs_funding=float(LIVE_TRADING_CONFIG.get('max_abs_funding', 0.001)),
        close_retry_cooldown_seconds=float(LIVE_TRADING_CONFIG.get('close_retry_cooldown_seconds', 120.0)),
        scale_in_enabled=bool(LIVE_TRADING_CONFIG.get('scale_in_enabled', True)),
        scale_in_max_entries=int(LIVE_TRADING_CONFIG.get('scale_in_max_entries', 4)),
        scale_in_trigger_mult=float(LIVE_TRADING_CONFIG.get('scale_in_trigger_mult', 1.5)),
        scale_in_min_interval_minutes=int(LIVE_TRADING_CONFIG.get('scale_in_min_interval_minutes', 30)),
        scale_in_signal_max_age_minutes=int(LIVE_TRADING_CONFIG.get('scale_in_signal_max_age_minutes', 30)),
        scale_in_max_total_notional_usdt=(
            float(LIVE_TRADING_CONFIG.get('scale_in_max_total_notional_usdt'))
            if float(LIVE_TRADING_CONFIG.get('scale_in_max_total_notional_usdt') or 0.0) > 0
            else None
        ),
        event_lookback_minutes=int(LIVE_TRADING_CONFIG.get('event_lookback_minutes', 30)),
        per_leg_notional_usdt=float(LIVE_TRADING_CONFIG.get('per_leg_notional_usdt', 50.0)),
        candidate_limit=int(LIVE_TRADING_CONFIG.get('candidate_limit', 50)),
        per_symbol_top_k=int(LIVE_TRADING_CONFIG.get('per_symbol_top_k', 3)),
        max_symbols_per_scan=int(LIVE_TRADING_CONFIG.get('max_symbols_per_scan', 8)),
        kick_driven=bool(LIVE_TRADING_CONFIG.get('kick_driven', True)),
    )
)


def _start_live_trading_components() -> bool:
    logger = logging.getLogger("live_trading")
    live_enabled = bool(live_trading_manager.config.enabled)
    pg_writer = watchlist_manager.pg_writer
    pg_writer_enabled = bool(pg_writer and pg_writer.config.enabled)

    if not live_enabled and not pg_writer_enabled:
        # Best-effort: keep tables for UI even when auto-trading is disabled.
        try:
            live_trading_manager.ensure_schema()
        except Exception as exc:
            logger.warning("live trading schema init failed: %s", exc)
        return True

    errors: List[str] = []
    schema_ok = True
    try:
        live_trading_manager.ensure_schema()
    except Exception as exc:
        schema_ok = False
        errors.append(f"schema: {exc}")

    if live_enabled and schema_ok:
        try:
            live_trading_manager.start()
        except Exception as exc:
            errors.append(f"start: {exc}")
        if not (live_trading_manager._thread_scan and live_trading_manager._thread_monitor):
            errors.append("not running after start")

    if pg_writer_enabled:
        try:
            pg_writer.start()
        except Exception as exc:
            errors.append(f"pg writer: {exc}")
        if not getattr(pg_writer, "_thread", None):
            errors.append("pg writer not running after start")

    if live_enabled and live_trading_manager._thread_scan and live_trading_manager._thread_monitor:
        # When a new watchlist event is inserted into PG, wake live trading immediately (kick-driven).
        try:
            watchlist_manager.set_live_trading_kick(lambda reason: live_trading_manager.kick(reason=reason))
        except Exception:
            pass

    if errors:
        logger.warning("live trading/pg writer start failed: %s", "; ".join(errors))
        return False
    return True


def _live_trading_self_heal_worker() -> None:
    logger = logging.getLogger("live_trading")
    delay = float(os.getenv("LIVE_TRADING_RETRY_BASE_SECONDS", "2.0"))
    max_delay = float(os.getenv("LIVE_TRADING_RETRY_MAX_SECONDS", "60.0"))
    while True:
        if _start_live_trading_components():
            logger.info("live trading self-heal completed")
            return
        logger.warning("live trading/pg writer retry in %.1fs", delay)
        time.sleep(delay)
        delay = min(max_delay, delay * 2.0)


if not _start_live_trading_components():
    threading.Thread(
        target=_live_trading_self_heal_worker,
        name="live-trading-self-heal",
        daemon=True,
    ).start()

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


_FUNDING_HISTORY_CACHE_LOCK = threading.Lock()
_FUNDING_HISTORY_CACHE: Dict[tuple, Dict[str, Any]] = {}
_FUNDING_HISTORY_CACHE_TTL_SECONDS = 60.0


def _dt_to_ms(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            v = int(value)
        except Exception:
            return None
        return v if v > 10_000_000_000 else v * 1000
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    return None


def _funding_fee_summary_since_open(
    exchange: str,
    symbol: str,
    opened_at: Any,
    *,
    notional_usdt: Optional[float] = None,
    pos_sign: Optional[int] = None,
    now_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """Best-effort: query account funding fee history for a single exchange+symbol.

    Returns:
      - funding_pnl_usdt: float (sum of funding fees, with sign)
      - last_fee_usdt: float|None
      - last_fee_time: ISO|None
      - currency: str|None
      - n_records: int
      - source: 'ledger' | 'rate_estimate'
      - error: str|None
    """
    ex = (exchange or "").lower().strip()
    sym = (symbol or "").upper().strip()
    start_ms = _dt_to_ms(opened_at)
    if not ex or not sym or start_ms is None:
        return {"funding_pnl_usdt": None, "last_fee_usdt": None, "last_fee_time": None, "currency": None, "n_records": 0, "error": "missing exchange/symbol/opened_at"}

    now_ms = int(now_ms or (time.time() * 1000))
    # Cache key includes current UTC hour bucket to ensure hourly refresh semantics.
    hour_bucket = int(now_ms // 3_600_000)
    cache_key = (ex, sym, int(start_ms), hour_bucket)
    with _FUNDING_HISTORY_CACHE_LOCK:
        cached = _FUNDING_HISTORY_CACHE.get(cache_key)
    if cached:
        try:
            if float(cached.get("expires_at") or 0) > time.time():
                return dict(cached.get("value") or {})
        except Exception:
            pass

    out: Dict[str, Any] = {
        "funding_pnl_usdt": 0.0,
        "last_fee_usdt": None,
        "last_fee_time": None,
        "currency": None,
        "n_records": 0,
        "source": "ledger",
        "error": None,
    }

    def _as_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            v = float(value)
        except Exception:
            return None
        if not (v == v) or v in (float("inf"), float("-inf")):
            return None
        return v

    def _as_iso_from_ms(ms: Any) -> Optional[str]:
        try:
            msv = int(float(ms))
        except Exception:
            return None
        if msv <= 0:
            return None
        return datetime.fromtimestamp(msv / 1000, tz=timezone.utc).isoformat()

    try:
        rows: List[Dict[str, Any]] = []
        if ex == "binance":
            rows = get_binance_funding_fee_income(symbol=sym, start_time_ms=start_ms, end_time_ms=now_ms)
            # time: ms, income: str
            for r in rows:
                fee = _as_float(r.get("income"))
                ts_ms = r.get("time")
                if fee is None:
                    continue
                out["funding_pnl_usdt"] += fee
                out["currency"] = str(r.get("asset") or "USDT").upper()
                iso = _as_iso_from_ms(ts_ms)
                if iso and (out["last_fee_time"] is None or iso > str(out["last_fee_time"])):
                    out["last_fee_time"] = iso
                    out["last_fee_usdt"] = fee

        elif ex == "okx":
            rows = get_okx_funding_fee_bills(symbol=sym, start_time_ms=start_ms, end_time_ms=now_ms)
            for r in rows:
                fee = _as_float(r.get("balChg") or r.get("balChgQty") or r.get("chgBal"))
                ts_ms = r.get("ts")
                if fee is None:
                    continue
                out["funding_pnl_usdt"] += fee
                out["currency"] = str(r.get("ccy") or "USDT").upper()
                iso = _as_iso_from_ms(ts_ms)
                if iso and (out["last_fee_time"] is None or iso > str(out["last_fee_time"])):
                    out["last_fee_time"] = iso
                    out["last_fee_usdt"] = fee

        elif ex == "bybit":
            rows = get_bybit_funding_fee_transactions(symbol=sym, start_time_ms=start_ms, end_time_ms=now_ms)
            for r in rows:
                # Ensure per-symbol: some accounts may ignore the symbol filter.
                try:
                    sym0 = str(r.get("symbol") or "")
                    if sym0 and sym0.upper() != f"{sym.upper()}USDT":
                        continue
                except Exception:
                    pass
                fee = _as_float(r.get("cashFlow") or r.get("cashFlowAmount") or r.get("amount"))
                ts_ms = r.get("transactionTime") or r.get("transTime") or r.get("execTime")
                if fee is None:
                    continue
                out["funding_pnl_usdt"] += fee
                out["currency"] = str(r.get("currency") or "USDT").upper()
                iso = _as_iso_from_ms(ts_ms)
                if iso and (out["last_fee_time"] is None or iso > str(out["last_fee_time"])):
                    out["last_fee_time"] = iso
                    out["last_fee_usdt"] = fee

        elif ex == "bitget":
            rows = get_bitget_funding_fee_bills(symbol=sym, start_time_ms=start_ms, end_time_ms=now_ms)
            for r in rows:
                try:
                    sym0 = str(r.get("symbol") or "")
                    if sym0 and sym0.upper().replace("_UMCBL", "") != f"{sym.upper()}USDT":
                        continue
                except Exception:
                    pass
                fee = _as_float(
                    r.get("amount")
                    or r.get("pnl")
                    or r.get("profit")
                    or r.get("value")
                    or r.get("cashFlow")
                )
                ts_ms = r.get("cTime") or r.get("ts") or r.get("createTime") or r.get("uTime")
                if fee is None:
                    continue
                out["funding_pnl_usdt"] += fee
                out["currency"] = str(r.get("marginCoin") or r.get("ccy") or "USDT").upper()
                iso = _as_iso_from_ms(ts_ms)
                if iso and (out["last_fee_time"] is None or iso > str(out["last_fee_time"])):
                    out["last_fee_time"] = iso
                    out["last_fee_usdt"] = fee

        elif ex == "grvt":
            rows = get_grvt_funding_payment_history(symbol=sym, start_time_ms=start_ms, end_time_ms=now_ms, limit=1000)
            for r in rows:
                # Normalized: income >0 means received (benefit), <0 means paid (cost)
                fee = _as_float(r.get("income"))
                ts_ms = r.get("time")
                if fee is None:
                    continue
                out["funding_pnl_usdt"] += fee
                out["currency"] = str(r.get("currency") or "USDT").upper()
                iso = _as_iso_from_ms(ts_ms)
                if iso and (out["last_fee_time"] is None or iso > str(out["last_fee_time"])):
                    out["last_fee_time"] = iso
                    out["last_fee_usdt"] = fee

        elif ex == "hyperliquid":
            rows = get_hyperliquid_user_funding_history(start_time_ms=start_ms, end_time_ms=now_ms)
            for r in rows:
                # Official SDK payload: {"time": ..., "delta": {"type":"funding","coin":"ETH","usdc":"..."}}
                delta = r.get("delta")
                if not isinstance(delta, dict):
                    continue
                if str(delta.get("type") or "").lower() != "funding":
                    continue
                coin = str(delta.get("coin") or "").upper()
                if coin and coin != sym.upper():
                    continue
                fee = _as_float(delta.get("usdc") or delta.get("amount"))
                ts_ms = r.get("time") or delta.get("time") or r.get("ts")
                if fee is None:
                    continue
                out["funding_pnl_usdt"] += fee  # treat USDC≈USDT for display
                out["currency"] = "USDC"
                iso = _as_iso_from_ms(ts_ms)
                if iso and (out["last_fee_time"] is None or iso > str(out["last_fee_time"])):
                    out["last_fee_time"] = iso
                    out["last_fee_usdt"] = fee

        else:
            out["funding_pnl_usdt"] = None
            out["error"] = f"unsupported exchange: {ex}"
            rows = []

        out["n_records"] = len(rows) if isinstance(rows, list) else 0
    except Exception as exc:
        out["funding_pnl_usdt"] = None
        out["error"] = f"{type(exc).__name__}: {exc}"

    # Fallback: estimate from public funding-rate history when ledger is unavailable/empty.
    # This is primarily used for Bybit/Bitget where private bill endpoints are inconsistent.
    if (
        (out.get("funding_pnl_usdt") in (0.0, None))
        and int(out.get("n_records") or 0) == 0
        and pos_sign in (-1, 1)
        and notional_usdt is not None
        and float(notional_usdt or 0.0) > 0
        and ex in {"bybit", "bitget"}
    ):
        try:
            from watchlist_outcome_worker import FundingHistoryFetcher  # type: ignore

            fetcher = getattr(_funding_fee_summary_since_open, "_RATE_FETCHER", None)
            if fetcher is None:
                fetcher = FundingHistoryFetcher()
                setattr(_funding_fee_summary_since_open, "_RATE_FETCHER", fetcher)

            start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
            end_dt = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
            points = fetcher.fetch(ex, sym.upper(), start_dt, end_dt) or []
            if points:
                total = 0.0
                last_fee = None
                last_time = None
                for ts, rate in points:
                    fee = -float(pos_sign) * float(rate) * float(notional_usdt)
                    total += fee
                    last_fee = fee
                    last_time = ts
                out["funding_pnl_usdt"] = total
                out["last_fee_usdt"] = last_fee
                out["last_fee_time"] = last_time.isoformat() if isinstance(last_time, datetime) else None
                out["currency"] = "USDT"
                out["n_records"] = len(points)
                out["source"] = "rate_estimate"
        except Exception as exc:
            # keep ledger=0 but attach error for visibility
            out["error"] = out.get("error") or f"rate_estimate_failed: {type(exc).__name__}: {exc}"

    with _FUNDING_HISTORY_CACHE_LOCK:
        _FUNDING_HISTORY_CACHE[cache_key] = {"expires_at": time.time() + _FUNDING_HISTORY_CACHE_TTL_SECONDS, "value": dict(out)}
    return out


_PUB_FUNDING_SCHED_CACHE_LOCK = threading.Lock()
_PUB_FUNDING_SCHED_CACHE: Dict[tuple, Dict[str, Any]] = {}


def _public_funding_schedule(exchange: str, base: str) -> Dict[str, Any]:
    """Public (no-auth) funding schedule snapshot for UI display (best-effort)."""
    ex = (exchange or "").lower().strip()
    base_u = (base or "").upper().strip()
    if not ex or not base_u:
        return {}

    key = (ex, base_u)
    with _PUB_FUNDING_SCHED_CACHE_LOCK:
        cached = _PUB_FUNDING_SCHED_CACHE.get(key)
    if cached:
        try:
            if float(cached.get("expires_at") or 0) > time.time():
                return dict(cached.get("value") or {})
        except Exception:
            pass

    headers = {"User-Agent": (REST_CONNECTION_CONFIG or {}).get("user_agent", "FR-Monitor/1.0"), "Accept": "application/json"}
    timeout = float((REST_CONNECTION_CONFIG or {}).get("timeout", 6))
    out: Dict[str, Any] = {}
    now = datetime.now(timezone.utc)

    def _req_json(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        try:
            resp = requests.get(url, params=params, timeout=timeout, headers=headers)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            return None
        return None

    try:
        if ex == "binance":
            sym = f"{base_u}USDT"
            data = _req_json("https://fapi.binance.com/fapi/v1/premiumIndex", params={"symbol": sym})
            if isinstance(data, dict):
                try:
                    fr = data.get("lastFundingRate")
                    if fr is not None:
                        out["funding_rate"] = float(fr)
                except Exception:
                    pass
                nft = normalize_next_funding_time(data.get("nextFundingTime"))
                if nft:
                    out["next_funding_time"] = nft
            interval = derive_funding_interval_hours("binance")
            if interval:
                out["funding_interval_hours"] = float(interval)

        elif ex == "bybit":
            sym = f"{base_u}USDT"
            data = _req_json("https://api.bybit.com/v5/market/tickers", params={"category": "linear", "symbol": sym})
            if isinstance(data, dict) and data.get("retCode") == 0:
                items = ((data.get("result") or {}) or {}).get("list") or []
                item = items[0] if isinstance(items, list) and items and isinstance(items[0], dict) else None
                if item:
                    try:
                        if item.get("fundingRate") is not None:
                            out["funding_rate"] = float(item.get("fundingRate"))
                    except Exception:
                        pass
                    nft = normalize_next_funding_time(item.get("nextFundingTime"))
                    if nft:
                        out["next_funding_time"] = nft
                    interval = derive_funding_interval_hours("bybit", item.get("fundingIntervalHour"))
                    if interval:
                        out["funding_interval_hours"] = float(interval)
            if "funding_interval_hours" not in out:
                interval = derive_funding_interval_hours("bybit")
                if interval:
                    out["funding_interval_hours"] = float(interval)

        elif ex == "okx":
            inst_id = f"{base_u}-USDT-SWAP"
            fr = _req_json("https://www.okx.com/api/v5/public/funding-rate", params={"instId": inst_id})
            if isinstance(fr, dict) and fr.get("code") == "0":
                data = fr.get("data") or []
                entry = data[0] if isinstance(data, list) and data and isinstance(data[0], dict) else None
                if entry:
                    try:
                        if entry.get("fundingRate") is not None:
                            out["funding_rate"] = float(entry.get("fundingRate"))
                    except Exception:
                        pass
                    ft = normalize_next_funding_time(entry.get("fundingTime"))
                    nft = normalize_next_funding_time(entry.get("nextFundingTime"))
                    try:
                        cand = []
                        for x in (ft, nft):
                            if not x:
                                continue
                            dt = datetime.fromisoformat(str(x).replace("Z", "+00:00"))
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            if dt > (now - timedelta(seconds=60)):
                                cand.append(dt.astimezone(timezone.utc))
                        if cand:
                            out["next_funding_time"] = min(cand).isoformat()
                        elif nft:
                            out["next_funding_time"] = nft
                        elif ft:
                            out["next_funding_time"] = ft
                    except Exception:
                        if nft:
                            out["next_funding_time"] = nft
                        elif ft:
                            out["next_funding_time"] = ft
                    interval = derive_interval_hours_from_times(entry.get("fundingTime"), entry.get("nextFundingTime"))
                    if interval:
                        out["funding_interval_hours"] = float(interval)
            if "funding_interval_hours" not in out:
                interval = derive_funding_interval_hours("okx")
                if interval:
                    out["funding_interval_hours"] = float(interval)

        elif ex == "bitget":
            sym = f"{base_u}USDT"
            sched = _req_json(
                "https://api.bitget.com/api/v2/mix/market/current-fund-rate",
                params={"productType": "USDT-FUTURES", "symbol": sym},
            )
            if isinstance(sched, dict) and sched.get("code") == "00000":
                data = sched.get("data") or []
                entry = data[0] if isinstance(data, list) and data and isinstance(data[0], dict) else None
                if entry:
                    try:
                        if entry.get("fundingRate") is not None:
                            out["funding_rate"] = float(entry.get("fundingRate"))
                    except Exception:
                        pass
                    nft = normalize_next_funding_time(
                        entry.get("nextSettleTime")
                        or entry.get("nextUpdate")
                        or entry.get("nextFundingTime")
                    )
                    if nft:
                        out["next_funding_time"] = nft
                    interval = derive_funding_interval_hours("bitget", entry.get("fundingRateInterval"), fallback=True)
                    if interval:
                        out["funding_interval_hours"] = float(interval)
            if "funding_interval_hours" not in out:
                interval = derive_funding_interval_hours("bitget")
                if interval:
                    out["funding_interval_hours"] = float(interval)

        elif ex == "hyperliquid":
            interval = derive_funding_interval_hours("hyperliquid")
            if interval:
                out["funding_interval_hours"] = float(interval)
            # next funding time: top-of-hour for 1h (Hyperliquid)
            step_hours = int(out.get("funding_interval_hours") or 1)
            floored = now.replace(minute=0, second=0, microsecond=0)
            next_t = floored + timedelta(hours=1 if step_hours <= 0 else step_hours)
            if next_t <= now:
                next_t = next_t + timedelta(hours=1 if step_hours <= 0 else step_hours)
            out["next_funding_time"] = next_t.isoformat()
            fmap = get_hyperliquid_funding_map(force_refresh=False) or {}
            ctx = fmap.get(base_u)
            if isinstance(ctx, dict) and ctx.get("funding") is not None:
                try:
                    out["funding_rate"] = float(ctx.get("funding"))
                except Exception:
                    pass
        else:
            out = {}
    except Exception:
        out = {}

    with _PUB_FUNDING_SCHED_CACHE_LOCK:
        _PUB_FUNDING_SCHED_CACHE[key] = {"expires_at": time.time() + 10.0, "value": dict(out)}
    return dict(out)


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


def _monitor_8010_default_quote() -> str:
    try:
        return str((MONITOR_8010_CONFIG or {}).get("default_quote") or "USDC").strip().upper()
    except Exception:
        return "USDC"


def _to_monitor_8010_symbol(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if not s:
        return ""
    if "-" in s:
        return s
    base = re.sub(r"[^A-Z0-9]", "", s)
    if not base:
        return ""
    return f"{base}-{_monitor_8010_default_quote()}-PERP"


def _fetch_monitor_8010_snapshot(
    *,
    symbols: List[str],
    exchanges: List[str],
    include_analysis: bool = False,
    include_tickers: bool = False,
) -> Optional[Dict[str, Any]]:
    if not bool(MONITOR_8010_CONFIG.get("enabled", True)):
        return None
    base = str(MONITOR_8010_CONFIG.get("base_url") or "").strip().rstrip("/")
    if not base:
        return None
    params: List[tuple] = [
        ("include_analysis", "true" if include_analysis else "false"),
        ("include_tickers", "true" if include_tickers else "false"),
    ]
    for ex in sorted({str(x).strip().lower() for x in (exchanges or []) if str(x).strip()}):
        params.append(("exchanges", ex))
    for sym in sorted({str(x).strip().upper() for x in (symbols or []) if str(x).strip()}):
        params.append(("symbols", sym))
    try:
        timeout = float(MONITOR_8010_CONFIG.get("timeout_seconds") or 3.0)
    except Exception:
        timeout = 3.0
    try:
        resp = requests.get(f"{base}/snapshot", params=params, timeout=timeout)
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data if isinstance(data, dict) else None
    except Exception:
        return None


_MONITOR_8010_KEEPALIVE_LOCK = threading.Lock()
_MONITOR_8010_KEEPALIVE_LAST_SENT: Dict[str, float] = {}
_MONITOR_8010_KEEPALIVE_CURSOR = 0


def _monitor_8010_keepalive_active_symbols(active_entries: List[Dict[str, Any]]) -> None:
    """
    Best-effort keepalive so 8010 can serve BBO for all active watchlist symbols.
    Important: do not persist locally; 8010 owns TTL and persistence.

    We call 8010 `/watchlist/add` with `exchanges=null` (omitted) so it subscribes all
    configured exchanges on its side.
    """
    if not active_entries:
        return
    if not bool(MONITOR_8010_CONFIG.get("enabled", True)):
        return
    base = str(MONITOR_8010_CONFIG.get("base_url") or "").strip().rstrip("/")
    if not base:
        return
    try:
        timeout = float(MONITOR_8010_CONFIG.get("timeout_seconds") or 3.0)
    except Exception:
        timeout = 3.0
    try:
        ttl_seconds = int(float(MONITOR_8010_CONFIG.get("watchlist_ttl_seconds") or 86400))
    except Exception:
        ttl_seconds = 86400
    try:
        min_interval_sec = float(MONITOR_8010_CONFIG.get("watchlist_signal_min_interval_sec") or 600.0)
    except Exception:
        min_interval_sec = 600.0
    source = str(MONITOR_8010_CONFIG.get("watchlist_source") or "FR_Monitor").strip() or "FR_Monitor"

    now_s = time.time()
    mon_syms: List[str] = []
    for e in active_entries:
        sym = str((e or {}).get("symbol") or "").strip().upper()
        if not sym:
            continue
        mon = _to_monitor_8010_symbol(sym)
        if mon:
            mon_syms.append(mon)
    mon_syms = sorted(set(mon_syms))
    if not mon_syms:
        return

    # Safety: cap per refresh cycle, but round-robin so all active symbols are eventually kept alive.
    cap = 50
    global _MONITOR_8010_KEEPALIVE_CURSOR
    total_n = len(mon_syms)
    if total_n > cap:
        start = int(_MONITOR_8010_KEEPALIVE_CURSOR) % total_n
        window = mon_syms[start : start + cap]
        if len(window) < cap:
            window = window + mon_syms[: cap - len(window)]
        mon_syms = window
        _MONITOR_8010_KEEPALIVE_CURSOR = (start + cap) % total_n

    with _MONITOR_8010_KEEPALIVE_LOCK:
        for mon_sym in mon_syms:
            last = float(_MONITOR_8010_KEEPALIVE_LAST_SENT.get(mon_sym) or 0.0)
            if now_s - last < min_interval_sec:
                continue
            payload = {
                "symbol": mon_sym,
                # exchanges omitted => 8010 subscribes all configured exchanges
                "ttl_seconds": ttl_seconds,
                "source": source,
                "reason": "watchlist_active_keepalive",
            }
            try:
                resp = requests.post(f"{base}/watchlist/add", json=payload, timeout=timeout)
                if resp.status_code == 200:
                    _MONITOR_8010_KEEPALIVE_LAST_SENT[mon_sym] = now_s
            except Exception:
                # best-effort
                pass


def _monitor_8010_supported_exchanges_for_display() -> List[str]:
    # Keep in sync with UI display ordering when possible.
    try:
        base = list(EXCHANGE_DISPLAY_ORDER or [])
    except Exception:
        base = []
    extra = []
    for x in ("edgex", "paradex"):
        if x not in base:
            extra.append(x)
    return [str(x).strip().lower() for x in (base + extra) if str(x).strip()]


def build_monitor_8010_bbo_by_exchange(entries: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Per-symbol per-exchange L1 BBO map for watchlist UI display.
    Output: { 'BTC': { 'okx': {...}, 'binance': {...} }, ... }
    Note: 8010 only returns data for (exchange,symbol) that are active in its watchlist.
    """
    out: Dict[str, Any] = {}
    if not entries:
        return out
    symbols: List[str] = []
    for entry in entries:
        sym = str(entry.get("symbol") or "").strip().upper()
        if not sym:
            continue
        mon_sym = _to_monitor_8010_symbol(sym)
        if mon_sym:
            symbols.append(mon_sym)
    if not symbols:
        return out
    snap = _fetch_monitor_8010_snapshot(
        symbols=symbols,
        # Do not filter by exchanges: return "as much as available" for each symbol.
        exchanges=[],
        include_analysis=False,
        include_tickers=False,
    )
    if snap is None:
        return None
    orderbooks = (snap or {}).get("orderbooks") if isinstance(snap, dict) else None
    if not isinstance(orderbooks, dict):
        return out

    for ex, sym_map in orderbooks.items():
        if not isinstance(sym_map, dict):
            continue
        ex_l = str(ex).strip().lower()
        for mon_sym, ob in sym_map.items():
            if not isinstance(ob, dict):
                continue
            try:
                base_sym = str(mon_sym).split("-", 1)[0].strip().upper()
            except Exception:
                base_sym = ""
            if not base_sym:
                continue
            out.setdefault(base_sym, {})[ex_l] = {
                "bid_price": ob.get("bid_price"),
                "bid_size": ob.get("bid_size"),
                "ask_price": ob.get("ask_price"),
                "ask_size": ob.get("ask_size"),
                "exchange_timestamp": ob.get("exchange_timestamp"),
                "received_timestamp": ob.get("received_timestamp"),
                "processed_timestamp": ob.get("processed_timestamp"),
                "monitor_symbol": str(mon_sym).upper(),
            }
    return out


def _entry_legs_for_monitor_8010_bbo(entry: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Return legs that the watchlist UI should show bid/ask for.
    Note: 8010 currently provides perp orderbooks only; spot legs will be left empty.
    """
    etype = entry.get("entry_type")
    trigger = entry.get("trigger_details") or {}
    symbol = entry.get("symbol")
    if not symbol:
        return []
    if etype == "A":
        return [
            {"exchange": "binance", "market_type": "spot"},
            {"exchange": "binance", "market_type": "perp"},
        ]
    if etype == "B":
        pair = trigger.get("pair") or []
        if len(pair) == 2:
            return [
                {"exchange": str(pair[0]).strip().lower(), "market_type": "perp"},
                {"exchange": str(pair[1]).strip().lower(), "market_type": "perp"},
            ]
    if etype == "C":
        spot_ex = trigger.get("spot_exchange")
        fut_ex = trigger.get("futures_exchange")
        if spot_ex and fut_ex:
            return [
                {"exchange": str(spot_ex).strip().lower(), "market_type": "spot"},
                {"exchange": str(fut_ex).strip().lower(), "market_type": "perp"},
            ]
    return []


def build_monitor_8010_bbo_annotation(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Use 8010 /snapshot L1 bid/ask to build watchlist 'orderbook' annotation.
    If 8010 data is missing/unhealthy for a leg, leave buy/sell empty (null) for that leg.
    """
    out: Dict[str, Any] = {}
    if not entries:
        return out
    exchanges: List[str] = []
    symbols_mon: List[str] = []
    entry_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for entry in entries:
        sym = str(entry.get("symbol") or "").strip().upper()
        if not sym:
            continue
        entry_by_symbol.setdefault(sym, []).append(entry)
        mon_sym = _to_monitor_8010_symbol(sym)
        if mon_sym:
            symbols_mon.append(mon_sym)
        for leg in _entry_legs_for_monitor_8010_bbo(entry):
            ex = str(leg.get("exchange") or "").strip().lower()
            if ex and ex not in exchanges and ex != "unsupported":
                exchanges.append(ex)

    snap = _fetch_monitor_8010_snapshot(symbols=symbols_mon, exchanges=exchanges, include_analysis=False, include_tickers=False)
    orderbooks = (snap or {}).get("orderbooks") if isinstance(snap, dict) else None
    if not isinstance(orderbooks, dict):
        return out

    for sym, e_list in entry_by_symbol.items():
        # Choose a "representative" entry: the UI is symbol-centric anyway.
        entry = e_list[0]
        mon_sym = _to_monitor_8010_symbol(sym)
        legs_meta = _entry_legs_for_monitor_8010_bbo(entry)
        legs: List[Dict[str, Any]] = []
        for leg in legs_meta:
            ex = str(leg.get("exchange") or "").strip().lower()
            mtype = str(leg.get("market_type") or "").strip().lower()
            price_obj: Dict[str, Any] = {"buy": None, "sell": None, "mid": None}
            meta_obj: Dict[str, Any] = {}
            if mtype == "perp" and ex and mon_sym:
                ob = orderbooks.get(ex, {}).get(mon_sym) if isinstance(orderbooks.get(ex), dict) else None
                if isinstance(ob, dict):
                    ask = ob.get("ask_price")
                    bid = ob.get("bid_price")
                    try:
                        ask_f = float(ask) if ask is not None else None
                        bid_f = float(bid) if bid is not None else None
                    except Exception:
                        ask_f = bid_f = None
                    if ask_f and bid_f and ask_f > 0 and bid_f > 0:
                        price_obj["buy"] = ask_f
                        price_obj["sell"] = bid_f
                        price_obj["mid"] = (ask_f + bid_f) / 2.0
                    meta_obj = {
                        "exchange_timestamp": ob.get("exchange_timestamp"),
                        "received_timestamp": ob.get("received_timestamp"),
                        "processed_timestamp": ob.get("processed_timestamp"),
                        "bid_size": ob.get("bid_size"),
                        "ask_size": ob.get("ask_size"),
                    }
            legs.append(
                {
                    "exchange": ex,
                    "market_type": mtype,
                    "price": price_obj,
                    "meta": meta_obj or None,
                }
            )
        spread = compute_orderbook_spread(legs) if len(legs) >= 2 else None
        out[sym] = (
            {
                "legs": legs,
                "forward": (spread or {}).get("forward") if isinstance(spread, dict) else None,
                "reverse": (spread or {}).get("reverse") if isinstance(spread, dict) else None,
            }
            if legs
            else {"legs": [], "forward": None, "reverse": None}
        )
    return out


def _ensure_watchlist_bbo_table_pg() -> bool:
    global WATCHLIST_BBO_PG_READY
    if WATCHLIST_BBO_PG_READY:
        return True
    if psycopg is None:
        return False
    if not bool(WATCHLIST_PG_CONFIG.get("enabled", False)):
        return False
    dsn = str(WATCHLIST_PG_CONFIG.get("dsn") or "").strip()
    if not dsn:
        return False
    with WATCHLIST_BBO_PG_LOCK:
        if WATCHLIST_BBO_PG_READY:
            return True
        try:
            ddl = """
            CREATE SCHEMA IF NOT EXISTS watchlist;
            CREATE TABLE IF NOT EXISTS watchlist.orderbook_bbo (
              ts timestamptz NOT NULL,
              symbol text NOT NULL,
              exchange text NOT NULL,
              market_type text NOT NULL,
              bid double precision NULL,
              ask double precision NULL,
              source text NOT NULL DEFAULT 'monitor8010',
              meta jsonb NULL
            );
            CREATE INDEX IF NOT EXISTS idx_orderbook_bbo_ts ON watchlist.orderbook_bbo (ts DESC);
            CREATE INDEX IF NOT EXISTS idx_orderbook_bbo_symbol_ts ON watchlist.orderbook_bbo (symbol, ts DESC);
            CREATE INDEX IF NOT EXISTS idx_orderbook_bbo_exchange_symbol_ts ON watchlist.orderbook_bbo (exchange, symbol, ts DESC);
            """
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute(ddl)
            WATCHLIST_BBO_PG_READY = True
            return True
        except Exception:
            return False


def _persist_monitor_8010_bbo_to_pg(orderbook_map: Dict[str, Any]) -> None:
    if not orderbook_map:
        return
    if psycopg is None:
        return
    if not _ensure_watchlist_bbo_table_pg():
        return
    dsn = str(WATCHLIST_PG_CONFIG.get("dsn") or "").strip()
    if not dsn:
        return

    now_dt = datetime.now(timezone.utc)
    rows: List[tuple] = []
    for sym, ob in (orderbook_map or {}).items():
        if not isinstance(ob, dict):
            continue
        for leg in (ob.get("legs") or []):
            if not isinstance(leg, dict):
                continue
            ex = str(leg.get("exchange") or "").strip().lower()
            market_type = str(leg.get("market_type") or "").strip().lower()
            price = leg.get("price") if isinstance(leg.get("price"), dict) else {}
            bid = price.get("sell")
            ask = price.get("buy")
            try:
                bid_f = float(bid) if bid is not None else None
            except Exception:
                bid_f = None
            try:
                ask_f = float(ask) if ask is not None else None
            except Exception:
                ask_f = None
            meta_obj = leg.get("meta") if isinstance(leg.get("meta"), dict) else None
            meta = dict(meta_obj) if meta_obj else {}
            meta["monitor_symbol"] = _to_monitor_8010_symbol(sym)
            if bid_f is None and ask_f is None:
                meta["missing"] = True
            rows.append((now_dt, str(sym).upper(), ex, market_type, bid_f, ask_f, "monitor8010", meta or None))

    if not rows:
        return
    try:
        from psycopg.types.json import Json  # type: ignore
    except Exception:  # pragma: no cover
        Json = None  # type: ignore

    params = []
    for ts, sym, ex, mtype, bid_f, ask_f, source, meta in rows:
        if Json is not None and meta is not None:
            meta = Json(meta)  # type: ignore[assignment]
        elif meta is not None and not isinstance(meta, (str, bytes)):
            try:
                meta = json.dumps(meta, ensure_ascii=False)
            except Exception:
                meta = None
        params.append((ts, sym, ex, mtype, bid_f, ask_f, source, meta))

    sql = """
    INSERT INTO watchlist.orderbook_bbo
      (ts, symbol, exchange, market_type, bid, ask, source, meta)
    VALUES
      (%s, %s, %s, %s, %s, %s, %s, %s::jsonb);
    """
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, params)
    except Exception:
        return


def _persist_monitor_8010_bbo_by_exchange_to_pg(bbo_by_exchange: Dict[str, Any]) -> None:
    """
    Persist 8010 BBO for all returned exchanges per symbol.
    This is independent from the UI "legs" notion.
    """
    if not bbo_by_exchange:
        return
    if psycopg is None:
        return
    if not _ensure_watchlist_bbo_table_pg():
        return
    dsn = str(WATCHLIST_PG_CONFIG.get("dsn") or "").strip()
    if not dsn:
        return
    now_dt = datetime.now(timezone.utc)
    rows: List[tuple] = []
    for sym, ex_map in bbo_by_exchange.items():
        if not isinstance(ex_map, dict):
            continue
        for ex, ob in ex_map.items():
            if not isinstance(ob, dict):
                continue
            bid = ob.get("bid_price")
            ask = ob.get("ask_price")
            try:
                bid_f = float(bid) if bid is not None else None
            except Exception:
                bid_f = None
            try:
                ask_f = float(ask) if ask is not None else None
            except Exception:
                ask_f = None
            meta = dict(ob)
            meta.pop("bid_price", None)
            meta.pop("ask_price", None)
            rows.append(
                (
                    now_dt,
                    str(sym).upper(),
                    str(ex).strip().lower(),
                    "perp",
                    bid_f,
                    ask_f,
                    "monitor8010",
                    meta or None,
                )
            )
    if not rows:
        return
    try:
        from psycopg.types.json import Json  # type: ignore
    except Exception:  # pragma: no cover
        Json = None  # type: ignore
    params = []
    for ts, sym, ex, mtype, bid_f, ask_f, source, meta in rows:
        if Json is not None and meta is not None:
            meta = Json(meta)  # type: ignore[assignment]
        elif meta is not None and not isinstance(meta, (str, bytes)):
            try:
                meta = json.dumps(meta, ensure_ascii=False)
            except Exception:
                meta = None
        params.append((ts, sym, ex, mtype, bid_f, ask_f, source, meta))
    sql = """
    INSERT INTO watchlist.orderbook_bbo
      (ts, symbol, exchange, market_type, bid, ask, source, meta)
    VALUES
      (%s, %s, %s, %s, %s, %s, %s, %s::jsonb);
    """
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, params)
    except Exception:
        return


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
    try:
        target_interval = float((WATCHLIST_CONFIG or {}).get("orderbook_refresh_seconds") or min_interval)
    except Exception:
        target_interval = min_interval
    if target_interval <= 0:
        target_interval = min_interval
    while True:
        start = time.time()
        try:
            prev_snapshot = WATCHLIST_ORDERBOOK_SNAPSHOT.copy()
            snapshot = watchlist_manager.snapshot()
            active_entries = [e for e in snapshot.get('entries', []) if e.get('status') == 'active']
            if not active_entries:
                WATCHLIST_ORDERBOOK_SNAPSHOT.update({
                    'orderbook': {},
                    'monitor8010_bbo': {},
                    'cross_spreads': {},
                    'timestamp': now_utc_iso(),
                    'error': None,
                    'stale_reason': 'no_active_entries',
                    'monitor8010_error': None,
                })
            else:
                try:
                    _monitor_8010_keepalive_active_symbols(active_entries)
                except Exception:
                    pass
                all_data = data_collector.get_all_data()
                # 订单簿/扫单口径：
                # - Type A：本地 REST 扫单订单簿（Binance spot/perp）。
                # - Type B：使用 8010 的 L1 BBO（perp vs perp）。
                # - Type C：需要 spot vs perp；8010 目前仅提供 perp 订单簿，所以 Type C 保留本地 REST 扫单订单簿。
                entries_a = [e for e in active_entries if str(e.get("entry_type") or "").upper() == "A"]
                entries_b = [e for e in active_entries if str(e.get("entry_type") or "").upper() == "B"]
                entries_c = [e for e in active_entries if str(e.get("entry_type") or "").upper() == "C"]

                orderbook: Dict[str, Any] = {}
                if entries_a or entries_c:
                    try:
                        orderbook.update(build_orderbook_annotation(entries_a + entries_c))
                    except Exception:
                        pass

                orderbook_8010 = build_monitor_8010_bbo_annotation(entries_b) if entries_b else {}
                orderbook.update(orderbook_8010)

                # For UI/DB: show/persist "as much data as available" per symbol across exchanges.
                # 8010 /snapshot will only return pairs that are active in its own watchlist, so this stays bounded.
                monitor8010_error = None
                bbo_by_exchange = build_monitor_8010_bbo_by_exchange(active_entries) if active_entries else {}
                if bbo_by_exchange is None:
                    # 8010 timeout/unreachable: keep last good snapshot so UI doesn't go blank.
                    monitor8010_error = "monitor8010_snapshot_failed"
                    bbo_by_exchange = (prev_snapshot.get("monitor8010_bbo") or {}) if isinstance(prev_snapshot, dict) else {}
                # Persist 8010 BBO snapshots even if UI is not open (best-effort; missing values kept as null).
                try:
                    _persist_monitor_8010_bbo_to_pg(orderbook_8010)
                except Exception:
                    pass
                try:
                    if monitor8010_error is None:
                        _persist_monitor_8010_bbo_by_exchange_to_pg(bbo_by_exchange)
                except Exception:
                    pass
                cross_spreads = build_cross_spread_matrix(active_entries, all_data)
                WATCHLIST_ORDERBOOK_SNAPSHOT.update({
                    'orderbook': orderbook,
                    'monitor8010_bbo': bbo_by_exchange,
                    'cross_spreads': cross_spreads,
                    'timestamp': now_utc_iso(),
                    'error': None,
                    'stale_reason': None,
                    'monitor8010_error': monitor8010_error,
                })
        except Exception as exc:
            WATCHLIST_ORDERBOOK_SNAPSHOT.update({
                'orderbook': WATCHLIST_ORDERBOOK_SNAPSHOT.get('orderbook') or {},
                'monitor8010_bbo': WATCHLIST_ORDERBOOK_SNAPSHOT.get('monitor8010_bbo') or {},
                'cross_spreads': WATCHLIST_ORDERBOOK_SNAPSHOT.get('cross_spreads') or {},
                'timestamp': now_utc_iso(),
                'error': str(exc),
                'stale_reason': 'exception',
                'monitor8010_error': WATCHLIST_ORDERBOOK_SNAPSHOT.get('monitor8010_error'),
            })
            logger.warning("watchlist orderbook cache refresh failed: %s", exc)
        # 订单簿刷新不应与 watchlist refresh_seconds 强绑定：watchlist 刷新可能被调得很慢，会导致前端订单簿长期为空。
        elapsed = time.time() - start
        sleep_seconds = max(min_interval, target_interval - elapsed)
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
    last_premium_calc = {}
    batch_size = MEMORY_OPTIMIZATION_CONFIG['batch_size']
    premium_refresh_seconds = float(MEMORY_OPTIMIZATION_CONFIG.get('premium_refresh_seconds', 300))
    # 默认全量落库（全币种/全交易所）；如需仅持久化核心+watchlist，可将其设为 'priority'
    persist_mode = str(MEMORY_OPTIMIZATION_CONFIG.get('persist_symbols_mode', 'all')).strip().lower()

    while True:
        try:
            # 获取所有数据（包含所有币种）
            all_data = data_collector.get_all_data()
            # 注意：下面的循环可能超过 60s；因此 observed_at/timestamp 不应只在外层取一次，
            # 否则会导致 1min 聚合出现系统性“少分钟/错分钟”。
            
            # 为每个币种保存历史数据 - 使用动态支持列表，兼容LINEA等新币
            # 落库范围：
            # - all：全币种/全交易所（默认，保证数据完整性）
            # - priority：仅 核心币种 + 当前 watchlist active + 当前前端币种（节省 IO/体积）
            if persist_mode == 'all':
                symbols = list(data_collector.supported_symbols or [])
            else:
                persist_set = set(CURRENT_SUPPORTED_SYMBOLS or [])
                try:
                    persist_set.update(watchlist_manager.active_symbols())
                except Exception:
                    pass
                try:
                    if data_collector.current_symbol:
                        persist_set.add(str(data_collector.current_symbol).upper())
                except Exception:
                    pass
                symbols = sorted(persist_set)

            for idx, symbol in enumerate(symbols):
                observed_at = datetime.now(timezone.utc)
                timestamp = observed_at.isoformat()

                # premium 计算对全市场较重，降频缓存（仍可通过前端查询实时计算）
                premium_data = {}
                last_p = last_premium_calc.get(symbol)
                if (not last_p) or (observed_at - last_p).total_seconds() >= premium_refresh_seconds:
                    premium_data = data_collector.calculate_premium(symbol)
                    last_premium_calc[symbol] = observed_at
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
                                    db.save_price_data_batch(batch_buffer)
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

                # 每 N 个币种让出一次 GIL，避免长时间占用导致接口阻塞
                if idx and idx % 100 == 0:
                    time.sleep(0)
            
            # 定期维护任务（每5分钟执行一次）
            current_time = datetime.now()
            if (current_time - last_maintenance).seconds >= maintenance_interval:
                try:
                    print("开始定期维护任务...")
                    
                    # 写入剩余的批处理数据
                    if batch_buffer:
                        print(f"写入剩余批处理数据: {len(batch_buffer)} 条记录")
                        db.save_price_data_batch(batch_buffer)
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

                    # 每日 04:00 UTC 额外做一次 WAL checkpoint + VACUUM（提前写完缓冲）
                    now_utc = datetime.utcnow()
                    if _should_run_daily_db_maintenance(now_utc):
                        print("开始每日数据库维护: WAL checkpoint + VACUUM")
                        db.checkpoint_and_vacuum()
                        _mark_daily_db_maintenance(now_utc)
                    
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
            _kick_live_trading("watchlist_refresh_loop")
        except Exception as exc:
            logger.warning("watchlist refresh failed: %s", exc)
        time.sleep(watchlist_manager.refresh_seconds)


def _kick_live_trading(reason: str) -> None:
    """Watchlist → Live trading fast path: wake the live trading loop to scan now."""
    try:
        ltm = globals().get("live_trading_manager")
        kick = getattr(ltm, "kick", None) if ltm is not None else None
        if callable(kick):
            kick(reason=str(reason or "watchlist"))
    except Exception as exc:
        logging.getLogger("live_trading").warning("live trading kick failed: %s", exc)


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
        # 检查 funding 列名，兼容旧版 SQLite（1min 表常见为 funding_rate_avg）
        cur.execute("PRAGMA table_info(price_data_1min)")
        cols = [row[1] for row in cur.fetchall()]
        funding_col = None
        if "funding_rate" in cols:
            funding_col = "funding_rate"
        elif "funding_rate_avg" in cols:
            funding_col = "funding_rate_avg"
        select_sql = """
            SELECT symbol, exchange, spot_price_close, futures_price_close, {funding}, timestamp
            FROM price_data_1min
            WHERE timestamp >= datetime(?, ?)
        """.format(
            funding=(f"{funding_col} AS funding_rate" if funding_col else "NULL AS funding_rate")
        )
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
                         exchange_order=EXCHANGE_DISPLAY_ORDER,
                         active_page='aggregated',
                         current_symbol=data_collector.current_symbol)

@app.route('/exchanges')
def exchanges_view():
    """按交易所展示页面"""
    return render_template('simple_index.html', 
                         symbols=data_collector.supported_symbols,
                         current_symbol=data_collector.current_symbol,
                         exchange_order=EXCHANGE_DISPLAY_ORDER,
                         active_page='exchanges')

@app.route('/aggregated')
def aggregated_index():
    """聚合页面（兼容路由）- 使用增强版聚合视图"""
    return render_template('enhanced_aggregated.html', 
                         symbols=data_collector.supported_symbols,
                         exchange_order=EXCHANGE_DISPLAY_ORDER,
                         active_page='aggregated',
                         current_symbol=data_collector.current_symbol)

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
                         chart_interval_label_map=CHART_INTERVAL_LABEL_MAP,
                         active_page='charts')


@app.route('/watchlist')
def watchlist_view():
    """Binance 资金费率驱动的关注列表页面"""
    return render_template('watchlist.html', active_page='watchlist')


@app.route('/watchlist/charts')
def watchlist_charts():
    """关注列表的独立图表页面"""
    return render_template('watchlist_charts.html', active_page='watchlist_charts')


@app.route('/live_trading')
def live_trading_view():
    """实盘交易（Type B）状态页：信号、订单、价差监听与平仓状态。"""
    return render_template('live_trading.html', active_page='live_trading')


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

    # On cold start, background threads may not have finished the first refresh yet.
    # If the watchlist is empty, do a best-effort one-shot bootstrap so UI isn't blank.
    try:
        summary0 = payload.get("summary") if isinstance(payload, dict) else None
        total0 = int((summary0 or {}).get("total_tracked") or 0)
        if total0 <= 0:
            global WATCHLIST_BOOTSTRAP_LAST_TS
            now_s = time.time()
            if now_s - float(WATCHLIST_BOOTSTRAP_LAST_TS) >= float(WATCHLIST_BOOTSTRAP_MIN_INTERVAL_SEC):
                if WATCHLIST_BOOTSTRAP_LOCK.acquire(blocking=False):
                    try:
                        WATCHLIST_BOOTSTRAP_LAST_TS = now_s
                        watchlist_manager.preload_from_database(db.db_path)
                        all_data = data_collector.get_all_data()
                        exchange_symbols = getattr(data_collector, "exchange_symbols", {}) or {}
                        watchlist_manager.refresh(all_data, exchange_symbols, WATCHLIST_ORDERBOOK_SNAPSHOT)
                        payload = watchlist_manager.snapshot()
                    finally:
                        WATCHLIST_BOOTSTRAP_LOCK.release()
    except Exception as exc:
        logging.getLogger("watchlist").warning("watchlist bootstrap failed: %s", exc)
    # Expose live trading thresholds so watchlist UI can show “Type B/C 入场信号”状态。
    try:
        payload['live_trading_thresholds'] = {
            'horizon_min': int(LIVE_TRADING_CONFIG.get('horizon_min', 240)),
            'pnl_threshold': float(LIVE_TRADING_CONFIG.get('pnl_threshold', 0.0085)),
            'win_prob_threshold': float(LIVE_TRADING_CONFIG.get('win_prob_threshold', 0.85)),
            'v2_enabled': bool(LIVE_TRADING_CONFIG.get('v2_enabled', True)),
            'v2_pnl_threshold_240': float(LIVE_TRADING_CONFIG.get('v2_pnl_threshold_240', 0.0065)),
            'v2_win_prob_threshold_240': float(LIVE_TRADING_CONFIG.get('v2_win_prob_threshold_240', 0.72)),
            'v2_pnl_threshold_1440': float(LIVE_TRADING_CONFIG.get('v2_pnl_threshold_1440', 0.0055)),
            'v2_win_prob_threshold_1440': float(LIVE_TRADING_CONFIG.get('v2_win_prob_threshold_1440', 0.75)),
        }
    except Exception:
        payload['live_trading_thresholds'] = None
    try:
        entries = payload.get('entries', [])
        active_entries = [e for e in entries if e.get('status') == 'active']
        all_data = data_collector.get_all_data()
        # 使用后台缓存的订单簿/跨所差价，避免请求路径阻塞
        ob_snapshot = WATCHLIST_ORDERBOOK_SNAPSHOT.copy()
        payload['orderbook'] = ob_snapshot.get('orderbook') or {}
        payload['monitor8010_bbo'] = ob_snapshot.get('monitor8010_bbo') or {}
        payload['monitor8010_error'] = ob_snapshot.get('monitor8010_error')
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

        # Attach the latest v2 inference snapshot for active Type B/C symbols (from PG watch_signal_event).
        # This keeps watchlist UI lightweight while still exposing v2 signals/meta.
        payload["event_v2"] = {}
        if psycopg is not None:
            try:
                v2_syms = sorted(
                    {
                        str(e.get("symbol") or "").upper()
                        for e in active_entries
                        if str(e.get("entry_type") or "").upper() in ("B", "C") and e.get("symbol")
                    }
                )

                def _summarize_factors_v2_meta(meta: Any) -> Optional[Dict[str, Any]]:
                    if not isinstance(meta, dict):
                        return None
                    out: Dict[str, Any] = {}
                    for k in ("ok", "ts", "reason", "lookback_min", "series_start", "series_end", "funding_edge_regime_label"):
                        if meta.get(k) is not None:
                            out[k] = meta.get(k)
                    for side in ("short_quality", "long_quality"):
                        q = meta.get(side)
                        if isinstance(q, dict):
                            out[side] = {
                                "missing_rate": q.get("missing_rate"),
                                "missing_points": q.get("missing_points"),
                                "total_points": q.get("total_points"),
                                "ffill_points": q.get("ffill_points"),
                                "max_gap_sec": q.get("max_gap_sec"),
                            }
                    # Keep leg identity (helpful for interpreting missing/series mapping).
                    for leg_k in ("short_leg", "long_leg"):
                        leg = meta.get(leg_k)
                        if isinstance(leg, dict):
                            out[leg_k] = {
                                "exchange": leg.get("exchange"),
                                "symbol": leg.get("symbol"),
                                "kind": leg.get("kind"),
                            }
                    return out or None

                def _summarize_orderbook_validation(ob: Any) -> Optional[Dict[str, Any]]:
                    if not isinstance(ob, dict):
                        return None
                    out: Dict[str, Any] = {}
                    for k in ("ok", "reason", "ts", "notional_usdt", "market_type", "topk_exchanges", "picked_high_low"):
                        if ob.get(k) is not None:
                            out[k] = ob.get(k)
                    return out or None

                def _summarize_factors_v2(factors: Any) -> Optional[Dict[str, Any]]:
                    if isinstance(factors, (str, bytes, bytearray)):
                        try:
                            factors = json.loads(factors)
                        except Exception:
                            return None
                    if not isinstance(factors, dict):
                        return None

                    def _clean_number(value: Any) -> Optional[float]:
                        if value is None:
                            return None
                        try:
                            f = float(value)
                        except Exception:
                            return None
                        if f != f or f == float("inf") or f == float("-inf"):
                            return None
                        return f

                    # Return the full v2 factor set (PG stored JSONB can include 30~100+ keys).
                    out: Dict[str, Any] = {}
                    for k in sorted(factors.keys(), key=lambda x: str(x)):
                        out[str(k)] = _clean_number(factors.get(k))
                    return out or None

                if v2_syms:
                    conn_kwargs: Dict[str, Any] = {"autocommit": True}
                    if dict_row:
                        conn_kwargs["row_factory"] = dict_row
                    with psycopg.connect(WATCHLIST_PG_CONFIG["dsn"], **conn_kwargs) as conn:
                        rows = conn.execute(
                            """
                            SELECT DISTINCT ON (e.symbol, btrim(e.signal_type))
                              e.symbol,
                              btrim(e.signal_type) AS signal_type,
                              e.id AS event_id,
                              e.exchange,
                              e.start_ts,
                              e.end_ts,
                              e.status,
                              COALESCE(
                                  (e.features_agg #>> '{meta_last,orderbook_validation,ts}')::timestamptz,
                                  (e.features_agg #>> '{meta_last,pred_v2_meta,ts}')::timestamptz,
                                  (e.features_agg #>> '{meta_last,factors_v2_meta,ts}')::timestamptz,
                                  e.start_ts
                              ) AS decision_ts,
                              (e.features_agg #> '{meta_last,factors_v2}') AS factors_v2,
                              (e.features_agg #> '{meta_last,pred_v2}') AS pred_v2,
                              (e.features_agg #> '{meta_last,pred_v2_meta}') AS pred_v2_meta,
                              (e.features_agg #> '{meta_last,factors_v2_meta}') AS factors_v2_meta,
                              (e.features_agg #> '{meta_last,orderbook_validation}') AS orderbook_validation
                            FROM watchlist.watch_signal_event e
                            WHERE e.symbol = ANY(%s)
                              AND e.signal_type IN ('B','C')
                            ORDER BY e.symbol, btrim(e.signal_type), e.start_ts DESC, e.id DESC;
                            """,
                            (v2_syms,),
                        ).fetchall()
                    for r in rows or []:
                        sym = str(r.get("symbol") or "").upper()
                        st = str(r.get("signal_type") or "").upper()
                        if not sym or st not in ("B", "C"):
                            continue
                        start_ts = r.get("start_ts")
                        end_ts = r.get("end_ts")
                        decision_ts = r.get("decision_ts")
                        payload["event_v2"].setdefault(sym, {})[st] = {
                            "event_id": int(r.get("event_id") or 0),
                            "exchange": r.get("exchange"),
                            "status": r.get("status"),
                            "start_ts": start_ts.isoformat() if hasattr(start_ts, "isoformat") else start_ts,
                            "end_ts": end_ts.isoformat() if hasattr(end_ts, "isoformat") else end_ts,
                            "decision_ts": decision_ts.isoformat() if hasattr(decision_ts, "isoformat") else decision_ts,
                            "factors_v2": _summarize_factors_v2(r.get("factors_v2")),
                            "pred_v2": r.get("pred_v2") if isinstance(r.get("pred_v2"), dict) else None,
                            "pred_v2_meta": r.get("pred_v2_meta") if isinstance(r.get("pred_v2_meta"), dict) else None,
                            "factors_v2_meta": _summarize_factors_v2_meta(r.get("factors_v2_meta")),
                            "orderbook_validation": _summarize_orderbook_validation(r.get("orderbook_validation")),
                        }
            except Exception as exc:
                payload["event_v2_error"] = str(exc)

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
        _kick_live_trading("api_watchlist_refresh")
        return jsonify({'message': 'ok', 'timestamp': now_utc_iso(), 'summary': watchlist_manager.snapshot().get('summary')})
    except Exception as exc:
        return jsonify({'error': str(exc), 'timestamp': now_utc_iso()}), 500


def _monitor_8010_base_url() -> Optional[str]:
    if not bool(MONITOR_8010_CONFIG.get("enabled", True)):
        return None
    url = str(MONITOR_8010_CONFIG.get("base_url") or "").strip()
    if not url:
        return None
    return url.rstrip("/")


def _monitor_8010_timeout() -> float:
    try:
        return float(MONITOR_8010_CONFIG.get("timeout_seconds") or 3.0)
    except Exception:
        return 3.0


def _proxy_get_8010(path: str) -> Any:
    base = _monitor_8010_base_url()
    if not base:
        return {"ok": False, "error": "MONITOR_8010 disabled"}
    resp = requests.get(f"{base}{path}", timeout=_monitor_8010_timeout())
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        return {"text": resp.text[:4000]}


def _proxy_post_8010(path: str, payload: Dict[str, Any]) -> Any:
    base = _monitor_8010_base_url()
    if not base:
        return {"ok": False, "error": "MONITOR_8010 disabled"}
    resp = requests.post(f"{base}{path}", json=payload, timeout=_monitor_8010_timeout())
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        return {"text": resp.text[:4000]}


@app.route('/api/monitor8010/health')
def api_monitor8010_health():
    try:
        return jsonify(_proxy_get_8010("/health"))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@app.route('/api/monitor8010/watchlist')
def api_monitor8010_watchlist():
    try:
        return jsonify(_proxy_get_8010("/watchlist"))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@app.route('/api/monitor8010/watchlist/add', methods=['POST'])
def api_monitor8010_watchlist_add():
    try:
        body = request.get_json(silent=True) or {}
        return jsonify(_proxy_post_8010("/watchlist/add", body))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@app.route('/api/monitor8010/watchlist/touch', methods=['POST'])
def api_monitor8010_watchlist_touch():
    try:
        body = request.get_json(silent=True) or {}
        return jsonify(_proxy_post_8010("/watchlist/touch", body))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@app.route('/api/monitor8010/watchlist/remove', methods=['POST'])
def api_monitor8010_watchlist_remove():
    try:
        body = request.get_json(silent=True) or {}
        return jsonify(_proxy_post_8010("/watchlist/remove", body))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@app.route('/watchlist/db')
def watchlist_db_view():
    """
    简易的 watchlist PG 浏览页面，随用随查。
    - 支持 raw/event/outcome 三张核心表。
    - 默认每页 10 条，可通过 ?page=2&limit=20 翻页。
    """
    if psycopg is None:
        return render_template('watchlist_db_view.html', error='psycopg 未安装，无法连接 PG', active_page='watchlist_db')

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

    def _stringify(val: Any) -> str:
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
                    rows.append({k: _stringify(v) for k, v in r.items()})
            else:
                cols = [d.name for d in cur.description] if cur.description else []
                for r in fetched:
                    rows.append({col: _stringify(val) for col, val in zip(cols, r)})
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
        active_page='watchlist_db',
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


@lru_cache(maxsize=8)
def _load_json_file_cached(path: str) -> Optional[Dict[str, Any]]:
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


@app.route('/api/watchlist/v2_model_info')
def get_watchlist_v2_model_info():
    """
    v2 模型结构说明（用于 watchlist 页面展示）：
    - Ridge: 预测 pnl_hat
    - Logistic: 预测 win_prob
    """
    try:
        import config as _cfg  # local import

        cfg = getattr(_cfg, "WATCHLIST_V2_PRED_CONFIG", {}) or {}
        out: Dict[str, Any] = {"timestamp": now_utc_iso(), "enabled": bool(cfg.get("enabled"))}
        horizons = (("240", cfg.get("model_path_240")), ("1440", cfg.get("model_path_1440")))

        def _top(features: List[str], coefs: List[Any], top_n: int = 12) -> List[Dict[str, Any]]:
            rows: List[Dict[str, Any]] = []
            for name, coef in zip(features, coefs):
                try:
                    c = float(coef)
                except Exception:
                    continue
                rows.append({"feature": str(name), "coef": c, "abs": abs(c)})
            rows.sort(key=lambda r: float(r.get("abs") or 0.0), reverse=True)
            for r in rows:
                r.pop("abs", None)
            return rows[:top_n]

        models: Dict[str, Any] = {}
        for h, path_raw in horizons:
            path = str(path_raw or "").strip()
            payload = _load_json_file_cached(path) if path else None
            if not payload:
                models[h] = {"ok": False, "path": path, "error": "model_not_loaded"}
                continue
            features = payload.get("features") or []
            ridge = payload.get("ridge") or {}
            logreg = payload.get("logistic") or {}
            models[h] = {
                "ok": True,
                "path": path,
                "feature_count": len(features) if isinstance(features, list) else 0,
                "ridge": {
                    "intercept": ridge.get("intercept"),
                    "top_coef": _top(list(features), list(ridge.get("coef") or [])),
                },
                "logistic": {
                    "intercept": logreg.get("intercept"),
                    "top_coef": _top(list(features), list(logreg.get("coef") or [])),
                },
            }
        out["models"] = models
        out["max_missing_ratio"] = cfg.get("max_missing_ratio")
        return jsonify(out), 200
    except Exception as exc:
        return jsonify({"timestamp": now_utc_iso(), "error": str(exc)}), 500

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
    request_start = time.time()
    chart_logger = logging.getLogger('charts')
    # 使用动态支持列表
    if symbol not in data_collector.supported_symbols:
        return jsonify({'error': 'Unsupported symbol'}), 400
    
    # 获取查询参数
    hours = request.args.get('hours', 6, type=int)  # 默认6小时用于图表
    exchange = request.args.get('exchange', None)
    interval = request.args.get('interval', DEFAULT_CHART_INTERVAL)
    interval = (interval or DEFAULT_CHART_INTERVAL).lower()
    
    # 限制查询范围，避免过大窗口拖慢数据库
    hours = min(hours, CHART_MAX_HOURS)
    
    supported_intervals = set(CHART_INTERVAL_MINUTES.keys()) | {'raw'}
    if interval not in supported_intervals:
        return jsonify({'error': 'Unsupported interval'}), 400
    
    db_interval = '1min' if interval in CHART_INTERVAL_MINUTES else interval
    
    cache_key = (symbol, exchange or '', interval, hours)
    cached_entry = _get_chart_cache_entry(cache_key)

    # 同一 (symbol, interval, hours) 请求在 60s 内直接返回缓存，避免重复重算/重查
    if cached_entry:
        payload = dict(cached_entry['payload'])
        payload['from_cache'] = True
        payload['cache_age_seconds'] = round(time.time() - cached_entry['timestamp'], 1)
        # 图表序列数据可能很大，precision_jsonify 会逐项格式化浮点数导致极慢；此处直接返回 JSON 数字更合适。
        return jsonify(payload)

    raw_data = None
    last_error = None
    retry_attempts = 3
    backoff_seconds = 0.4
    db_seconds = 0.0
    resample_seconds = 0.0
    build_seconds = 0.0

    # 图表默认 7 天窗口；如果走 PriceDatabase.get_historical_data 会构造大量 dict（GIL 重）导致偶发卡住。
    # 这里直接用 sqlite3 + SQL 侧分桶聚合，把数据量压到“每交易所每桶一行”，显著降低 CPU/GIL 占用。
    interval_minutes = CHART_INTERVAL_MINUTES.get(interval) if interval in CHART_INTERVAL_MINUTES else None

    def _normalize_ts(ts_value: Any) -> Optional[str]:
        if ts_value is None:
            return None
        if isinstance(ts_value, datetime):
            return ts_value.strftime("%Y-%m-%d %H:%M:%S")
        if not isinstance(ts_value, str):
            return None
        text = ts_value.strip()
        if not text:
            return None
        text = text.replace("T", " ")
        if text.endswith("Z"):
            text = text[:-1]
        if "." in text:
            text = text.split(".", 1)[0]
        if len(text) >= 19:
            return text[:19]
        return text

    for attempt in range(retry_attempts):
        try:
            t0 = time.time()
            conn = sqlite3.connect(db.db_path, timeout=2.0)
            cursor = conn.cursor()
            # 用 DB 内最新时间作为窗口终点，避免 sqlite datetime('now') 与本地写入/时区基准不一致导致取不到数据。
            if exchange:
                cursor.execute(
                    "SELECT MAX(timestamp) FROM price_data_1min WHERE symbol=? AND exchange=?",
                    (symbol, exchange),
                )
            else:
                cursor.execute(
                    "SELECT MAX(timestamp) FROM price_data_1min WHERE symbol=?",
                    (symbol,),
                )
            end_ts = cursor.fetchone()[0]
            if not end_ts:
                conn.close()
                rows = []
                db_seconds += time.time() - t0
                break
            end_dt = _parse_db_timestamp(end_ts) or datetime.utcnow()
            start_dt = end_dt - timedelta(hours=hours)
            start_ts = start_dt.strftime("%Y-%m-%d %H:%M:%S")
            cols_sql = (
                "timestamp, exchange, "
                "spot_price_close, futures_price_close, "
                "funding_rate_avg, premium_percent_avg, "
                "funding_interval_hours, next_funding_time"
            )
            if interval_minutes and interval_minutes > 1:
                # 直接在 SQLite 内做分桶，避免拉取 50k 行到 Python 再处理（大币种会非常慢）。
                bucket_expr = (
                    "strftime('%Y-%m-%d %H:', timestamp) || "
                    "printf('%02d', (CAST(strftime('%M', timestamp) AS integer) / :bucket) * :bucket) || ':00'"
                )
                # 使用命名参数，避免 tuple 参数顺序错误导致“rows=0”但无异常的隐性 bug。
                where_exchange_sql = "AND exchange = :exchange" if exchange else ""
                params = {"bucket": int(interval_minutes), "symbol": symbol}
                if exchange:
                    params["exchange"] = exchange
                params["start_ts"] = start_ts
                cursor.execute(
                    f"""
                    WITH filtered AS (
                        SELECT
                            timestamp,
                            exchange,
                            spot_price_close,
                            futures_price_close,
                            funding_rate_avg,
                            premium_percent_avg,
                            funding_interval_hours,
                            next_funding_time,
                            ({bucket_expr}) AS bucket_ts
                        FROM price_data_1min
                        WHERE symbol = :symbol
                          {where_exchange_sql}
                          AND timestamp >= :start_ts
                    ),
                    ranked AS (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (PARTITION BY exchange, bucket_ts ORDER BY timestamp DESC) AS rn,
                            AVG(COALESCE(funding_rate_avg, 0.0)) OVER (PARTITION BY exchange, bucket_ts) AS funding_rate_bucket,
                            AVG(COALESCE(premium_percent_avg, 0.0)) OVER (PARTITION BY exchange, bucket_ts) AS premium_bucket
                        FROM filtered
                    )
                    SELECT
                        bucket_ts,
                        exchange,
                        spot_price_close,
                        futures_price_close,
                        funding_rate_bucket,
                        premium_bucket,
                        funding_interval_hours,
                        next_funding_time
                    FROM ranked
                    WHERE rn = 1
                    ORDER BY bucket_ts ASC;
                    """,
                    params,
                )
                rows = cursor.fetchall()
            else:
                # interval=1min 直接拉取 close 序列
                if exchange:
                    cursor.execute(
                        f"""
                        SELECT {cols_sql}
                          FROM price_data_1min
                         WHERE symbol = ?
                           AND exchange = ?
                           AND timestamp >= ?
                         ORDER BY timestamp DESC
                         LIMIT 50000;
                        """,
                        (symbol, exchange, start_ts),
                    )
                else:
                    cursor.execute(
                        f"""
                        SELECT {cols_sql}
                          FROM price_data_1min
                         WHERE symbol = ?
                           AND timestamp >= ?
                         ORDER BY timestamp DESC
                         LIMIT 50000;
                        """,
                        (symbol, start_ts),
                    )
                rows = cursor.fetchall()
            conn.close()
            fetch_dt = time.time() - t0
            db_seconds += fetch_dt
            if hours >= 24 and fetch_dt > 2.0:
                chart_logger.warning(
                    "chart db slow symbol=%s hours=%s interval=%s rows=%s dt=%.3fs attempt=%s",
                    symbol,
                    hours,
                    interval,
                    len(rows),
                    fetch_dt,
                    attempt + 1,
                )

            if not rows:
                raw_data = []
                break

            # interval=1min: 直接返回每分钟 close；interval>1min: 分桶聚合（close=桶内最新，rate/premium=桶内均值）
            if not interval_minutes or interval_minutes <= 1:
                raw_data = []
                for (ts, ex, spot_close, fut_close, fund, prem, fund_int, next_ft) in rows:
                    ts_text = _normalize_ts(ts)
                    if not ts_text:
                        continue
                    raw_data.append({
                        "timestamp": ts_text,
                        "exchange": ex,
                        "spot_price_close": spot_close,
                        "futures_price_close": fut_close,
                        "funding_rate_avg": fund,
                        "premium_percent_avg": prem,
                        "funding_interval_hours": fund_int,
                        "next_funding_time": next_ft,
                    })
                break

            t1 = time.time()
            raw_data = []
            for (bucket_ts, ex, spot_close, fut_close, fund_bucket, prem_bucket, fund_int, next_ft) in rows:
                ts_text = _normalize_ts(bucket_ts)
                if not ts_text:
                    continue
                raw_data.append({
                    "timestamp": ts_text,
                    "exchange": ex,
                    "spot_price_close": spot_close,
                    "futures_price_close": fut_close,
                    "funding_rate_avg": fund_bucket,
                    "premium_percent_avg": prem_bucket,
                    "funding_interval_hours": fund_int,
                    "next_funding_time": next_ft,
                })
            resample_seconds += time.time() - t1
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

        t_build = time.time()
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
        build_seconds += time.time() - t_build

        response_payload = {
            'symbol': symbol,
            'hours': hours,
            'interval': interval,
            'chart_data': chart_data,
            'timestamp': now_utc_iso()
        }
        total_points = sum(len(v['labels']) for v in chart_data.values())
        if total_points <= 0:
            # 启动初期/聚合尚未就绪时避免缓存空结果（否则会把空数据缓存 15 分钟导致前端一直空白）。
            return jsonify({'error': 'no chart data available', 'symbol': symbol, 'hours': hours, 'interval': interval}), 404

        _set_chart_cache_entry(cache_key, response_payload)
        chart_logger.warning(
            "chart response built symbol=%s hours=%s interval=%s rows=%s elapsed=%.3fs db=%.3fs resample=%.3fs build=%.3fs from_cache=%s",
            symbol,
            hours,
            interval,
            total_points,
            time.time() - request_start,
            db_seconds,
            resample_seconds,
            build_seconds,
            False,
        )
        return jsonify(response_payload)

    except Exception as exc:
        chart_logger.warning(
            "chart response failed symbol=%s hours=%s interval=%s elapsed=%.3fs error=%s",
            symbol,
            hours,
            interval,
            time.time() - request_start,
            exc,
        )
        return jsonify({'error': str(exc)}), 500


@app.route('/api/live_trading/overview')
def live_trading_overview():
    """
    Minimal live trading status endpoint (Phase 1).
    Reads PG tables:
      - watchlist.live_trade_signal
      - watchlist.live_trade_order
      - watchlist.live_trade_error
    """
    if psycopg is None:
        return jsonify({'error': 'psycopg 未安装，无法连接 PG', 'timestamp': now_utc_iso()}), 500

    limit = min(max(int(request.args.get('limit', 50) or 50), 1), 200)
    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    try:
        conn_kwargs: Dict[str, Any] = {"autocommit": True}
        if dict_row:
            conn_kwargs["row_factory"] = dict_row
        with psycopg.connect(WATCHLIST_PG_CONFIG['dsn'], **conn_kwargs) as conn:
            rows = conn.execute(
                """
                SELECT *
                  FROM watchlist.live_trade_signal
                 ORDER BY created_at DESC
                 LIMIT %s;
                """,
                (limit,),
            ).fetchall()
            errors = conn.execute(
                """
                SELECT *
                  FROM watchlist.live_trade_error
                 ORDER BY ts DESC
                 LIMIT %s;
                """,
                (min(limit, 200),),
            ).fetchall()
    except Exception as exc:
        return jsonify({'error': str(exc), 'timestamp': now_utc_iso()}), 500

    def _jsonable(val: Any) -> Any:
        if isinstance(val, datetime):
            return val.astimezone(timezone.utc).isoformat()
        return val

    signals_out = []
    for r in rows or []:
        if isinstance(r, dict):
            signals_out.append({k: _jsonable(v) for k, v in r.items()})
        else:
            signals_out.append(r)

    errors_out = []
    for r in errors or []:
        if isinstance(r, dict):
            errors_out.append({k: _jsonable(v) for k, v in r.items()})
        else:
            errors_out.append(r)

    return jsonify(
        {
            'timestamp': now_utc_iso(),
            'live_trading_enabled': bool(LIVE_TRADING_CONFIG.get('enabled')),
            'live_trading_config': {
                'horizon_min': int(LIVE_TRADING_CONFIG.get('horizon_min', 240)),
                'pnl_threshold': float(LIVE_TRADING_CONFIG.get('pnl_threshold', 0.0085)),
                'win_prob_threshold': float(LIVE_TRADING_CONFIG.get('win_prob_threshold', 0.85)),
                'v2_enabled': bool(LIVE_TRADING_CONFIG.get('v2_enabled', True)),
                'v2_pnl_threshold_240': float(LIVE_TRADING_CONFIG.get('v2_pnl_threshold_240', 0.0065)),
                'v2_win_prob_threshold_240': float(LIVE_TRADING_CONFIG.get('v2_win_prob_threshold_240', 0.72)),
                'v2_pnl_threshold_1440': float(LIVE_TRADING_CONFIG.get('v2_pnl_threshold_1440', 0.0055)),
                'v2_win_prob_threshold_1440': float(LIVE_TRADING_CONFIG.get('v2_win_prob_threshold_1440', 0.75)),
                'per_leg_notional_usdt': float(LIVE_TRADING_CONFIG.get('per_leg_notional_usdt', 50.0)),
                'event_lookback_minutes': int(LIVE_TRADING_CONFIG.get('event_lookback_minutes', 30)),
                'max_concurrent_trades': int(LIVE_TRADING_CONFIG.get('max_concurrent_trades', 10)),
                'allowed_exchanges': str(LIVE_TRADING_CONFIG.get('allowed_exchanges') or ''),
                'scan_interval_seconds': float(LIVE_TRADING_CONFIG.get('scan_interval_seconds', 20.0)),
                'monitor_interval_seconds': float(LIVE_TRADING_CONFIG.get('monitor_interval_seconds', 60.0)),
                'take_profit_ratio': float(LIVE_TRADING_CONFIG.get('take_profit_ratio', 0.7)),
                'max_hold_days': int(LIVE_TRADING_CONFIG.get('max_hold_days', 7)),
            },
            'signals': signals_out,
            'errors': errors_out,
        }
    )


@app.route('/api/live_trading/candidates')
def live_trading_candidates():
    """Debug: list top watchlist candidates + orderbook revalidation (no orders placed)."""
    try:
        if not live_trading_manager or not getattr(live_trading_manager, "config", None):
            return jsonify({'candidates': [], 'error': 'live trading manager not initialized', 'timestamp': now_utc_iso()}), 200

        limit_symbols = max(1, min(int(request.args.get('limit_symbols', 10) or 10), 50))
        per_symbol = max(1, min(int(request.args.get('per_symbol', 1) or 1), 5))

        out: List[Dict[str, Any]] = []
        with live_trading_manager._conn() as conn:  # type: ignore[attr-defined]
            candidates = live_trading_manager._fetch_candidates(conn)  # type: ignore[attr-defined]
            by_symbol: Dict[str, List[Dict[str, Any]]] = {}
            symbol_order: List[str] = []
            for row in candidates or []:
                sym = str(row.get('symbol') or '').upper()
                if not sym:
                    continue
                if sym not in by_symbol:
                    by_symbol[sym] = []
                    symbol_order.append(sym)
                if len(by_symbol[sym]) < per_symbol:
                    by_symbol[sym].append(row)

            for sym in symbol_order[:limit_symbols]:
                rows = by_symbol.get(sym) or []
                for event in rows:
                    event_id = int(event.get('id') or 0)
                    high_low = live_trading_manager._pick_high_low(  # type: ignore[attr-defined]
                        symbol=sym,
                        trigger_details=event.get('trigger_details'),
                        leg_a_exchange=event.get('leg_a_exchange'),
                        leg_b_exchange=event.get('leg_b_exchange'),
                        leg_a_price_last=event.get('leg_a_price_last'),
                        leg_b_price_last=event.get('leg_b_price_last'),
                    )
                    if not high_low:
                        out.append({'symbol': sym, 'event_id': event_id, 'ok': False, 'reason': 'missing_pair_prices'})
                        continue
                    high_ex, low_ex = high_low
                    if not (
                        live_trading_manager._supported_exchange(high_ex)  # type: ignore[attr-defined]
                        and live_trading_manager._supported_exchange(low_ex)  # type: ignore[attr-defined]
                    ):
                        out.append(
                            {
                                'symbol': sym,
                                'event_id': event_id,
                                'ok': False,
                                'reason': 'exchange_not_allowed',
                                'high_ex': high_ex,
                                'low_ex': low_ex,
                            }
                        )
                        continue

                    factors = event.get('factors') or {}
                    if not isinstance(factors, dict):
                        out.append(
                            {
                                'symbol': sym,
                                'event_id': event_id,
                                'ok': False,
                                'reason': 'missing_factors',
                                'high_ex': high_ex,
                                'low_ex': low_ex,
                            }
                        )
                        continue

                    reval = live_trading_manager._revalidate_with_orderbook(  # type: ignore[attr-defined]
                        symbol=sym,
                        high_exchange=high_ex,
                        low_exchange=low_ex,
                        base_factors=factors,
                    )
                    if isinstance(reval, dict) and not reval.get('ok') and not reval.get('reason') and reval.get('pnl_hat') is not None:
                        reval = dict(reval)
                        reval['reason'] = 'below_threshold'
                    out.append(
                        {
                            'symbol': sym,
                            'event_id': event_id,
                            'start_ts': str(event.get('start_ts') or ''),
                            'high_ex': high_ex,
                            'low_ex': low_ex,
                            'ok': bool(isinstance(reval, dict) and reval.get('ok')),
                            'pnl_hat_ob': reval.get('pnl_hat') if isinstance(reval, dict) else None,
                            'win_prob_ob': reval.get('win_prob') if isinstance(reval, dict) else None,
                            'reason': reval.get('reason') if isinstance(reval, dict) else 'revalidation_failed',
                        }
                    )

        out.sort(key=lambda x: (x.get('ok') is True, x.get('pnl_hat_ob') or -1e9, x.get('win_prob_ob') or -1e9), reverse=True)
        return jsonify(
            {
                'timestamp': now_utc_iso(),
                'live_trading_config': {
                    'horizon_min': int(LIVE_TRADING_CONFIG.get('horizon_min', 240)),
                    'pnl_threshold': float(LIVE_TRADING_CONFIG.get('pnl_threshold', 0.013)),
                    'win_prob_threshold': float(LIVE_TRADING_CONFIG.get('win_prob_threshold', 0.94)),
                    'per_leg_notional_usdt': float(LIVE_TRADING_CONFIG.get('per_leg_notional_usdt', 50.0)),
                    'event_lookback_minutes': int(LIVE_TRADING_CONFIG.get('event_lookback_minutes', 30)),
                    'allowed_exchanges': str(LIVE_TRADING_CONFIG.get('allowed_exchanges') or ''),
                },
                'candidates': out[:200],
            }
        )
    except Exception as exc:
        return jsonify({'error': str(exc), 'timestamp': now_utc_iso()}), 500


@app.route('/api/live_trading/signals')
def live_trading_signals():
    """Query live trading signals with lightweight filters."""
    if psycopg is None:
        return jsonify({'error': 'psycopg 未安装，无法连接 PG', 'timestamp': now_utc_iso()}), 500

    limit = min(max(int(request.args.get('limit', 200) or 200), 1), 500)
    signal_id = request.args.get('signal_id')
    event_id = request.args.get('event_id')
    # Multi-status filter: accept repeated ?status=... or comma-separated list.
    raw_status_list = request.args.getlist('status') or []
    status_list: List[str] = []
    for raw in raw_status_list:
        if raw is None:
            continue
        for part in str(raw).split(','):
            v = part.strip()
            if v:
                status_list.append(v)
    symbol = request.args.get('symbol')
    signal_type = request.args.get('signal_type')
    exchange = request.args.get('exchange')

    def _float_arg(name: str) -> Optional[float]:
        raw = request.args.get(name)
        if raw is None or str(raw).strip() == "":
            return None
        try:
            return float(raw)
        except Exception:
            raise ValueError(f"invalid {name}")

    def _dt_arg(name: str) -> Optional[datetime]:
        raw = request.args.get(name)
        if raw is None or str(raw).strip() == "":
            return None
        try:
            # Accept ISO8601 (with or without timezone); assume UTC if naive.
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            raise ValueError(f"invalid {name}")

    try:
        pnl_hat_ob_min = _float_arg("pnl_hat_ob_min")
        pnl_hat_ob_max = _float_arg("pnl_hat_ob_max")
        win_prob_ob_min = _float_arg("win_prob_ob_min")
        win_prob_ob_max = _float_arg("win_prob_ob_max")
        realized_pnl_min = _float_arg("realized_pnl_min")
        realized_pnl_max = _float_arg("realized_pnl_max")
        funding_pnl_min = _float_arg("funding_pnl_min")
        funding_pnl_max = _float_arg("funding_pnl_max")
        created_after = _dt_arg("created_after")
        created_before = _dt_arg("created_before")
    except ValueError as exc:
        return jsonify({'error': str(exc), 'timestamp': now_utc_iso()}), 400

    include_prices = str(request.args.get('include_prices', '1') or '1').lower() not in ('0', 'false', 'no')
    include_funding = str(request.args.get('include_funding', '1') or '1').lower() not in ('0', 'false', 'no')
    # realized_pnl 需要 open/close 的成交均价/数量；若用户请求按 realized_pnl 筛选，强制开启 include_prices
    if realized_pnl_min is not None or realized_pnl_max is not None:
        include_prices = True

    where = []
    params: List[Any] = []
    if signal_id:
        where.append("id=%s")
        try:
            params.append(int(signal_id))
        except Exception:
            return jsonify({'error': 'invalid signal_id', 'timestamp': now_utc_iso()}), 400
    if event_id:
        where.append("event_id=%s")
        try:
            params.append(int(event_id))
        except Exception:
            return jsonify({'error': 'invalid event_id', 'timestamp': now_utc_iso()}), 400
    if status_list:
        if len(status_list) == 1:
            where.append("status=%s")
            params.append(str(status_list[0]))
        else:
            where.append("status = ANY(%s)")
            params.append(status_list)
    if signal_type:
        where.append("signal_type=%s")
        params.append(str(signal_type).strip().upper()[:1])
    if symbol:
        where.append("upper(symbol)=upper(%s)")
        params.append(str(symbol))
    if exchange:
        where.append("(upper(leg_long_exchange)=upper(%s) OR upper(leg_short_exchange)=upper(%s))")
        params.append(str(exchange))
        params.append(str(exchange))
    if pnl_hat_ob_min is not None:
        where.append("pnl_hat_ob >= %s")
        params.append(float(pnl_hat_ob_min))
    if pnl_hat_ob_max is not None:
        where.append("pnl_hat_ob <= %s")
        params.append(float(pnl_hat_ob_max))
    if win_prob_ob_min is not None:
        where.append("win_prob_ob >= %s")
        params.append(float(win_prob_ob_min))
    if win_prob_ob_max is not None:
        where.append("win_prob_ob <= %s")
        params.append(float(win_prob_ob_max))
    if funding_pnl_min is not None:
        where.append("funding_pnl_usdt >= %s")
        params.append(float(funding_pnl_min))
    if funding_pnl_max is not None:
        where.append("funding_pnl_usdt <= %s")
        params.append(float(funding_pnl_max))
    if created_after is not None:
        where.append("created_at >= %s")
        params.append(created_after)
    if created_before is not None:
        where.append("created_at <= %s")
        params.append(created_before)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    try:
        conn_kwargs: Dict[str, Any] = {"autocommit": True}
        if dict_row:
            conn_kwargs["row_factory"] = dict_row
        with psycopg.connect(WATCHLIST_PG_CONFIG['dsn'], **conn_kwargs) as conn:
            # realized_pnl 是基于 orders 表的 best-effort 计算；需要更大的候选集后再做后置过滤。
            fetch_limit = limit
            if realized_pnl_min is not None or realized_pnl_max is not None:
                fetch_limit = min(max(limit * 5, limit), 5000)
            rows = conn.execute(
                f"""
                SELECT *
                  FROM watchlist.live_trade_signal
                  {where_sql}
                 ORDER BY created_at DESC
                 LIMIT %s;
                """,
                (*params, fetch_limit),
            ).fetchall()

            price_map: Dict[Tuple[int, str, str], Dict[str, Any]] = {}
            if include_prices and rows:
                signal_ids: List[int] = []
                for r in rows:
                    if isinstance(r, dict) and r.get('id') is not None:
                        try:
                            signal_ids.append(int(r['id']))
                        except Exception:
                            continue
                if signal_ids:
                    order_rows = conn.execute(
                        """
                        SELECT DISTINCT ON (signal_id, action, leg)
                               signal_id, action, leg, avg_price, filled_qty, exchange, status, created_at, id
                          FROM watchlist.live_trade_order
                         WHERE signal_id = ANY(%s)
                         ORDER BY signal_id, action, leg, created_at DESC, id DESC;
                        """,
                        (signal_ids,),
                    ).fetchall()
                    for orow in order_rows or []:
                        if isinstance(orow, dict):
                            try:
                                sid = int(orow.get('signal_id'))
                            except Exception:
                                continue
                            key = (sid, str(orow.get('action') or ''), str(orow.get('leg') or ''))
                            price_map[key] = {
                                'avg_price': orow.get('avg_price'),
                                'filled_qty': orow.get('filled_qty'),
                                'notional_usdt': orow.get('notional_usdt'),
                                'exchange': orow.get('exchange'),
                                'status': orow.get('status'),
                                'created_at': orow.get('created_at'),
                            }
    except Exception as exc:
        return jsonify({'error': str(exc), 'timestamp': now_utc_iso()}), 500

    def _jsonable(val: Any) -> Any:
        if isinstance(val, datetime):
            return val.astimezone(timezone.utc).isoformat()
        return val

    # Cache funding computation per UTC hour to avoid hammering private APIs.
    _FUNDING_SIG_LOCK = getattr(live_trading_signals, "_FUNDING_SIG_LOCK", None)
    _FUNDING_SIG_CACHE = getattr(live_trading_signals, "_FUNDING_SIG_CACHE", None)
    if _FUNDING_SIG_LOCK is None or _FUNDING_SIG_CACHE is None:
        _FUNDING_SIG_LOCK = threading.Lock()
        _FUNDING_SIG_CACHE = {}
        setattr(live_trading_signals, "_FUNDING_SIG_LOCK", _FUNDING_SIG_LOCK)
        setattr(live_trading_signals, "_FUNDING_SIG_CACHE", _FUNDING_SIG_CACHE)

    now_ms = int(time.time() * 1000)
    hour_bucket = int(now_ms // 3_600_000)
    with _FUNDING_SIG_LOCK:
        if _FUNDING_SIG_CACHE.get("_hour_bucket") != hour_bucket:
            _FUNDING_SIG_CACHE.clear()
            _FUNDING_SIG_CACHE["_hour_bucket"] = hour_bucket

    out = []
    for r in rows or []:
        if isinstance(r, dict):
            item = {k: _jsonable(v) for k, v in r.items()}
            if include_prices:
                try:
                    sid = int(r.get('id'))  # type: ignore[arg-type]
                except Exception:
                    sid = None
                if sid is not None:
                    def _get_px(action: str, leg: str) -> Any:
                        got = price_map.get((sid, action, leg)) or {}
                        return _jsonable(got.get('avg_price'))

                    def _get_filled(action: str, leg: str) -> Any:
                        got = price_map.get((sid, action, leg)) or {}
                        return got.get('filled_qty')

                    def _get_notional(action: str, leg: str) -> Any:
                        got = price_map.get((sid, action, leg)) or {}
                        return got.get('notional_usdt')

                    item['open_long_avg_price'] = _get_px('open', 'long')
                    item['close_long_avg_price'] = _get_px('close', 'long')
                    item['open_short_avg_price'] = _get_px('open', 'short')
                    item['close_short_avg_price'] = _get_px('close', 'short')

                    item['open_long_filled_qty'] = _get_filled('open', 'long')
                    item['close_long_filled_qty'] = _get_filled('close', 'long')
                    item['open_short_filled_qty'] = _get_filled('open', 'short')
                    item['close_short_filled_qty'] = _get_filled('close', 'short')
                    item['open_long_notional_usdt'] = _get_notional('open', 'long')
                    item['open_short_notional_usdt'] = _get_notional('open', 'short')

                    realized_pnl = None
                    try:
                        olp = item.get('open_long_avg_price')
                        clp = item.get('close_long_avg_price')
                        osp = item.get('open_short_avg_price')
                        csp = item.get('close_short_avg_price')
                        ql = min(float(item.get('open_long_filled_qty') or 0), float(item.get('close_long_filled_qty') or 0))
                        qs = min(float(item.get('open_short_filled_qty') or 0), float(item.get('close_short_filled_qty') or 0))
                        pnl_l = 0.0
                        pnl_s = 0.0
                        ok_l = ql > 0 and olp is not None and clp is not None
                        ok_s = qs > 0 and osp is not None and csp is not None
                        # Only compute realized PnL when both legs have a completed open+close,
                        # otherwise the "realized" value is misleading (still holding exposure).
                        if ok_l and ok_s:
                            pnl_l = (float(clp) - float(olp)) * ql
                            pnl_s = (float(osp) - float(csp)) * qs
                            realized_pnl = pnl_l + pnl_s
                    except Exception:
                        realized_pnl = None
                    item['realized_pnl_usdt'] = realized_pnl

            if include_funding:
                # Funding fields are persisted by LiveTradingManager:
                # - Active trades: updated at most once per UTC hour.
                # - Closed trades: finalized using (opened_at → closed_at).
                # Keep the API as a pure read so page refresh doesn't hammer private bill endpoints.
                pass
            # Post-filter realized_pnl when requested (best-effort, depends on fills).
            if realized_pnl_min is not None or realized_pnl_max is not None:
                rp = item.get('realized_pnl_usdt')
                if rp is None:
                    continue
                try:
                    rp_f = float(rp)
                except Exception:
                    continue
                if realized_pnl_min is not None and rp_f < float(realized_pnl_min):
                    continue
                if realized_pnl_max is not None and rp_f > float(realized_pnl_max):
                    continue
            out.append(item)
        else:
            out.append(r)
        if len(out) >= limit:
            break
    return jsonify({'timestamp': now_utc_iso(), 'signals': out})


@app.route('/api/live_trading/samples')
def live_trading_samples():
    """Return recent spread monitor samples."""
    if psycopg is None:
        return jsonify({'error': 'psycopg 未安装，无法连接 PG', 'timestamp': now_utc_iso()}), 500

    signal_id = request.args.get('signal_id')
    if not signal_id:
        return jsonify({'error': 'missing signal_id', 'timestamp': now_utc_iso()}), 400
    limit = min(max(int(request.args.get('limit', 200) or 200), 1), 1000)

    try:
        conn_kwargs: Dict[str, Any] = {"autocommit": True}
        if dict_row:
            conn_kwargs["row_factory"] = dict_row
        with psycopg.connect(WATCHLIST_PG_CONFIG['dsn'], **conn_kwargs) as conn:
            rows = conn.execute(
                """
                SELECT *
                  FROM watchlist.live_trade_spread_sample
                 WHERE signal_id=%s
                 ORDER BY ts DESC
                 LIMIT %s;
                """,
                (int(signal_id), limit),
            ).fetchall()
    except Exception as exc:
        return jsonify({'error': str(exc), 'timestamp': now_utc_iso()}), 500

    def _jsonable(val: Any) -> Any:
        if isinstance(val, datetime):
            return val.astimezone(timezone.utc).isoformat()
        return val

    out = []
    for r in rows or []:
        if isinstance(r, dict):
            out.append({k: _jsonable(v) for k, v in r.items()})
        else:
            out.append(r)
    return jsonify({'timestamp': now_utc_iso(), 'samples': out})


@app.route('/api/live_trading/positions')
def live_trading_positions():
    """
    Query live positions/balances (REST) for exchanges.
    Note: this endpoint calls exchange private APIs using config_private credentials.
    """
    if psycopg is None:
        return jsonify({'error': 'psycopg 未安装，无法连接 PG', 'timestamp': now_utc_iso()}), 500

    signal_id = request.args.get('signal_id')
    limit = min(max(int(request.args.get('limit', 50) or 50), 1), 100)
    query_all = str(request.args.get('all') or '').lower() in {'1', 'true', 'yes', 'y'}
    only_nonzero = str(request.args.get('nonzero') or '1').lower() not in {'0', 'false', 'no', 'n'}
    persist_balances = str(request.args.get('persist') or '1').lower() not in {'0', 'false', 'no', 'n'}

    try:
        conn_kwargs: Dict[str, Any] = {"autocommit": True}
        if dict_row:
            conn_kwargs["row_factory"] = dict_row
        with psycopg.connect(WATCHLIST_PG_CONFIG['dsn'], **conn_kwargs) as conn:
            if signal_id:
                signals = conn.execute(
                    "SELECT * FROM watchlist.live_trade_signal WHERE id=%s LIMIT 1;",
                    (int(signal_id),),
                ).fetchall()
            else:
                signals = conn.execute(
                    """
                    SELECT *
                      FROM watchlist.live_trade_signal
                     WHERE status IN ('open','closing')
                     ORDER BY opened_at DESC NULLS LAST, created_at DESC
                     LIMIT %s;
                    """,
                    (limit,),
                ).fetchall()
    except Exception as exc:
        return jsonify({'error': str(exc), 'timestamp': now_utc_iso()}), 500

    opened_at_map: Dict[tuple, Any] = {}
    for s in signals or []:
        if not isinstance(s, dict):
            continue
        sym = str(s.get("symbol") or "").upper()
        if not sym:
            continue
        opened_at = s.get("opened_at") or s.get("created_at")
        for ex in [s.get("leg_long_exchange"), s.get("leg_short_exchange")]:
            exl = str(ex or "").lower().strip()
            if exl:
                opened_at_map[(exl, sym)] = opened_at

    def _pos_nonzero(exchange: str, row: Any) -> bool:
        if not only_nonzero:
            return True
        if not isinstance(row, dict):
            return False
        ex = (exchange or '').lower()
        try:
            if ex == 'binance':
                return abs(float(row.get('positionAmt') or 0.0)) > 1e-12
            if ex == 'okx':
                return abs(float(row.get('pos') or row.get('sz') or 0.0)) > 1e-12
            if ex == 'bybit':
                return abs(float(row.get('size') or row.get('qty') or 0.0)) > 1e-12
            if ex == 'bitget':
                raw = row.get('available') or row.get('total') or row.get('openQty') or row.get('pos') or 0.0
                return abs(float(raw or 0.0)) > 1e-12
            if ex == 'hyperliquid':
                p = row.get('position') if isinstance(row.get('position'), dict) else row
                return abs(float(p.get('szi') or 0.0)) > 1e-12
            if ex == 'lighter':
                raw = row.get('position') or row.get('size') or 0.0
                return abs(float(raw or 0.0)) > 1e-12
            if ex == 'grvt':
                return abs(float(row.get('size') or 0.0)) > 1e-12
        except Exception:
            return True
        return True

    def _pos_is_nonzero(exchange: str, row: Any) -> bool:
        """Check whether a position row is non-zero (ignores `only_nonzero`)."""
        if not isinstance(row, dict):
            return False
        ex = (exchange or '').lower()
        try:
            if ex == 'binance':
                return abs(float(row.get('positionAmt') or 0.0)) > 1e-12
            if ex == 'okx':
                return abs(float(row.get('pos') or row.get('sz') or 0.0)) > 1e-12
            if ex == 'bybit':
                return abs(float(row.get('size') or row.get('qty') or 0.0)) > 1e-12
            if ex == 'bitget':
                raw = row.get('available') or row.get('total') or row.get('openQty') or row.get('pos') or 0.0
                return abs(float(raw or 0.0)) > 1e-12
            if ex == 'hyperliquid':
                p = row.get('position') if isinstance(row.get('position'), dict) else row
                return abs(float(p.get('szi') or 0.0)) > 1e-12
            if ex == 'lighter':
                raw = row.get('position') or row.get('size') or 0.0
                return abs(float(raw or 0.0)) > 1e-12
            if ex == 'grvt':
                return abs(float(row.get('size') or 0.0)) > 1e-12
        except Exception:
            return True
        return True

    supported = set()
    try:
        backends = get_supported_trading_backends() if callable(get_supported_trading_backends) else None
        if isinstance(backends, dict):
            supported = set([str(x).lower() for x in (backends.get("perpetual") or [])])
        elif isinstance(backends, (list, tuple, set)):
            supported = set([str(x).lower() for x in backends])
    except Exception:
        supported = set()
    if supported:
        supported.add("lighter")
    default_exchanges = ['binance', 'okx', 'bybit', 'bitget', 'hyperliquid', 'lighter', 'grvt']
    exchanges = [ex for ex in default_exchanges if not supported or ex in supported]

    # Public mark price / funding enrichment (best-effort, cached).
    # This avoids relying on private position payloads which are inconsistent across exchanges.
    import threading
    import time
    import requests
    from datetime import datetime, timezone, timedelta
    from typing import Tuple
    from funding_utils import (
        derive_funding_interval_hours,
        derive_interval_hours_from_times,
        normalize_next_funding_time,
        normalize_and_advance_next_funding_time,
    )

    _ua = None
    try:
        from config import REST_CONNECTION_CONFIG  # type: ignore
        _ua = (REST_CONNECTION_CONFIG or {}).get("user_agent")
        _timeout = float((REST_CONNECTION_CONFIG or {}).get("timeout", 6))
    except Exception:
        _timeout = 6.0

    def _now_utc() -> datetime:
        return datetime.now(timezone.utc)

    def _next_utc_boundary_iso(step_hours: int, now: Optional[datetime] = None) -> str:
        step_hours = int(step_hours) if int(step_hours) > 0 else 1
        now = (now or _now_utc()).astimezone(timezone.utc)
        floored = now.replace(minute=0, second=0, microsecond=0)
        base_hour = (floored.hour // step_hours) * step_hours
        base = floored.replace(hour=base_hour)
        candidate = base + timedelta(hours=step_hours)
        if candidate <= now:
            candidate = candidate + timedelta(hours=step_hours)
        return candidate.isoformat()

    _PUB_CACHE_LOCK = getattr(live_trading_positions, "_PUB_CACHE_LOCK", None)
    _PUB_CACHE = getattr(live_trading_positions, "_PUB_CACHE", None)
    if _PUB_CACHE_LOCK is None or _PUB_CACHE is None:
        _PUB_CACHE_LOCK = threading.Lock()
        _PUB_CACHE = {}
        setattr(live_trading_positions, "_PUB_CACHE_LOCK", _PUB_CACHE_LOCK)
        setattr(live_trading_positions, "_PUB_CACHE", _PUB_CACHE)

    def _cache_get(key: Tuple[str, str]) -> Optional[Dict[str, Any]]:
        with _PUB_CACHE_LOCK:
            item = _PUB_CACHE.get(key)
        if not item:
            return None
        try:
            if float(item.get("expires_at") or 0) < time.time():
                return None
        except Exception:
            return None
        val = item.get("value")
        return val if isinstance(val, dict) else None

    def _cache_set(key: Tuple[str, str], value: Dict[str, Any], ttl_s: float = 0.0) -> None:
        # For positions UI we prefer accuracy over caching; default ttl=0 disables cache.
        try:
            if float(ttl_s) <= 0:
                return
        except Exception:
            return
        with _PUB_CACHE_LOCK:
            _PUB_CACHE[key] = {"expires_at": time.time() + float(ttl_s), "value": value}

    def _req_json(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        headers = {"User-Agent": _ua or "FR-Monitor/1.0", "Accept": "application/json"}
        try:
            resp = requests.get(url, params=params, timeout=_timeout, headers=headers)
            if resp.status_code == 200:
                return resp.json()
            return None
        except Exception:
            return None

    def _public_funding_mark(exchange: str, base: str) -> Dict[str, Any]:
        ex = (exchange or "").lower()
        base_u = (base or "").upper()
        if not base_u:
            return {}
        cached = _cache_get((ex, base_u))
        if cached is not None:
            return dict(cached)

        out: Dict[str, Any] = {}
        now = _now_utc()

        if ex == "binance":
            sym = f"{base_u}USDT"
            data = _req_json("https://fapi.binance.com/fapi/v1/premiumIndex", params={"symbol": sym})
            if isinstance(data, dict):
                try:
                    if data.get("markPrice") is not None:
                        out["mark_price"] = float(data.get("markPrice"))
                except Exception:
                    pass
                try:
                    fr = data.get("lastFundingRate")
                    if fr is not None:
                        out["funding_rate"] = float(fr)
                except Exception:
                    pass
                nft = normalize_next_funding_time(data.get("nextFundingTime"))
                if nft:
                    out["next_funding_time"] = nft
            interval = derive_funding_interval_hours("binance")
            if interval:
                out["funding_interval_hours"] = float(interval)

        elif ex == "bybit":
            sym = f"{base_u}USDT"
            data = _req_json("https://api.bybit.com/v5/market/tickers", params={"category": "linear", "symbol": sym})
            if isinstance(data, dict) and data.get("retCode") == 0:
                items = ((data.get("result") or {}) or {}).get("list") or []
                item = items[0] if isinstance(items, list) and items and isinstance(items[0], dict) else None
                if item:
                    try:
                        if item.get("markPrice") is not None:
                            out["mark_price"] = float(item.get("markPrice"))
                    except Exception:
                        pass
                    try:
                        if item.get("fundingRate") is not None:
                            out["funding_rate"] = float(item.get("fundingRate"))
                    except Exception:
                        pass
                    nft = normalize_next_funding_time(item.get("nextFundingTime"))
                    if nft:
                        out["next_funding_time"] = nft
                    interval = derive_funding_interval_hours("bybit", item.get("fundingIntervalHour"))
                    if interval:
                        out["funding_interval_hours"] = float(interval)

        elif ex == "okx":
            inst_id = f"{base_u}-USDT-SWAP"
            fr = _req_json("https://www.okx.com/api/v5/public/funding-rate", params={"instId": inst_id})
            if isinstance(fr, dict) and fr.get("code") == "0":
                data = fr.get("data") or []
                entry = data[0] if isinstance(data, list) and data and isinstance(data[0], dict) else None
                if entry:
                    try:
                        if entry.get("fundingRate") is not None:
                            out["funding_rate"] = float(entry.get("fundingRate"))
                    except Exception:
                        pass
                    # OKX 的 funding-rate 接口会同时返回 fundingTime 与 nextFundingTime。
                    # 对于部分合约，fundingTime 可能就是“下一次结算时间”（而不是上一期结算），
                    # 此时应该取 (fundingTime, nextFundingTime) 中“最早且在未来”的那个作为 next。
                    ft = normalize_next_funding_time(entry.get("fundingTime"))
                    nft = normalize_next_funding_time(entry.get("nextFundingTime"))
                    try:
                        cand = []
                        for x in (ft, nft):
                            if not x:
                                continue
                            dt = datetime.fromisoformat(str(x).replace("Z", "+00:00"))
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            if dt > (now - timedelta(seconds=60)):
                                cand.append(dt.astimezone(timezone.utc))
                        if cand:
                            out["next_funding_time"] = min(cand).isoformat()
                        elif nft:
                            out["next_funding_time"] = nft
                        elif ft:
                            out["next_funding_time"] = ft
                    except Exception:
                        if nft:
                            out["next_funding_time"] = nft
                        elif ft:
                            out["next_funding_time"] = ft
                    interval = derive_interval_hours_from_times(entry.get("fundingTime"), entry.get("nextFundingTime"))
                    if interval:
                        out["funding_interval_hours"] = float(interval)
            mp = _req_json("https://www.okx.com/api/v5/public/mark-price", params={"instType": "SWAP", "instId": inst_id})
            if isinstance(mp, dict) and mp.get("code") == "0":
                data = mp.get("data") or []
                entry = data[0] if isinstance(data, list) and data and isinstance(data[0], dict) else None
                if entry:
                    try:
                        if entry.get("markPx") is not None:
                            out["mark_price"] = float(entry.get("markPx"))
                    except Exception:
                        pass
            if "funding_interval_hours" not in out:
                interval = derive_funding_interval_hours("okx")
                if interval:
                    out["funding_interval_hours"] = float(interval)

        elif ex == "bitget":
            sym = f"{base_u}USDT"
            ticker_rate = None
            ticker = _req_json(
                "https://api.bitget.com/api/v2/mix/market/ticker",
                params={"productType": "USDT-FUTURES", "symbol": sym},
            )
            if isinstance(ticker, dict) and ticker.get("code") == "00000":
                data = ticker.get("data") or []
                entry = data[0] if isinstance(data, list) and data and isinstance(data[0], dict) else None
                if entry:
                    try:
                        if entry.get("markPrice") is not None:
                            out["mark_price"] = float(entry.get("markPrice"))
                    except Exception:
                        pass
                    try:
                        if entry.get("fundingRate") is not None:
                            ticker_rate = float(entry.get("fundingRate"))
                    except Exception:
                        ticker_rate = None
            sched = _req_json(
                "https://api.bitget.com/api/v2/mix/market/current-fund-rate",
                params={"productType": "USDT-FUTURES", "symbol": sym},
            )
            if isinstance(sched, dict) and sched.get("code") == "00000":
                data = sched.get("data") or []
                entry = data[0] if isinstance(data, list) and data and isinstance(data[0], dict) else None
                if entry:
                    try:
                        fr = entry.get("fundingRate") or entry.get("fundingRateStr")
                        if fr is not None:
                            out["funding_rate"] = float(fr)
                    except Exception:
                        pass
                    nft = normalize_next_funding_time(
                        entry.get("nextSettleTime")
                        or entry.get("nextUpdate")
                        or entry.get("nextFundingTime")
                    )
                    if nft:
                        out["next_funding_time"] = nft
                    interval = derive_funding_interval_hours("bitget", entry.get("fundingRateInterval"), fallback=True)
                    if interval:
                        out["funding_interval_hours"] = float(interval)
            if out.get("funding_rate") is None and ticker_rate is not None:
                out["funding_rate"] = ticker_rate
            if "funding_interval_hours" not in out:
                interval = derive_funding_interval_hours("bitget")
                if interval:
                    out["funding_interval_hours"] = float(interval)

        elif ex == "hyperliquid":
            try:
                from rest_collectors import get_hyperliquid_funding_map  # type: ignore
                fmap = get_hyperliquid_funding_map()
            except Exception:
                fmap = {}
            ctx = fmap.get(base_u) if isinstance(fmap, dict) else None
            if isinstance(ctx, dict):
                try:
                    if ctx.get("markPx") is not None:
                        out["mark_price"] = float(ctx.get("markPx"))
                except Exception:
                    pass
                try:
                    if ctx.get("funding") is not None:
                        out["funding_rate"] = float(ctx.get("funding"))
                except Exception:
                    pass
            interval = derive_funding_interval_hours("hyperliquid")
            if interval:
                out["funding_interval_hours"] = float(interval)
                out["next_funding_time"] = _next_utc_boundary_iso(int(interval or 1), now=now)

        elif ex == "grvt":
            base_url = (getattr(config, "GRVT_REST_BASE_URL", "") or "https://market-data.grvt.io").rstrip("/")
            inst = f"{base_u}_USDT_Perp"
            try:
                resp = requests.post(
                    f"{base_url}/full/v1/ticker",
                    json={"instrument": inst},
                    timeout=_timeout,
                    headers={"User-Agent": _ua or "FR-Monitor/1.0", "Accept": "application/json"},
                )
                if resp.status_code == 200:
                    data = resp.json()
                else:
                    data = None
            except Exception:
                data = None
            row = data.get("result") if isinstance(data, dict) else None
            if isinstance(row, dict):
                try:
                    if row.get("mark_price") is not None:
                        out["mark_price"] = float(row.get("mark_price"))
                except Exception:
                    pass
                try:
                    # GRVT funding fields are expressed in percentage points.
                    fr = row.get("funding_rate") or row.get("funding_rate_8h_curr")
                    if fr is not None:
                        out["funding_rate"] = float(fr) / 100.0
                except Exception:
                    pass
                nft = normalize_next_funding_time(row.get("next_funding_time"))
                if nft:
                    out["next_funding_time"] = nft
            interval = derive_funding_interval_hours("grvt")
            if interval:
                out["funding_interval_hours"] = float(interval)

        # Normalize next_funding_time and ensure it is in the future (best-effort).
        next_ft, interval_hours = normalize_and_advance_next_funding_time(
            now=now,
            next_funding_time=out.get("next_funding_time"),
            interval_hours=out.get("funding_interval_hours"),
        )
        if next_ft:
            out["next_funding_time"] = next_ft
        if interval_hours is not None:
            out["funding_interval_hours"] = float(interval_hours)

        _cache_set((ex, base_u), out, ttl_s=0.0)
        return dict(out)

    targets: List[Dict[str, str]] = []
    if query_all:
        for ex in exchanges:
            targets.append({'exchange': ex, 'symbol': ''})
    else:
        for s in signals or []:
            if not isinstance(s, dict):
                continue
            sym = str(s.get('symbol') or '').upper()
            if not sym:
                continue
            for exch in [s.get('leg_long_exchange'), s.get('leg_short_exchange')]:
                ex = str(exch or '').lower()
                if ex:
                    targets.append({'exchange': ex, 'symbol': sym})

    uniq = {(t['exchange'], t['symbol']) for t in targets}
    results: List[Dict[str, Any]] = []
    for ex, sym in sorted(uniq):
        try:
            balance = None
            positions: Any = []
            if ex == 'binance':
                if sym:
                    balance = get_binance_perp_usdt_balance()
                    positions = get_binance_perp_positions(symbol=f"{sym}USDT")
                else:
                    # Efficient path for "all=1": fetch balance+positions in one signed call.
                    # /fapi/v2/account contains both assets and positions.
                    acct = get_binance_perp_account()
                    usdt = None
                    assets = acct.get("assets") or []
                    if isinstance(assets, list):
                        for item in assets:
                            if not isinstance(item, dict):
                                continue
                            if str(item.get("asset") or "").upper() == "USDT":
                                usdt = item
                                break
                    if isinstance(usdt, dict):
                        balance = {
                            "currency": "USDT",
                            "wallet_balance": usdt.get("walletBalance"),
                            "available_balance": usdt.get("availableBalance"),
                            "margin_balance": usdt.get("marginBalance"),
                            "unrealized_pnl": usdt.get("unrealizedProfit"),
                        }
                    else:
                        balance = {"currency": "USDT"}
                    positions = acct.get("positions") or []
            elif ex == 'okx':
                balance = get_okx_account_balance(ccy="USDT")
                positions = get_okx_swap_positions(symbol=sym) if sym else get_okx_swap_positions()
                if isinstance(positions, list) and positions:
                    for p in positions:
                        if not isinstance(p, dict):
                            continue
                        try:
                            inst = str(p.get("instId") or "")
                            base_sym = inst.split("-")[0].upper() if inst else (sym or "")
                            if base_sym:
                                ct_val = float(get_okx_swap_contract_value(base_sym))
                                raw_pos = p.get("pos") or p.get("sz") or 0
                                base_size = float(raw_pos or 0) * ct_val
                                p["_ctVal"] = ct_val
                                p["_base_size"] = base_size
                        except Exception:
                            continue
            elif ex == 'bybit':
                balance = get_bybit_wallet_balance(coin="USDT", account_type="UNIFIED")
                positions = (
                    get_bybit_linear_positions(symbol=f"{sym}USDT", category="linear")
                    if sym
                    else get_bybit_linear_positions(category="linear", settle_coin="USDT")
                )
            elif ex == 'bitget':
                balance = get_bitget_usdt_balance(margin_coin="USDT")
                positions = get_bitget_usdt_perp_positions(symbol=f"{sym}USDT") if sym else get_bitget_usdt_perp_positions()
            elif ex == 'hyperliquid':
                balance = get_hyperliquid_balance_summary()
                positions = get_hyperliquid_perp_positions()
            elif ex == 'lighter':
                balance = get_lighter_balance_summary()
                raw_positions = None
                if isinstance(balance, dict):
                    raw = balance.get("raw_account")
                    if isinstance(raw, dict):
                        raw_positions = raw.get("positions")
                positions = raw_positions if isinstance(raw_positions, list) else []
            elif ex == 'grvt':
                balance = get_grvt_balance_summary()
                positions = get_grvt_perp_positions(symbol=sym) if sym else get_grvt_perp_positions()
            else:
                balance = None
                positions = []

            if isinstance(positions, list):
                positions = [p for p in positions if _pos_nonzero(ex, p)]
                # Attach public mark/funding fields for each position row (best-effort).
                # IMPORTANT: only enrich when `only_nonzero=1`; otherwise `all=1&nonzero=0`
                # could fan out into hundreds/thousands of public REST calls and trigger bans.
                if not only_nonzero:
                    results.append({'exchange': ex, 'symbol': sym or None, 'balance': balance, 'positions': positions, 'error': None})
                    continue

                for p in [x for x in positions if _pos_is_nonzero(ex, x)]:
                    if not isinstance(p, dict):
                        continue
                    try:
                        base = None
                        exl = (ex or "").lower()
                        if exl == "binance":
                            sym0 = str(p.get("symbol") or "")
                            base = sym0[:-4] if sym0.upper().endswith("USDT") else sym0
                        elif exl == "bybit":
                            sym0 = str(p.get("symbol") or "")
                            base = sym0[:-4] if sym0.upper().endswith("USDT") else sym0
                        elif exl == "bitget":
                            sym0 = str(p.get("symbol") or "")
                            base = sym0[:-4] if sym0.upper().endswith("USDT") else sym0
                        elif exl == "okx":
                            inst = str(p.get("instId") or "")
                            base = inst.split("-")[0] if inst else None
                        elif exl == "hyperliquid":
                            pos = p.get("position") if isinstance(p.get("position"), dict) else p
                            base = str(pos.get("coin") or "")
                        elif exl == "grvt":
                            inst = str(p.get("instrument") or "")
                            base = inst.split("_")[0] if inst and "_" in inst else inst
                        base = (base or "").upper()
                        if base:
                            extra = _public_funding_mark(exl, base)
                            if isinstance(extra, dict) and extra:
                                p["mark_price"] = extra.get("mark_price")
                                p["funding_rate"] = extra.get("funding_rate")
                                p["funding_interval_hours"] = extra.get("funding_interval_hours")
                                p["next_funding_time"] = extra.get("next_funding_time")
                            opened_at = opened_at_map.get((exl, base))
                            if opened_at is not None:
                                pos_sign = None
                                notional_usdt = None
                                try:
                                    if exl == "binance":
                                        amt = float(p.get("positionAmt") or 0.0)
                                        pos_sign = 1 if amt >= 0 else -1
                                        notional_usdt = float(p.get("notional") or 0.0) if p.get("notional") is not None else None
                                    elif exl == "okx":
                                        pos_side = str(p.get("posSide") or "").lower()
                                        if pos_side in {"short"}:
                                            pos_sign = -1
                                        elif pos_side in {"long"}:
                                            pos_sign = 1
                                        notional_usdt = float(p.get("notionalUsd") or 0.0) if p.get("notionalUsd") is not None else None
                                    elif exl == "bybit":
                                        side = str(p.get("side") or "").lower()
                                        pos_sign = -1 if side == "sell" else 1
                                        notional_usdt = float(p.get("positionValue") or 0.0) if p.get("positionValue") is not None else None
                                    elif exl == "bitget":
                                        hold = str(p.get("holdSide") or "").lower()
                                        pos_sign = -1 if hold == "short" else 1
                                        notional_usdt = float(p.get("totalUSDT") or p.get("usdtValue") or 0.0) if (p.get("totalUSDT") is not None or p.get("usdtValue") is not None) else None
                                    elif exl == "hyperliquid":
                                        pos = p.get("position") if isinstance(p.get("position"), dict) else p
                                        szi = float((pos or {}).get("szi") or 0.0)
                                        pos_sign = 1 if szi >= 0 else -1
                                        notional_usdt = float((pos or {}).get("positionValue") or 0.0) if (pos or {}).get("positionValue") is not None else None
                                    elif exl == "grvt":
                                        sz = float(p.get("size") or 0.0)
                                        pos_sign = 1 if sz >= 0 else -1
                                        notional_usdt = abs(float(p.get("notional") or 0.0)) if p.get("notional") is not None else None
                                except Exception:
                                    pos_sign = None
                                    notional_usdt = None
                                fsum = _funding_fee_summary_since_open(exl, base, opened_at, notional_usdt=notional_usdt, pos_sign=pos_sign)
                                p["funding_pnl_usdt"] = fsum.get("funding_pnl_usdt")
                                p["funding_last_fee_usdt"] = fsum.get("last_fee_usdt")
                                p["funding_last_fee_time"] = fsum.get("last_fee_time")
                                p["funding_source"] = fsum.get("source")
                                p["funding_opened_at"] = opened_at.astimezone(timezone.utc).isoformat() if isinstance(opened_at, datetime) else str(opened_at)
                                if fsum.get("error"):
                                    p["funding_fee_error"] = fsum.get("error")
                    except Exception:
                        continue
            results.append({'exchange': ex, 'symbol': sym or None, 'balance': balance, 'positions': positions, 'error': None})
        except TradeExecutionError as exc:
            results.append({'exchange': ex, 'symbol': sym or None, 'balance': None, 'positions': None, 'error': str(exc)})
        except Exception as exc:
            results.append({'exchange': ex, 'symbol': sym or None, 'balance': None, 'positions': None, 'error': f"{type(exc).__name__}: {exc}"})

    balance_rows = [
        {'exchange': r.get('exchange'), 'balance': r.get('balance'), 'error': r.get('error')}
        for r in (results or [])
        if isinstance(r, dict)
    ]
    totals: Dict[str, Any] = {}
    snapshot_id: Optional[int] = None
    try:
        if live_trading_manager:
            totals = live_trading_manager._compute_balance_totals(balance_rows)  # type: ignore[attr-defined]
            if persist_balances:
                with psycopg.connect(WATCHLIST_PG_CONFIG['dsn'], autocommit=True) as conn:
                    snapshot_id = live_trading_manager._insert_balance_snapshot(  # type: ignore[attr-defined]
                        conn,
                        source="manual",
                        balance_rows=balance_rows,
                        totals=totals,
                        context={
                            "endpoint": "/api/live_trading/positions",
                            "mode": "all" if query_all else "active",
                            "only_nonzero": only_nonzero,
                            "targets": [{"exchange": ex, "symbol": sym} for (ex, sym) in list(uniq)],
                        },
                    )
    except Exception:
        snapshot_id = None

    return jsonify(
        {
            'timestamp': now_utc_iso(),
            'mode': 'all' if query_all else 'active',
            'only_nonzero': only_nonzero,
            'targets': list(uniq),
            'results': results,
            'balance_totals': totals,
            'balance_snapshot_id': snapshot_id,
        }
    )


@app.route('/api/live_trading/force_close', methods=['POST'])
def live_trading_force_close():
    """Manual one-click flatten for a live trade signal (best-effort)."""
    if psycopg is None:
        return jsonify({'error': 'psycopg 未安装，无法连接 PG', 'timestamp': now_utc_iso()}), 500

    payload = request.get_json(silent=True) or {}
    signal_id = payload.get('signal_id') or request.args.get('signal_id')
    try:
        signal_id_i = int(signal_id)
    except Exception:
        return jsonify({'error': 'invalid signal_id', 'timestamp': now_utc_iso()}), 400

    try:
        result = live_trading_manager.manual_force_close(signal_id_i)
        if not result.get('ok'):
            return jsonify({'error': result.get('error') or 'force_close failed', 'result': result, 'timestamp': now_utc_iso()}), 400
        return jsonify({'timestamp': now_utc_iso(), 'result': result})
    except Exception as exc:
        return jsonify({'error': str(exc), 'timestamp': now_utc_iso()}), 500


@app.route('/api/live_trading/flatten_position', methods=['POST'])
def live_trading_flatten_position():
    """Manual flatten a single exchange+symbol position (best-effort, safe)."""
    if psycopg is None:
        return jsonify({'error': 'psycopg 未安装，无法连接 PG', 'timestamp': now_utc_iso()}), 500

    payload = request.get_json(silent=True) or {}
    exchange = (payload.get('exchange') or '').strip().lower()
    symbol = (payload.get('symbol') or '').strip().upper()
    if not exchange or not symbol:
        return jsonify({'error': 'missing exchange/symbol', 'timestamp': now_utc_iso()}), 400

    try:
        result = live_trading_manager.manual_flatten_position(exchange=exchange, symbol=symbol)
        if not result.get('ok'):
            return jsonify({'error': result.get('error') or 'flatten failed', 'result': result, 'timestamp': now_utc_iso()}), 400
        return jsonify({'timestamp': now_utc_iso(), 'result': result})
    except Exception as exc:
        return jsonify({'error': str(exc), 'timestamp': now_utc_iso()}), 500


@app.route('/api/live_trading/orders')
def live_trading_orders():
    """Return recent live-trade order records (optionally filter by signal_id)."""
    if psycopg is None:
        return jsonify({'error': 'psycopg 未安装，无法连接 PG', 'timestamp': now_utc_iso()}), 500

    limit = min(max(int(request.args.get('limit', 200) or 200), 1), 500)
    signal_id = request.args.get('signal_id')

    try:
        conn_kwargs: Dict[str, Any] = {"autocommit": True}
        if dict_row:
            conn_kwargs["row_factory"] = dict_row
        with psycopg.connect(WATCHLIST_PG_CONFIG['dsn'], **conn_kwargs) as conn:
            if signal_id:
                rows = conn.execute(
                    """
                    SELECT *
                      FROM watchlist.live_trade_order
                     WHERE signal_id=%s
                     ORDER BY created_at DESC
                     LIMIT %s;
                    """,
                    (int(signal_id), limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT *
                      FROM watchlist.live_trade_order
                     ORDER BY created_at DESC
                     LIMIT %s;
                    """,
                    (limit,),
                ).fetchall()
    except Exception as exc:
        return jsonify({'error': str(exc), 'timestamp': now_utc_iso()}), 500

    def _jsonable(val: Any) -> Any:
        if isinstance(val, datetime):
            return val.astimezone(timezone.utc).isoformat()
        return val

    out = []
    for r in rows or []:
        if isinstance(r, dict):
            out.append({k: _jsonable(v) for k, v in r.items()})
        else:
            out.append(r)
    return jsonify({'timestamp': now_utc_iso(), 'orders': out})

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

    # 用最近 1 分钟数据库数据做一次冷启动，避免跨所列表长时间为空（放到后台线程，避免阻塞 Web 启动）
    def _cold_start_bootstrap():
        try:
            cold_start_watchlist_from_db(limit_minutes=1)
            _kick_live_trading("cold_start_watchlist")
        except Exception as exc:
            logging.getLogger('watchlist').warning("cold start at bootstrap failed: %s", exc)

    threading.Thread(target=_cold_start_bootstrap, name="cold-start", daemon=True).start()
    
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

    print("🔥 启动图表预热线程（7天大窗口）...")
    chart_prewarm_thread = threading.Thread(target=prewarm_chart_cache, name="chart-prewarm", daemon=True)
    chart_prewarm_thread.start()

    print("🌐 启动Web服务器...")
    print("📊 系统状态监控: http://localhost:4002/api/system/status")
    
    # 启动Flask应用
    app.run(debug=False, host='0.0.0.0', port=4002, threaded=True)
