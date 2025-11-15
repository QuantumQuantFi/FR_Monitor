from flask import Flask, render_template, jsonify, request
import json
import threading
import time
import gc
import logging
from logging.handlers import RotatingFileHandler
import psutil
import os
import builtins
import sys
import decimal
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional
from exchange_connectors import ExchangeDataCollector
from arbitrage import ArbitrageMonitor
from config import (
    DATA_REFRESH_INTERVAL,
    CURRENT_SUPPORTED_SYMBOLS,
    MEMORY_OPTIMIZATION_CONFIG,
    WS_UPDATE_INTERVAL,
    WS_CONNECTION_CONFIG,
    DB_WRITE_INTERVAL_SECONDS,
)
from database import PriceDatabase
from market_info import get_dynamic_symbols, get_market_report
from trading.trade_executor import (
    TradeExecutionError,
    execute_dual_perp_market_order,
    execute_perp_market_batch,
    get_supported_trading_backends,
    get_bybit_linear_positions,
    get_bitget_usdt_perp_positions,
)

LOG_DIR = os.environ.get("SIMPLE_APP_LOG_DIR", os.path.join("logs", "simple_app"))
LOG_FILE_NAME = "simple_app.log"
LOG_MAX_BYTES = 20 * 1024 * 1024  # 20MB per file
LOG_BACKUP_COUNT = 3
_LOGGING_CONFIGURED = False
EXCHANGE_DISPLAY_ORDER = ['binance', 'okx', 'bybit', 'bitget', 'grvt', 'lighter', 'hyperliquid']


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
    stream_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
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
    def _process_value(self, key, value):
        """处理单个值，特别关注资金费率字段的精度"""
        if key == 'funding_rate' and isinstance(value, (int, float)) and value != 0:
            # 保持12位小数精度，去除末尾零，避免科学计数法
            return f"{value:.12f}".rstrip('0').rstrip('.')
        elif isinstance(value, float) and value != 0:
            # 对其他浮点数也避免科学计数法，保持合适精度
            if abs(value) < 0.001:
                return f"{value:.12f}".rstrip('0').rstrip('.')
            elif abs(value) < 1:
                return f"{value:.8f}".rstrip('0').rstrip('.')
            else:
                return value
        return value
    
    def _process_object(self, obj):
        """递归处理对象，保持数值精度"""
        if isinstance(obj, dict):
            return {key: self._process_object(self._process_value(key, value)) 
                   for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._process_object(item) for item in obj]
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
                         exchange_order=EXCHANGE_DISPLAY_ORDER)

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
    interval = request.args.get('interval', '1min')  # 数据间隔，默认1分钟
    
    # 限制查询范围以提高性能
    hours = min(hours, 168)  # 最多7天
    
    try:
        data = db.get_historical_data(symbol, exchange, hours, interval)
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
    interval = request.args.get('interval', '1min')
    
    # 限制查询范围
    hours = min(hours, 168)  # 最多7天
    
    try:
        raw_data = db.get_historical_data(symbol, exchange, hours, interval)

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
                    'premiums': []
                }
            
            # 使用时间戳作为标签
            timestamp = row['timestamp']
            chart_data[exchange_name]['labels'].append(to_utc_iso(timestamp))
            
            # 添加价格数据
            if interval == '1min':
                chart_data[exchange_name]['spot_prices'].append(row.get('spot_price_close', 0))
                chart_data[exchange_name]['futures_prices'].append(row.get('futures_price_close', 0))
                chart_data[exchange_name]['funding_rates'].append(row.get('funding_rate_avg', 0))
                chart_data[exchange_name]['premiums'].append(row.get('premium_percent_avg', 0))
            else:
                chart_data[exchange_name]['spot_prices'].append(row.get('spot_price', 0))
                chart_data[exchange_name]['futures_prices'].append(row.get('futures_price', 0))
                chart_data[exchange_name]['funding_rates'].append(row.get('funding_rate', 0))
                chart_data[exchange_name]['premiums'].append(row.get('premium_percent', 0))
        
        return jsonify({
            'symbol': symbol,
            'hours': hours,
            'interval': interval,
            'chart_data': chart_data,
            'timestamp': now_utc_iso()
        })
        
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
        db.cleanup_old_data()
        
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
    
    print("🔄 启动后台数据处理...")
    # 启动后台数据收集线程
    background_thread = threading.Thread(target=background_data_collection, daemon=True)
    background_thread.start()
    
    print("🌐 启动Web服务器...")
    print("📊 系统状态监控: http://localhost:4002/api/system/status")
    
    # 启动Flask应用
    app.run(debug=False, host='0.0.0.0', port=4002, threaded=True)
