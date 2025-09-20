#!/usr/bin/env python3
"""
Binance期货API数据结构详细说明
用于理解trade_executor返回的数据格式，便于后续开发数据存储等功能

所有示例数据均来自真实API响应
"""

import time
from typing import Dict, Any, List, Optional
from dataclasses import dataclass


# =============================================================================
# 1. 下单响应数据结构 (place_binance_perp_market_order 返回)
# =============================================================================

@dataclass
class BinanceOrderResponse:
    """
    Binance期货下单API响应结构
    API: POST /fapi/v1/order
    """

    # 核心订单标识
    orderId: int              # 系统订单号 - 唯一标识，用于后续查询/撤销
    clientOrderId: str        # 客户自定义订单号 - 用户指定的唯一标识
    symbol: str              # 交易对 - 如 "ETHUSDT"

    # 订单状态信息
    status: str              # 订单状态: "NEW"(新建) | "FILLED"(完全成交) | "CANCELED"(已撤销) | "REJECTED"(被拒绝)
    type: str               # 订单类型: "MARKET"(市价) | "LIMIT"(限价) | "STOP"(止损) 等
    origType: str           # 原始订单类型 - 通常与type相同
    side: str               # 买卖方向: "BUY"(买入) | "SELL"(卖出)
    positionSide: str       # 持仓方向: "LONG"(多头) | "SHORT"(空头) | "BOTH"(双向持仓模式)

    # 数量和价格信息 (重要: 均为字符串格式，需要转换为float)
    price: str              # 委托价格 - 市价单通常为 "0.00"
    origQty: str            # 原始委托数量 - 用户下单的数量
    executedQty: str        # 已成交数量 - 实际成交的数量 ⭐ 关键字段
    cumQty: str             # 累计成交数量 - 通常等于executedQty
    avgPrice: str           # 平均成交价格 - 实际成交的均价 ⭐ 关键字段
    cumQuote: str           # 累计成交金额(USDT) - 实际花费/收到的USDT金额 ⭐ 关键字段

    # 时间信息
    updateTime: int         # 最后更新时间戳(毫秒) - Unix时间戳

    # 其他订单设置
    timeInForce: str        # 有效时间: "GTC"(撤销前有效) | "IOC"(立即成交或撤销) | "FOK"(全部成交或撤销)
    reduceOnly: bool        # 是否为仅减仓订单 - 只能减少持仓，不能增加
    closePosition: bool     # 是否为平仓订单 - 平掉当前持仓
    stopPrice: str          # 触发价格 - 条件单使用
    workingType: str        # 条件价格类型: "CONTRACT_PRICE" | "MARK_PRICE"
    priceProtect: bool      # 价格保护 - 防止价格偏离过大
    priceMatch: str         # 价格匹配模式: "NONE" | "OPPONENT" | "OPPONENT_5" 等
    selfTradePreventionMode: str  # 自成交防护: "EXPIRE_MAKER" | "EXPIRE_TAKER" 等
    goodTillDate: int       # GTD订单的过期时间戳


# 实际API响应示例:
BINANCE_ORDER_RESPONSE_EXAMPLE = {
    "orderId": 8389765962052627938,                    # int - 系统订单号
    "symbol": "ETHUSDT",                               # str - 交易对
    "status": "NEW",                                   # str - 订单状态 (刚提交时为NEW)
    "clientOrderId": "test_return_info_1758389407",    # str - 客户订单号
    "price": "0.00",                                   # str - 委托价格 (市价单为0)
    "avgPrice": "0.00",                                # str - 成交均价 (初始为0，成交后更新)
    "origQty": "0.005",                                # str - 委托数量 (0.005 ETH)
    "executedQty": "0.000",                            # str - 已成交数量 (初始为0)
    "cumQty": "0.000",                                 # str - 累计成交数量
    "cumQuote": "0.00000",                             # str - 累计成交金额 (初始为0 USDT)
    "timeInForce": "GTC",                              # str - 有效时间类型
    "type": "MARKET",                                  # str - 订单类型
    "reduceOnly": False,                               # bool - 是否仅减仓
    "closePosition": False,                            # bool - 是否平仓
    "side": "BUY",                                     # str - 买卖方向
    "positionSide": "LONG",                            # str - 持仓方向
    "stopPrice": "0.00",                               # str - 止损价格
    "workingType": "CONTRACT_PRICE",                   # str - 条件价格类型
    "priceProtect": False,                             # bool - 价格保护
    "origType": "MARKET",                              # str - 原始订单类型
    "priceMatch": "NONE",                              # str - 价格匹配
    "selfTradePreventionMode": "EXPIRE_MAKER",         # str - 自成交防护
    "goodTillDate": 0,                                 # int - GTD过期时间
    "updateTime": 1758389407259                        # int - 更新时间戳(毫秒)
}


# =============================================================================
# 2. 持仓查询数据结构 (get_binance_perp_positions 返回)
# =============================================================================

@dataclass
class BinancePositionInfo:
    """
    Binance期货持仓信息结构
    API: GET /fapi/v2/positionRisk
    """

    # 基本持仓信息
    symbol: str                # 交易对 - 如 "ETHUSDT"
    positionAmt: str          # 持仓数量 - 正数为多头，负数为空头，0为无持仓 ⭐ 关键字段
    entryPrice: str           # 入场价格 - 开仓均价 ⭐ 关键字段
    markPrice: str            # 标记价格 - 用于计算盈亏的参考价格

    # 盈亏信息
    unRealizedProfit: str     # 未实现盈亏(USDT) - 当前浮动盈亏 ⭐ 关键字段
    percentage: str           # 盈亏比例(%) - 投资回报率

    # 保证金和杠杆
    isolatedMargin: str       # 逐仓保证金 - 逐仓模式下的保证金金额
    notional: str            # 名义价值(USDT) - 持仓总价值 ⭐ 关键字段
    leverage: str            # 杠杆倍数 - 如 "1", "10", "20" ⭐ 关键字段

    # 持仓模式
    positionSide: str        # 持仓方向: "LONG"(多头) | "SHORT"(空头) | "BOTH"(双向持仓)

    # 风险管理
    maxNotionalValue: str    # 最大名义价值 - 该交易对最大可开仓金额
    marginType: str          # 保证金模式: "isolated"(逐仓) | "cross"(全仓)
    isolatedWallet: str      # 逐仓钱包余额
    updateTime: int          # 更新时间戳(毫秒)


# 实际API响应示例:
BINANCE_POSITION_RESPONSE_EXAMPLE = {
    "symbol": "ETHUSDT",                      # str - 交易对
    "positionAmt": "0.006",                   # str - 持仓数量 (0.006 ETH多头)
    "entryPrice": "4482.01",                  # str - 入场价格 ($4482.01)
    "markPrice": "4482.01485271",             # str - 标记价格
    "unRealizedProfit": "0.00002911",         # str - 未实现盈亏 (+$0.00002911)
    "percentage": "0",                        # str - 盈亏比例 (0%)
    "isolatedMargin": "0.00000000",           # str - 逐仓保证金 (全仓模式为0)
    "notional": "26.89208911",                # str - 名义价值 ($26.89)
    "leverage": "1",                          # str - 杠杆倍数 (1倍)
    "positionSide": "LONG",                   # str - 持仓方向 (多头)
    "maxNotionalValue": "1200000000",         # str - 最大名义价值
    "marginType": "cross",                    # str - 保证金模式 (全仓)
    "isolatedWallet": "0",                    # str - 逐仓钱包
    "updateTime": 1758389236000               # int - 更新时间戳
}


# =============================================================================
# 3. 价格查询数据结构 (get_binance_perp_price 返回)
# =============================================================================

@dataclass
class BinancePriceInfo:
    """
    Binance期货价格查询结构
    API: GET /fapi/v1/ticker/price
    """
    symbol: str    # 交易对
    price: str     # 最新价格 ⭐ 关键字段 (需要转为float使用)
    time: int      # 价格时间戳(毫秒)


# 实际API响应示例:
BINANCE_PRICE_RESPONSE_EXAMPLE = {
    "symbol": "ETHUSDT",        # str - 交易对
    "price": "4482.00",         # str - 最新价格 (需要float("4482.00")转换)
    "time": 1758389407000       # int - 价格时间戳
}


# =============================================================================
# 4. 数据存储建议
# =============================================================================

"""
数据库设计建议:

1. 订单表 (orders):
   - order_id (BIGINT PRIMARY KEY) - 来自orderId
   - client_order_id (VARCHAR) - 来自clientOrderId
   - symbol (VARCHAR) - 来自symbol
   - side (VARCHAR) - 来自side
   - position_side (VARCHAR) - 来自positionSide
   - orig_qty (DECIMAL) - 来自origQty (转换为数值)
   - executed_qty (DECIMAL) - 来自executedQty (转换为数值)
   - avg_price (DECIMAL) - 来自avgPrice (转换为数值)
   - cum_quote (DECIMAL) - 来自cumQuote (转换为数值)
   - status (VARCHAR) - 来自status
   - order_type (VARCHAR) - 来自type
   - created_at (TIMESTAMP) - 来自updateTime (毫秒转换为时间戳)
   - raw_response (JSON) - 存储完整原始响应

2. 持仓表 (positions):
   - symbol (VARCHAR PRIMARY KEY) - 来自symbol
   - position_amt (DECIMAL) - 来自positionAmt (转换为数值)
   - entry_price (DECIMAL) - 来自entryPrice (转换为数值)
   - mark_price (DECIMAL) - 来自markPrice (转换为数值)
   - unrealized_profit (DECIMAL) - 来自unRealizedProfit (转换为数值)
   - notional (DECIMAL) - 来自notional (转换为数值)
   - leverage (INTEGER) - 来自leverage (转换为整数)
   - position_side (VARCHAR) - 来自positionSide
   - updated_at (TIMESTAMP) - 来自updateTime (毫秒转换为时间戳)
   - raw_response (JSON) - 存储完整原始响应

3. 价格表 (prices):
   - symbol (VARCHAR) - 来自symbol
   - price (DECIMAL) - 来自price (转换为数值)
   - timestamp (TIMESTAMP) - 来自time (毫秒转换为时间戳)
   - PRIMARY KEY (symbol, timestamp)
"""


# =============================================================================
# 5. 数据转换工具函数
# =============================================================================

def convert_order_response(raw_response: Dict[str, Any]) -> Dict[str, Any]:
    """将Binance订单响应转换为适合存储的格式"""
    return {
        'order_id': int(raw_response['orderId']),
        'client_order_id': raw_response['clientOrderId'],
        'symbol': raw_response['symbol'],
        'side': raw_response['side'],
        'position_side': raw_response['positionSide'],
        'orig_qty': float(raw_response['origQty']),
        'executed_qty': float(raw_response['executedQty']),
        'avg_price': float(raw_response['avgPrice']) if raw_response['avgPrice'] != '0.00' else 0.0,
        'cum_quote': float(raw_response['cumQuote']),
        'status': raw_response['status'],
        'order_type': raw_response['type'],
        'created_at': raw_response['updateTime'] / 1000,  # 毫秒转秒
        'raw_response': raw_response
    }


def convert_position_response(raw_response: Dict[str, Any]) -> Dict[str, Any]:
    """将Binance持仓响应转换为适合存储的格式"""
    return {
        'symbol': raw_response['symbol'],
        'position_amt': float(raw_response['positionAmt']),
        'entry_price': float(raw_response['entryPrice']) if raw_response['entryPrice'] != '0.00' else 0.0,
        'mark_price': float(raw_response['markPrice']),
        'unrealized_profit': float(raw_response['unRealizedProfit']),
        'notional': float(raw_response['notional']),
        'leverage': int(raw_response['leverage']),
        'position_side': raw_response['positionSide'],
        'updated_at': raw_response['updateTime'] / 1000,  # 毫秒转秒
        'raw_response': raw_response
    }


def convert_price_response(raw_response: Dict[str, Any]) -> Dict[str, Any]:
    """将Binance价格响应转换为适合存储的格式"""
    raw_timestamp = raw_response.get('time') or raw_response.get('closeTime')
    if raw_timestamp:
        timestamp = float(raw_timestamp) / 1000
    else:
        timestamp = time.time()
    return {
        'symbol': raw_response['symbol'],
        'price': float(raw_response['price']),
        'timestamp': timestamp,
        'raw_response': raw_response
    }


# =============================================================================
# 6. 使用示例
# =============================================================================

if __name__ == "__main__":
    """数据结构使用示例"""

    # 示例1: 处理下单响应
    order_result = BINANCE_ORDER_RESPONSE_EXAMPLE
    converted_order = convert_order_response(order_result)
    print("转换后的订单数据:", converted_order)

    # 示例2: 处理持仓响应
    position_result = BINANCE_POSITION_RESPONSE_EXAMPLE
    converted_position = convert_position_response(position_result)
    print("转换后的持仓数据:", converted_position)

    # 示例3: 处理价格响应
    price_result = BINANCE_PRICE_RESPONSE_EXAMPLE
    converted_price = convert_price_response(price_result)
    print("转换后的价格数据:", converted_price)
