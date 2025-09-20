#!/usr/bin/env python3
"""Binance ETH 永续合约详细交易测试脚本。

脚本展示三件事:
1. 如何通过 :func:`trade_executor.get_binance_perp_positions` 查询当前持仓。
2. 如何把 "想交易的 USDT 金额" 换算成必须提交给 Binance 的 "ETH 数量"。
3. 如何调用 :func:`trade_executor.place_binance_perp_market_order` 依次买入、卖出。

注意: Binance U 本位永续只接受 `quantity` (基础币种数量)，不能直接传 USDT 金额。
"""

import time
import sys
import os
from typing import Dict, Any, List

# 添加父目录到路径以便导入配置
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import trade_executor as te


def print_separator(title: str = ""):
    """打印分隔线"""
    if title:
        print(f"\n{'='*20} {title} {'='*20}")
    else:
        print("="*60)


def format_position_detail(position: Dict[str, Any]) -> str:
    """格式化持仓详情"""
    return f"""
📊 持仓详情:
   币种: {position.get('symbol', 'N/A')}
   持仓数量: {position.get('positionAmt', '0')} ETH
   持仓方向: {'多仓' if float(position.get('positionAmt', '0')) > 0 else '空仓' if float(position.get('positionAmt', '0')) < 0 else '无持仓'}
   入场价格: ${position.get('entryPrice', '0')}
   标记价格: ${position.get('markPrice', '0')}
   持仓价值: ${position.get('notional', '0')} USDT
   未实现盈亏: ${position.get('unRealizedProfit', '0')} USDT
   盈亏比例: {position.get('percentage', '0')}%
   保证金: ${position.get('isolatedMargin', '0')} USDT
   杠杆倍数: {position.get('leverage', '0')}x
   持仓模式: {position.get('positionSide', 'N/A')}
   最大可减仓: ${position.get('maxNotionalValue', '0')} USDT
   """


def format_order_detail(order: Dict[str, Any]) -> str:
    """格式化订单详情"""
    return f"""
📄 订单详情:
   订单ID: {order.get('orderId', 'N/A')}
   客户订单ID: {order.get('clientOrderId', 'N/A')}
   币种: {order.get('symbol', 'N/A')}
   订单状态: {order.get('status', 'N/A')}
   订单类型: {order.get('type', 'N/A')}
   方向: {order.get('side', 'N/A')}
   持仓模式: {order.get('positionSide', 'N/A')}
   原始数量: {order.get('origQty', '0')} ETH
   执行数量: {order.get('executedQty', '0')} ETH
   累计成交金额: ${order.get('cumQuote', '0')} USDT
   平均价格: ${order.get('avgPrice', '0')}
   订单时间: {order.get('time', 'N/A')}
   更新时间: {order.get('updateTime', 'N/A')}
   """


def query_position_detailed(symbol: str = "ETHUSDT") -> Dict[str, Any]:
    """查询详细持仓信息"""
    print(f"🔍 查询 {symbol} 持仓...")

    try:
        positions = te.get_binance_perp_positions(symbol=symbol)

        if not positions:
            print(f"❌ 未找到 {symbol} 持仓数据")
            return {}

        # 查找非零持仓
        for pos in positions:
            position_amt = float(pos.get('positionAmt', '0'))
            if position_amt != 0:
                print(f"✅ 找到 {symbol} 持仓:")
                print(format_position_detail(pos))
                return pos

        print(f"ℹ️  {symbol} 当前无持仓")
        return positions[0] if positions else {}

    except Exception as e:
        print(f"❌ 查询持仓失败: {e}")
        return {}


def derive_quantity_from_usdt(symbol: str, notional_usdt: float) -> float:
    """将目标 USDT 金额转换为基础币种数量, 方便传给下单函数."""
    current_price = te.get_binance_perp_price(symbol)
    quantity = notional_usdt / current_price

    # ETH 永续最小步长 0.001, 保留三位小数足够
    quantity = round(quantity, 3)

    print(f"🧮 {symbol} 最新价格: ${current_price:.2f}")
    print(f"🧮 目标名义 {notional_usdt} USDT ≈ {quantity} {symbol.replace('USDT', '')}")
    print("ℹ️  请记得 Binance 只接受基础币种数量 (例如 ETH) 作为 quantity 参数")
    return quantity


def execute_buy_order(quantity_eth: float = 0.006) -> Dict[str, Any]:  # 直接使用0.006ETH
    """执行买入订单"""
    print_separator("买入订单")
    print(f"🎯 目标: 买入 {quantity_eth} ETH 的ETHUSDT永续合约")

    # 获取当前价格用于显示预估金额
    current_price = te.get_binance_perp_price("ETHUSDT")
    estimated_usdt = quantity_eth * current_price

    print(f"🧮 ETHUSDT 最新价格: ${current_price:.2f}")
    print(f"🧮 {quantity_eth} ETH ≈ ${estimated_usdt:.2f} USDT")
    print(f"📊 下单参数: quantity={quantity_eth} ETH (基础币种数量)")

    try:
        print(f"\n🚀 执行买入订单...")
        result = te.place_binance_perp_market_order(
            symbol="ETHUSDT",
            side="BUY",
            quantity=quantity_eth,  # 直接使用指定的ETH数量
            position_side="LONG",
            client_order_id=f"detailed_buy_{int(time.time())}"
        )

        print(f"✅ 买入订单提交成功!")
        print(format_order_detail(result))
        return result

    except te.TradeExecutionError as e:
        print(f"❌ 买入订单失败: {e}")
        return {}
    except Exception as e:
        print(f"❌ 买入订单异常: {e}")
        return {}


def execute_sell_order(position_amt: float) -> Dict[str, Any]:
    """执行卖出订单"""
    print_separator("卖出订单")
    print(f"🎯 目标: 卖出持仓 {position_amt} ETH (基础币种数量)")

    if position_amt <= 0:
        print("❌ 无有效持仓可卖出")
        return {}

    try:
        print(f"\n🚀 执行卖出订单...")
        result = te.place_binance_perp_market_order(
            symbol="ETHUSDT",
            side="SELL",
            quantity=abs(position_amt),
            position_side="LONG",
            client_order_id=f"detailed_sell_{int(time.time())}"
        )

        print(f"✅ 卖出订单提交成功!")
        print(format_order_detail(result))
        return result

    except te.TradeExecutionError as e:
        print(f"❌ 卖出订单失败: {e}")
        return {}
    except Exception as e:
        print(f"❌ 卖出订单异常: {e}")
        return {}


def calculate_trading_summary(buy_order: Dict[str, Any], sell_order: Dict[str, Any]) -> None:
    """计算交易汇总"""
    print_separator("交易汇总")

    if not buy_order or not sell_order:
        print("⚠️  交易不完整，无法计算汇总")
        return

    try:
        # 提取关键数据
        buy_qty = float(buy_order.get('executedQty', '0'))
        sell_qty = float(sell_order.get('executedQty', '0'))
        buy_value = float(buy_order.get('cumQuote', '0'))
        sell_value = float(sell_order.get('cumQuote', '0'))
        buy_price = float(buy_order.get('avgPrice', '0'))
        sell_price = float(sell_order.get('avgPrice', '0'))

        # 计算盈亏
        pnl_usdt = sell_value - buy_value
        pnl_percentage = (pnl_usdt / buy_value * 100) if buy_value > 0 else 0

        print(f"📈 交易统计:")
        print(f"   买入数量: {buy_qty} ETH")
        print(f"   卖出数量: {sell_qty} ETH")
        print(f"   买入均价: ${buy_price}")
        print(f"   卖出均价: ${sell_price}")
        print(f"   买入金额: ${buy_value} USDT")
        print(f"   卖出金额: ${sell_value} USDT")
        print(f"   净盈亏: ${pnl_usdt:.4f} USDT")
        print(f"   盈亏比例: {pnl_percentage:.4f}%")

        if pnl_usdt > 0:
            print(f"🎉 盈利: +${pnl_usdt:.4f} USDT")
        elif pnl_usdt < 0:
            print(f"📉 亏损: ${pnl_usdt:.4f} USDT")
        else:
            print(f"🤝 打平: ${pnl_usdt:.4f} USDT")

    except Exception as e:
        print(f"❌ 计算交易汇总失败: {e}")


def main():
    """主测试函数"""
    print("🚀 详细的Binance ETH永续合约交易测试")
    print("目标: 买入0.006ETH → 卖出 → 查询持仓")
    print_separator()

    # 步骤1: 查询初始持仓
    print_separator("初始持仓查询")
    initial_position = query_position_detailed()
    initial_amt = float(initial_position.get('positionAmt', '0'))
    print(f"📊 初始持仓: {initial_amt} ETH")

    # 步骤2: 执行买入
    buy_result = execute_buy_order(0.006)  # 使用0.006 ETH进行测试

    if not buy_result:
        print("❌ 买入失败，终止测试")
        return

    # 等待订单处理
    print(f"\n⏳ 等待5秒让订单完全处理...")
    time.sleep(5)

    # 步骤3: 查询买入后持仓
    print_separator("买入后持仓查询")
    position_after_buy = query_position_detailed()
    position_amt = float(position_after_buy.get('positionAmt', '0'))

    if position_amt == 0:
        print("⚠️  买入后持仓为0，可能订单还在处理中")
        print("⏳ 再等待5秒...")
        time.sleep(5)
        position_after_buy = query_position_detailed()
        position_amt = float(position_after_buy.get('positionAmt', '0'))

    # 步骤4: 执行卖出
    sell_result = {}
    if position_amt > 0:
        sell_result = execute_sell_order(position_amt)

        if sell_result:
            # 等待订单处理
            print(f"\n⏳ 等待5秒让卖出订单完全处理...")
            time.sleep(5)
    else:
        print("❌ 无持仓可卖出")

    # 步骤5: 查询最终持仓
    print_separator("最终持仓查询")
    final_position = query_position_detailed()
    final_amt = float(final_position.get('positionAmt', '0'))
    print(f"📊 最终持仓: {final_amt} ETH")

    # 步骤6: 计算交易汇总
    calculate_trading_summary(buy_result, sell_result)

    # 步骤7: 最终状态
    print_separator("测试完成")
    if buy_result and sell_result and final_amt == 0:
        print("🎉 测试完全成功!")
        print("✅ 买入订单已执行")
        print("✅ 卖出订单已执行")
        print("✅ 持仓已清零")
    elif buy_result and not sell_result:
        print("⚠️  买入成功但卖出失败")
        print(f"⚠️  当前持仓: {position_amt} ETH")
        print("💡 建议手动平仓")
    else:
        print("❌ 测试未完全成功")
        print("请检查订单状态和持仓情况")


if __name__ == "__main__":
    main()
