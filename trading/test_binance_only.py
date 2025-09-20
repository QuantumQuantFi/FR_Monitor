#!/usr/bin/env python3
"""
专门测试Binance trade executor的脚本
按照用户要求：先测试买入10U的ETHUSDT 1倍杠杆永续合约，然后市价卖出
"""

import time
from typing import Dict, Any
import trade_executor as te


def format_binance_result(result: Dict[str, Any]) -> str:
    """格式化Binance订单结果用于显示"""
    return f"订单ID: {result.get('orderId')}, " \
           f"状态: {result.get('status')}, " \
           f"币种: {result.get('symbol')}, " \
           f"方向: {result.get('side')}, " \
           f"执行数量: {result.get('executedQty')}, " \
           f"成交金额: {result.get('cumQuote')} USDT"


def test_binance_buy():
    """测试Binance买入ETHUSDT永续合约"""
    print("🔸 测试 Binance 买入 ETHUSDT 永续合约...")
    print("目标: 买入约23U价值的ETHUSDT，1倍杠杆 (满足Binance 20U最小要求)")

    try:
        # 使用说明:
        # - Binance 永续下单必须提供基础币种数量 (quantity)，不能传 USDT 名义金额。
        # - 0.007 ETH ≈ 23 USDT (视行情而定)，满足 Binance 20 USDT 的最小名义要求。
        # - position_side 在双向持仓模式下必填，单向模式可以省略。

        result = te.place_binance_perp_market_order(
            symbol="ETHUSDT",
            side="BUY",
            quantity=0.007,  # 0.007 ETH，约$23价值(假设ETH=$3300)，满足20 USDT最小要求
            position_side="LONG",  # 尝试LONG持仓方向
            client_order_id=f"test_buy_bn_{int(time.time())}"
        )

        print(f"✅ Binance 买入成功!")
        print(f"📄 订单详情: {format_binance_result(result)}")
        return result

    except te.TradeExecutionError as e:
        print(f"❌ Binance 买入失败: {e}")
        return None
    except Exception as e:
        print(f"❌ Binance 买入异常: {e}")
        return None


def test_binance_sell(buy_result: Dict[str, Any]):
    """测试Binance卖出ETHUSDT永续合约"""
    print("\n🔸 测试 Binance 卖出 ETHUSDT 永续合约...")

    if not buy_result:
        print("❌ 无法卖出：买入失败")
        return None

    try:
        # 获取买入的执行数量
        executed_qty = float(buy_result.get("executedQty", "0"))

        if executed_qty <= 0:
            print("❌ 无法卖出：买入执行数量为0")
            return None

        print(f"📊 将卖出数量: {executed_qty} ETH")

        result = te.place_binance_perp_market_order(
            symbol="ETHUSDT",
            side="SELL",
            quantity=executed_qty,  # 卖出买入的全部数量
            position_side="LONG",  # 平多仓
            client_order_id=f"test_sell_bn_{int(time.time())}"
        )

        print(f"✅ Binance 卖出成功!")
        print(f"📄 订单详情: {format_binance_result(result)}")
        return result

    except te.TradeExecutionError as e:
        print(f"❌ Binance 卖出失败: {e}")
        return None
    except Exception as e:
        print(f"❌ Binance 卖出异常: {e}")
        return None


def main():
    """主测试函数"""
    print("🚀 Binance Trade Executor 专项测试")
    print("=" * 60)
    print("测试目标:")
    print("  1. 买入约10U价值的ETHUSDT永续合约 (1倍杠杆)")
    print("  2. 市价卖出买入的合约")
    print("⚠️  请确保:")
    print("  - config_private.py 已配置Binance API密钥")
    print("  - API密钥有期货交易权限")
    print("  - 账户有足够USDT余额")
    print("=" * 60)

    # 步骤1: 测试买入
    buy_result = test_binance_buy()

    if buy_result:
        # 等待一下再卖出
        print("\n⏳ 等待3秒后执行卖出...")
        time.sleep(3)

        # 步骤2: 测试卖出
        sell_result = test_binance_sell(buy_result)

        # 打印最终结果
        print("\n" + "=" * 60)
        print("📊 测试结果汇总")
        print("=" * 60)

        if buy_result and sell_result:
            print("🎉 测试完全成功!")
            print("✅ 买入订单已执行")
            print("✅ 卖出订单已执行")

            # 计算盈亏
            buy_price = float(buy_result.get('avgPrice', '0'))
            sell_price = float(sell_result.get('avgPrice', '0'))
            quantity = float(buy_result.get('executedQty', '0'))

            if buy_price > 0 and sell_price > 0:
                pnl = (sell_price - buy_price) * quantity
                print(f"📈 交易概况:")
                print(f"   买入价格: ${buy_price}")
                print(f"   卖出价格: ${sell_price}")
                print(f"   交易数量: {quantity} ETH")
                print(f"   盈亏: ${pnl:.4f} USDT")
        elif buy_result:
            print("⚠️  买入成功，但卖出失败")
            print("⚠️  请手动平仓或检查卖出逻辑")
        else:
            print("❌ 测试失败，买入未成功")

    else:
        print("\n❌ 买入失败，跳过卖出测试")
        print("请检查:")
        print("  - API密钥配置是否正确")
        print("  - 是否有足够的账户余额")
        print("  - API密钥是否有期货交易权限")
        print("  - IP是否在白名单中")


if __name__ == "__main__":
    main()
