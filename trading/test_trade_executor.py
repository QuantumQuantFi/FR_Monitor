#!/usr/bin/env python3
"""
Test script for trade executor functionality.
Tests buying 10U worth of ETHUSDT perpetual contracts with 1x leverage on all exchanges,
then selling them back.
"""

import time
from typing import Dict, Any, List
import trade_executor as te


def format_order_result(exchange: str, result: Dict[str, Any]) -> str:
    """Format order result for display."""
    if exchange == "binance":
        return f"Order ID: {result.get('orderId')}, Status: {result.get('status')}, " \
               f"Symbol: {result.get('symbol')}, Side: {result.get('side')}, " \
               f"Executed Qty: {result.get('executedQty')}, " \
               f"Cumulative Quote Qty: {result.get('cumQuote')}"
    elif exchange == "okx":
        order_data = result.get('data', [{}])[0] if result.get('data') else {}
        return f"Order ID: {order_data.get('ordId')}, Client Order ID: {order_data.get('clOrdId')}, " \
               f"Symbol: {order_data.get('instId')}, Side: {order_data.get('side')}"
    elif exchange == "bybit":
        result_data = result.get('result', {})
        return f"Order ID: {result_data.get('orderId')}, Order Link ID: {result_data.get('orderLinkId')}, " \
               f"Symbol: {result_data.get('symbol')}, Side: {result_data.get('side')}"
    elif exchange == "bitget":
        result_data = result.get('data', {})
        return f"Order ID: {result_data.get('orderId')}, Client Order ID: {result_data.get('clientOid')}, " \
               f"Symbol: {result_data.get('symbol')}, Side: {result_data.get('side')}"
    else:
        return str(result)


def test_buy_orders():
    """Test buying 10U worth of ETHUSDT contracts on all exchanges."""
    print("=" * 80)
    print("开始测试买入订单 - 每个交易所买入10U的ETHUSDT永续合约")
    print("=" * 80)

    results = {}

    # Test Binance
    print("\n🔸 测试 Binance...")
    try:
        # 问题修复记录:
        # 使用说明:
        # Binance 永续 API 仅接受基础币种数量 (quantity)。
        # 这里直接填写 0.003 ETH，约等于 10 USDT (视当前价格浮动)。
        result = te.place_binance_perp_market_order(
            symbol="ETHUSDT",
            side="BUY",
            quantity=0.003,  # 0.003 ETH ≈ $10 at $3000/ETH
            client_order_id=f"test_buy_bn_{int(time.time())}"
        )
        results["binance"] = {"success": True, "data": result}
        print(f"✅ Binance 买入成功: {format_order_result('binance', result)}")
    except te.TradeExecutionError as e:
        results["binance"] = {"success": False, "error": str(e)}
        print(f"❌ Binance 买入失败: {e}")
    except Exception as e:
        results["binance"] = {"success": False, "error": str(e)}
        print(f"❌ Binance 买入异常: {e}")

    time.sleep(1)  # Avoid rate limiting

    # Test OKX
    print("\n🔸 测试 OKX...")
    try:
        try:
            leverage_resp = te.set_okx_swap_leverage(
                symbol="ETH-USDT-SWAP",
                leverage=1,
                td_mode="cross",
            )
            leverage_info = leverage_resp.get("data", [{}])[0] if leverage_resp.get("data") else {}
            print(
                "ℹ️  OKX 杠杆设置成功: "
                f"{leverage_info.get('lever', '1')}x / {leverage_info.get('mgnMode', 'cross')}"
            )
        except te.TradeExecutionError as lev_err:
            print(f"⚠️ OKX 杠杆设置失败（继续使用现有设置）: {lev_err}")

        target_notional = None
        derived_size = None
        last_error = None
        for candidate in (30, 40, 50, 60, 80):
            try:
                derived_size = te.derive_okx_swap_size_from_usdt(
                    symbol="ETH-USDT-SWAP",
                    notional_usdt=candidate,
                )
                target_notional = candidate
                break
            except te.TradeExecutionError as err:
                last_error = err
                continue

        if derived_size is None:
            raise last_error or te.TradeExecutionError("无法计算合约数量")

        print(f"ℹ️  OKX 目标名义 {target_notional} USDT -> 合约数量 {derived_size}")

        result = te.place_okx_swap_market_order(
            symbol="ETH-USDT-SWAP",
            side="buy",
            size=derived_size,
            client_order_id=f"test_buy_okx_{int(time.time())}"
        )
        results["okx"] = {"success": True, "data": result, "context": {"size": derived_size}}
        print(f"✅ OKX 买入成功: {format_order_result('okx', result)}")
    except te.TradeExecutionError as e:
        results["okx"] = {"success": False, "error": str(e)}
        print(f"❌ OKX 买入失败: {e}")
    except Exception as e:
        results["okx"] = {"success": False, "error": str(e)}
        print(f"❌ OKX 买入异常: {e}")

    time.sleep(1)

    # Test Bybit
    print("\n🔸 测试 Bybit...")
    try:
        # For Bybit, qty is in base currency (ETH)
        # Let's use a small amount for testing
        result = te.place_bybit_linear_market_order(
            symbol="ETHUSDT",
            side="Buy",
            qty="0.003",  # 0.003 ETH ≈ $10 at $3000/ETH
            client_order_id=f"test_buy_bybit_{int(time.time())}"
        )
        results["bybit"] = {"success": True, "data": result}
        print(f"✅ Bybit 买入成功: {format_order_result('bybit', result)}")
    except te.TradeExecutionError as e:
        results["bybit"] = {"success": False, "error": str(e)}
        print(f"❌ Bybit 买入失败: {e}")
    except Exception as e:
        results["bybit"] = {"success": False, "error": str(e)}
        print(f"❌ Bybit 买入异常: {e}")

    time.sleep(1)

    # Test Bitget
    print("\n🔸 测试 Bitget...")
    try:
        # 问题修复记录:
        # 原问题: size="0.003" 导致 "less than the minimum order quantity" 错误
        # 原因: Bitget ETHUSDT永续合约最小订单数量大于0.003
        # 修改: 增加到0.01 ETH (约$30，确保满足最小数量要求)
        result = te.place_bitget_usdt_perp_market_order(
            symbol="ETHUSDT_UMCBL",
            side="buy",
            size="0.01",  # 增加到0.01 ETH以满足最小订单要求
            client_order_id=f"test_buy_bitget_{int(time.time())}"
        )
        results["bitget"] = {"success": True, "data": result}
        print(f"✅ Bitget 买入成功: {format_order_result('bitget', result)}")
    except te.TradeExecutionError as e:
        results["bitget"] = {"success": False, "error": str(e)}
        print(f"❌ Bitget 买入失败: {e}")
    except Exception as e:
        results["bitget"] = {"success": False, "error": str(e)}
        print(f"❌ Bitget 买入异常: {e}")

    return results


def test_sell_orders(buy_results: Dict[str, Any]):
    """Test selling back the positions."""
    print("\n" + "=" * 80)
    print("开始测试卖出订单 - 市价卖出之前买入的合约")
    print("=" * 80)

    results = {}

    # Test Binance sell
    if buy_results.get("binance", {}).get("success"):
        print("\n🔸 测试 Binance 卖出...")
        try:
            # Get the executed quantity from buy order
            buy_data = buy_results["binance"]["data"]
            executed_qty = float(buy_data.get("executedQty", "0"))

            if executed_qty > 0:
                result = te.place_binance_perp_market_order(
                    symbol="ETHUSDT",
                    side="SELL",
                    quantity=executed_qty,
                    client_order_id=f"test_sell_bn_{int(time.time())}"
                )
                results["binance"] = {"success": True, "data": result}
                print(f"✅ Binance 卖出成功: {format_order_result('binance', result)}")
            else:
                results["binance"] = {"success": False, "error": "No quantity to sell"}
                print("❌ Binance 卖出失败: 没有持仓数量")
        except Exception as e:
            results["binance"] = {"success": False, "error": str(e)}
            print(f"❌ Binance 卖出异常: {e}")
    else:
        print("⏭️  跳过 Binance 卖出 (买入失败)")

    time.sleep(1)

    # Test OKX sell
    if buy_results.get("okx", {}).get("success"):
        print("\n🔸 测试 OKX 卖出...")
        try:
            okx_size = "1"
            context = buy_results.get("okx", {}).get("context") or {}
            okx_size = context.get("size", okx_size)
            result = te.place_okx_swap_market_order(
                symbol="ETH-USDT-SWAP",
                side="sell",
                size=okx_size,
                client_order_id=f"test_sell_okx_{int(time.time())}"
            )
            results["okx"] = {"success": True, "data": result}
            print(f"✅ OKX 卖出成功: {format_order_result('okx', result)}")
        except Exception as e:
            results["okx"] = {"success": False, "error": str(e)}
            print(f"❌ OKX 卖出异常: {e}")
    else:
        print("⏭️  跳过 OKX 卖出 (买入失败)")

    time.sleep(1)

    # Test Bybit sell
    if buy_results.get("bybit", {}).get("success"):
        print("\n🔸 测试 Bybit 卖出...")
        try:
            result = te.place_bybit_linear_market_order(
                symbol="ETHUSDT",
                side="Sell",
                qty="0.003",  # Same qty as buy order
                client_order_id=f"test_sell_bybit_{int(time.time())}"
            )
            results["bybit"] = {"success": True, "data": result}
            print(f"✅ Bybit 卖出成功: {format_order_result('bybit', result)}")
        except Exception as e:
            results["bybit"] = {"success": False, "error": str(e)}
            print(f"❌ Bybit 卖出异常: {e}")
    else:
        print("⏭️  跳过 Bybit 卖出 (买入失败)")

    time.sleep(1)

    # Test Bitget sell
    if buy_results.get("bitget", {}).get("success"):
        print("\n🔸 测试 Bitget 卖出...")
        try:
            # 修改: 卖出数量与买入数量保持一致，改为0.01 ETH
            result = te.place_bitget_usdt_perp_market_order(
                symbol="ETHUSDT_UMCBL",
                side="sell",
                size="0.01",  # 与买入数量一致
                client_order_id=f"test_sell_bitget_{int(time.time())}"
            )
            results["bitget"] = {"success": True, "data": result}
            print(f"✅ Bitget 卖出成功: {format_order_result('bitget', result)}")
        except Exception as e:
            results["bitget"] = {"success": False, "error": str(e)}
            print(f"❌ Bitget 卖出异常: {e}")
    else:
        print("⏭️  跳过 Bitget 卖出 (买入失败)")

    return results


def print_summary(buy_results: Dict[str, Any], sell_results: Dict[str, Any]):
    """Print test summary."""
    print("\n" + "=" * 80)
    print("测试结果总结")
    print("=" * 80)

    exchanges = ["binance", "okx", "bybit", "bitget"]

    for exchange in exchanges:
        print(f"\n📊 {exchange.upper()}:")

        buy_status = "✅ 成功" if buy_results.get(exchange, {}).get("success") else "❌ 失败"
        sell_status = "✅ 成功" if sell_results.get(exchange, {}).get("success") else "❌ 失败"

        print(f"  买入: {buy_status}")
        if not buy_results.get(exchange, {}).get("success"):
            print(f"    错误: {buy_results.get(exchange, {}).get('error', 'N/A')}")

        print(f"  卖出: {sell_status}")
        if not sell_results.get(exchange, {}).get("success"):
            print(f"    错误: {sell_results.get(exchange, {}).get('error', 'N/A')}")

    # Count successes
    successful_buys = sum(1 for r in buy_results.values() if r.get("success"))
    successful_sells = sum(1 for r in sell_results.values() if r.get("success"))

    print(f"\n📈 总体统计:")
    print(f"  成功买入: {successful_buys}/4 个交易所")
    print(f"  成功卖出: {successful_sells}/4 个交易所")

    if successful_buys == 4 and successful_sells == 4:
        print("🎉 所有测试完全成功!")
    elif successful_buys > 0:
        print("⚠️  部分测试成功，请检查失败的交易所配置")
    else:
        print("❌ 所有测试失败，请检查配置和凭据")


def main():
    """Main test function."""
    print("🚀 Trade Executor 测试脚本")
    print("测试目标: 每个交易所买入约10U的ETHUSDT永续合约，然后市价卖出")
    print("⚠️  请确保:")
    print("  1. config_private.py 文件已正确配置")
    print("  2. 各交易所API密钥有期货交易权限")
    print("  3. 账户有足够余额进行测试")

    print("\n🚀 开始自动测试...")

    try:
        # Test buy orders
        buy_results = test_buy_orders()

        # Wait a bit before selling
        print("\n⏳ 等待3秒后开始卖出测试...")
        time.sleep(3)

        # Test sell orders
        sell_results = test_sell_orders(buy_results)

        # Print summary
        print_summary(buy_results, sell_results)

    except KeyboardInterrupt:
        print("\n\n❌ 测试被用户中断")
    except Exception as e:
        print(f"\n\n❌ 测试过程中发生未预期错误: {e}")


if __name__ == "__main__":
    main()
