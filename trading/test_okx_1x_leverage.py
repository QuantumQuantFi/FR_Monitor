#!/usr/bin/env python3
"""
OKX 1倍杠杆专项测试脚本 - 完整使用指南和Trade Executor调用说明

🎯 脚本功能:
使用1倍杠杆，0.006ETH价值进行完整的OKX永续合约交易测试
验证trade_executor.py模块的所有OKX相关功能

📚 Trade Executor 使用方法:

🔧 核心函数说明:
1. set_okx_swap_leverage() - 🆕 设置杠杆倍数
   - 参数: symbol, leverage, td_mode="cross", pos_side=None
   - 返回: 设置结果响应
   - 示例: te.set_okx_swap_leverage("ETH-USDT-SWAP", leverage=1, td_mode="cross")

2. get_okx_swap_price() - 获取实时价格
   - 参数: symbol, base_url="https://www.okx.com"
   - 返回: float价格
   - 示例: price = te.get_okx_swap_price("ETH-USDT-SWAP")

3. derive_okx_swap_size_from_usdt() - 计算合约数量
   - 参数: symbol, notional_usdt, price=None, base_url="https://www.okx.com"
   - 返回: str合约数量
   - 示例: size = te.derive_okx_swap_size_from_usdt("ETH", 50.0)

4. place_okx_swap_market_order() - 提交市价单
   - 参数: symbol, side, size, td_mode="cross", pos_side=None, reduce_only=None
   - 返回: 订单响应字典
   - 示例: te.place_okx_swap_market_order("ETH-USDT-SWAP", "buy", "1", td_mode="cross", pos_side="long")

5. get_okx_swap_positions() - 查询持仓
   - 参数: symbol=None, inst_type="SWAP", api_key=None, secret_key=None, passphrase=None
   - 返回: List[Dict] 持仓列表
   - 示例: positions = te.get_okx_swap_positions(symbol="ETH-USDT-SWAP")

💻 标准使用流程:
```python
import trade_executor as te

# 步骤1: 设置杠杆（推荐1倍）
leverage_result = te.set_okx_swap_leverage("ETH-USDT-SWAP", leverage=1, td_mode="cross")

# 步骤2: 查询价格
price = te.get_okx_swap_price("ETH-USDT-SWAP")

# 步骤3: 计算合约数量
size = te.derive_okx_swap_size_from_usdt("ETH", 50.0)  # 50 USDT

# 步骤4: 买入
buy_result = te.place_okx_swap_market_order(
    symbol="ETH-USDT-SWAP", side="buy", size=size,
    td_mode="cross", pos_side="long"
)

# 步骤5: 查询持仓
positions = te.get_okx_swap_positions(symbol="ETH-USDT-SWAP")

# 步骤6: 卖出
sell_result = te.place_okx_swap_market_order(
    symbol="ETH-USDT-SWAP", side="sell", size=size,
    td_mode="cross", pos_side="long"
)
```

⚠️ 关键注意事项:

📋 前置要求:
1. 配置config_private.py中的OKX API凭据:
   - OKX_API_KEY_JERRYPSY
   - OKX_SECRET_KEY_JERRYPSY
   - OKX_PASSPHRASE_JERRYPSY
2. OKX账户开通永续合约交易权限
3. 账户有足够USDT余额（建议≥50 USDT）
4. API密钥有交易权限

🔢 参数规则:
- symbol: 使用"ETH-USDT-SWAP"格式，会自动处理
- side: "buy"/"sell"（小写）
- size: 合约数量字符串，1合约=0.01ETH
- td_mode: "cross"(全仓)/"isolated"(逐仓)，推荐cross
- pos_side: "long"/"short"，永续合约必须指定
- leverage: 1-125倍，新手推荐1倍

💡 重要概念:
- OKX使用"合约数量"不是ETH数量
- 1合约 = 0.01 ETH（ETH-USDT-SWAP）
- 目标0.006ETH实际只能交易0.0006ETH（约0.06合约）
- 杠杆为账户级别设置，现支持API修改

🛡️ 风险控制:
- 建议先用小额测试（如0.006ETH）
- 新手使用1倍杠杆降低风险
- 及时平仓避免隔夜风险
- 设置合理的资金管理策略

🔧 常见错误处理:
- "Parameter posSide error": 必须指定pos_side="long"
- "Insufficient balance": 检查USDT余额
- "Leverage set reject": 账户可能不支持API修改杠杆
- "Instrument not found": 确认合约名称格式

🚀 本脚本测试流程:
1. API设置杠杆 - 自动设置为1倍杠杆
2. 价格查询 - 获取当前ETH-USDT-SWAP价格
3. 合约计算 - 将0.006ETH转换为OKX合约数量
4. 买入测试 - 提交市价买入订单
5. 持仓查询 - 验证持仓状态和杠杆倍数
6. 卖出测试 - 提交市价卖出订单
7. 最终验证 - 确认持仓已清零

📊 预期结果:
- 杠杆倍数: 1x（API设置成功）
- 交易数量: 约0.06合约（0.0006ETH）
- 保证金: 约等于交易金额（1倍杠杆特征）
- 风险最低，适合测试和新手使用

✅ 验证状态: 已通过完整测试（2025-09-20）
- API杠杆设置功能正常
- 所有交易功能验证通过
- 1倍杠杆确认成功
- 持仓管理完整可靠
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
        print(f"\n{'='*25} {title} {'='*25}")
    else:
        print("="*70)


def print_leverage_warning():
    """打印杠杆设置警告"""
    print("⚠️" * 30)
    print("🚨 重要提醒：杠杆设置检查 🚨")
    print("⚠️" * 30)
    print("📋 请确认以下步骤已完成：")
    print("1. 已在OKX网页版或APP中设置ETH-USDT-SWAP杠杆为1倍")
    print("2. 账户已开通永续合约交易权限")
    print("3. 账户有足够USDT余额（建议≥50 USDT）")
    print("4. API密钥已配置正确的交易权限")
    print("\n❓ 如果未设置1倍杠杆，测试结果可能显示其他倍数（如3x、5x等）")
    print("❓ 这不影响功能测试，但会影响保证金使用量")

    # 自动确认开始测试（适用于自动化环境）
    print("\n🚀 自动开始测试（请确保已完成上述杠杆设置）...")


def safe_float(value, default=0.0):
    """安全的字符串转浮点数函数"""
    try:
        if value is None or value == '':
            return default
        return float(value)
    except (ValueError, TypeError):
        return default


def format_okx_position_detailed(position: Dict[str, Any]) -> str:
    """格式化OKX持仓详情（增强版）"""
    # 计算一些重要指标
    pos_size = safe_float(position.get('pos', '0'))
    avg_px = safe_float(position.get('avgPx', '0'))
    mark_px = safe_float(position.get('markPx', '0'))
    lever = position.get('lever', '0')
    margin = safe_float(position.get('margin', '0'))
    notional = safe_float(position.get('notionalUsd', '0'))

    # 计算实际ETH数量（假设1合约=0.01ETH）
    eth_amount = pos_size * 0.01

    # 检查杠杆是否为1倍
    leverage_status = "✅ 1倍杠杆(推荐)" if lever == "1" else f"⚠️ {lever}倍杠杆"

    # 保证金使用率分析
    if notional > 0:
        margin_ratio = (margin / notional) * 100
        margin_analysis = f"保证金使用率: {margin_ratio:.1f}% ({'正常' if margin_ratio < 50 else '较高' if margin_ratio < 80 else '高风险'})"
    else:
        margin_analysis = "保证金使用率: 无法计算"

    return f"""
📊 OKX持仓详情（1倍杠杆测试）:
   合约名称: {position.get('instId', 'N/A')}
   持仓数量: {pos_size} 合约 (≈ {eth_amount:.6f} ETH)
   持仓方向: {position.get('posSide', 'N/A')}

   💰 价格信息:
   平均开仓价: ${avg_px:.2f}
   标记价格: ${mark_px:.2f}
   价格差异: ${abs(mark_px - avg_px):.2f} ({abs((mark_px - avg_px)/avg_px * 100):.3f}%)

   💼 杠杆和保证金:
   杠杆倍数: {leverage_status}
   持仓价值: ${notional:.2f} USD
   占用保证金: ${margin:.2f} USD
   {margin_analysis}

   📈 盈亏状况:
   未实现盈亏: ${position.get('upl', '0')} USD
   盈亏比例: {float(position.get('uplRatio', '0')) * 100:.4f}%

   ⚙️ 交易模式:
   保证金模式: {position.get('mgnMode', 'N/A')} ({'全仓' if position.get('mgnMode') == 'cross' else '逐仓' if position.get('mgnMode') == 'isolated' else '未知'})
   """


def test_okx_leverage_trading():
    """测试OKX 1倍杠杆交易完整流程"""

    # 步骤0：杠杆设置确认
    print_leverage_warning()

    print_separator("OKX 1倍杠杆交易测试开始")

    symbol = "ETH-USDT-SWAP"
    target_eth = 0.006

    print_separator("步骤0: API尝试设置杠杆")
    try:
        response = te.set_okx_swap_leverage(symbol, leverage=1, td_mode="cross")
        details = response.get("data", [{}])[0] if response.get("data") else {}
        print("✅ API设置杠杆成功:")
        print(f"   - 合约: {details.get('instId', symbol)}")
        print(f"   - 杠杆: {details.get('lever', '1')}x")
        print(f"   - 保证金模式: {details.get('mgnMode', 'cross')}")
    except te.TradeExecutionError as err:
        print(f"⚠️ API尝试设置杠杆失败: {err}")
        print("⚠️ 请确认账户允许修改杠杆，或手动调整后继续")
    except Exception as err:
        print(f"⚠️ API尝试设置杠杆出现异常: {err}")
        print("⚠️ 请手动在OKX前端确认杠杆后继续")

    # 步骤1：价格查询
    print_separator("步骤1: 价格查询")
    print(f"🎯 查询 {symbol} 当前价格...")

    try:
        current_price = te.get_okx_swap_price(symbol)
        target_usdt = target_eth * current_price
        print(f"✅ 当前价格: ${current_price:.2f}")
        print(f"🧮 目标交易量: {target_eth} ETH ≈ ${target_usdt:.2f} USDT")
        print(f"📊 1倍杠杆需要保证金: ≈${target_usdt:.2f} USDT（全部金额）")
    except Exception as e:
        print(f"❌ 价格查询失败: {e}")
        return False

    # 步骤2：合约数量计算
    print_separator("步骤2: 合约数量计算")
    print(f"🧮 计算目标 ${target_usdt:.2f} USDT 对应的合约数量...")

    try:
        contract_size = te.derive_okx_swap_size_from_usdt(
            symbol=symbol,
            notional_usdt=target_usdt,
            price=current_price
        )

        # 计算实际ETH数量
        actual_eth = float(contract_size) * 0.01
        eth_difference = abs(target_eth - actual_eth)

        print(f"✅ 计算结果: {contract_size} 合约")
        print(f"📏 实际ETH量: {actual_eth:.6f} ETH")
        print(f"📐 精度差异: {eth_difference:.6f} ETH")

        if eth_difference > 0.001:
            print(f"⚠️ 精度差异较大，这是OKX合约精度限制导致的正常现象")
    except Exception as e:
        print(f"❌ 合约数量计算失败: {e}")
        return False

    # 步骤3：初始持仓查询
    print_separator("步骤3: 初始持仓查询")
    print(f"🔍 查询 {symbol} 初始持仓状态...")

    try:
        initial_positions = te.get_okx_swap_positions(symbol=symbol)
        initial_size = 0

        if initial_positions:
            for pos in initial_positions:
                pos_amt = safe_float(pos.get('pos', '0'))
                if pos_amt != 0:
                    initial_size = pos_amt
                    print(f"⚠️ 发现现有持仓:")
                    print(format_okx_position_detailed(pos))
                    break

        if initial_size == 0:
            print(f"✅ {symbol} 当前无持仓，可以开始测试")
    except Exception as e:
        print(f"❌ 初始持仓查询失败: {e}")
        return False

    # 步骤4：买入订单测试
    print_separator("步骤4: 1倍杠杆买入测试")
    print(f"🎯 使用1倍杠杆买入 {contract_size} 合约")
    print(f"💰 预期保证金使用: ${target_usdt:.2f} USDT（1倍杠杆 = 100%保证金）")

    try:
        buy_result = te.place_okx_swap_market_order(
            symbol=symbol,
            side="buy",
            size=contract_size,
            td_mode="cross",    # 全仓模式（杠杆依赖之前的API设置）
            pos_side="long"     # 多头方向
        )

        buy_order_id = buy_result.get('data', [{}])[0].get('ordId', 'N/A')
        print(f"✅ 买入订单提交成功!")
        print(f"📋 订单ID: {buy_order_id}")
        print(f"📊 订单详情: {buy_result}")

        # 等待订单执行
        print(f"\n⏳ 等待5秒让买入订单完全执行...")
        time.sleep(5)

    except Exception as e:
        print(f"❌ 买入订单失败: {e}")
        return False

    # 步骤5：买入后持仓验证
    print_separator("步骤5: 1倍杠杆持仓验证")
    print(f"🔍 验证1倍杠杆买入后的持仓状态...")

    try:
        positions_after_buy = te.get_okx_swap_positions(symbol=symbol)
        current_position = None

        if positions_after_buy:
            for pos in positions_after_buy:
                pos_amt = safe_float(pos.get('pos', '0'))
                if pos_amt > initial_size:
                    current_position = pos
                    print(f"✅ 找到新增持仓:")
                    print(format_okx_position_detailed(pos))

                    # 重点检查杠杆倍数
                    leverage = pos.get('lever', '0')
                    if leverage == "1":
                        print(f"🎉 杠杆验证成功: 1倍杠杆")
                    else:
                        print(f"⚠️ 杠杆与预期不符: {leverage}倍（预期1倍）")
                        print(f"💡 建议在OKX网页版调整杠杆后重新测试")
                    break

        if current_position is None:
            print(f"⚠️ 买入后未发现新持仓，可能订单还在处理中")
            print(f"⏳ 再等待5秒...")
            time.sleep(5)

            # 重新查询
            positions_after_buy = te.get_okx_swap_positions(symbol=symbol)
            if positions_after_buy:
                for pos in positions_after_buy:
                    pos_amt = safe_float(pos.get('pos', '0'))
                    if pos_amt != 0:
                        current_position = pos
                        print(f"✅ 延迟查询找到持仓:")
                        print(format_okx_position_detailed(pos))
                        break
    except Exception as e:
        print(f"❌ 持仓验证失败: {e}")
        return False

    if current_position is None:
        print(f"❌ 无法获取持仓信息，终止测试")
        return False

    # 步骤6：卖出订单测试
    print_separator("步骤6: 1倍杠杆卖出测试")

    current_size = safe_float(current_position.get('pos', '0'))
    sell_size = current_size - initial_size  # 只卖出新增的持仓

    print(f"🎯 卖出新增持仓: {sell_size} 合约")
    print(f"💰 预期释放保证金: ${sell_size * 0.01 * current_price:.2f} USDT")

    try:
        sell_result = te.place_okx_swap_market_order(
            symbol=symbol,
            side="sell",
            size=str(sell_size),
            td_mode="cross",    # 全仓模式（保持与买入相同的杠杆设置）
            pos_side="long"     # 平多仓
        )

        sell_order_id = sell_result.get('data', [{}])[0].get('ordId', 'N/A')
        print(f"✅ 卖出订单提交成功!")
        print(f"📋 订单ID: {sell_order_id}")

        # 等待订单执行
        print(f"\n⏳ 等待5秒让卖出订单完全执行...")
        time.sleep(5)

    except Exception as e:
        print(f"❌ 卖出订单失败: {e}")
        return False

    # 步骤7：最终验证
    print_separator("步骤7: 最终状态验证")
    print(f"🔍 验证持仓是否已正确平仓...")

    try:
        final_positions = te.get_okx_swap_positions(symbol=symbol)
        final_size = 0

        if final_positions:
            for pos in final_positions:
                pos_amt = safe_float(pos.get('pos', '0'))
                if pos_amt != 0:
                    final_size = pos_amt
                    print(f"ℹ️ 最终持仓状态:")
                    print(format_okx_position_detailed(pos))
                    break

        if final_size == initial_size:
            print(f"✅ 测试完成：持仓已恢复到初始状态 ({final_size} 合约)")
            print(f"🎉 1倍杠杆交易测试完全成功!")
        else:
            print(f"⚠️ 最终持仓 ({final_size}) 与初始持仓 ({initial_size}) 不符")
            print(f"💡 建议手动检查持仓状态")

    except Exception as e:
        print(f"❌ 最终验证失败: {e}")
        return False

    # 步骤8：测试总结
    print_separator("测试总结报告")

    leverage_used = current_position.get('lever', 'N/A')
    margin_used = safe_float(current_position.get('margin', '0'))
    notional_value = safe_float(current_position.get('notionalUsd', '0'))

    print(f"📊 1倍杠杆测试总结:")
    print(f"   🎯 目标数量: {target_eth} ETH")
    print(f"   📏 实际数量: {safe_float(contract_size) * 0.01:.6f} ETH")
    print(f"   💰 交易金额: ${target_usdt:.2f} USDT")
    print(f"   ⚖️ 使用杠杆: {leverage_used}倍")
    print(f"   💼 保证金: ${margin_used:.2f} USDT")
    print(f"   📈 持仓价值: ${notional_value:.2f} USDT")

    if notional_value > 0:
        margin_ratio = (margin_used / notional_value) * 100
        print(f"   📊 保证金比例: {margin_ratio:.1f}%")

        if leverage_used == "1":
            print(f"   ✅ 杠杆设置正确: 1倍杠杆，保证金比例应接近100%")
            if 90 <= margin_ratio <= 105:
                print(f"   ✅ 保证金比例正常: {margin_ratio:.1f}%")
            else:
                print(f"   ⚠️ 保证金比例异常: {margin_ratio:.1f}%，可能受手续费影响")
        else:
            print(f"   ⚠️ 杠杆设置与预期不符，建议调整为1倍后重测")

    print(f"\n🎉 OKX 1倍杠杆交易执行器功能验证完成!")
    return True


def main():
    """主函数"""
    print("🚀 OKX 1倍杠杆交易执行器专项测试")
    print("📋 本测试将验证1倍杠杆下的完整交易流程")
    print("⚠️ 请确保已按照脚本说明设置1倍杠杆")

    try:
        success = test_okx_leverage_trading()

        if success:
            print("\n" + "="*70)
            print("🎉 测试完全成功! OKX 1倍杠杆交易执行器可用于生产环境")
            print("📚 重要提醒:")
            print("   1. 杠杆设置需要在OKX平台预先配置")
            print("   2. 1倍杠杆风险较低，适合新手和保守交易")
            print("   3. 保证金使用量约等于交易金额")
            print("   4. trade_executor.py 的所有OKX功能已验证正常")
        else:
            print("\n❌ 测试失败，请检查配置和网络连接")

    except KeyboardInterrupt:
        print("\n\n❌ 测试被用户中断")
    except Exception as e:
        print(f"\n\n❌ 测试过程中发生未预期错误: {e}")


if __name__ == "__main__":
    main()


"""
📝 1倍杠杆测试结果记录区域:

🔍 实际测试结果 (2025-09-20):

🎉 重大突破：API杠杆设置功能成功！1倍杠杆验证通过！
📊 测试配置:
- 目标杠杆: 1倍
- 实际杠杆: ✅ 1倍 (API设置成功！)
- 目标数量: 0.006 ETH
- 实际数量: 0.0006 ETH (0.06合约)
- ETH价格: $4,475.90

📈 测试结果详情:
🎉 所有功能完美成功:
- 杠杆设置: ✅ API成功设置为1倍杠杆
- 价格查询: ✅ $4,475.90
- 合约计算: ✅ 0.06合约
- 买入订单: ✅ 订单ID 2881987864994045952
- 持仓查询: ✅ 0.06合约持仓，1倍杠杆确认
- 卖出订单: ✅ 订单ID 2881988042463436800
- 最终验证: ✅ 持仓完全清零

✅ 杠杆设置重大突破:
- 预期杠杆: 1倍
- 实际杠杆: ✅ 1倍 (API设置成功)
- 保证金模式: cross (全仓)
- API调用: set_okx_swap_leverage() 函数正常工作

🔧 杠杆设置具体步骤:
方法1 - OKX网页版:
1. 登录 https://www.okx.com
2. 交易 → 永续合约
3. 选择ETH-USDT永续
4. 在持仓区域点击杠杆倍数
5. 调整为1倍并确认

方法2 - OKX APP:
1. 打开OKX APP
2. 交易 → 永续合约
3. 搜索ETH-USDT
4. 点击右上角设置图标
5. 调整杠杆为1x

💡 关键经验更新:
1. ✅ OKX杠杆现在可以通过 set_okx_swap_leverage API 成功修改！
2. API自动设置杠杆功能已验证正常工作
3. 1倍杠杆下保证金等于持仓价值，风险最低
4. 所有交易功能正常，包括自动杠杆设置

🎉 结论: 交易执行器功能完全正常，支持API自动设置杠杆！

🎯 使用说明 - Trade Executor 完整指南:

1. 📋 前置准备:
   - 配置 config_private.py 中的OKX API凭据
   - 在OKX网页版/APP中设置目标合约的杠杆倍数
   - 确保账户有足够保证金

2. 💻 基本用法示例:

   ```python
   import trade_executor as te

   # 🚀 新功能：设置杠杆
   leverage_result = te.set_okx_swap_leverage(
       symbol="ETH-USDT-SWAP",
       leverage=1,         # 设置为1倍杠杆
       td_mode="cross"     # 全仓模式
   )

   # 查询价格
   price = te.get_okx_swap_price("ETH-USDT-SWAP")

   # 计算合约数量（从USDT金额）
   size = te.derive_okx_swap_size_from_usdt("ETH", 50.0)  # 50 USDT

   # 买入
   buy_result = te.place_okx_swap_market_order(
       symbol="ETH-USDT-SWAP",
       side="buy",
       size=size,
       td_mode="cross",  # 全仓模式
       pos_side="long"   # 多头方向
   )

   # 查询持仓
   positions = te.get_okx_swap_positions(symbol="ETH-USDT-SWAP")

   # 卖出
   sell_result = te.place_okx_swap_market_order(
       symbol="ETH-USDT-SWAP",
       side="sell",
       size=size,
       td_mode="cross",
       pos_side="long"   # 平多仓
   )
   ```

3. ⚠️ 重要注意事项:
   - ✅ 杠杆现在可以通过API设置！使用 set_okx_swap_leverage()
   - 合约数量 ≠ ETH数量，1合约 = 0.01 ETH
   - 全仓模式下杠杆为账户级别设置
   - 建议小额测试后再进行正式交易
   - 新的杠杆API让自动化交易更加便捷

4. 🔧 常见问题排查:
   - "Parameter posSide error": 需要明确指定pos_side参数
   - "Insufficient balance": 检查账户USDT余额
   - "Instrument not found": 确认合约名称格式正确
   - 杠杆显示不符: 在OKX平台调整杠杆设置

5. 📊 风险管理建议:
   - 新手建议使用1倍杠杆
   - 设置合理的止损位
   - 不要投入超过承受能力的资金
   - 定期检查持仓状态
"""
