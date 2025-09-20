#!/usr/bin/env python3
"""
Bitget 永续合约专项测试脚本 - 验证 trade_executor 的 Bitget 接入能力

📊 测试结果记录 (2025-09-20):
========================================

✅ 成功的功能:
1. **杠杆设置** - set_bitget_usdt_perp_leverage():
   - 成功设置为1倍杠杆 (long=1x / short=1x)
   - 保证金模式: crossed (全仓模式) ✅

2. **价格查询** - get_bitget_usdt_perp_price():
   - ETH当前价格: $4471.55 ✅

3. **合约数量计算** - derive_bitget_usdt_perp_size_from_usdt():
   - 45 USDT → 0.01 ETH (≈ $44.72) ✅
   - 满足最小下单量要求 ✅

4. **市价买入** - place_bitget_usdt_perp_market_order():
   - 订单ID: 1353371439955849222 ✅
   - 买入成功执行 ✅

❌ 遇到问题:
1. **持仓查询** - get_bitget_usdt_perp_positions():
   - 错误: {'code': '40404', 'msg': 'Request URL NOT FOUND'}
   - 可能是API endpoint变更或权限问题

2. **平仓未执行**:
   - 原因: 持仓查询异常导致程序提前退出，跳过了平仓逻辑
   - 修复: 已添加异常处理，确保持仓查询失败时仍能执行平仓
   - 状态: ✅ 代码逻辑已完善

3. **余额限制**:
   - 当前错误: {'code': '40762', 'msg': 'The order amount exceeds the balance'}
   - 说明: 上次买入订单消耗了可用余额，需要手动平仓或充值

📝 关键验证:
- ✅ Bitget最小下单量: 0.01ETH (约$45 USDT)
- ✅ 1倍杠杆设置成功，风险控制到位
- ✅ 核心交易功能(价格、计算、下单)工作正常
- ⚠️ 持仓查询API需要进一步调试

📌 覆盖目标
- 杠杆设置: set_bitget_usdt_perp_leverage()
- 价格查询: get_bitget_usdt_perp_price()
- 名义金额换算: derive_bitget_usdt_perp_size_from_usdt()
- 市价下单: place_bitget_usdt_perp_market_order()
- 持仓查询: get_bitget_usdt_perp_positions()

⚠️ 运行前置
- 请在 config_private.py 中配置 Bitget API Key/Secret/Passphrase
- 建议账户为单向持仓 + 全仓模式
- ETHUSDT 永续合约最小下单量为 0.01 ETH (约 45 USDT)

🧪 测试步骤
1. 设置 1x 杠杆
2. 获取最新行情并推导合约数量
3. 市价买入 -> 查询持仓
4. 市价卖出 -> 验证平仓

运行方式:
`python3 trading/test_bitget_only.py`
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any, Dict, Optional, Tuple

# 将项目根目录加入路径，方便直接导入 trade_executor
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import trading.trade_executor as te

TARGET_SYMBOL = "ETHUSDT_UMCBL"
MARGIN_COIN = "USDT"
TARGET_LEVERAGE = 1
CANDIDATE_NOTIONALS = [45, 50, 55, 60, 70]


def print_banner(title: str) -> None:
    print("\n" + "=" * 24 + f" {title} " + "=" * 24)


def ensure_leverage() -> None:
    print_banner("杠杆设置")
    try:
        resp = te.set_bitget_usdt_perp_leverage(
            symbol=TARGET_SYMBOL,
            leverage=TARGET_LEVERAGE,
            margin_coin=MARGIN_COIN,
        )
        data = resp.get("data", {})
        long_leverage = data.get("longLeverage") or data.get("leverage")
        short_leverage = data.get("shortLeverage") or data.get("leverage")
        print(
            "✅ 杠杆设置成功: "
            f"long={long_leverage}x / short={short_leverage}x / marginMode={data.get('marginMode')}"
        )
    except te.TradeExecutionError as err:
        print(f"⚠️ 杠杆设置失败: {err} (继续使用账户当前杠杆)")


def choose_trade_size() -> Tuple[str, float]:
    print_banner("换算下单数量")
    price = te.get_bitget_usdt_perp_price(TARGET_SYMBOL)
    print(f"🎯 最新价格: ${price:.2f}")

    last_error: Optional[Exception] = None
    for notional in CANDIDATE_NOTIONALS:
        try:
            size = te.derive_bitget_usdt_perp_size_from_usdt(
                TARGET_SYMBOL,
                notional_usdt=notional,
                price=price,
            )
            base_qty = float(size)
            approx_value = base_qty * price
            print(f"✅ {notional} USDT -> {size} ETH (≈ ${approx_value:.2f})")
            return size, approx_value
        except te.TradeExecutionError as err:
            last_error = err
            print(f"⚠️ {notional} USDT 不满足下单要求: {err}")

    raise te.TradeExecutionError(
        f"无法根据候选名义金额推导合约数量: {last_error}"
    )


def place_buy(size: str) -> Dict[str, Any]:
    print_banner("市价买入")
    response = te.place_bitget_usdt_perp_market_order(
        symbol=TARGET_SYMBOL,
        side="buy",
        size=size,
        margin_coin=MARGIN_COIN,
        client_order_id=f"bitget_buy_{int(time.time())}",
    )
    data = response.get("data", {})
    print(
        "✅ 买入成功: "
        f"orderId={data.get('orderId')} size={data.get('size')} side={data.get('side')}"
    )
    return response


def inspect_positions() -> None:
    print_banner("持仓查询")
    positions = te.get_bitget_usdt_perp_positions(symbol=TARGET_SYMBOL)
    if not positions:
        print("⚠️ 未返回持仓记录，请确认账户已有开仓数据")
        return

    for entry in positions:
        print(
            "📊 持仓信息: "
            f"side={entry.get('holdSide')} total={entry.get('total')} avgOpen={entry.get('averageOpenPrice')} "
            f"unrealized={entry.get('unrealizedPL')}"
        )


def place_sell(size: str) -> Dict[str, Any]:
    print_banner("市价卖出")
    response = te.place_bitget_usdt_perp_market_order(
        symbol=TARGET_SYMBOL,
        side="sell",
        size=size,
        margin_coin=MARGIN_COIN,
        reduce_only=True,
        client_order_id=f"bitget_sell_{int(time.time())}",
    )
    data = response.get("data", {})
    print(
        "✅ 卖出成功: "
        f"orderId={data.get('orderId')} size={data.get('size')} side={data.get('side')}"
    )
    return response


def main() -> int:
    ensure_leverage()
    try:
        size, approx = choose_trade_size()
    except te.TradeExecutionError as err:
        print(f"❌ 无法推导合约数量: {err}")
        return 1

    try:
        place_buy(size)
    except te.TradeExecutionError as err:
        print(f"❌ 买入下单失败: {err}")
        return 2

    time.sleep(2)
    try:
        inspect_positions()
    except te.TradeExecutionError as err:
        print(f"⚠️ 持仓查询失败: {err} (继续执行平仓)")

    try:
        place_sell(size)
    except te.TradeExecutionError as err:
        print(f"❌ 卖出下单失败: {err}")
        return 3

    print("\n🎉 Bitget 永续合约流程测试执行完毕")
    return 0


if __name__ == "__main__":
    sys.exit(main())
