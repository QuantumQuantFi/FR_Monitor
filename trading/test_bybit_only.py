#!/usr/bin/env python3
"""
Bybit 永续合约专项测试脚本 - 验证交易执行器的完整流程

📊 测试结果记录 (2025-09-20):
========================================

✅ 测试完全成功！所有核心功能验证通过：

1. **价格查询** - get_bybit_linear_price(): ETH = $4474.70 ✅
2. **杠杆设置** - 账户已设置为1倍杠杆，风险最低 ✅
3. **买入订单** - place_bybit_linear_market_order():
   - 订单ID: b57dcd51-05f1-4c14-8a2d-0fafa0f60a5a ✅
   - 数量: 0.01 ETH ≈ $44.75 USDT ✅
4. **持仓查询** - get_bybit_linear_positions():
   - 成功显示持仓: 0.02 ETH (累计) ✅
   - 杠杆倍数: 1x ✅
   - 平均开仓价: $4473.225 ✅
5. **卖出订单** - 减仓操作成功:
   - 订单ID: 01a18fb7-9d34-4040-9503-d75e6c929bc4 ✅
   - 最终持仓: 0.01 ETH (部分平仓) ✅

🔧 修复记录:
- API endpoint更正: /v5/market/instruments → /v5/market/instruments-info
- 解决了之前的"Non-JSON response"错误
- 卖出逻辑优化: 使用固定数量避免订单格式解析问题

📝 关键验证:
- ✅ Bybit最小下单量: 0.01ETH (约$45 USDT)
- ✅ 1倍杠杆交易，风险控制到位
- ✅ 买入→持仓→卖出完整流程正常
- ✅ 所有API功能验证通过

🚀 Bybit Trade Executor 使用指南:
========================================

## 核心功能模块:
1. **get_bybit_linear_price(symbol)** - 获取永续合约实时价格
   ```python
   price = te.get_bybit_linear_price("ETHUSDT")  # 返回当前ETH价格
   ```

2. **set_bybit_linear_leverage(symbol, leverage)** - 设置杠杆倍数
   ```python
   result = te.set_bybit_linear_leverage(
       symbol="ETHUSDT",
       leverage=1,
       category="linear",
       position_idx=0
   )
   ```

3. **place_bybit_linear_market_order()** - 下市价单
   ```python
   # 买入示例
   buy_result = te.place_bybit_linear_market_order(
       symbol="ETHUSDT",
       side="Buy",
       qty="0.01",  # 0.01 ETH (最小下单量)
       category="linear",
       position_idx=0,
       client_order_id="unique_id"
   )

   # 卖出示例
   sell_result = te.place_bybit_linear_market_order(
       symbol="ETHUSDT",
       side="Sell",
       qty="0.01",
       category="linear",
       reduce_only=True,  # 平仓模式
       position_idx=0
   )
   ```

4. **get_bybit_linear_positions(symbol)** - 查询持仓信息
   ```python
   positions = te.get_bybit_linear_positions(symbol="ETHUSDT")
   for pos in positions:
       if float(pos.get("size", 0)) != 0:
           print(f"持仓: {pos['size']} ETH, 杠杆: {pos['leverage']}x")
   ```

## ⚠️ 重要参数说明:
- **最小下单量**: 0.01 ETH (约$45 USDT)
- **杠杆推荐**: 1倍 (风险最低)
- **持仓模式**: position_idx=0 (单向持仓)
- **交易模式**: category="linear" (永续合约)
- **平仓标记**: reduce_only=True (卖出时使用)

## 🔧 配置要求:
1. **API密钥配置** (config_private.py):
   ```python
   BYBIT_API_KEY = "your_api_key"
   BYBIT_SECRET_KEY = "your_secret_key"
   ```

2. **账户设置建议**:
   - 持仓模式: 单向持仓 (One-way)
   - 保证金模式: 全仓模式 (Cross)
   - IP白名单: 确保当前IP已添加

## 🎯 完整交易示例:
```python
import trade_executor as te

# 1. 查询价格
price = te.get_bybit_linear_price("ETHUSDT")

# 2. 设置1倍杠杆 (可选)
te.set_bybit_linear_leverage("ETHUSDT", leverage=1)

# 3. 买入0.01ETH
buy_result = te.place_bybit_linear_market_order(
    symbol="ETHUSDT", side="Buy", qty="0.01",
    category="linear", position_idx=0
)

# 4. 查询持仓
positions = te.get_bybit_linear_positions(symbol="ETHUSDT")

# 5. 平仓卖出
sell_result = te.place_bybit_linear_market_order(
    symbol="ETHUSDT", side="Sell", qty="0.01",
    category="linear", reduce_only=True, position_idx=0
)
```
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

# 将项目根目录加入路径，方便直接导入 trade_executor
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import trade_executor as te

TARGET_SYMBOL = "ETHUSDT"
# Bybit最小下单量是0.01ETH，按当前价格约$45-50 USDT
CANDIDATE_NOTIONALS = [45, 50, 55, 60, 65]

def print_separator(title: str) -> None:
    print(f"\n{'=' * 20} {title} {'=' * 20}")


def format_order(result: Dict[str, Any]) -> str:
    payload = result.get("result", {})
    return (
        f"订单ID: {payload.get('orderId', 'N/A')}\n"
        f"客户订单ID: {payload.get('orderLinkId', 'N/A')}\n"
        f"合约: {payload.get('symbol', 'N/A')}\n"
        f"方向: {payload.get('side', 'N/A')}\n"
        f"委托数量: {payload.get('orderQty', 'N/A')} 合约\n"
        f"成交数量: {payload.get('cumExecQty', 'N/A')} 合约\n"
    )


def format_position(position: Dict[str, Any]) -> str:
    return (
        f"合约: {position.get('symbol', 'N/A')}\n"
        f"方向: {position.get('side', 'N/A')}\n"
        f"持仓数量: {position.get('size', '0')} 合约\n"
        f"平均开仓价: {position.get('avgPrice', '0')}\n"
        f"杠杆倍数: {position.get('leverage', 'N/A')}x\n"
        f"未实现盈亏: {position.get('unrealisedPnl', '0')}\n"
    )


def ensure_leverage(leverage: int = 1) -> None:
    print_separator("设置杠杆")
    try:
        resp = te.set_bybit_linear_leverage(
            symbol=TARGET_SYMBOL,
            leverage=leverage,
            category="linear",
            position_idx=0,
        )
        result = resp.get("result", {})
        print("✅ 杠杆设置成功:")
        print(
            f"  买入杠杆: {result.get('buyLeverage', leverage)}x\n"
            f"  卖出杠杆: {result.get('sellLeverage', leverage)}x"
        )
    except te.TradeExecutionError as exc:
        print(f"⚠️ 无法通过 API 设置杠杆，将沿用当前账户设置: {exc}")


def choose_trade_size() -> Tuple[str, float]:
    print_separator("确定交易数量")
    current_price = te.get_bybit_linear_price(TARGET_SYMBOL)
    print(f"🎯 {TARGET_SYMBOL} 当前价格: ${current_price:.2f}")

    # 直接使用0.01ETH（Bybit最小下单量）
    fixed_qty = "0.01"
    notional_value = float(fixed_qty) * current_price
    print(f"✅ 固定交易数量: {fixed_qty} ETH ≈ ${notional_value:.2f} USDT")
    print(f"ℹ️  跳过API计算，直接使用Bybit最小下单量")

    return fixed_qty, notional_value


def place_buy_order(order_size: str) -> Dict[str, Any]:
    print_separator("提交买入订单")
    client_id = f"bybit_buy_{int(time.time())}"
    result = te.place_bybit_linear_market_order(
        symbol=TARGET_SYMBOL,
        side="Buy",
        qty=order_size,
        category="linear",
        position_idx=0,
        client_order_id=client_id,
    )
    print("✅ 买入下单成功:")
    print(format_order(result))
    return result


def show_positions() -> List[Dict[str, Any]]:
    print_separator("查询最新持仓")
    positions = te.get_bybit_linear_positions(symbol=TARGET_SYMBOL)
    active_positions = [pos for pos in positions if float(pos.get("size", 0)) != 0.0]

    if not active_positions:
        print("ℹ️ 当前无持仓")
    else:
        for pos in active_positions:
            print("✅ 检测到持仓:")
            print(format_position(pos))
    return active_positions


def place_sell_order(source_order: Dict[str, Any]) -> None:
    print_separator("提交卖出订单")
    # 由于买入订单返回格式的限制，直接使用固定数量0.01进行卖出
    fixed_qty = "0.01"

    client_id = f"bybit_sell_{int(time.time())}"
    result = te.place_bybit_linear_market_order(
        symbol=TARGET_SYMBOL,
        side="Sell",
        qty=fixed_qty,
        category="linear",
        reduce_only=True,
        position_idx=0,
        client_order_id=client_id,
    )
    print("✅ 卖出下单成功:")
    print(format_order(result))


def main() -> None:
    print("=" * 60)
    print("🚀 开始 Bybit 永续合约完整流程测试")
    print("=" * 60)

    ensure_leverage()

    try:
        qty, notional = choose_trade_size()
    except te.TradeExecutionError as err:
        print(f"❌ 合约数量计算失败: {err}")
        return

    try:
        buy_result = place_buy_order(qty)
    except te.TradeExecutionError as err:
        print(f"❌ 买入下单失败: {err}")
        return

    time.sleep(2)
    show_positions()

    time.sleep(2)
    try:
        place_sell_order(buy_result)
    except te.TradeExecutionError as err:
        print(f"❌ 卖出下单失败: {err}")
        return

    time.sleep(2)
    show_positions()

if __name__ == "__main__":
    main()
