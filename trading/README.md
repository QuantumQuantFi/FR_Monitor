# Trading 模块

这个文件夹包含所有与交易执行相关的代码和工具。

## 📁 文件结构

### 核心模块
- **`trade_executor.py`** - 主要交易执行模块
  - `place_binance_perp_market_order()` - Binance期货市价单
  - `get_binance_perp_positions()` - 查询Binance期货持仓
  - `get_binance_perp_price()` - 获取Binance期货实时价格
  - 支持OKX、Bybit、Bitget等其他交易所

- **`binance_data_structures.py`** - 数据结构文档
  - 详细的Binance API响应格式说明
  - 数据转换工具函数
  - 数据库设计建议
  - 所有字段的完整注释

### 测试文件
- **`test_detailed_trading.py`** - 完整交易流程测试
  - 买入 → 持仓查询 → 卖出 → 结果验证
  - 包含详细的测试结果注释
  - 展示完整的API使用方法

- **`test_binance_only.py`** - Binance专项测试
  - 专门测试Binance交易所
  - 包含各种错误处理和修复记录

- **`test_trade_executor.py`** - 多交易所综合测试
  - 测试所有支持的交易所
  - 包含IP白名单等问题的解决方案

## 🚀 快速使用

### 基本交易示例
```python
import sys
sys.path.append('..')
import trading.trade_executor as te

# 1. 获取当前价格
price = te.get_binance_perp_price("ETHUSDT")
print(f"ETH当前价格: ${price}")

# 2. 执行买入订单
result = te.place_binance_perp_market_order(
    symbol="ETHUSDT",
    side="BUY",
    quantity=0.006,  # 0.006 ETH
    position_side="LONG"
)

if result:
    order_id = result['orderId']
    status = result['status']
    print(f"订单 {order_id} 状态: {status}")

# 3. 查询持仓
positions = te.get_binance_perp_positions(symbol="ETHUSDT")
for pos in positions:
    amt = float(pos['positionAmt'])
    if amt != 0:
        print(f"持仓: {amt} ETH")
```

### 数据存储示例
```python
from trading.binance_data_structures import convert_order_response

# 转换订单数据为适合存储的格式
order_data = convert_order_response(result)
# 可以直接存储到数据库
```

## 📊 数据结构说明

详见 `binance_data_structures.py` 文件，包含：
- 订单响应结构 (20+ 字段详细说明)
- 持仓信息结构 (15+ 字段详细说明)
- 价格查询结构
- 数据库设计建议
- 类型转换工具

## ⚙️ 配置要求

1. **API密钥配置** - 在 `config_private.py` 中配置：
   ```python
   # Binance
   BN_API_KEY_ACCOUNT2 = "your_binance_api_key"
   BN_SECRET_KEY_ACCOUNT2 = "your_binance_secret"

   # 其他交易所...
   ```

2. **最小交易金额**：
   - Binance: 20 USDT (约0.005 ETH)
   - 其他交易所类似限制

3. **网络要求**：
   - 某些交易所需要IP白名单
   - 建议使用稳定的网络连接

## 🧪 测试

运行完整测试：
```bash
python test_detailed_trading.py
```

运行单个交易所测试：
```bash
python test_binance_only.py
```

## 📝 开发说明

1. **错误处理**: 所有函数都会抛出 `TradeExecutionError` 异常
2. **数据格式**: API返回的数字字段为字符串，需要转换为 `float()` 使用
3. **持仓模式**: 支持单向持仓 (`LONG`/`SHORT`) 和双向持仓 (`BOTH`)
4. **时间戳**: 使用毫秒级Unix时间戳

## 🔒 安全提醒

- 永远不要将API密钥提交到版本控制
- 测试时使用小额资金
- 生产环境前充分测试所有功能
- 定期检查API权限设置