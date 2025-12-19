# Binance / Hyperliquid Connector 能力清单（代码现状）

本文基于当前仓库代码的“实现现状”，列出 Binance 与 Hyperliquid connector 已覆盖的功能，并给出可运行的自检脚本。

自检脚本：
- `test_connector_audit_binance_hyperliquid.py`
  - 默认只跑公共 API（不会下单）
  - `--private` 会额外测试私有账户/持仓/资金费历史（仍不会下单）
  - `--trade --confirm-live-trade` 才会用小额 ETH 做一次“开仓→立刻平仓”的实盘 smoke test

## 1. Binance（USDT 永续）

### 1.1 公共 API（行情/资金费/订单簿）
来源模块：
- `rest_collectors.py`：批量行情/资金费快照（用于 watchlist/监控）
- `orderbook_utils.py`：按“扫盘可成交均价”计算买/卖均价（用于 live trading 复核/验算）
- `trading/trade_executor.py`：单 symbol 的最新价格（用于把 notional 换算成 base quantity）

能力点（代码里已有）：
- 批量行情：`rest_collectors.fetch_binance()`
- 资金费率/下一次结算/间隔：`rest_collectors.fetch_binance()`（内部会缓存 `fundingInfo` 推断 interval）
- 订单簿深度：`orderbook_utils.fetch_orderbook_prices("binance", <symbol>, "perp", notional=...)`
- 最新价：`trading.trade_executor.get_binance_perp_price(<symbol>)`

### 1.2 私有 API（余额/持仓/下单/订单/资金费收入）
来源模块：
- `trading/trade_executor.py`

能力点（代码里已有）：
- 余额（USDT）：`get_binance_perp_usdt_balance()`
- 账户信息：`get_binance_perp_account()`
- 持仓：`get_binance_perp_positions(symbol=...)`
- 设置杠杆：`set_binance_perp_leverage(symbol, leverage)`
- 下单（市价）：`place_binance_perp_market_order(symbol, side, quantity, ...)`
- 查单：`get_binance_perp_order(symbol, order_id=... / client_order_id=...)`
- 资金费/收入：`get_binance_funding_fee_income(start_time_ms, end_time_ms=...)`

注意：
- Binance 下单 quantity 必须是“基础币数量”（例如 ETH），不能直接传 50U。
- 账户如果是 Hedge Mode（`dualSidePosition=true`），下单/平仓需要 `position_side`（`LONG/SHORT`）。

## 2. Hyperliquid（USDC 永续）

### 2.1 公共 API（行情/资金费/订单簿）
来源模块：
- `rest_collectors.py`：`POST /info` 的 metaAndAssetCtxs 缓存 + 批量快照
- `orderbook_utils.py`：`POST /info` type=l2Book 拉 L2 订单簿并扫盘计算买/卖均价

能力点（代码里已有）：
- 批量行情/资金费快照：`rest_collectors.fetch_hyperliquid()`
- funding_map（缓存）：`rest_collectors.get_hyperliquid_funding_map()`
- 订单簿深度：`orderbook_utils.fetch_orderbook_prices("hyperliquid", <symbol>, "perp", notional=...)`

### 2.2 私有 API（余额/持仓/资金费历史/下单）
来源模块：
- `trading/trade_executor.py`（依赖 `hyperliquid-python-sdk` + `eth-account`）

能力点（代码里已有）：
- 账户 state：`get_hyperliquid_user_state()`
- 余额汇总：`get_hyperliquid_balance_summary()`
- 持仓：`get_hyperliquid_perp_positions()`
- 用户资金费历史：`get_hyperliquid_user_funding_history(start_time_ms, end_time_ms=...)`
- 下单：
  - 开仓：`place_hyperliquid_perp_market_order(symbol, "long"/"short", size, ...)`
  - 平仓：`place_hyperliquid_perp_market_order(..., reduce_only=True, ...)`（内部走 IOC reduce-only limit，确保有回执便于落库）

## 3. 自检脚本运行方式

### 3.1 公共接口（推荐先跑）
```bash
./venv/bin/python test_connector_audit_binance_hyperliquid.py --symbol ETH --notional 20
```

### 3.2 私有接口（不下单）
```bash
./venv/bin/python test_connector_audit_binance_hyperliquid.py --private --symbol ETH --notional 20
```

### 3.3 实盘开平仓 smoke test（会真实下单）
```bash
./venv/bin/python test_connector_audit_binance_hyperliquid.py --private --trade --confirm-live-trade --symbol ETH --notional 20
```

脚本会把结果保存到 `reports/connector_audit_<symbol>_<ts>.json`，用于复盘 raw 回执、错误与耗时。

