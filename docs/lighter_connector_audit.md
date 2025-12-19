# Lighter Connector 能力清单 + 自检方式（代码现状）

目标：让 Lighter 像 Binance / Hyperliquid 一样具备“公共行情/订单簿/资金费 + 私有余额/持仓 + 下单”的可复用接口，便于后续策略与实盘系统对接。

## 1) 已实现的接口（本仓库）

### 公共 API（无需密钥）
- 市场/合约元信息：`trading/trade_executor.py:get_lighter_market_meta()`（GET `/api/v1/orderBooks`）
- 订单簿深度：`trading/trade_executor.py:get_lighter_orderbook_orders()`（GET `/api/v1/orderBookOrders`）
- 最近成交：`trading/trade_executor.py:get_lighter_recent_trades()`（GET `/api/v1/recentTrades`）
- 资金费率（全市场）：`trading/trade_executor.py:get_lighter_funding_rates_map()`（GET `/api/v1/funding-rates`）
- 扫盘可成交均价（用于策略验算）：`orderbook_utils.py:fetch_orderbook_prices("lighter", ...)`

### 私有/账户（需要 account_index，但余额接口是公开查询）
- 余额/持仓快照（含 positions）：`trading/trade_executor.py:get_lighter_balance_summary()`（GET `/api/v1/account?by=index&value=...`）

### 实盘下单（需要 Lighter API key 私钥）
- 市价单开/平仓：`trading/trade_executor.py:place_lighter_perp_market_order()`
  - 内部使用 `lighter` 官方 SDK 的 `SignerClient.create_market_order(...)`（async），并做了同步包装 `_run_async(...)`
  - 下单前会读取 public 订单簿 best bid/ask，按 `max_slippage_bps` 生成一个保护性 price（避免无界滑点）
  - `reduce_only=True` 用于平仓单

## 2) 自检脚本

脚本：`test_connector_audit_lighter.py`

### 只测公共接口（不会下单）
```bash
./venv/bin/python test_connector_audit_lighter.py --symbol ETH --notional 20
```

### 加测私有接口（仍不会下单）
```bash
./venv/bin/python test_connector_audit_lighter.py --private --symbol ETH --notional 20
```

### 实盘开平仓 smoke test（会真实下单）
```bash
./venv/bin/python test_connector_audit_lighter.py --private --trade --confirm-live-trade --symbol ETH --notional 20
```

结果会落到：`reports/connector_audit_lighter_<symbol>_<ts>.json`，便于复盘 raw 回执。

## 3) 需要的配置（config_private.py）

脚本/下单依赖以下字段（名称以当前仓库约定为准）：
- `LIGHTER_PRIVATE_KEY`
- `LIGHTER_ACCOUNT_INDEX`
- `LIGHTER_KEY_INDEX`

