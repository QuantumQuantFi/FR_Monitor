# 2026-01-14 提交说明（Live Trading 记账审计 + simple_app 保活）

本次改动按功能拆分为 4 个提交，重点是：
- Live Trading 的 fee/funding 口径更可靠（减少“手续费缺失导致净PnL被高估”）
- 增加回填/审计脚本，便于对账与修复历史缺口
- 增加 simple_app watchdog，记录退出原因并自动拉起

## Commit 1：Live Trading：补齐手续费解析并对齐资金费窗口
hash：`349fbe7`

内容：
- OKX/Bybit/Bitget：在 `trading/live_trading_manager.py` 的 `_parse_fill_fields()` 解析订单详情中的手续费字段，写入 `watchlist.live_trade_order.fee_usdt/fee_currency`
- Hyperliquid：补齐 `cum_quote` 推导；当 fills 匹配失败时用 taker 费率做保守兜底（best-effort）
- Funding：refresh/finalize 资金费窗口边界优先用 `open_long_at/open_short_at` 与 `close_long_at/close_short_at`，减少结算点附近漏计
- Bybit：新增 `trading/trade_executor.py:get_bybit_execution_list()`（/v5/execution/list），供手续费回填/审计使用

## Commit 2：Web：净PnL计算避免因手续费缺失被高估
hash：`3fe1abf`

内容：
- `simple_app.py`：
  - `live_trading_signals` 接口输出 `fee_orders/fee_orders_with_fee/fee_complete`
  - 当手续费覆盖不完整时，`fee_pnl_usdt` 与 `net_pnl_usdt` 置空（避免静默把缺失当 0）
  - `live_trading_stats_pnl` 的净PnL曲线同样要求手续费覆盖完整，否则不纳入统计

## Commit 3：Tools：增加实盘手续费回填与PnL组件审计脚本
hash：`4008d58`

内容：
- `scripts/backfill_live_trade_order_fees.py`：对缺失 `fee_usdt` 的订单做 best-effort 回填（OKX/Bybit/Bitget/Hyperliquid）
- `scripts/audit_live_trade_pnl_components.py`：输出 fee/funding 缺失情况与“可严格计算净PnL”的覆盖率，并给出样本

常用命令：
```bash
./venv/bin/python scripts/audit_live_trade_pnl_components.py --limit 20
./venv/bin/python scripts/backfill_live_trade_order_fees.py --since-hours 336 --exchange bybit --limit 200 --dry-run
./venv/bin/python scripts/backfill_live_trade_order_fees.py --since-hours 336 --exchange bybit --limit 200
```

## Commit 4：Ops：增加 simple_app 保活 watchdog 与 systemd 单元
hash：`0a2d0ea`

内容：
- `scripts/simple_app_watchdog.py`：监控 `:4002`，异常自动重启；记录退出原因（含 dmesg OOM tail + app log tail），并对 `nohup_simple_app.out` 做轮转
- `scripts/systemd/simple_app_watchdog.service`：systemd 常驻守护
- `scripts/systemd/install_simple_app_watchdog.sh`：一键安装/启用 systemd 服务

常用命令：
```bash
nohup ./venv/bin/python scripts/simple_app_watchdog.py > logs/simple_app_watchdog.stdout 2>&1 &
tail -f logs/simple_app_watchdog.log

# systemd（需要 root 权限）
./scripts/systemd/install_simple_app_watchdog.sh
systemctl status --no-pager simple_app_watchdog.service
```

