# FR_Monitor 当前状态梳理

> 更新时间：2026-05-10  
> 目的：把 `FR_Monitor` 从“功能很多但主线分散”的状态收口，明确哪些资产可复用、哪些链路半成品、哪些风险必须先审计。

---

## 0. 一句话结论

`FR_Monitor` 已经不是单纯资金费率看板，而是一个半成品的 **跨交易所 perp/spot 数据底座 + 事件 watchlist + 订单簿复核 + 小额实盘执行/审计系统**。

下一阶段不应继续横向堆因子，而应围绕新主线重组：

> **资金费率 + 拥挤交易 + 控盘币事件，目标找厚 alpha，不再追逐薄 IC。**

已达成的策略决策记录见：`docs/STRATEGY_DECISION_2026-05-10_EVENT_FIRST_ALPHA.md`。

---

## 1. 代码资产地图

### 1.1 核心数据与服务

| 模块 | 文件/目录 | 当前作用 | 状态 |
| --- | --- | --- | --- |
| 运行配置 | `config.py` | 交易所端点、watchlist、PG、8010、live trading、风控阈值 | 可用，但配置很多，需分层解释 |
| Web 主服务 | `simple_app.py` | Flask 服务，端口 4002，聚合行情、watchlist、交易接口、页面 API | 可用，承担职责偏重 |
| 交易所连接 | `exchange_connectors.py` | WebSocket + REST 行情采集、合并、动态币种行情 | 可用，复杂度高 |
| 市场发现 | `market_info.py` | 动态币种发现、交易所市场覆盖 | 可用，历史上修过 Bitget 字段坑 |
| SQLite 存储 | `database.py` | `price_data`、`price_data_1min` 秒级/分钟级数据 | 可用，但大查询易慢，需谨慎 |
| funding 工具 | `funding_utils.py` / `precision_utils.py` | funding interval、精度归一 | 可复用 |
| 历史 funding 过滤 | `funding_history_filter.py` | Type B 开仓前检查 48h funding 稳定性 | 可复用 |

### 1.2 Watchlist / Event / 回测

| 模块 | 文件/目录 | 当前作用 | 状态 |
| --- | --- | --- | --- |
| Watchlist 生成 | `watchlist_manager.py` | 生成 A/B/C 三类信号，接 PG writer 和 live kick | 核心资产 |
| PG 写入 | `watchlist_pg_writer.py` | raw/event/future outcome 双写，event 合并，订单簿验算 | 核心资产，需持续审计 |
| 指标计算 | `watchlist_metrics.py` | spread/funding/series 因子计算 | 可用，时间窗口口径需继续校准 |
| 预测模型 | `watchlist_pnl_regression_model.py` | 5 因子与 v2 预测入口 | 可做排序器，不宜做唯一信仰 |
| 回测脚本 | `backtest/` | IC/IR、Ridge/Logistic 训练/评估 | 可复用为事件后过滤研究 |
| 报告 | `reports/` | IC/IR、线性模型、v2 模型、live audit | 有价值，需转化为路线约束 |

### 1.3 执行与实盘

| 模块 | 文件/目录 | 当前作用 | 状态 |
| --- | --- | --- | --- |
| 通用执行器 | `trading/trade_executor.py` | Binance/OKX/Bybit/Bitget perp/spot 下单、持仓、精度 | 核心资产 |
| Live manager | `trading/live_trading_manager.py` | Type B/C 自动开仓、监控、止盈/止损、scale-in | 可用但风险高，默认策略需谨慎 |
| 交易 API | `simple_app.py:/api/trade/dual` | 多腿交易、开多/开空/平多/平空/全部平仓 | 可用 |
| 测试脚本 | `trading/test_*.py` | 各交易所小额实盘/私有 API 验证 | 可复用 |
| 审计脚本 | `scripts/audit_*.py` | live signal/order/funding/pnl 审计 | 必须继续加强 |

### 1.4 DEX / 外部 BBO 服务

| 模块 | 文件/目录 | 当前作用 | 状态 |
| --- | --- | --- | --- |
| 8010 服务接入 | `MONITOR_8010_CONFIG` / `watchlist_postgres_plan.md` | 外部 BBO/snapshot/watchlist proxy | 可接入，但只适合作 L1/BBO；深度仍需 REST sweep |
| DEX bot | `dex/` | EdgeX/Backpack/Paradex/Aster/Lighter/GRVT/Extended 刷量/对冲机器人 | 可拆连接器，不能直接当策略主线 |
| Lighter/GRVT/Hyperliquid 配置 | `config.py` | DEX/perp 端点与代理配置 | 是 CEX-DEX funding/basis 方向的基础 |

---

## 2. 现有信号类型

### Type A：Funding Spike

**含义**：单交易所/单币资金费率异常，加入观察。

**现状**：早期逻辑，自动化价值尚未完全展开；报告中 Type A 样本经常不足。

**后续定位**：作为事件发现源之一，不单独作为 live 主策略。

---

### Type B：Perp-Perp 跨交易所价差

**含义**：同一 symbol 在不同交易所的 perp 存在价差，高价所 short，低价所 long，押注价差收敛。

**当前链路**：

1. 行情采集得到各交易所 perp price/funding；
2. `watchlist_manager.py` 识别价差；
3. funding 用 `net_cost` 和历史稳定性过滤；
4. `watchlist_pg_writer.py` 写 raw/event，并做订单簿复核；
5. `watchlist_pnl_regression_model.py` 给 pnl/win_prob 排序；
6. `live_trading_manager.py` 可小额实盘；
7. 审计脚本复盘订单和 PnL。

**现状判断**：这是目前最成熟的主干，但它更像“价差套利”，还没充分表达“拥挤交易/控盘事件”。

---

### Type C：Spot-Perp 价差

**含义**：现货低于永续，理论上可 long spot + short perp。

**当前链路**：

- Binance/OKX/Bybit/Bitget spot 私有接口已补齐；
- `spot_trading_enabled` 默认关闭；
- 可先 signal-only；
- 订单簿复核已支持 spot/perp 口径。

**现状判断**：结构比 Type B 更干净，但执行/余额/残币/手续费更麻烦。适合在控盘币事件中作为高价值候选，而不是全市场盲扫。

---

## 3. 研究结果摘要

### 3.1 IC/IR 报告的有效信息

报告：

- `reports/first_backtest_ic_ir_last1d.md`
- `reports/first_backtest_ic_ir_last1d_spread.md`
- `reports/first_backtest_ic_ir_last1d_funding.md`

可用因子包括：

- `raw_slope_3m`
- `raw_drift_ratio`
- `spread_log_short_over_long`
- `raw_crossings_1h`
- `raw_best_buy_high_sell_low`
- `funding_edge_short_minus_long`
- `time_to_next_funding_min`

**解释**：这些因子有预测力，但大多是“事件后的排序/过滤器”，不是独立信仰。继续全市场挖薄 IC 的边际价值下降。

### 3.2 5 因子线性模型

报告：`reports/pnl_linear_regression_5factors_fee10bps.md`

观察：

- Type B/C 的 top 预测分位有更高 mean pnl 和 win_rate；
- 但报告也明确写着 in-sample、fee/slippage 需要重新解释；
- 胜率阈值 `fee_threshold=0.001` 不等于真实四笔交易后的净利润阈值。

**定位**：可做候选排序和阈值辅助，不可直接作为 live 充分条件。

### 3.3 V2 Ridge + Logistic

报告：`reports/v2_ridge_logistic_summary.md`

有效样本：约 15k，valid_days 仅 7。大致结果：

- 60m valid AUC ≈ 0.717；
- 240m valid AUC ≈ 0.723；
- 480m valid AUC ≈ 0.724；
- 1440m valid AUC ≈ 0.725。

**定位**：模型有排序价值，但 valid window 短、费用假设偏轻。下一阶段应进入事件条件下的 OOS/paper/live 审计。

---

## 4. 当前关键风险

### 4.1 价格口径风险

禁止用 last/mark 直接判断可交易收益。必须优先使用：

1. BBO bid/ask；
2. 按目标名义金额 sweep 后的成交均价；
3. last/mark 仅作 fallback 和展示。

### 4.2 Funding interval 风险

不同交易所 funding interval 可能是 1h/4h/8h，且 Binance 也存在可变 interval。必须按小时归一：

```text
funding_per_hour = funding_rate / interval_hours
net_carry_per_hour = short_funding_per_hour - long_funding_per_hour
```

不要简单把 daily funding 加总当信号。

### 4.3 DEX 接入风险

DEX/CEX 套利不是只看 funding spread。还要考虑：

- L1/BBO 延迟；
- 深度不足；
- 下单延迟；
- 限仓/禁开；
- mark/index 机制差异；
- 结算币与保证金差异；
- API 稳定性。

### 4.4 控盘币逆势风险

PIPPIN/SIREN/币安人生这类币可能继续被控盘拉升。不能因为 funding 高就裸空。

更安全的第一选择是结构化交易：

- 高价所 short + 低价所 long；
- spot long + perp short；
- 或等衰竭确认后再做方向 fade。

### 4.5 Live trading 审计不足

`reports/live_trading_audit_970.md` 显示已有小额实盘，但 fee 数据不完整，净 PnL 不能只看 realized_pnl。

必须补齐：

- fee；
- funding PnL；
- slippage；
- rejected/skipped reason；
- close reason；
- mark-to-market path。

---

## 5. 可复用资产清单

下一阶段最应该复用的资产：

1. **行情底座**：CEX/DEX price/funding/premium/volume；
2. **PG event schema**：raw/event/future_outcome；
3. **订单簿复核链路**：Top-K exchange、bid/ask、sweep；
4. **交易执行器**：perp/spot 多交易所下单；
5. **模型排序器**：5 因子 + v2，仅做辅助；
6. **live audit 脚本**：必须升级为每笔交易强制产物；
7. **8010 BBO 服务**：作为低延迟 L1 数据源，但不替代深度 sweep；
8. **DEX connector 代码**：拆连接器，不照搬刷量策略。

---

## 6. 建议默认策略状态

| 功能 | 建议状态 | 原因 |
| --- | --- | --- |
| Type B signal | 开启 | 当前最成熟 |
| Type C signal | 开启 | 结构有价值，但先 signal/paper |
| Type A signal | 开启为事件源 | 不单独 live |
| Live trading 自动开仓 | 谨慎/小额/白名单 | 审计还不够完整 |
| DEX-CEX live | 关闭 | 先 paper 2 周 |
| 控盘币事件雷达 | 新增，优先做 | 最可能找到厚 alpha |
| 大量全市场 IC 挖掘 | 降级 | 边际下降，易回到薄 alpha |

---

## 7. 下一阶段北极星指标

不要用“IC 是否显著”作为唯一目标。改用：

1. **事件捕捉率**：PIPPIN/SIREN/币安人生这类币是否能及时进入 event universe；
2. **可成交厚度**：50U/100U/500U sweep 后是否仍有正 edge；
3. **paper 净收益**：包含 fee/slippage/funding/basis；
4. **live 审计闭环率**：每笔是否能解释盈亏来源；
5. **机会稀缺性**：宁可每天 1-5 个高质量事件，不要 100 个薄信号。
