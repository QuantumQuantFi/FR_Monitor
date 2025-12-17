# Watchlist → 实盘交易系统研发计划（含 PostgreSQL 记录方案）

本文目标：基于现有 watchlist（预计 PnL / 胜率）+ 现有实盘交易组件，规划并落地一个“自动开仓 + 监控止盈/强平 + 手动交易 + 页面展示 + 全链路审计落库”的实盘系统。

---

## 0. 当前代码现状（已具备的组件）
- Watchlist 信号与预测：`watchlist_manager.py` 生成 `trigger_details` + `pnl_regression`（含 `pnl_hat` / `win_prob`，展示在 `templates/watchlist.html`）。
- Watchlist 落库：`watchlist_pg_writer.py` 双写到 PostgreSQL（schema=`watchlist`），核心表：`watch_signal_raw` / `watch_signal_event` / `future_outcome` / `watchlist_series_agg` / `watchlist_outcome`。
- 实盘交易接口封装：`trading/trade_executor.py` 支持 `binance/okx/bybit/bitget` 的永续市价单、部分杠杆设置与持仓查询。
- Web 侧交易入口：`simple_app.py` 已有 `/api/trade/options` + `/api/trade/dual`（多腿、USDT金额换算、reduceOnly/closeAll），`templates/chart_index.html` 已有“对冲交易执行”面板。
- 订单簿价差工具：`orderbook_utils.py` 已有 REST 拉取与扫单价差计算，可用来做 1min 频率监听。

结论：我们不需要从 0 写“交易所 SDK / 下单 / 基础 UI”，主要工作是把 watchlist 信号“可靠地”转成交易任务，并补齐去重、记录、监控与页面展示。

---

## 0.1 回归预测审计（潜在失败点/不准确来源）

### 0.1.1 计算失败（`pnl_hat/win_prob` 为空）的常见原因
- **因子缺失**：线性模型必须同时拿到 5 个因子 `raw_slope_3m/raw_drift_ratio/spread_log_short_over_long/raw_crossings_1h/raw_best_buy_high_sell_low`，任一缺失会导致 `predict_bc()` 返回 `None`。
  - 典型触发：跨所腿的历史分钟序列在本地 SQLite 不完整（某交易所/某币种未持续入库）→ `compute_metrics_for_legs()` 退化为 `no_data` → `slope_3m/drift_ratio/crossings_1h` 为空。
  - 典型触发：Type B 触发明细 `trigger_details.prices` 缺失/结构变化 → 无法补齐 `spread_log_short_over_long/raw_best_buy_high_sell_low`。
- **采样频率不一致**：`watchlist_metrics.py` 的部分窗口指标默认按“点数≈分钟”计算；当分钟点不是严格 1min（掉点/稀疏/重采样）时，`raw_slope_3m/raw_crossings_1h/raw_drift_ratio` 会产生偏差，进而影响预测。

### 0.1.2 不准确（预测偏乐观/偏悲观）的主要来源
- **价格口径差异**：watchlist 侧用于补齐 `spread_log_short_over_long/raw_best_buy_high_sell_low` 的价格可能来自 last/mark（而非 bid/ask + 扫单滑点），会偏离“可成交价差”；实盘侧必须用订单簿按名义金额扫单复算。
- **胜率定义口径**：当前 `win_prob` 的定义为 `P(pnl > fee_threshold)`，默认 `fee_threshold=0.001(10bps)`；这不等同于“净利润>0”（更不含开/平四笔手续费+滑点）。实盘阈值解释与 fee 假设需要对齐，并建议后续把 `fee_threshold` 参数化到策略配置。
- **统计假设**：`win_prob` 通过残差 `Normal(resid_std)` 近似得到，且报告为 in-sample；真实分布可能重尾/异方差，容易导致概率失真。建议后续做滚动时间切分 OOS 验证与概率校准（例如按 `pnl_hat` 分箱的经验胜率校准）。

### 0.1.3 已做的防护/修复（与本计划强相关）
- **watchlist 侧缺失回退**：当 `trigger_details` 无法提供价格时，允许用 snapshot 的 `best_buy_high_sell_low` 退化补齐 `raw_best_buy_high_sell_low`，并用 `log(1+spread)` 补 `spread_log_short_over_long`，避免因子不齐导致预测直接失败（仍建议以订单簿复算为准）。
- **实盘侧方向一致性**：订单簿复算时，必须使用“高所 bid(sell) / 低所 ask(buy)”计算 `spread_log_short_over_long` 与 `raw_best_buy_high_sell_low`，确保与开仓方向一致。

### 0.1.4 下一步建议（不影响现有功能，但提升稳定性/准确性）
- 增加“预测覆盖率”监控：`watch_signal_event` 中 `meta_last.factors`/`pnl_regression` 的缺失占比、缺失原因分布（按交易所/币种/时间）。
- 将 `watchlist_metrics.py` 的关键窗口（`3m slope/1h crossings/rolling mean/std`）改为**按时间戳截取窗口**，而不是按点数，降低掉点/稀疏导致的偏差。
- 将 `fee_threshold` 与交易所费率/预估滑点联动：为不同交易所/腿组合动态估算“净胜阈值”，并落库到 `live_trade_signal.threshold` 方便回放。
- 对“分钟数据不完整”的现实做策略适配：仍保持 **全币种全交易所落库**，但在因子计算（尤其 Type B/C 跨所两腿）阶段允许用 `price_data` 的最近邻数据按分钟网格对齐（配置 `WATCHLIST_METRICS_CONFIG.legs_nearest_max_gap_seconds`），避免因 `price_data_1min` 的同分钟交集太少导致预测失败。

---

## 1. 信号规则与过去 24h 频次校准（你提出的第 1 条）

### 1.1 规则定义（建议先参数化）
- 触发条件（最终确认）：`horizon=240m`，且 **同时满足**：
  - `pnl_hat > 1.3%`（0.013）
  - `win_prob > 94%`（0.94）
- 关键参数：信号只做 **Type B（perp-perp 跨所）**；Type C 后续引入现货交易后再做自动化（文档中保留 TODO）。
- 下单前二次验算：用“订单簿扫单价”（与实际下单名义一致，默认 20U/腿）重新计算 `spread_log_short_over_long` 等关键因子，并重新运行 `predict_bc(signal_type='B', horizon=240)`；若验算后仍不满足阈值才允许下单。

### 1.2 数据库实测：过去 24h 有多少次？
我们已连接本机 PostgreSQL（DSN 见 `config.py:WATCHLIST_PG_CONFIG`），从 `watchlist.watch_signal_event.features_agg.meta_last.pnl_regression` 统计：
- 以 `240m` 为准：`pnl_hat>1% & win_prob>92%` => **80** 次（distinct symbol 51）
- 以 `60m` 为准：**67** 次
- 以 `480m` 为准：**81** 次
- 任一 horizon 满足（60/240/480 任一）：**88** 次

因此：该阈值在过去 24h **远大于 2 次**，不需要降低；反而对“实盘频率/风控/稳定性”来说可能偏频繁，需要我们用“去重 + 冷静期 + 最大并发 + 每日上限”来控制风险。

### 1.2.1 你的最新阈值（1.3%/94%）对频率的影响（待你上线后复测）
因为你刚刚提升了阈值，我们会在落地代码后补一条 SQL 统计脚本，持续观察 24h 频率与成交成功率，并据此微调（主要靠冷静期与 max-concurrent 控制风险）。

### 1.3 如果你希望减少交易频率（可选的更“实盘友好”阈值）
仍以 `240m` 为例（仅建议，不改你原始目标）：
- `pnl_hat>1.5% & win_prob>92%` => 13 次/24h
- `pnl_hat>2.0% & win_prob>92%` => 2 次/24h
- `pnl_hat>1.0% & win_prob>97%` => 19 次/24h

---

## 2. 总体架构（信号 → 执行 → 监控 → 展示 → 审计）

### 2.1 组件划分（推荐）
- `SignalGate`：从 PG 或内存 watchlist 读取候选信号；按阈值/交易所支持/流动性等过滤；产出“交易信号”。
- `TradeCoordinator`：幂等去重（同币种/同腿组合/同方向）；落库信号；把任务派发给执行器。
- `TradeExecutor`：复用 `simple_app.py:_execute_multi_leg_trade()` 或 `trading/trade_executor.py:execute_perp_market_batch()`，执行两腿市价单（1x、20U/腿），产出订单回执。
- `PositionMonitor`：每 1min 用 REST 同时拉两家订单簿（同一时刻），计算当前价差/估算可平仓 PnL；满足止盈或超时则触发平仓。
- `Web UI`：新增一个“实盘页面”展示：最近信号、执行状态、当前持仓、当前委托（至少展示我们下过的单/最近回执）、监控状态；并提供手动交易按钮。
- `Postgres`：全链路记录（信号、执行、两腿订单、价差采样、平仓原因/结果），便于复盘与改进阈值。

### 2.2 落地路径（尽量不破坏现有主流程）
- 自动交易逻辑作为 `simple_app.py` 内的后台线程（类似现有 orderbook snapshot/metrics snapshot），先跑起来验证；后续如果你想独立进程再抽出来。

---

## 3. 自动开仓流程（你提出的第 2 条：去重 + 双腿下单）

### 3.1 信号对象（建议字段）
- 来源：`watch_signal_event.id`（event_id）+ `symbol` + `signal_type` + `features_agg.meta_last.trigger_details` + `features_agg.meta_last.pnl_regression`。
- 交易腿：`leg_long` / `leg_short`，每条包含 `exchange`、`symbol`、`market_type`（先只做 perpetual）、`notional_usdt=20`、`leverage=1`。
- 目标：两腿同时下单（尽量并发，但要考虑 API 限频与失败回滚策略）。

### 3.2 方向判定（如何从 watchlist 得到 long/short）
- Type B（跨交易所永续）：从 `trigger_details.prices` 找到“高价交易所/低价交易所”：
  - `short` 高价 perp（价差收敛时盈利）
  - `long` 低价 perp
- Type C（现货 vs 永续）：一期 **不自动交易**（当前交易模块未实现 spot 下单）。后续加入现货交易后，再实现 Type C 自动开仓与平仓。
- Type A（同所现货 vs 永续、资金费逻辑）：同样受限于 spot 下单与策略口径，建议先不自动化，先做记录与展示。

### 3.3 幂等去重（必须做，否则会被连续信号刷爆）
规则：实盘系统收到开仓信号后，先检查同币种是否已开仓或正在开仓；如果是重复信号则丢弃。
推荐实现：
- DB 层：为“活跃交易”建立唯一约束/幂等键（例如 `(symbol, leg_long_exchange, leg_short_exchange, status in ('opening','open'))`）。
- 进程内：再加一层内存锁（`symbol` 级别 mutex），避免并发线程重复触发。
- 冷静期：信号即便关闭，也建议对同 `symbol+pair` 设置 30~120 分钟 cooldown（参数化），避免短周期反复进出。
- 多交易所多配对：同一 `symbol` 可能同时出现多组交易所配对触发；系统应在单周期内对同一 `symbol` 只选择 **验算后 pnl_hat/win_prob 最优** 的一组配对尝试开仓。
- 同时持仓上限：系统全局最多允许 **10 对腿**（10 笔对冲交易）处于 `opening/open/closing` 状态；超出则忽略新信号并记录原因。

---

## 4. 交易执行细节（你提出的第 2 条：市价、1x、20U/腿）
- 市价单：统一走现有 `/api/trade/dual` 的 legs 协议或直接调用 `execute_perp_market_batch()`。
- 杠杆：对 OKX/Bybit/Bitget 先调用 `set_*_leverage(symbol, 1)`（失败可降级继续，但要记录）；Binance 目前可先不显式设置（或二期补）。
- 金额：每腿 `50 USDT`（避免部分交易所最小下单数量/名义额导致拒单）；由现有换算逻辑 `_convert_notional_to_quantity()` 完成。
- client order id：建议用 `signal_id` 派生（便于串联审计），例如：`wl{event_id}-{symbol}-{ts}-L01`。
- 异常与回滚：
  - 若一腿下单成功、另一腿失败：立刻尝试把已下单腿“反向 reduceOnly 市价平掉”（需要可靠的成交数量与交易所支持），并标记该次执行为 `partial_failed`。
- 错误记录（你提出的第 5 点）：所有阶段错误（验算失败/下单拒绝/网络超时/最小数量不足/杠杆设置失败）必须写入 PG 的 `live_trade_error`，用于后续迭代稳定性。

---

## 5. 页面展示（你提出的第 3 条：委托单/持仓简要展示）
建议在 `simple_app.py` 增加一个页面（例如 `/live_trading`）：
- 当前活跃交易列表：symbol、pair、开仓时间、预计 pnl/win、当前价差、止盈目标、已持有时长、状态。
- 当前持仓：优先展示我们系统“正在跟踪的 symbol+exchange” 的持仓（通过 `get_*_positions()` 拉取）。
- 当前委托：交易模块目前未实现“open orders”查询；一期先展示“我们提交的订单回执 + 最新一次查询结果”，二期再补齐各所 open orders API。

---

## 6. 交易记录落库（你提出的第 4 条）
建议：继续使用 **现有 PostgreSQL（schema=`watchlist`）**，新增几张“实盘交易表”，避免把实盘细节塞进 `watchlist_outcome`（语义不同）。

推荐最小表（一期）：
1) `watchlist.live_trade_signal`
   - `id bigserial pk`, `created_at timestamptz`, `event_id bigint`, `symbol text`, `signal_type char(1)`
   - `horizon_min int`, `pnl_hat double precision`, `win_prob double precision`, `threshold jsonb`
   - `leg_long_exchange text`, `leg_short_exchange text`, `status text`（new/ignored/opening/open/closing/closed/failed）
   - `reason text`, `payload jsonb`（保存 trigger_details/pnl_regression 快照）
2) `watchlist.live_trade_order`
   - `id bigserial pk`, `signal_id bigint`, `leg text`（long/short）, `exchange text`, `client_order_id text`
   - `submitted_at timestamptz`, `order_resp jsonb`, `normalized_qty text`, `status text`
3) `watchlist.live_trade_spread_sample`
   - `id bigserial pk`, `signal_id bigint`, `ts timestamptz`, `bidask jsonb`, `spread double precision`, `pnl_est double precision`

二期再加：
- `live_trade_fill`（成交明细/均价）、`live_trade_exit`（平仓原因、最终 pnl）、`live_trade_error`（异常）。

---

## 7. 价差监听与止盈/强平（你提出的第 5 条）
- 频率：1min（可配置）；每次同时拉两家订单簿（并发），用同一时间窗计算价差，避免“时序错位”。
- 数据源：优先 REST（你要求的实时性与一致性），复用 `orderbook_utils.fetch_orderbook_prices()`；扫单名义建议用 `20~50 USDT`，更贴近我们实际平仓滑点。
- 止盈：当“实时可平仓 pnl_est”达到 `0.8 * pnl_hat`（同一 horizon 的预测）触发平仓。
- 强平：持有时间超过 7 天强制平仓（不管 pnl）。
- 额外风控（建议）：
  - 最大同时持仓数（例如 3~10）
  - 单币种单方向只允许 1 笔活跃交易
  - 交易所 API 错误时退避（指数 backoff）并告警/打日志

---

## 8. 手动交易按钮与测试（你提出的第 6 条）
你要求“手动开多/开空/平多/平空”，并且每次测试可用 `BTC 20 USDT`（如果交易所最小下单限制导致失败，则提升到 `50 USDT`）：
- 一期：复用 `templates/chart_index.html` 的交易面板逻辑，抽成可复用组件放到新 `/live_trading` 页面，并增加快捷按钮：
  - `BTC 开多 20U`、`BTC 开空 20U`、`BTC 平多 20U`、`BTC 平空 20U`
- 交易正确性以 **交易所 API 返回 + 实盘持仓变化** 为准（不是页面展示），每次测试后在页面上刷新持仓确认。

---

## 9. 分阶段实施路线图（逐步带你实现）
Phase 1（最小可用，先跑通）：
1) 定义“自动开仓信号”的数据结构与阈值配置（horizon/阈值/冷静期/每日上限）。
2) 新增 PG 表：`live_trade_signal` / `live_trade_order` / `live_trade_spread_sample`。
3) 实现 `SignalGate`：从 `watch_signal_event` 拉取候选，过滤并写入 `live_trade_signal`（先不下单）。
4) 接入下单：用 `/api/trade/dual` 执行两腿 20U、1x；并写 `live_trade_order`。
5) UI：增加 `/live_trading` 页面展示信号与执行状态；加 BTC 20U 手动按钮（复用现有交易面板）。

Phase 2（监控与平仓闭环）：
6) 1min 订单簿监听与 pnl_est 计算；达到 0.8*pnl_hat 止盈；>7d 强平。
7) 完善失败回滚、重试、告警日志；补齐 open orders 查询（若确实需要展示委托）。

Phase 3（策略与质量）：
8) Type C/A 的自动化策略口径定稿（是否引入 spot，或仅做 perp-perp 替代）。
9) 与 `future_outcome` 对齐口径，做“预测 vs 实盘结果”偏差分析，迭代阈值/因子。

---

下面保留现有的“Watchlist PG 建设规划”原文，作为 watchlist 数据层与 outcome 计算的背景与附录。

## 目标与约束
- 目的：记录 watchlist 的触发信号、执行/结果、回测特征输入，支撑因子迭代，并支撑现有前端页面的展示与图表。
- 约束：当前服务器已在跑 7 个交易所、约 800 个币种，CPU/磁盘紧张；必须关注写入开销、索引数量和存储占用。
- 优先级：先落地“触发信号 + 结果”两张主表；用精简序列支撑现有 watchlist 页面；再逐步丰富特征与回测标签。

## 预估数据量与资源占用
- 新约束：关注币种通常 <20 个；可接受 1~5 分钟落库一次（watchlist 及因子）。
- 量级重估（按 20 个符号，每 1 分钟）：20 * 60/h ≈ 1.2k 行/小时，≈ 28.8k 行/天；行尺寸 ~200B/行，则日增 ~6MB，7 天 ~42MB。
- 若 5 分钟落库：日增 ~1.2MB，基本可忽略。触发事件行数更低（<千级/天）。
- 实测（2025-12-09 07:40 UTC）：两天 raw 分区合计 ~27MB（含订单簿精简数据）；全库 ~29MB。推算 30 天约 0.6GB 上下（含索引/WAL 预留 <1GB）。  
- 结论：在 20 符号、1~5m 频率下，存储占用极小，保留期可拉长（raw 30 天，event/outcome 180 天），仍保持分区与周期清理以防膨胀。

## 与现有 watchlist 页面/接口的对应
- 页面：`templates/watchlist.html`（列表+指标）与 `templates/watchlist_charts.html`（12h 时序）。
- 新增：`/watchlist/db` 简易 PG 浏览页，可分页查看 raw/event/outcome；默认 10 行，便于随用随查。
- 接口：`/api/watchlist`（列表）、`/api/watchlist/metrics`（Type A 指标）、`/api/watchlist/series`（12h 图表）。
- 列表字段：Type A/B/C、资金费率、Spread 最新/均值/σ、区间(1h/12h)、斜率/穿越/Drift、跨所差价矩阵（订单簿扫单）、下一次资金费时间等。
- 图表需求：12h 分钟级价差（spot/futures）、15m 中线、基线、开/平仓点、资金费时间标注。
- 影响：数据库至少要支撑 (a) 5m~1m 粒度价差序列（仅 watchlist 符号），(b) Type A 指标所需窗口统计，(c) 触发/结果的回溯与标签；全量行情仍走现有高频通道，不进 PG。

## 最小可行表结构（迭代式上线，兼顾因子/IC）
1) `watch_signal_raw`（分钟级原始因子池）
   - 作用：保留复合 A/B/C 条件的分钟级快照（正样本池），用于回测/因子标签与后续分析。
   - 范围：仅对当轮扫描中满足 A/B/C 触发条件的币种/腿组合写入；不覆盖全市场低信号样本。若未来需要负样本，可另设抽样表或离线作业补齐。
   - 字段：`id bigserial pk`, `ts timestamptz`, `exchange`, `symbol`, `signal_type`(A/B/C)、核心特征列（价差、资金费、成交额、深度/滑点等）、`triggered bool`, `meta jsonb`。
   - 分区：日分区，保留 7~14 天；索引仅 (`exchange`,`symbol`,`ts`) + (`ts` DESC)。

2) `watch_signal_event`（事件归并）
   - 作用：将连续多分钟满足阈值的信号合并为单一 event，避免重复统计。
   - 字段：`id bigserial pk`, `exchange`, `symbol`, `signal_type`, `start_ts`, `end_ts`, `duration_sec`, `features_agg jsonb`（均值/极值/首末值）、`reason text`, `status text`（new/confirmed/dropped）。
   - 索引：(`exchange`,`symbol`,`start_ts`), (`start_ts` DESC)。
   - 保留：90~180 天。

3) `future_outcome`（未来表现/IC 核心表）
   - 作用：记录事件后若干窗口的结果，用于因子收益与 IC/IR 计算。
   - 字段：`id bigserial pk`, `event_id fk`, `horizon_min int`, `pnl`, `spread_change`, `funding_change`, `max_drawdown`, `volatility`, `label jsonb`（正/负例，或分位标签）。
   - 索引：(`event_id`), (`horizon_min`)。

4) `watchlist_outcome`（执行/结果/回测对齐，保留原设计）
   - 字段：`id`, `trigger_id fk`(可指向 event)，`opened_at`, `closed_at`, `pnl`, `max_drawdown`, `holding_hours`, `benchmark_return`, `labels jsonb`, `notes`。
   - 索引：(`trigger_id`), (`opened_at`)。

5) `watchlist_symbol_ref`（元数据）
   - 字段：`exchange`, `symbol`, `quote`, `base`, `sector/industry`, `listed`, `status`, `liquidity_tier`, `updated_at`。
   - 索引：(`exchange`, `symbol`)。

6) `watchlist_series_agg`（前端 12h 图表序列）
   - 粒度：先 5m，再视效果降到 1m；仅 watchlist 符号。
   - 字段：`ts`, `exchange`, `symbol`, `spot_price`, `futures_price`, `spread_rel`, `funding_rate`, `funding_interval_hours`, `next_funding_time`, `series_meta jsonb`。
   - 分区：按天；保留 3~7 天。
   - 来源：行情窗口聚合，避免全量逐笔。

### 字段建议（首批最小集，控制宽度）
- 公共：`exchange` (text)、`symbol` (text)、`signal_type` (char)、`ts/ start_ts/ end_ts` (timestamptz)。
- 价差/资金费：`spread_rel`、`funding_rate`、`funding_interval_hours`、`next_funding_time`。
- 交易量/流动性：`volume_quote`（24h USDT）、`book_imbalance`（如有）、`sweep_impact`（100 USDT 成交滑点，双向取 max）。
- 波动/形态：`range_1h`、`range_12h`、`volatility`（σ）、`slope_3m`、`crossings_1h`、`drift_ratio`。
- 状态：`triggered`（bool）、`status`（event 状态）。
- 长尾/调试：`meta jsonb` / `features_agg jsonb`。核心数值列尽量单列存储，其他放 JSONB。

### 字段细化（贴合 watchlist API/前端复杂结构）
- 记录粒度：`watch_signal_raw` 仍按“符号 + 时间”一行，核心标量开列，复杂跨所细节压缩在 JSONB，避免表爆宽。
- 核心标量（raw）：`spread_rel`（基差）、`funding_rate`（基准所的当前资金费）、`funding_interval_hours`、`next_funding_time`、`range_1h/12h`、`volatility`、`slope_3m`、`crossings_1h`、`drift_ratio`、`best_buy_high_sell_low`、`best_sell_high_buy_low`（双向可实现价差，百分比）、`type_class`（A/B/C）、`triggered`、`status`。
- 跨所明细（raw.meta JSONB，结构建议）：
  ```json
  {
    "pairs_top": [
      {"high_exch": "bitget", "low_exch": "bybit", "spread_pct": 0.00339, "dir": "buy_high_sell_low"},
      {"high_exch": "bybit", "low_exch": "bitget", "spread_pct": 0.00115, "dir": "sell_high_buy_low"}
    ],
    "funding_diff_top": [
      {"high_exch": "bitget", "low_exch": "okx", "funding_diff": 0.0006},
      {"high_exch": "binance", "low_exch": "bybit", "funding_diff": 0.0004}
    ],
    "exchanges": [
      {
        "name": "binance",
        "kind": "perp" | "spot",
        "funding_rate": 0.0001,
        "funding_interval_hours": 8,
        "funding_cap_high": 0.02,
        "funding_cap_low": -0.02,
        "price_bid": 0.065527,
        "price_ask": 0.065495,
        "oi_usd": 1490000,
        "volume_24h_quote": 1390000,
        "insurance_fund": 2.58e9,
        "funding_hist": {"mean": 0.00003, "std": 0.0001, "last": 0.0001, "n": 20},
        "index_diff": -0.00045
      }
      // ...仅保留有报价的所，空值过滤
    ],
    "matrix": [
      {"exch_a": "bitget", "exch_b": "bybit", "spread_pct": 0.00339, "dir": "a_minus_b"},
      {"exch_a": "okx", "exch_b": "bybit", "spread_pct": -0.00106, "dir": "a_minus_b"}
    ],
    "notes": "front-end hints or debug"
  }
  ```
- 订单簿/矩阵落库（已上线）：在 raw.meta.orderbook 存 sweep 价格与前 5 对跨所价差（forward/reverse），字段：legs[exchange,type,buy,sell,mid,error]，forward_spread/reverse_spread，cross_pairs[top5]，带快照 ts/stale_reason，控制宽度。
- 双腿字段（已上线）：raw/event 现已写入 `leg_a_*`、`leg_b_*`（exchange/symbol/kind/price/funding/next_funding_time）；老数据缺腿信息，outcome worker 会跳过。
- 事件表（event.features_agg）：保留首/末/均/极值的核心标量（同上），再附带 `pairs_top` 与 `funding_diff_top` 的首末快照，以便回溯。
- 事件表补充（2025-12-12）：`watch_signal_event` 新增并回填了 funding schedule 列（仅修复 schedule，不修改 funding_rate）：
  - event 顶层：`funding_interval_hours`、`next_funding_time`
  - event 腿级：`leg_a_funding_interval_hours`、`leg_a_next_funding_time`、`leg_b_funding_interval_hours`、`leg_b_next_funding_time`
- 序列表（`watchlist_series_agg`）：继续按 5m 聚合开列 `spot_price`、`futures_price`、`spread_rel`、`funding_rate`、`funding_interval_hours`、`next_funding_time`，其余如矩阵/跨所资金费差保持在 `series_meta jsonb`（最多存当时 top1 跨所价差与 top1 资金费差，控制行宽）。

### 因子补充（从现有行情/资金费/历史序列可计算）
- 价差行为：`spread_zscore_1h/12h`、`spread_mean_revert_speed`（AR(1) 半衰期）、`spread_momentum_15m/60m`、`spread_skew_kurtosis_12h`、`drawdown_12h`、`spread_rsi`、`spread_macd`/`signal`、`spread_adx`、`bollinger_pct_b`。
- 资金费行为：`funding_zscore_7d`、`funding_trend_3d`、`funding_vol_7d`（波动）、`funding_diff_max`（最大跨所资金费差）、`funding_regime`（分位标签）、`funding_term_slope`（不同周期之间斜率）、`funding_reversal_prob`（以历史均值回归概率近似）。
- 成交/流动性：`oi_trend_1d`、`oi_jump_around_funding`（资金费前后 OI 变化）、`volume_trend_1d`、`depth_imbalance`（多档合并）、`slippage_impact_5bps`（模拟扫单滑点）、`bid_ask_spread`、`microprice_imbalance`。
- 波动/均值回归：`rv_1h/rv_12h`（实现波动）、`hv_ratio`（短长波比）、`cross_freq_3h`（穿越频次，可扩 crossings_1h）、`vol_of_vol`（波动率的波动）、`tail_risk_score`（基于偏度/峰度）。
- 跨所结构：`triangular_spread_max`、`basis_term_structure`（不同 funding 周期的基差斜率）、`carry_score`（资金费 - 预期回归组合）、`index_mark_divergence`（指数价 vs 标记价差距）、`spot_perp_correlation`（相关性弱/偏离时可能均值回归）、`premium_index_diff`（各所溢价指数的最大/最小/分位差）。
- 标签/持久性：`signal_persistence`（连续触发分钟数）、`regime_label`（波动/流动性档位）、`hour_of_day`（时段季节性）、`session_label`（亚/欧/美盘）。
- 现货 vs 合约体量与控盘：`spot_volume_24h`、`perp_volume_24h`、`spot_perp_volume_ratio`、`oi_to_volume_ratio`（OI/成交额）、`perp_oi_dominance`（某所 OI/全所 OI）、`volume_spike_zscore`（短期量激增）、`volume_volatility`（量波动）、`turnover_rate_est`（估算换手，需流通量或总量可得时补充）。这些可用现有 24h 额/OI 计算，缺口字段放 meta。
- 落库策略：核心标量（Z 分数/趋势/RV/HV 比/funding_diff_max/term_slope/oi_jump/bid_ask_spread 等）可开列；长尾或实验性（MACD/ADX/RSI/vol_of_vol/triangular_spread_max 等）放 `meta jsonb`，事件层在 `features_agg` 做首/末/均/极值聚合，控制表宽。
- 落库策略：核心标量（如 Z 分数、趋势、RV/HV 比、funding_diff_max）可开列；长尾/实验性因子放 `meta jsonb`，事件层在 `features_agg` 做首/末/均/极值聚合，控制表宽。

### 事件归并规则（草案）
- 触发条件：连续 N 分钟满足阈值才生成 event（默认 N=2）；冷静期 M 分钟内重复满足视为同一 event（默认 M=3）。
- 事件窗口（以代码实现为准）：`start_ts`=满足 N 连续触发时刻（即 “确认入场时点/decision_ts”，不是首次触发时刻）；`end_ts`=最后一次满足或冷静期结束；`duration_sec`=差值。
- 特征聚合：`features_agg` 记录首末值、极值（当前已聚合 spread/funding/跨所最优差等）；**注意回测避免泄露**：event 会在后续分钟持续更新 `features_agg.last/min/max`，不能直接拿“最终版 features_agg”当入场因子。
- 去重/防抖：相同 `exchange+symbol+signal_type` 且 `start_ts` 间隔 < M 的 event 合并。

## 分阶段落地计划
- 阶段 0：环境与容量守则  
  选型：本机 PG 小配置（`shared_buffers` ~512MB，`max_connections` <30，`wal_compression=on`）。数据目录放最快磁盘；启用 `log_min_duration_statement` 查慢 SQL。现有配置已可满足 20 符号、1~5m 写入。
- 阶段 1：核心表与分区（已完成）  
  建库/用户/表均已就绪，分区脚本与 cron 已上线。保留期可按 14~30 天（raw）、180 天（event/outcome）执行。
- 阶段 2：写入路径（精简版）  
  仅对“当前 watchlist 符号”写入；默认 1~5m 触发一批。每次 refresh（30s 现有节奏，可接受）后批量写 PG；失败可降级到文件/SQLite（待补）。事件归并逻辑继续沿用。
- 阶段 3：因子计算与落库  
  快速因子（用当前快照可得的）在 refresh 路径计算并写 raw；慢指标（长窗口）用后台 worker 每 5~15m 跑，只针对 watchlist 符号/近期触发符号补齐，结果写回 raw 或 event.features_agg。
- 阶段 4：回测/IC 链路  
  outcome worker 定时扫描新事件，按 30/60/180/360m 计算结果写 `future_outcome`。可选导出/物化视图供回测。
- 阶段 5：监控/清理  
  监控写入失败率、分区大小；每日分区滚动已由 cron 处理。磁盘占用极低，但仍保留自动清理。

## 性能与效率建议（结合现有页面与因子需求）
- 写入侧：仅入库“原始信号/归并事件/精简序列”，不存全量逐笔；高频特征以窗口统计（均值/σ/极值）压缩；批量 INSERT 或 `COPY`，减少索引。  
- 存储侧：分区 + 短保留优先，压缩次之；核心数值列拆开，长尾字段放 JSONB。  
- 查询侧：`/api/watchlist` 与 `/api/watchlist/metrics` 先读内存缓存/Redis（保持与现有前端刷新节奏一致），PG 作为冷启动或回溯数据源；`/api/watchlist/series` 直接查 `watchlist_series_agg` 12h 窗口；回测/看板/IC 计算走 `watch_signal_event + future_outcome` 或物化视图，限制时间范围避免扫热表。
  - IC/IR 计算：优先从 `watch_signal_event` 拉事件，再 join `future_outcome` 多 horizon（例如 30/60/180/360 分钟）；对照基准用 `watchlist_symbol_ref` 或外部行情。

## 回测/IC：用 event 预测 outcome PnL（推荐口径）
目标：把 `watch_signal_event` 当作“可交易机会的触发点”，用事件时点可得的因子去预测 `future_outcome` 的未来收益（`pnl/spread_change/funding_change`），并对每个因子计算 IC/IR。

### 1) 数据集定义（避免信息泄露）
- 决策时点：`decision_ts = watch_signal_event.start_ts`（代码里是满足 N 连续触发的那一分钟）。
- 因子快照（强烈建议）：用 `watch_signal_raw` **在 `ts=decision_ts` 的那一行**作为因子输入（它是当时 refresh 计算出来的快照，不会被“事件后续分钟”覆盖）。
  - join 方式：`watch_signal_event` → LATERAL 子查询取 `watch_signal_raw`（同 `exchange/symbol/signal_type/ts`），`ORDER BY id DESC LIMIT 1`。
  - 同时保留 event 的腿字段（`leg_*_exchange/symbol/kind/price_first/funding_rate_first/...`）作为结构化特征来源。
- label：用 `future_outcome`（`event_id + horizon_min` 唯一）：
  - 默认 label：`pnl`（现阶段= `spread_change + funding_change`，不含手续费/滑点）。
  - 辅助 label：`spread_change`（只看价差回归）、`funding_change`（只看资金费贡献）。
  - Type B 特别注意：当前 `spread_change/pnl` 的符号会受 “腿顺序（leg_a/leg_b）” 影响；做 IC/回测前需要做 **canonical 化**（例如：perp-perp 情况下若 `leg_a_price_first > leg_b_price_first` 则对 `spread_change` 取负，使 “价差收敛” 恒为正 label），或在 worker 中直接落库 `spread_change_canon/pnl_canon`。
  - 建议顺序：先用 `spread_change_canon` 做第一版 IC/IR（只评估价差回归，不受 funding 方向/结算点缺口影响），再在 funding 链路审计完全稳定后引入 `pnl`。
- 样本范围：保持 “复合 A/B/C 条件触发池” 原样（raw 只写 active/watchlist，天然是正样本池）；回测/IC 的结论仅对该触发池有效（存在选择偏差属预期）。

### 2) 因子库（第一版建议）
从 `raw + event legs` 直接可得、且不引入未来信息的因子（示例）：
- 价差：`spread_rel`（raw），或由 `leg_*_price_first` 计算；`abs(spread_rel)`。
- 波动/区间/趋势（raw）：`volatility`、`range_1h`、`range_12h`、`slope_3m`、`crossings_1h`、`drift_ratio`。
- 资金费结构（event legs）：`leg_a_funding_rate_first`、`leg_b_funding_rate_first`、`funding_rate_diff = a-b`、`max_abs_funding`。
- 资金费时点/周期（event legs）：`time_to_next_funding_min = min(perp_legs(next_funding_time - decision_ts))`、`funding_interval_hours`（perp leg）。
- 盘口/跨所最优差（raw）：`best_buy_high_sell_low`、`best_sell_high_buy_low`、`funding_diff_max`、`premium_index_diff`（若非空）。

### 3) IC/IR 计算口径（建议先用横截面 IC）
- 对每个 horizon 单独算一套 IC/IR（例如 60m/240m/480m）。
- 分桶：把事件按 `decision_ts` 做时间分桶（例如每 60 分钟一个 bucket，或按自然日）。
- 在每个 bucket 内做横截面相关：
  - IC：`SpearmanCorr(factor, label)`（推荐秩相关，鲁棒些）。
  - IR：`mean(IC_t) / std(IC_t)`（也可输出 t-stat：`mean / (std/sqrt(n))`）。
- 去重/重叠处理（可选但推荐）：同一 `symbol+signal_type` 在同一 bucket 只保留第一条/最大绝对价差一条，避免重复样本抬高置信度；horizon 重叠导致的自相关要在报告里明确。

### 4) “回测能否开始”的验收门槛（最小集）
- 覆盖率：目标 horizon（如 60m/240m/480m）在 `future_outcome` 覆盖率 >95%（`pnl` 非空）。
- 可用性：`watch_signal_raw` 在 `ts=event.start_ts` 的 join 命中率 >99%（否则说明写入/对齐仍有问题）。
- schedule：perp 腿 `next_funding_time/funding_interval_hours` 缺失率低（回测可先只做 `spread_change`，再逐步引入 funding label）。

### 5) 落地步骤（从“能算 IC”到“能回测策略”）
1) 先做数据集抽取（SQL/view）并固化字段清单（raw + legs + outcome）。
2) 先跑 “单 horizon + 少量因子” 的 IC/IR，确认方向正确、无明显泄露。
3) 扩到多 horizon、多分组（signal_type/交易所组合/币种流动性分层）。
4) 才引入策略约束做回测：并发上限、冷静期、持仓时长、止损/止盈、费用/滑点、资金费结算点对齐审计。

### 工具与命令（现成可跑）
- IC/IR 脚本（直接读 PG）：`backtest/pg_event_ic_ir.py`
  - 示例：`venv/bin/python backtest/pg_event_ic_ir.py --days 7 --horizon-min 240 --signal-types B,C --label spread_change_canon --bucket-min 60 --min-bucket-n 10`
- 首次回测报告（多 horizon + 分组 + CSV/Markdown）：`backtest/first_backtest_report.py`
  - 示例（最近 1 天，先用 canonical outcome）：`venv/bin/python backtest/first_backtest_report.py --days 1 --horizons 60,240,480 --signal-types A,B,C --label pnl --bucket-min 60 --min-bucket-n 10 --quantiles 5 --dedup max_abs_spread --require-position-rule --breakdown signal_type --out-dir reports`
- 数据集抽取 SQL 模板：`scripts/watchlist_event_factor_outcome.sql`
- Outcome 口径回填/重算（用于修复 canonical 与 funding 方向后覆盖历史）：`watchlist_outcome_worker.py`
  - 示例（按窗口分批，从最近往前扫）：`venv/bin/python watchlist_outcome_worker.py --recompute-since 2025-12-11T00:00:00+00:00 --recompute-until 2025-12-12T00:00:00+00:00 --recompute-horizons all --recompute-limit 50000 --max-tasks 5000`
  - 抽样审计（重算对比）：`venv/bin/python scripts/audit_future_outcome_pnl_components.py --days 1 --horizon-min 480 --limit 50 --use-rest`

## 事件 / outcome 实施细节（结合现有代码）
- 触发入口：`watchlist_manager.refresh()` 现有刷新周期；在刷新后调用写入适配层，按符号产生 raw 记录。
- 事件归并：写入层维护最近触发状态（per exchange+symbol+signal_type）。满足 N 连续分钟后生成/扩展 event；超出冷静期 M 后关闭 event 并落表。
- outcome 计算：后台 worker 定时扫描未完成的 event，等待指定 horizon（30/60/180/360 分钟等），从行情源拉终点数据计算收益：
  - 数据源优先级：PG `watchlist_series_agg`（若已启用）> SQLite `price_data_1min` > 实时行情缓存。
  - 指标：`pnl`（按基差回归或虚拟仓位）、`spread_change`（当前 spread -> 未来 spread）、`funding_change`、`max_drawdown`、`volatility`（未来窗口 σ），可派生标签（正/负例或分位）。
  - 结果写入 `future_outcome`；失败重试/告警，避免阻塞刷新线程。
- API/页面兼容：现有接口仍读内存缓存；新增写入不影响展示。若切换 `/api/watchlist/series` 到 PG，需留开关并验证性能。

## 待决策/依赖
- 是否允许安装扩展：`pg_partman` / `timescaledb`（影响分区与压缩方案）。
- 触发频率与记录粒度：5m 是否过高；是否只落“进入/离开 watchlist”事件以进一步降量。
- 磁盘预算：为 watchlist 数据预留多少（如 10GB），以便提前设计清理阈值。

## 近期动作清单（建议 1 周内完成）
- [ ] 确认可用扩展与磁盘/内存上限（已知目前压力很小，20 符号场景可直接使用现配置）。
- [x] 定稿最小字段集（核心标量 + JSONB 长尾），覆盖 `/api/watchlist` 展示 + 因子/IC 必需特征。
- [x] 建表/分区/cron 已上线。
- [x] 在 `watchlist_pg_writer` 补齐字段映射（针对 watchlist 符号、1~5m 批）；前端因子弹窗可查看 `meta.factors`。
- [ ] 确认事件归并参数（N、M）并用真实数据验证；必要时临时放宽 N=1 观测。
- [ ] 跑 24h 试写（20 符号/1~5m），记录行数与磁盘增量；调批大小与参数。
- [ ] 实现慢指标/回溯 worker（5~15m）仅针对 watchlist 符号；实现 outcome worker。
- [ ] 视需要补导出/物化视图，供回测/IC。

## Outcome 计算与落地方案（新增）
- 目标：对每个 `watch_signal_event` 计算未来窗口的表现，用于回测/IC。关注 horizon：15m、30m、1h、4h、8h、24h、48h、96h。
- 数据源优先级：`watchlist_series_agg`（若已启用 5m 聚合）> SQLite `price_data_1min` > 实时行情缓存。需保证同一符号的资金费时间/周期准确。
- 资金费处理（从均值估算升级为结算点累加）：
  - 记录并使用资金费结算时间/周期：raw 已写入 `funding_interval_hours/next_funding_time`；event 侧通过新增列 + 回填脚本补齐 schedule（见下方“历史回填”）；worker 按时间轴枚举 horizon 内的结算点逐笔累加，而非简单均值估算。
  - 周期动态：若 interval 变化，则分段累加；若缺 `next_funding_time`，用最近值推算，超出容忍则标记缺失。
  - REST 补全：若本地缺资金费序列，调用交易所历史资金费接口（当前 worker 已接入 Binance `/fapi/v1/fundingRate`，其他所可按需扩展）仅拉取 horizon 覆盖时间段，命中率不足再退回本地数据/推算。每轮有调用上限，避免过载。
- 价差收益（canonical）：用对冲后的 **log 比值**作为价差度量：`spread_metric = log(short_price/long_price)`，并定义 `pnl_spread = spread_metric_start - spread_metric_end`（价差向 0 收敛为正收益）。
  - Type C：固定 `long=spot`、`short=perp`（现货低于永续）；Type B：固定 `long=低价 perp`、`short=高价 perp`；Type A：默认按“收资金费”方向选择 `long/short`（见下方 `label.position_rule`）。
- 资金费收益（canonical）：按结算点逐笔累加 realized funding rates（REST 优先），并按持仓方向取符号：`long` 视作 **支付** `+rate`，`short` 视作 **收取** `+rate`，因此单腿资金费贡献为 `pnl_funding_leg = -pos_sign * Σ(rate)`（`pos_sign=+1` long，`-1` short）。不再按 `interval_hours/24` 二次折算（API 返回的 fundingRate 已是该次结算应计比例）。
- 总收益：`pnl_total = pnl_spread + pnl_funding`。同时在 `future_outcome.label` 里落库 `spread_metric_start/end`、`pnl_spread/pnl_funding/pnl_total` 与 `position_rule`，用于审计与回测口径锁定。
- 落库字段（`future_outcome`，以当前表结构为准）：`event_id`，`horizon_min`，`pnl`（= `pnl_total`），`spread_change`（= `pnl_spread`），`funding_change`（= `pnl_funding`），`max_drawdown`，`volatility`，`funding_applied jsonb`（记录 funding 时间点、费率、来源、持仓方向），`label jsonb`（落库 canonical 口径与审计信息）。
- 计算流程（worker）：
  1) 周期扫描 `watch_signal_event` 表，选取 `status in ('open','closed')` 且尚未产生目标 horizon outcome 的事件。
  2) 对每个 horizon（15/30/60/240/480/1440/2880/5760m），检查当前时间是否已超过 `start_ts + horizon`。未到期跳过，已到期则计算。
  3) 拉取起点/终点行情：优先 `watchlist_series_agg`（5m 窗口内取最近一条），若缺则查 SQLite `price_data_1min`。必要时回退实时接口。
  4) 资金费：收集事件期间的 funding schedule（raw/event 的 `funding_interval_hours`、`next_funding_time` + 本地历史 funding_rate_avg）。按结算时间逐笔累加；缺口时可尝试 REST funding 历史，仅限 horizon 范围内小窗口查询。
  5) 计算 `pnl_spread`、`pnl_funding`、`pnl_total`、`max_drawdown`、`volatility`。保存到 `future_outcome`；若数据缺失，置 NULL 并记录 `label.missing=true`。
  6) 幂等：对同一 `event_id+horizon` upsert，避免重复写。
- 任务节奏：worker 每 5~10 分钟跑一轮；horizon 多，计算量小（事件数量低）。
- 前端/回测使用：IC/IR 直接 join `watch_signal_event` 与 `future_outcome`；可加物化视图按 horizon 展平。
- 待办：实现 worker 中的 funding 结算点累加、REST 缺口补偿；为 `funding_applied` 写入来源/时间；在 plan 中保持“资金费周期可能变动”的提示，并在结果中保留审计信息。
  - 审计脚本：`scripts/audit_future_outcome_pnl_components.py`（抽样重算并对比 PG 已落库的 `pnl/spread_change/funding_change`）。

### Outcome 实施现状（2025-12-12 审计）
- 数据量（当前 PG）：
  - raw 145,229 行（2025-12-08 起）；A=16,149（binance），B=106,893（multi），C=22,187（multi）。
  - event 5,939 行；双腿字段缺失=0。
  - future_outcome：15m=5,860，30m=5,854，60m=5,823，240m=5,688，480m=5,534，1440m=4,081，2880m=1,788；`pnl` 均非空；`funding_applied` 仅少数为空（15/30/60/240/480m 各 2 行）。
  - watchlist_series_agg / watchlist_symbol_ref / watchlist_outcome：仍为空（未启用/未落库）。
- 发现的问题/缺口（以回测/一致性为目标）：
  - Type B/C（exchange=multi）funding schedule 仍不稳定：
    - raw 顶层：`funding_interval_hours` NULL=254；`next_funding_time` NULL=2,623；`next_funding_time <= ts-1m`=13,626。
    - raw 腿级：`leg_a_kind='perp'` 且 `leg_a_next_funding_time` 缺失=11,743 / 过期=10,523；`leg_b_kind='perp'` 缺失=13,379 / 过期=10,189。
    - 主要集中：`hyperliquid/grvt` 无 next_funding_time；`lighter` next_funding_time 多为“已过去的 funding_timestamp”；`bybit/bitget` 存在“字段抖动”（WS 增量包不带 nextFundingTime 时会把旧值冲掉）。
  - Type B/C 窗口指标仍有缺口（multi rows）：B 约 19,7xx 行缺 range/vol/slope/drift；C 约 1,26x 行缺（主要集中在 lighter/hyperliquid 组合，SQLite 序列缺失/对齐失败）。
  - 资金费周期为“按交易对动态”（Binance/OKX/Bybit 等已出现 1h/4h/8h 混合）；当前 SQLite/PG 中 `funding_interval_hours` 仍存在错误/抖动，导致资金费累加与审计不够可靠。
  - watchlist_series_agg 未落库，outcome 仍主要依赖 SQLite；PG 侧回放/可视化与抽样审计成本偏高。
- 当前运行快照：raw/event/outcome 持续增长；outcome worker 已常驻轮询，多数事件已覆盖短/中 horizon。

## 现状快照（2025-12-09）
- PG 占用：~29MB，总体含两日 raw 分区（20.27MB / 7.17MB）+ event ~0.8MB + outcome ~0.17MB；推算 30 天约 0.6GB。
- 进程：simple_app + outcome worker 已常驻（nohup），outcome 每 600s 轮询。
- 新功能：raw.meta.orderbook 持久化扫单价与跨所矩阵；/watchlist/db 浏览页上线。
- 数据量（当前）：raw 27,101 行，event 462 行，future_outcome 93 行；新增事件已带双腿信息，老事件缺腿且被 worker 自动跳过。

## 下一步（短期）
- 优先级（结合 2025-12-12 审计，以回测可用性为准）：
  - 1) 修复 Type B/C 完整性：补腿级 `funding_interval_hours`/`next_funding_time`；窗口指标按两腿真实交易所回查并填满。
  - 2) 启用 `watchlist_series_agg` 的 5m 聚合落库与回读，提高 PG 侧可回放性与 outcome 对齐能力。
  - 3) 写入 `watchlist_symbol_ref`（exchange/symbol/状态/流动性/行业等），支撑后续分组分析。
  - 4) 增加腿级审计字段（`leg_*_ts`/`leg_*_source`）并提供抽样对照脚本，解决价格源/时点差异难审计的问题。
  - 5) 在策略侧收敛 watchlist 规模（阈值/冷静期/上限），回到“关注池”语义并降低噪声。
- 资金费历史 fetch：Binance/OKX/Bybit/Bitget 已接入（用于 future_outcome）；当前更大的缺口在于“结算周期/next 时间”落库稳定性与动态周期来源（见上方审计）。
- 对旧事件缺腿信息：worker 已跳过；如需样本可重放。新增事件已写双腿。
- 监控：每周查看分区大小与 outcome 覆盖率；确保 funding_applied/funding_hist 持续写入。
- 下一步改进：
  - 将 “funding schedule” 从依赖 `next_funding_time/interval` 改为优先使用 REST funding history 的时间戳序列（按结算点直接累加），从根源上降低对 next/interval 字段完整性的依赖。
  - 采集层补齐/稳定写入：Binance 使用 `/fapi/v1/fundingInfo` 提供 per-symbol interval；OKX 用 `nextFundingTime-fundingTime` 推断 per-symbol interval；Bitget 用 `current-fund-rate` 获取 nextUpdate/interval；Lighter 修正 funding_timestamp 语义并推算 next。
  - 若 REST 失败且本地无 next_funding_time，则标记 outcome 缺失（label.missing=true），避免推算。
  - 在 outcome 中写入 `funding_applied`（包含结算时间、费率、来源 rest/local）。
  - 增加 cron（每 10 分钟）或后台线程自动跑 worker（已以 nohup 600s 跑，但可独立 cron 化）。
  - 监控/日志：统计缺数据比例、REST 命中率、调用次数，便于调优。
  - 资金费/订单簿/双腿字段已写入 raw/event；outcome 用两腿价差与两腿资金费累加，现货腿资金费=0（继续验证）。

## 当前资源快照（阶段 0 执行）
- 磁盘：`/` 232G，总用 141G，可用 ~92G（61% 已用）；短期可预留 10GB 给 PG，需保持 <80%。
- 内存：15Gi，总用 4.5Gi，free 0.39Gi，buff/cache 10Gi，可用 ~10Gi；Swap 未开，需避免 PG 占用过大。
- 负载：load average 2.03/2.15/2.02（长期 92d uptime）。
- 高占用进程：`python3 -u simple_app.py` ~123% CPU，`code-server`/Node/`kronos` 等常驻；说明已有热点 CPU 负载，PG 写入必须异步批量、低频查询。
- Postgres 现状：已安装 16.10，监听 5432（进程用户 `ollama`）；已有连接 `hbot`→`hummingbot_api`。
- 双写可行性：SQLite (`market_data.db` 29G) 继续作为在线缓存/回退；新增 PG 写入采用异步批量，不影响现有 API。
- 已有 PG 数据目录与占用：`/proc/592481/root/var/lib/postgresql/data`（容器内），总占用 ~47MB（极小，当前仅 `hummingbot_api` 库）。风险：直接停用会影响该项目；推荐在同一实例新建 `watchlist` 库与 `wl_writer/wl_reader` 角色（最省资源）。若必须隔离，可在不同端口（如 5433）新建轻量实例，`shared_buffers` ≤512MB，避免抢占现有服务。
- 决策：采用现有实例，在 5432 新建独立库 `watchlist`，创建独立账号（`wl_writer`/`wl_reader`），仅赋权此库/Schema，保持与 `hummingbot_api` 隔离；继续使用 SQLite 作回退，准备双写。

## 已执行（数据库/表）
- 创建角色：`wl_writer`（写）/`wl_reader`（读），search_path=watchlist,public；默认权限已配置（新表/序列自动授予）。
- 创建库：`watchlist`（owner=wl_writer）；Schema：`watchlist`（owner=wl_writer）。
- 表：
  - `watch_signal_raw`：按 `ts` RANGE 分区，主键(`ts`,`id`)，核心标量列 + `meta jsonb`。
  - `watch_signal_event`：事件聚合。
  - `future_outcome`：事件未来表现标签。
  - `watchlist_series_agg`：按 `ts` RANGE 分区，5m 精简序列。
  - `watchlist_outcome`：执行/持仓结果。
  - `watchlist_symbol_ref`：符号元数据。
- 索引：raw/series (`exchange`,`symbol`,`ts`) + `ts desc`; event (`exchange`,`symbol`,`start_ts`) + `start_ts desc`; outcome (`event_id`,`horizon_min`)。
- 分区：raw/series 已建今日起 7 天的日分区；需后续 cron/脚本每日滚动创建/删除。
- 连接示例：`PGPASSWORD=wl_reader_A3f9xB2 psql -h 127.0.0.1 -U wl_reader -d watchlist`（writer 密码 `wl_writer_A3f9xB2`）。
- 脚本/适配：新增 `scripts/manage_watchlist_partitions.sh`（日分区滚动，默认 raw 14d、series 7d）；新增 `watchlist_pg_writer.py`（PG 写入缓冲骨架，含事件归并占位、双写开关）。
- Cron：已添加每日 00:05 UTC 运行 `scripts/manage_watchlist_partitions.sh`（日志 `logs/partition_maintenance.log`），默认 raw 14d、series 7d、预建 2d。
- 依赖：`requirements.txt` 已加入 `psycopg[binary]`（通过 venv 安装），`requests` 升级至 2.32.3 以兼容 grvt-pysdk。
- 运行状况（双写）：simple_app 以 venv 重启，`WATCHLIST_PG_ENABLED=1`。`watch_signal_raw` 持续写入；事件表已生成带双腿字段的 open 事件（N=2/M=3 规则），覆盖率仍在提升。分区脚本手动运行成功。
- 事件归并实现：`watchlist_pg_writer` 内置 N 连续/M 冷静归并（默认 N=2、M=3 分钟），聚合首/末/极值，开/关事件写入 `watch_signal_event`。当前实时数据未出现事件（需更多触发样本）；本地测试用例可写入/关闭事件。
- 因子落库：`watchlist_manager` 计算并填充价差类因子（range/vol/slope/crossings/drift）及 `extra_factors`（zscore/momentum/RSI/MACD/rv/hv/skew/kurt/drawdown/资金费7d zscore与趋势/vol、premium_index_diff、volume_quote_24h 等）存入 `meta.factors`；前端 watchlist 页面新增“因子”按钮异步展示。

## 进展概览（截至 2025-12-08）
- 已落库：价差/波动类因子全套；资金费长窗（7d zscore/3d trend/7d vol）；premium_index_diff；volume_quote_24h（当前多为 NULL，源数据待补）；其他长尾因子均在 `meta.factors`。
- 事件：已开启默认事件合并（N=2/M=3），`watch_signal_event` 已产生少量 open 事件（当前 9 行），冷静期后会更新/关闭。
- 未落库：OI/盘口相关（bid_ask_spread、depth_imbalance、book_imbalance、sweep_impact、oi_trend 等）因缺数据源；series/outcome 仍为空。
- 前端：新增“因子”弹窗，异步拉取 metrics，减轻首屏延迟。
- 24h 成交额现状：`price_data_1min` 已有 `volume_24h_avg` 列；近 1 天非 Binance 行有有效值，Binance 行几乎全为 0/NULL（WS 不带 24h 量）。当前 metrics 仅用 Binance 数据 → `volume_quote_24h` 多为 NULL。后续需在采集层为 watchlist 符号（≤20）补一次轻量 REST `ticker/24hr`（现货/永续各 1 次）更新 `volume_24h`，再经 SQLite→PG 传递。其他交易所有效值会随 SQLite→PG 自动同步，无需额外改动。

## 下一步（短期可执行）
- 补数据源并填值：检查 SQLite `price_data_1min` 是否写入 `premium_percent_avg`、`volume_24h_avg`；若缺，采集端补全以消除 NULL；若有盘口/OI 数据，扩展快照填 `bid_ask_spread`/`depth_imbalance`/`book_imbalance`/`oi_trend`。
- 运行验证：当前已跑 >24h，raw/event 持续增长；可继续观察 `meta.factors` 覆盖度与磁盘增量，必要时临时调事件归并 N=1 观测。
- 健康与回退：PG writer 已常驻；仍需写失败降级（文件/SQLite）与日志。`watchlist_series_agg` 分区已创建但尚未大量写入，后续补 5m 聚合；outcome worker 已以 nohup 每 600s 跑一轮，多 horizon 标签已写入 93 条，仍需提高覆盖率。

## 审计对照（2025-12-12）
- 运行状态：simple_app + outcome worker 常驻轮询；PG 双写开启，raw/event/outcome 持续写入。
- 数据范围：raw 仅记录复合 A/B/C 条件的触发快照（正样本池），符合回测范围设定。
- SQLite（近 24h，price_data_1min）funding schedule 完整性问题突出：
  - `bybit`：`funding_interval_hours<=0` 121,617 行，`next_funding_time` 缺失 231,684 行（同一 symbol 可出现分钟级抖动）。
  - `bitget`：`funding_interval_hours<=0` 161,540 行，`next_funding_time` 缺失 162,312 行（但可用 REST `current-fund-rate` 补齐）。
  - `okx`：`funding_interval_hours<=0` 92,332 行，`next_funding_time` 缺失 92,498 行（存在动态 4h/8h）。
  - `lighter`：`next_funding_time` 大多为过去时间（110,204/112,339），应视作 last_funding_time 并推算 next。
  - `hyperliquid/grvt`：`next_funding_time` 近乎全缺失（当前采集未提供）。
- REST 抽样一致性（以交易所为准）：
  - Binance：`/fapi/v1/fundingInfo` 显示不同合约存在 1h/4h/8h（如 `LRCUSDT=1h`、`ATUSDT=4h`）；现有 SQLite/PG 仍出现 8h 默认值与抖动。
  - OKX：`/api/v5/public/funding-rate` 可用 `nextFundingTime-fundingTime` 推断动态周期（如 `BARD-USDT-SWAP` 为 4h）。
  - Bybit：ticker 返回 `fundingIntervalHour`（如 `BARDUSDT=1h`、`CYSUSDT=4h`、`BTCUSDT=8h`）。
  - Bitget：`/api/v2/mix/market/current-fund-rate` 返回 `fundingRateInterval` 与 `nextUpdate`（可补齐 next_funding_time）。

### 现状更新：修复后“新写入”已对齐（2025-12-12）
- 结论：对 **修复上线后的新增数据**，`funding_interval_hours` 与 `next_funding_time` 已能稳定对齐交易所权威口径（并消除“字段抖动/空值覆盖/next 过期写入”）。
- 验证方式：
  - 使用 `scripts/audit_funding_schedule_pg_vs_rest.py` 对 `watch_signal_raw(exchange=binance)` 和 `watch_signal_raw(exchange=multi).meta.snapshots.*` 做窗口抽样对照 REST。
  - 以“近 1 分钟”为窗口时，抽样结果可达到 `bad=0`（可能因触发符号变化/窗口过窄而偶尔无样本，需扩大窗口复查）。
- 注意事项：
  - 该结论仅覆盖“修复后新写入”行；**修复前的历史行**仍可能存在 interval 默认 8h、next 过期/NULL 等问题，需要单独回填/矫正后才能宣称“全量历史准确”。当前已增加 event schedule 列并按 REST funding history 做了回填（见下方）。
  - `CURRENT_SUPPORTED_SYMBOLS` 之外的 symbol 仍可能出现在 multi 事件腿（来自跨所触发/别名/动态发现），建议按 “watchlist 触发池” 而不是静态全量列表来做 schedule 回查与约束。

### Funding schedule 权威口径（逐交易所，必须全覆盖）
目标：对每条 perp 快照都能稳定得到：
1) `funding_interval_hours`（当前资金费率周期，按合约动态）；2) `next_funding_time`（距离下次资金费时间 = `next_funding_time - snapshot_ts`）。

**Binance（USDT-M perpetual）**
- 权威字段：
  - `nextFundingTime`：`GET https://fapi.binance.com/fapi/v1/premiumIndex`（单 symbol 或全量）。
  - `fundingIntervalHours`：`GET https://fapi.binance.com/fapi/v1/fundingInfo`（可全量返回；禁止用“距离 nextFundingTime 的剩余时间”反推周期）。
- 已落地修复：
  - `rest_collectors.py`：用 `fundingInfo` 全量缓存获取 per-symbol interval；`premiumIndex` 仅提供 nextFundingTime/lastFundingRate，不再错误推断 interval。
  - `exchange_connectors.py`：WS `markPrice` 更新不再用默认 8h 覆盖 interval，且不再用空值覆盖旧 schedule。
- 校验方式：随机抽样 symbol，比对 interval（1/4/8h）与 nextFundingTime（允许 <90s 偏差）。

**OKX（SWAP）**
- 权威字段：`GET https://www.okx.com/api/v5/public/funding-rate?instId=XXX-USDT-SWAP`，返回 `fundingTime/nextFundingTime`，周期=差值（存在动态 4h/8h）。
- 已落地修复：`exchange_connectors.py` OKX WS `funding-rate` 频道用 `nextFundingTime - fundingTime` 推导 per-symbol interval，避免写死 8h。
- 补充（建议）：`rest_collectors.py` 也应对 **当轮出现的 bases** 做小批量 funding-rate 回查，确保即使 WS 缺字段也能补齐 interval/next（避免 meta.snapshots 内缺失）。
- 校验方式：抽样多 symbol，确认 interval 在 {4,8} 等合理集合且 nextFundingTime 总是未来时间。

**Bybit（linear perpetual）**
- 权威字段：`GET https://api.bybit.com/v5/market/tickers?category=linear&symbol=XXXUSDT`，返回 `fundingIntervalHour` 与 `nextFundingTime`。
- 已落地修复：
  - `exchange_connectors.py`：修复 “全量 ticker 包缺字段时覆盖掉旧 next/interval” 的抖动；增量包也支持独立更新 interval（不再依赖 nextFundingTime 字段是否存在）。
  - `exchange_connectors.py`：REST merge 在 WS 很新时仍可补齐 schedule（避免 WS 缺字段导致长期 NULL）。
- 校验方式：对同一 symbol 连续观察数分钟，`funding_interval_hours/next_funding_time` 不应出现分钟级来回变空/变默认值。

## 历史回填：event schedule（2025-12-12）
- 背景：event 是回测/审计的“机会级别”归并表；修复前 event 表缺乏可直接查询的 funding schedule 字段，且历史 raw 的 schedule 也曾存在抖动/缺失。
- 原则：仅回填/修复 `funding_interval_hours` + `next_funding_time`（含腿级字段）；**不修改** event/legs 的 `funding_rate`（监控时刻 funding rate 与结算时刻 realized funding rate 本就不同）。
- 数据源：对 Binance/OKX/Bybit/Bitget，使用交易所 REST “历史资金费率”接口的时间戳序列作为权威；其余（如无历史接口）按规则兜底（UTC 边界对齐）。
- 一次性回填脚本：`scripts/backfill_watchlist_event_schedule_from_rest.py`
  - 示例（全量窗口按需调 days）：`venv/bin/python scripts/backfill_watchlist_event_schedule_from_rest.py --days 10`
  - 增量补齐最近数据：`venv/bin/python scripts/backfill_watchlist_event_schedule_from_rest.py --days 1 --until-hours-ago 0`
- 快速验收 SQL（示例）：
  - 腿级 perp 不应缺 next：`SELECT COUNT(*) FROM watchlist.watch_signal_event WHERE leg_a_kind='perp' AND leg_a_next_funding_time IS NULL;`
  - next 不应早于 start_ts：`SELECT COUNT(*) FROM watchlist.watch_signal_event WHERE (leg_a_kind='perp' OR leg_b_kind='perp') AND next_funding_time <= start_ts - interval '1 minute';`

**Bitget（USDT-FUTURES）**
- 权威字段：`GET https://api.bitget.com/api/v2/mix/market/current-fund-rate?productType=USDT-FUTURES&symbol=XXXUSDT`，返回 `fundingRateInterval` 与 `nextUpdate`。
- 已落地修复：`rest_collectors.py` 增加 schedule 缓存 + 对当轮 bases 小批量补齐，确保 `next_funding_time/interval` 在 meta.snapshots 中稳定可用。
- 校验方式：抽样 symbol，nextUpdate 必须未来，interval 必须 >0（常见 8h，部分可为 4h/1h）。

**Hyperliquid**
- 权威口径：官方文档表述 “funding hourly”；公共 API 不直接返回 next funding timestamp。
- 实施口径：`funding_interval_hours=1`；`next_funding_time` 对齐到下一个 UTC 整点小时（rule-based）。
- 已落地修复：`rest_collectors.py` 与 `exchange_connectors.py` 补齐 next 并避免 WS 覆盖掉 schedule。
- 校验方式：DB 中 next_funding_time 应始终落在整点小时且 > snapshot_ts。

**Lighter**
- 权威口径：面板/公告标注每小时结算；WS 字段 `funding_timestamp` 语义不稳定（审计观察更像 last funding time）。
- 实施口径：若提供的 funding timestamp 在过去，则按 interval=1h 推进到下一个结算点；否则视作 next funding time。
- 已落地修复：
  - `dex/exchanges/lighter.py`：不再把 `funding_timestamp` 直接标为 `next_funding_time`（改为原始字段，交由上游归一化）。
  - `exchange_connectors.py`：对 `funding_timestamp/next_funding_time` 做“归一化+推进”，避免写入 past next。
  - `rest_collectors.py`：补齐 rule-based 的 next（UTC 整点）。
- 校验方式：next_funding_time 不得长期落在过去（允许 60s 容忍），并且应当接近整点。

**GRVT**
- 权威字段：若 SDK/ticker 返回 `next_funding_time`，直接采用；否则先按 8h 边界（0/8/16 UTC）规则兜底，并保留审计标记。
- 已落地修复：`rest_collectors.py` 缺 next 时补齐 8h 边界；`exchange_connectors.py` 统一对 next 做“归一化+推进”。

### 统一审计脚本（DB vs REST）
- 新增：`scripts/audit_funding_schedule_pg_vs_rest.py`
  - 运行：`./venv/bin/python scripts/audit_funding_schedule_pg_vs_rest.py --minutes 60 --limit 50`
  - 说明：对 `watch_signal_raw(exchange=binance)` 与 `watch_signal_raw(exchange=multi).meta.snapshots.*` 的 schedule 抽样对照 REST；`lighter/hyperliquid/grvt` 用 rule/SDK 口径。
- 回测就绪结论（阶段性）：
  - 可开始：以 `watch_signal_event` + `future_outcome` 做第一版回测/IC（spread 为主），并把 “资金费相关” 先限定在 legs=Binance/OKX/Bybit 且 REST 可回查的事件子集（目前约 1,168 个 event）。
  - 仍阻塞：若要做“资金费严格对齐/跨所全覆盖”的回测与审计，必须先修复采集层对 `funding_interval_hours/next_funding_time` 的写入稳定性与动态周期来源，并对历史样本做回填/重算（至少覆盖 Bybit/OKX/Binance/Bitget 的周期与 next 时间）。
- 规模偏离：触发写入涉及符号数仍偏大（>300），高于“关注池 <20”假设；需在策略侧继续收敛阈值/限流或增加冷静期。
