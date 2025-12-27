# Watchlist → 实盘交易系统研发计划（含 PostgreSQL 记录方案）

本文目标：基于现有 watchlist（预计 PnL / 胜率）+ 现有实盘交易组件，规划并落地一个“自动开仓 + 监控止盈/强平 + 手动交易 + 页面展示 + 全链路审计落库”的实盘系统。

---

## 0. 当前代码现状（已具备的组件）
- Watchlist 信号与预测：`watchlist_manager.py` 生成 `trigger_details` + `pnl_regression`（含 `pnl_hat` / `win_prob`，展示在 `templates/watchlist.html`）。
- Watchlist 落库：`watchlist_pg_writer.py` 双写到 PostgreSQL（schema=`watchlist`），核心表：`watch_signal_raw` / `watch_signal_event` / `future_outcome` / `watchlist_series_agg` / `watchlist_outcome`。
- Watchlist 事件写入时订单簿验算（Type B）：`watchlist_pg_writer.py` 在“首次 INSERT 新 event”时按 Top‑K 交易所抓订单簿（每交易所一次），枚举所有 pair+方向并选出最优 `chosen`，写入 `features_agg.meta_last.orderbook_validation/pnl_regression_ob`（与 live trading 口径对齐，避免只验证“last 价差最大的一对”而漏掉更可成交的 pair）。
- 实盘交易接口封装：`trading/trade_executor.py` 支持 `binance/okx/bybit/bitget` 的永续市价单、部分杠杆设置与持仓查询。
- Web 侧交易入口：`simple_app.py` 已有 `/api/trade/options` + `/api/trade/dual`（多腿、USDT金额换算、reduceOnly/closeAll），`templates/chart_index.html` 已有“对冲交易执行”面板。
- 订单簿价差工具：`orderbook_utils.py` 已有 REST 拉取与扫单价差计算，可用来做 1min 频率监听。

结论：我们不需要从 0 写“交易所 SDK / 下单 / 基础 UI”，主要工作是把 watchlist 信号“可靠地”转成交易任务，并补齐去重、记录、监控与页面展示。

---

## 0.2 外部 BBO/Snapshot 服务（端口 8010/8000）现状与接入价值（待接入）

你给的“Dashboard 同款聚合快照 / 结构化快照 / 动态 watchlist REST”接口设计，目标非常明确：用 **WS 订单簿流**产出**可被外部程序直接消费**的 BBO/分析快照，并通过 TTL watchlist 控制订阅面（避免靠静态配置塞满全市场）。

### 0.2.1 预期接口（你提供的契约）
- 探活：`GET http://<host>:8010/health`
- Dashboard 同款聚合快照：`GET http://<host>:8010/ui/data?top_spreads=200&top_opps=50&symbol_like=BTC&min_abs_spread_pct=0.2`
- 结构化快照（给程序用）：`GET http://<host>:8010/snapshot?include_analysis=true&include_tickers=false`
  - 过滤：`/snapshot?exchanges=okx&exchanges=binance&symbols=BTC-USDC-PERP&symbols=ETH-USDC-PERP`
- 动态订阅面（watchlist TTL）：
  - `GET /watchlist`
  - `POST /watchlist/add` body：`{"symbol":"BTC-USDC-PERP","exchanges":["okx","binance"],"ttl_seconds":86400,"source":"my-app","reason":"need quotes"}`
  - `POST /watchlist/touch`、`POST /watchlist/remove`

### 0.2.1.1 重要：USDT 永续的符号归一规则（对接必须遵守）
8010 服务内部可以订阅到交易所的 **USDT 永续**，但对外 API 的“标准符号”**必须使用** `*-USDC-PERP`：
- ✅ 订阅交易所的 USDT 永续：已支持（但标准符号仍用 `*-USDC-PERP`）
  - `binance/okx/grvt` 在将标准符号转交易所符号时，会把 `USDC` 映射成 `USDT`
  - 所以你传 `BTC-USDC-PERP`，实际订阅的是交易所的 `BTCUSDT` / `BTC-USDT-SWAP` / `BTC_USDT_Perp`
  - 代码位置：`/root/jerry/open-trading/core/services/arbitrage_monitor/utils/symbol_converter.py:60`、`/root/jerry/open-trading/core/services/arbitrage_monitor/utils/symbol_converter.py:251`
- ❌ API 直接支持/回传标准符号 `*-USDT-PERP`：当前不支持
  - 反向转换会把 `USDT` 强制归一到 `USDC`
  - 这会导致如果你在 watchlist 里 add `BTC-USDT-PERP`，回调数据会被转换成 `BTC-USDC-PERP` 从而被过滤掉
  - 代码位置：`/root/jerry/open-trading/core/services/arbitrage_monitor/utils/symbol_converter.py:345`

### 0.2.2 本机实测结果（2025-12-26）
- `127.0.0.1:8010`：未监听（连接失败），因此无法验证 `/health`、`/snapshot` 是否只返回买一卖一（BBO）还是包含多档深度。
- `127.0.0.1:8000`：当前是 `Hummingbot API`（uvicorn），`/` 返回 `{"name":"Hummingbot API","version":"1.0.1","status":"running"}`；但你给的 `/health`、`/snapshot`、`/watchlist/*`、`/ui/data` 在该端口均为 404。

结论：**这组 8010 端口的 BBO/Snapshot 服务当前在本机未运行（或端口/路径不一致）**；在它上线前，我们只能继续依赖现有 `orderbook_utils.py` 的“按需 REST 扫单”来做成交口径验证。

（待服务上线后）建议用以下最小命令做一次自检，避免“端口 OK 但数据为空/过期”：
```bash
curl -fsS http://127.0.0.1:8010/health
curl -fsS 'http://127.0.0.1:8010/snapshot?include_analysis=true&include_tickers=false' | python -m json.tool | head
curl -fsS 'http://127.0.0.1:8010/ui/data?top_spreads=10&top_opps=10&symbol_like=BTC&min_abs_spread_pct=0.2' | python -m json.tool | head
curl -fsS http://127.0.0.1:8010/watchlist | python -m json.tool | head
```

### 0.2.2.1 复测（同日稍后：8010 已启动且接口可用）
- `127.0.0.1:8010` 当前由 `python/uvicorn` 监听，以下接口返回 200：
  - `/health`：包含 `watchlist_pairs`、处理队列与延迟统计（例如 `orderbook_delay_p95_ms`/`ticker_delay_p95_ms`），以及各交易所健康度（healthy/degraded/unhealthy）。
  - `/watchlist`：返回 `items[]`（exchange+symbol 粒度），当前约 315 pairs（已明显超出你之前提到的 “8 所 45 币”压测阈值，且延迟 p95 已到秒级，符合“扩容会变慢”的预期）。
  - `/snapshot`：支持 query 参数 `exchanges[]`、`symbols[]`、`include_analysis`、`include_tickers`。
  - `/ui/data`：返回 `queue_info` 字符串（含 `ob_p95`/`tk_p95`）以及 `top_spread_rows`（用于 dashboard）。
- `127.0.0.1:8000` 仍是 `Hummingbot API`，与该 8010 服务无关（上述路径仍为 404）。

### 0.2.3 如何验证“只返回买一卖一”以及对我们意味着什么
待服务跑起来后，用下面的最小检查判断返回的是 **L1 BBO** 还是 **含深度**：
- L1（买一卖一）：每个 exchange+symbol 只有 `bid/ask`（以及可选 `bid_size/ask_size`），没有 `bids[]/asks[]` 档位数组；适合做**展示/排序/粗略 spread**，不适合估算大额滑点。
- 含深度：存在 `bids[]/asks[]`（价格+数量，多档），或提供 `sweep_price(notional/qty)` 之类字段；可用于**成交口径**的“扫单价差/滑点”估算，并替换更多 last 价逻辑。

本机实测（`/snapshot?exchanges=okx&symbols=0G-USDC-PERP`）：订单簿对象仅包含
`bid_price/bid_size/ask_price/ask_size` + timestamps（`exchange_timestamp/received_timestamp/processed_timestamp`），**没有** `bids[]/asks[]` 等深度字段；因此该服务当前更像“L1 BBO +（可选）funding/ticker + analysis”，不能直接替代我们现有的“按名义金额扫盘”滑点估算。

### 0.2.4 “有限替换 last 价”的接入策略（建议分两层落地）
约束前提：你之前压测发现该服务在“WS 订单簿订阅”模式下 **最多覆盖约 8 个交易所、45 个币种**，再往上会出现订单簿延迟；因此我们必须把它当作 **高精度但容量有限** 的报价源，而不是全量行情源。

**第一层（低成本、收益高）：L1 BBO 替换 watchlist 的 last/mark 用途（用于排序/筛选/展示）**
- watchlist 当前存在“用 last/mark 补齐价差因子”的路径（例如 `watchlist_manager.py` 构造 `market_snapshots` 时用 `futures.get('price') or futures.get('last_price')`），这会导致 `spread_rel` 与 `trigger_details.prices` 对“可成交价差”偏乐观/偏悲观。
- 建议改造为：若 BBO 可得，生成 `mid=(bid+ask)/2` 并同时保留 `bid/ask`；用于：
  - 生成/展示更真实的 `best_buy_high_sell_low`（应使用“高所 bid / 低所 ask”）
  - 对 `Type B` 的候选 pair 排序与预筛更稳（减少“last 跳价”误触发）
- 仍保留 last/mark 作为 fallback（BBO 不可得或过期时），并把 `price_source` 与 `staleness` 写入 meta 便于复盘。

**第二层（高成本、必须限流）：扫单价差用于“能不能下单/下多少/是否立刻平仓”**
- 实盘与验算仍应以“按名义金额/数量扫盘得到的可成交均价”为准（目前我们已有 `orderbook_utils.fetch_orderbook_prices*()` + `watchlist_pg_writer.py` 的 Top‑K 验算链路）。
- 若外部服务未来提供深度或 `sweep_price`，可把“扫盘动作”从 N 个交易所 REST 拉取，替换为一次 `/snapshot` 或专门的 `/sweep` RPC（但必须做容量隔离、并且仍要有 REST fallback）。

### 0.2.4.1 关键门控（避免“延迟订单簿”污染决策）
从 `/health` 与 `/ui/data` 暴露的队列与延迟来看，该服务在订阅面较大时会出现明显延迟（p95 可能到 300ms~1s+）。因此接入侧必须显式做门控：
- `snapshot_age_ms`/`orderbook_delay_p95_ms` 或 `queue_info.ob_p95` 超阈值：禁止用于“触发/下单/平仓”，仅用于展示；触发路径强制回退到 `orderbook_utils` 的 REST 扫盘复核。
- exchange 级健康度为 `unhealthy`：直接忽略该所的 BBO（例如本机复测中 `binance/hyperliquid` 多次处于 unhealthy，导致 snapshot 中缺失该所 orderbooks）。

### 0.2.5 后续优化方向（分：端口服务、以及 FR_Monitor 侧接入）

**A) 端口服务（8010）建议改造点**
- 把接口拆成“轻/重”两类：`/bbo`（仅 L1，payload 小）、`/snapshot`（可选 include_analysis/tickers）、`/depth` 或 `/sweep`（按 symbol 请求多档或扫单价）。
- 订阅面做 TTL 与容量守则：对每个 exchange 设定 `max_symbols`、对全局设定 `max_pairs`；超过上限直接拒绝 `/watchlist/add` 或降级为 L1。
- 做 backpressure：当处理队列积压时丢弃旧 tick（保留最新），并在 `queue_info` 暴露 `ob_p95/tk_p95`、`dropped_updates`、`snapshot_age_ms`，让调用方能做“数据新鲜度门控”。
- 深度优化（如果未来需要）：默认只维护 top‑N 档（例如 5/10），或只维护 L1 并按需临时拉 REST 深度；避免全深度 book 的 CPU/内存与延迟爆炸。
- 覆盖扩展路线：按交易所拆分 worker（进程/容器分片）、或按 symbol shard；让“8 所 45 币”的上限变成可水平扩展的工程约束，而不是硬上限。

**B) FR_Monitor（watchlist/live trading）接入建议**
- 增加“报价源优先级”配置：`bbo_service -> rest_orderbook -> last/mark`，并把每次计算使用的源与 staleness 记录到 `watch_signal_raw.meta` 与 UI（否则很难定位误差来源）。
- watchlist 侧先做“有限替换”：只在 **Type B 预筛与展示**替换为 BBO；真正触发 event 与实盘下单仍以 `orderbook_validation`（扫单口径）为准，避免“BBO 很好看但扫单不可成交”。
- live trading 增加“新鲜度门控”：下单前检查 `ob_p95`/`snapshot_age_ms`，超过阈值则跳过或强制走 REST 扫单复核，避免用延迟订单簿做决策。
- 用 watchlist TTL 控制订阅面：只给“当前 watchlist active + 近期触发 + 已持仓/待平仓”的符号开 BBO（其余回退 last/REST），把容量用在最需要的地方。

### 0.2.6 落地实现（对齐 open-trading：8010 自己持久化，FR_Monitor 不抢活）
你在 `/root/jerry/open-trading` 的 Monitor/V2 已经实现：
- 默认 baseline 仅 `BTC-USDC-PERP`、`ETH-USDC-PERP`
- watchlist TTL 与持久化：SQLite `data/monitor_v2_watchlist.sqlite3`，重启自动恢复未过期条目并重新订阅
- REST：`POST /watchlist/add` / `POST /watchlist/touch` / `GET /watchlist`

因此 **FR_Monitor 不应再维护一套独立的 watchlist 持久化/keepalive**（否则会破坏 TTL 语义、导致过期回收失效、并且“抢 8010 的工作”）。

FR_Monitor 的最小责任应是：
- 在合适时点向 8010 发送 `add/touch` 信号（例如：raw/event 触发时、或你显式调用“继续关注”）
- 读取 `snapshot/ui/data/health` 作为更精确的 L1 BBO 数据源（并支持降级）
- 作为调试便利：提供一组 “proxy API” 转发到 8010（便于从 4002 统一查看/操作）

### 0.2.7 已落地（2025-12-27）：RAW 触发后订阅 8010 全量交易所
目标：当 watchlist 的 RAW 信号触发（开始关注/持续关注）时，`POST /watchlist/add` 覆盖 8010 端口能提供服务的**所有交易所**，而不局限于当前价差的两腿交易所。

落地内容（FR_Monitor 侧）：
- RAW 触发时机：`watchlist_pg_writer.PgWriter.enqueue_raw()` 对 `triggered=True && signal_type in ('B','C')` 调用 8010 的 `/watchlist/add`（best-effort）。
- 默认全量交易所：当 `MONITOR_8010_WL_EXCHANGES` 为空（默认）或设置为 `all/*` 时，会先 `GET /health` 解析出 8010 支持的交易所列表，并把该列表作为 `exchanges[]` 发送到 `/watchlist/add`；若 `/health` 不可用则回退到“两腿交易所 + snapshots 推断交易所”的集合。
- 可控覆盖面：仍支持 `MONITOR_8010_WL_EXCHANGES=binance,okx,bybit,...` 显式指定；此时才会应用 `MONITOR_8010_WL_MAX_EXCH` 的上限（默认已改为 0=不限制）。
- 前端/接口取全量：`simple_app.py` 拉取 8010 `/snapshot` 时不再带 `exchanges=` 过滤参数，返回“尽可能多的交易所 BBO”；`templates/watchlist.html` 也会展示不在固定排序里的额外交易所。

验证建议：
- `curl -fsS http://127.0.0.1:8010/health` 确认支持交易所列表存在且可解析。
- 触发任意 B/C 信号后：`curl -fsS http://127.0.0.1:8010/watchlist | python -m json.tool | head` 检查该 symbol 是否被加到了多交易所订阅面。

目前 FR_Monitor 已改为 proxy（不落盘、不续命）：
- `GET /api/monitor8010/health` → 8010 `/health`
- `GET /api/monitor8010/watchlist` → 8010 `/watchlist`
- `POST /api/monitor8010/watchlist/add|touch|remove` → 8010 对应接口

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
- **避免漏掉更可成交的 pair**：watchlist 触发 event 时的 pair 基于 last/mark 价差，可能被“浅盘口/跳价”误导；因此 event 写入阶段会按 Top‑K 交易所抓订单簿并在内存枚举所有 pair+方向，最终用订单簿口径选出 `chosen`（例如 BN‑Lighter 看似最大，但 BN‑Bitget 更可成交，则 `chosen` 会落到 BN‑Bitget）。
- **修复 event 更新覆盖问题**：event 后续 UPDATE 会刷新 `features_agg.meta_last`；为避免覆盖掉 INSERT 阶段写入的 `orderbook_validation/pnl_regression_ob`，writer 在聚合更新时会把 enrich 合并回 `meta_last`（保证证据持续可复盘、live trading 可读取）。

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

### 3.2.1 Watchlist→Event→Live Trading 的“时刻”与延迟来源（重要）

为了复盘“为什么 raw 看到价差、但最终没成交/被 skipped”，需要明确三个不同的时间点（口径不同）：

1) **raw 信号产生时刻（last/metrics 口径）**
   - 表：`watchlist.watch_signal_raw`
   - 字段：`ts`、`spread_rel`、`meta.trigger_details`
   - 含义：基于监控系统的最新价格/快照计算的“观察价差”，未必等于“市价可成交价差”。

2) **event 首次写入时刻（订单簿 Top‑K 验算口径）**
   - 表：`watchlist.watch_signal_event`
   - 字段：`start_ts`（事件起点）、以及 `features_agg.meta_last.orderbook_validation.ts`
   - 含义：当 event 第一次 INSERT 时，满足预筛门槛会触发 Top‑K 订单簿验算：
     - 每交易所一次按名义金额扫盘（`per_leg_notional_usdt`）得到 buy/sell 可成交价
     - 内存枚举候选 pairs+方向，选出 `chosen.long_exchange/short_exchange`
     - 用订单簿覆盖关键因子后计算 `pnl_regression_ob`（horizon=240）
   - 影响 event 延迟的主要参数：
     - `WATCHLIST_PG_CONFIG.consecutive_required`：连续触发分钟数（已改为 1，以降低事件生成延迟）
     - `WATCHLIST_PG_CONFIG.flush_seconds` / batch：PG writer 刷新与队列延迟
     - `WATCHLIST_PG_CONFIG.enable_event_merge`：是否启用事件归并

3) **live trading 下单准备时刻（execution 口径）**
   - 表：`watchlist.live_trade_signal`
   - 字段：`payload.orderbook_execution.ts`（真正准备发市价单前再次扫盘）
   - 含义：在准备发市价单前，对 long/short 两端各再扫一次订单簿，得到实际用于算 qty 的 `long_buy_px / short_sell_px`。

**常见“价差消失”的位置**
- raw→event：若 `consecutive_required>1` 或 writer flush 较慢，会引入分钟级等待，秒级价差可能已收敛。
- event→live trading：kick-driven 通常很短；scan 仅作兜底。
- revalidation→execution：同一轮内延迟很短，但市场秒级波动，execution 时入场价差可能明显收敛。

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

### 4.1 为什么“预测 pnl 很高，但 execution 入场价差很小”仍会被尝试？

这是当前策略的刻意设计（先看预测，再用 execution 兜底拦截）：

- **准入门槛（event 选择）**：以 `pnl_hat_ob / win_prob_ob`（回归输出）为主；这是 240min horizon 的“未来预期”判断。
- **可执行兜底（真正发单前）**：以 execution 扫盘的 `entry_spread_metric` 与 TP 目标比较做硬拦截：
  - `take_profit_pnl = take_profit_ratio * pnl_hat_ob`
  - 若 `entry_spread_metric < take_profit_pnl`，则即便预测达标也会拒绝开仓（避免“成交时刻无价差”的开仓）。

因此会出现一种常见现象：**raw/回归预测很强，但市价可成交价差很小**，最终会在 `_open_trade` 阶段被判为 skipped/failed（取决于错误类别是否属于“未发单类”）。

---

## 5. 页面展示（你提出的第 3 条：委托单/持仓简要展示）
建议在 `simple_app.py` 增加一个页面（例如 `/live_trading`）：
- 当前活跃交易列表：symbol、pair、开仓时间、预计 pnl/win、当前价差、止盈目标、已持有时长、状态。
- 当前持仓：优先展示我们系统“正在跟踪的 symbol+exchange” 的持仓（通过 `get_*_positions()` 拉取）。
- 当前委托：交易模块目前未实现“open orders”查询；一期先展示“我们提交的订单回执 + 最新一次查询结果”，二期再补齐各所 open orders API。

---

## 6. 交易记录落库（你提出的第 4 条）
建议：继续使用 **现有 PostgreSQL（schema=`watchlist`）**，新增几张“实盘交易表”，避免把实盘细节塞进 `watchlist_outcome`（语义不同）。

推荐最小表（一期，已落地并持续迭代）：
1) `watchlist.live_trade_signal`
   - `id bigserial pk`, `created_at timestamptz`, `event_id bigint`, `symbol text`, `signal_type char(1)`
   - `horizon_min int`, `pnl_hat double precision`, `win_prob double precision`, `threshold jsonb`
   - `leg_long_exchange text`, `leg_short_exchange text`, `status text`（new/ignored/skipped/opening/open/closing/closed/failed）
   - 关键风控/复盘字段（建议重点关注）：
     - `entry_spread_metric`：Type B 的“入场可成交价差口径”（按实际下单名义金额扫盘得到的 log 指标），用于后续实时监控与出场判定基准
     - `pnl_hat_ob`：开仓前用订单簿复核后的预测值（同一 horizon；用于 TP 阈值）
     - `take_profit_pnl`：止盈阈值（默认 `take_profit_ratio * pnl_hat_ob`，例如 0.8×）
     - `last_spread_metric / last_pnl_spread`：持仓期间每 ~1min 计算并刷新（页面 `last_pnl` 展示的就是 `last_pnl_spread`，不是回归预测）
     - `opened_at / close_requested_at / closed_at / force_close_at`：开仓/平仓请求/完成/强平时间点（开仓与平仓是两个不同时间点，必须分别记录）
   - `reason text`, `payload jsonb`（保存 trigger_details/pnl_regression + 订单簿复核与执行的关键快照，便于复盘）
2) `watchlist.live_trade_order`
   - `id bigserial pk`, `signal_id bigint`, `leg text`（long/short）, `exchange text`, `client_order_id text`
   - `submitted_at timestamptz`, `order_resp jsonb`, `status text`
   - 成交回填（best-effort）：`filled_qty / avg_price / cum_quote / exchange_order_id`
     - 说明：部分交易所“下单回执”不含成交信息，需要二次查询订单/成交回填；因此允许为空，但原始 `order_resp` 必须保留以便后续回填/审计
3) `watchlist.live_trade_spread_sample`
   - `id bigserial pk`, `signal_id bigint`, `ts timestamptz`
   - `long_sell_px / short_buy_px`（按当前持仓数量扫盘得到的“可平仓价格口径”）
   - `spread_metric / pnl_spread / pnl_hat_ob / take_profit_pnl / decision / context(jsonb)`（保存每次监控的原始订单簿摘要与决策依据）

二期再加：
- `live_trade_fill`（成交明细/均价）、`live_trade_exit`（平仓原因、最终 pnl）、`live_trade_error`（异常）。

---

## 7. 价差监听与止盈/强平（你提出的第 5 条）
- 频率：约 1min（可配置）；每次 **同时** 拉两家订单簿（并发），避免“时序错位”导致假价差。
- 数据源：优先 REST（你要求的实时性与一致性），用 `orderbook_utils.fetch_orderbook_prices_for_quantity()` 按“当前持仓数量”扫盘，口径尽可能贴近市价单可成交均价。
- `last_pnl` 的含义（页面展示列）：它对应 `last_pnl_spread`，不是回归输出；是“如果现在立刻平仓，按两边订单簿扫盘可成交价格估算的价差收益指标”。
  - 计算口径（Type B）：
    - `spread_now = ln(short_buy / long_sell)`，其中 `long_sell`=卖出平多的扫盘均价、`short_buy`=买入平空的扫盘均价
    - `pnl_spread_now = entry_spread_metric - spread_now`
    - 持仓期间每次监控都会把 `last_spread_metric/last_pnl_spread` 刷新到 `live_trade_signal`，并同时把一条样本写入 `live_trade_spread_sample` 便于复盘
- 止盈：当 `pnl_spread_now >= take_profit_pnl` 触发“平仓候选”，其中：
  - `take_profit_pnl` 默认 `take_profit_ratio * pnl_hat_ob`（例如 0.8×）
  - 为防止盘口瞬时跳变导致“触发即打回”，会做短间隔多次确认（默认 3 次、间隔约 0.7s），全部通过才真正进入平仓下单
- 强平：持有时间超过 `max_hold_days` 强制平仓（不依赖 pnl）。当前业务要求为 **7 天**；如果配置从 7 天缩短到更短周期，已开仓的单也应同步缩短其 `force_close_at`（避免旧仓位继续按旧周期滞留）。

### 7.1 开仓前订单簿复核（两段：watchlist‑event 选 pair + live trading 执行确认；已落地）
**目标**：避免“watchlist last 价差很大但盘口不可成交”的假信号，并防止只验证一对交易所导致漏掉更可成交的 pair。

#### 7.1.1 watchlist 写入 event 时的 Top‑K 订单簿验算（选择最合适交易所组合）
- 触发时机：`watchlist_pg_writer.py` 在“首次 INSERT 新 event”时 best‑effort 执行（不阻断 event 入库）。
- **不是每个 event 都打订单簿**：只有通过预筛才会触发订单簿抓取（当前实现为硬编码门槛，后续可配置化）：
  - `pnl_hat_240 >= 0.009` 且 `win_prob_240 >= 0.85`
  - 且 `trigger_details.spread`（或候选 spread/factors 退化值）`>= 0.003`
  - 且交易所在 `LIVE_TRADING_CONFIG.allowed_exchanges`
- Top‑K 交易所选择（K 默认 5，配置 `LIVE_TRADING_WATCHLIST_EVENT_TOPK_EXCH`）：
  - 必选：`trigger_details.pair` 的两家交易所
  - 补充：按 `trigger_details.candidate_pairs`（watchlist 两两组合按 last 价差排序的 Top‑N）从大到小补齐到 K
  - 若仍不足：用 `trigger_details.candidate_exchanges` 补齐
  - 最后按 `allowed_exchanges` 过滤
- 订单簿请求策略（控制 REST 压力）：
  - **每交易所一次**：对 Top‑K 交易所各抓 1 次订单簿（`fetch_orderbook_prices(exchange, symbol, market_type='perp', notional=per_leg_notional_usdt)`），得到 `buy`（扫 ask）与 `sell`（扫 bid）
  - **内存枚举 pair**：对 K 个交易所两两组合，并枚举两个方向（long/short 交换）计算 `tradable_spread` / `entry_spread_metric`，不再额外打 REST
  - 用订单簿口径覆盖两个关键因子并调用 `predict_bc(..., horizons=(240,))` 得到 `pnl_hat_ob/win_prob_ob`，选出最优 `chosen`
- 落库字段（供复盘与 live trading 使用）：
  - `features_agg.meta_last.orderbook_validation`：包含 `topk_exchanges/orderbooks/candidates/chosen`
  - `features_agg.meta_last.pnl_regression_ob`：订单簿口径的回归预测（当前仅写 240min）

#### 7.1.2 live trading 开仓确认与执行（优先使用 chosen，仍保留 execution 扫盘确认）
- live trading 候选筛选：优先用 `COALESCE(pnl_regression_ob, pnl_regression)` 的 `pnl_hat/win_prob` 做阈值过滤与排序（减少无意义复核）。
- **优先使用 `chosen`**：若 event 内已有 `orderbook_validation.ok=true` 的 `chosen`，live trading 会把它作为 initial direction（避免重复订单簿复核与额外 REST）。
- 仍保留执行前的确认：在真正下单前会做一次 execution 级别的扫盘确认（与下单名义金额一致），防止“选 pair 时刻 OK，但执行时价差已消失”。
- skipped 复盘：若确认失败/不可成交，会以 `skipped` 记录并在 payload 中保存当时的复核证据，便于解释“为什么没成交”。

### 7.2 分类止损 + 价差扩张补救加仓（最多 4 次开仓，推荐）
你提出的核心思想：不要机械“止损”，而是先判断亏损来自 **资金费（funding）** 还是 **价差扩大（spread widening）**。  
结论：资金费变负属于“结构性风险”，应立即止损；价差扩大属于“可均值回归”的主场景，可用有限次数加仓补救以提高最终平仓概率。

#### 7.2.1 概念与目标
- **第一笔开仓**：例如在 `entry_spread_pct≈3%` 开仓，原计划在 `exit_spread_pct≈2%` 平仓（价差回落 1%）。
- **补救加仓触发（不平仓）**：当价差扩大到开仓价差的 `1.5×`（例：`4.5%`）允许第二次开仓；再扩大到 `1.5×`（例：`6.75%`）允许第三次；最多 4 次（总 4 笔/同一对冲方向）。
- **共享平仓时间与目标**：所有仓位共享同一个 `force_close_at`（最大持仓 7 天），并使用“整体盈利达到初始预计盈利 70~80%”作为止盈阈值（更容易尽快平掉并锁定收益）。

#### 7.2.2 决策分类（monitor loop）
在每次 `PositionMonitor` 采样里（约 1min），把“当前不利”拆成两类：
- **资金费亏损（硬止损）**：任一条件成立立刻触发平仓
  - `funding_rate_per_hour <= -stop_loss_funding_per_hour_pct`（已实现的硬阈值）
  - 或（可选增强）`funding_pnl_usdt < -max_funding_loss_usdt` / `funding_pnl_pct < -max_funding_loss_pct`
- **价差扩大（允许补救）**：`exit_spread_pct_now` 持续上行导致 `pnl_spread_now < 0`，但 funding 未触发硬止损；此时不直接平仓，而是进入“补救加仓判定”。

#### 7.2.3 补救加仓触发规则（核心）
1) **基准**：以第一笔开仓的 `entry_spread_pct_first` 为基准（用 execution 扫盘得到的 `exit_spread_pct_now` 同口径比较）。
2) **阈值序列**：
   - `trigger_mult = 1.5`
   - `trigger_k = 1..(max_entries-1)`
   - `trigger_spread_pct_k = entry_spread_pct_first * (trigger_mult ** trigger_k)`
3) **触发条件（建议全都满足）**：
   - 当前 `exit_spread_pct_now >= trigger_spread_pct_next`
   - 当前总开仓次数 `entries_count < max_entries`（默认 4）
   - funding 未触发硬止损（见 7.2.2）
   - `hedge_health == ok`（两腿都在、数量一致；避免 unhedged 时加仓）
   - 两次加仓之间满足最小间隔 `min_scale_in_interval_minutes`（防止震荡频繁加仓）
4) **watchlist 信号 gating（你要求“由 watchlist 信号触发二次开仓”）**：
   - 只有当 watchlist 侧在最近 `scale_in_signal_max_age_minutes` 内产出“同方向/同交易所组合”的 event，并且 `pnl_hat/win_prob`（v1/v2 任一路）仍达标，才允许执行加仓。
   - 推荐一期先做 **同 pair 加仓**（必须匹配当前 `long_ex/short_ex`），避免变成多 pair 混仓导致监控/平仓复杂度爆炸；二期再讨论“允许切换 pair”的方案。
5) **每次加仓规模**：
   - 推荐每次加仓使用与首笔相同的 `per_leg_notional_usdt`（等额加仓，最多 4 笔），并设置 `max_total_notional_usdt_per_symbol` 兜底。

#### 7.2.4 平仓规则（整体仓位）
目标：让“补救仓位”更容易在回归后整体出场，而不是等到回到首笔的原始止盈阈值。
- **共享强平时间**：`force_close_at = opened_at_first + max_hold_days`（默认 7 天），后续加仓不延长 `force_close_at`。
- **整体止盈阈值**（推荐采用“初始预计盈利的 70~80%”）：
  - 以首笔的 `take_profit_pnl_initial` 为基准（对应“初始预计盈利/止盈参数”）。
  - 当整体收益达到 `rescue_take_profit_ratio * take_profit_pnl_initial`（`rescue_take_profit_ratio` 推荐 0.7~0.8，可配置）则触发平仓候选。
- **整体收益的计算口径（一期/二期）**：
  - 一期（快速落地）：用“名义额加权的平均入场价差”计算整体 `pnl_spread_total`：  
    `entry_spread_metric_avg = Σ(entry_spread_metric_i * notional_i) / Σ(notional_i)`，  
    `pnl_spread_total = entry_spread_metric_avg - spread_now`
  - 二期（更精确）：同时计算 `total_pnl_usdt_est = position_pnl_usdt_est + funding_pnl_usdt`，并以 USDT 口径做最终平仓判定（更贴近真实）。

#### 7.2.5 落库与可观测性（必须做，否则无法审计）
建议把 scale-in 状态写进 `watchlist.live_trade_signal.payload`（避免一期就改表结构），结构建议：
```json
{
  "scale_in": {
    "enabled": true,
    "entries_count": 2,
    "max_entries": 4,
    "trigger_mult": 1.5,
    "entry_spread_pct_first": 0.030,
    "next_trigger_spread_pct": 0.045,
    "rescue_take_profit_ratio": 0.75,
    "take_profit_pnl_initial": 0.0082,
    "entries": [
      {"k": 1, "opened_at": "...", "per_leg_notional": 50, "entry_spread_metric": 0.0296, "entry_spread_pct": 0.0301, "orders": {"long": 123, "short": 124}},
      {"k": 2, "opened_at": "...", "per_leg_notional": 50, "entry_spread_metric": 0.0448, "entry_spread_pct": 0.0458, "orders": {"long": 125, "short": 126}}
    ]
  }
}
```
同时在 `live_trade_spread_sample.context` 里补充：
- `loss_cause`：`funding` / `spread` / `none`
- `scale_in`：当前 `entries_count/next_trigger_spread_pct/trigger_hit`
- `close_target`：`take_profit_pnl_initial/rescue_take_profit_ratio/target_now`

#### 7.2.6 实现落点（代码改动范围）
- `trading/live_trading_manager.py`
  - 放开“同 symbol 去重”的逻辑：当已 `open` 时允许进入 `scale_in` 流程（而不是一律忽略新信号）。
  - 在 monitor loop 增加 `loss_cause` 判定与 `scale_in` 状态机（pending/confirmed/executing）。
  - 新增“加仓执行”路径：复用现有两腿市价单执行与回滚逻辑，产生新 `live_trade_order` 记录；并将新 entry 写入 `payload.scale_in.entries`。
- `watchlist_pg_writer.py` / `watch_signal_event` 接口
  - 提供“最近 N 分钟 event（含 pred v1/v2 + chosen）”的快速查询，供 scale-in gating 使用（避免 live trading 侧做复杂聚合）。
- `simple_app.py` + `templates/`
  - `/live_trading` 列表新增：`entries_count`、`next_trigger_spread_pct`、`loss_cause`、`force_close_at`（7 天）与“手动触发加仓/禁用加仓”开关（必要时人工干预）。

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
6) 1min 订单簿监听与 `last_pnl_spread` 计算；达到 `take_profit_ratio * pnl_hat_ob` 止盈（默认 0.8×）；>`max_hold_days` 强平（当前 7 天）。
6b) 分类止损 + 价差扩张补救加仓：资金费亏损立刻平仓；价差扩大按 `1.5×` 阶梯最多加仓 4 次；整体盈利达到首笔预计盈利的 70~80% 触发平仓。
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

### 阶段 4.1：第二版回测（反转信号/多时间框趋势/4H 历史/资金费变化/趋势指标）
目标：在不引入未来信息泄露的前提下，把“watchlist 的价差均值回归信号”升级为“价差 + 趋势 + 资金费 +（可选）趋势指标”的统一回测框架，用来决定哪些因子/阈值值得进入下一版模型与实盘准入。

#### 4.1.1 复用我们第一版回测（当前已有）
我们已有 v1 回测链路（见 `reports/first_backtest_ic_ir_*.md`，脚本 `backtest/first_backtest_report.py`）口径是：
- 决策时点：`watch_signal_event.start_ts`
- 因子快照：`watch_signal_raw.ts = event.start_ts`（避免未来信息泄露；不要直接用“最终版 features_agg”）
- 标签：`future_outcome.{pnl|spread_change|funding_change}`（默认 pnl）
- 评估：按桶（`bucket-min`）做横截面 Spearman IC/IR + 分位收益

第二版回测将保留上述三段式对齐（decision_ts/factor_ts/label_ts），只扩展“因子来源与类型”与“分层/稳健性评估”。

#### 4.1.2 第二版因子设计（建议最小增量集合）
优先围绕 **价差序列**（canonical 的 `spread_metric = log(short/long)`）计算；腿级趋势作为补充；指标从简到繁逐步验证增量。

**时间粒度（与实盘对齐）**
- 主粒度：**1min bar**（因为实盘与 watchlist 都是 1min 一算，回测必须能复现同样的节奏与噪声水平）。
- 参照粒度：5min bar（用于验证“降噪后仍有效”的稳健性；同时更适合 MACD/RSI 等传统指标）。
- 对齐方法：所有因子均以 `decision_ts` 为 anchor，只取 `<= decision_ts` 的历史点（asof）；严禁使用 `decision_ts` 之后的点。

**A) 反转/拐点类（推荐优先做）**
- `spread_reversal_5m/15m/30m`：窗口末端斜率/动量发生符号翻转的标志（捕捉“收敛→再扩张 / 扩张→再收敛”）。
- `spread_mean_reversion_speed`：均值回归速度（可先用 AR(1) 半衰期近似或 rolling autocorr 近似）。
- `spread_exhaustion`：短期收益极值后回撤比例（区分“可持续收敛” vs “一次性跳变”）。

**B) 多时间框趋势/动量（5m/15m/30m + 4H）**
- `spread_ret_{5,15,30,240}m`、`spread_slope_{5,15,30,240}m`：窗口收益/斜率（点数不足则降级/置空，并记录质量标识）。
- `spread_vol_{15,30,240}m`、`spread_vol_ratio_short_over_long`：短长波动结构（判断“更像趋势延续还是均值回归”）。
- `trend_alignment_legs`：两腿各自趋势方向是否一致（一致时往往是系统性行情驱动，对均值回归策略可能更不利，需要回测验证）。

**C) 资金费率变化/净成本（与你的 TypeB 新准入对齐）**
从“限制 funding 绝对值”升级为“限制按方向产生的 carry 净成本”：
- `funding_edge_short_minus_long`（已有/可复用）：按 canonical long/short 方向计算资金费边际。
- `funding_edge_change_{1,3,6}h`：资金费边际的变化与加速度（用最近 N 个结算点或分钟序列近似）。
- `time_to_next_funding_min`、`funding_interval_h`（已有）：结合 edge 评估“临近结算点的跳变/滑点风险”。
- `carry_net_cost_horizon`：按“计划持有 horizon（例如 240m/1d）”估算资金费净成本；允许为负（盈利），只约束净成本上限。

**D) 趋势交易常用指标（可选，建议先只做 spread 版本）**
- `spread_macd`（默认 12/26/9，建议基于 5m bar 更稳）、`spread_rsi_14`、`spread_boll_pct_b`、`spread_adx_14`。
原则：只要在 IC/IR、分位收益或策略级回测中没有带来可验证的增量，就不进入实盘准入逻辑（避免复杂度膨胀）。

#### 4.1.3 4H 历史与缺失容错（必须写死口径）
长窗口指标会被“缺失/稀疏/延迟更新”显著污染，第二版回测需要把缺失处理规则固化并可审计：
- 对齐：以 `decision_ts` 为 anchor，取 `<=decision_ts` 最近点做 asof join：
  - 1min bar：`tol` 建议 90s（允许少量采集抖动，但避免过旧点污染）
  - 5min bar：`tol` 建议 180s
- 有限前向填充：仅允许有限长度的 forward-fill（例如最多 2 个 bar），并输出 `quality_flag`：`ok/sparse/stale`。
- 回测输出需要分层：至少同时给出 “仅 quality=ok” 与 “包含 sparse” 的结果，避免把数据问题当成模型能力。

#### 4.1.4 第二版回测的固定输出（你最终会拿它做决策）
在 v1 的 IC/IR 基础上，第二版建议固定输出三类结果：
1) **增量信息**：加入新因子后，IC/IR、分位收益、覆盖率、缺失率如何变化（按 horizon 报告）。
2) **环境分层**：按“反转/趋势/资金费 regime/流动性”分层，输出同阈值的胜率/回撤差异（用于制定准入与风控）。
3) **（可选）策略级回测**：事件驱动模拟 entry/exit（tp/stop/time），费用/资金费/滑点参数化，用来验证“预测→可交易收益”的映射。

#### 4.1.5 建议的落地顺序（先规划，后写代码）
- M1：先把 `spread_ret/slope(5/15/30/240)` + `spread_reversal_15m` + `carry_net_cost_horizon` 加进回测（只做 IC/IR）。
- M2：补齐 4H 缺失容错与质量标识；把“质量分层结果”写进报告模板。
- M3：引入 MACD/RSI 等（仅 spread 版本），做增量对比；无增益就移除。
- M4（可选）：做事件驱动策略回测（与 live trading 口径尽量一致：价差用 log，资金费按结算点累加，手续费/滑点参数化）。

#### 4.1.6 第二版回测：可执行任务清单（以 240m 为主，60/480/1440 对照）
**T0：确认数据映射（1 次性）**
- 从 PG `watchlist.watch_signal_event` 获取事件与腿信息（用于 canonical long/short 的“腿定义”）：
  - 事件主键：`id`，决策时点：`start_ts`（timestamptz，UTC）
  - 腿字段：`leg_a_exchange/leg_a_symbol/leg_a_kind`、`leg_b_exchange/leg_b_symbol/leg_b_kind`
  - 腿快照（仅作参考/审计，不作为序列源）：`leg_*_price_first/last`、`leg_*_funding_rate_first/last`、`leg_*_next_funding_time/interval_hours`
- 从 PG `watchlist.watch_signal_raw` 获取“当分钟快照因子”（用于与 v1 因子兼容，且避免未来信息泄露）：
  - join 条件：`watch_signal_raw.ts = watch_signal_event.start_ts` 且 `exchange/symbol/signal_type` 相同（LATERAL + `ORDER BY id DESC LIMIT 1`）
  - raw 内已有：`spread_rel/funding_rate/range_1h/range_12h/volatility/slope_3m/crossings_1h/drift_ratio/...` 以及盘口/流动性代理（`bid_ask_spread/depth_imbalance/...`）
- 从 SQLite `market_data.db` 获取两条腿的 1min 时间序列（用于 v2 的“价差序列因子/传统指标/资金费变化”）：
  - 表：`price_data_1min`
  - 核心列：
    - 时间：`timestamp`（文本 DATETIME，形如 `YYYY-MM-DD HH:MM:SS`；当前观测为 **UTC**）
    - 键：`exchange`、`symbol`（与 PG event 的 `leg_*_exchange/leg_*_symbol` 字符串一致）
    - 价格：`futures_price_close`（perp）、`spot_price_close`（spot）
    - 资金费：`funding_rate_avg`（分钟均值；你当前口径是“每分钟记录”可直接使用）
    - 资金费 schedule：`funding_interval_hours`、`next_funding_time`（文本，通常是 ISO8601）
    - 数据质量：`data_points`（该分钟聚合使用的原始点数；可用于 quality_flag）
  - 兜底表：`price_data`（更高频/最新点，列为 `futures_price`、`funding_rate`，但时间戳粒度与更新节奏可能更抖；v2 以 1min 表为主）
- 序列口径（Type B perp-perp 为主）：
  - `P_long(t)`：`leg_long.kind='perp'` → `futures_price_close`（若未来加入 spot-leg，则用 `spot_price_close`）
  - `P_short(t)`：同上
  - canonical `spread_metric(t) = ln(P_short(t) / P_long(t))`
  - `funding_edge(t) = funding_short(t) - funding_long(t)`，其中 `funding_*` 取 `funding_rate_avg`
- 缺失/异常值处理（必须写死，避免 silent failure）：
  - 价格：`NULL` 或 `<=0` 一律视为缺失（SQLite 表存在 `DEFAULT 0.0` 的历史遗留）
  - funding：`NULL` 视为缺失；若为 0 需结合 `data_points` 判断（data_points=0 时视为缺失）
  - asof 对齐：以 `decision_ts` 为 anchor，取 `<=t` 最近点；1min 使用 `tol=90s`，超出 tol 视为 stale
  - forward-fill：最多 2 根 bar；超过则该因子置空，并把 `quality_flag=stale`
- 性能约束（避免扫 43GB 全表）：
  - SQLite 查询必须带时间范围：`WHERE timestamp BETWEEN (start_ts - lookback) AND start_ts`
  - 由于 `price_data_1min` 有 `UNIQUE(timestamp, symbol, exchange)`，按 timestamp 范围过滤能命中索引路径；严禁对全表做 group-by 统计
- 产出一个“事件→序列”抽取器（v2 回测的数据基线）：
  - 输入：`event_id, lookback_minutes, bar='1m'|'5m'`
  - 输出：对齐后的 `spread_metric(t)`、`funding_edge(t)`、以及 `quality`（missing_rate、stale_count、data_points 分布）
  - 同时输出可审计 meta：使用了哪个价格列（perp= `futures_price_close`）、最终有效点数、最大时间戳偏差

**统一口径（公式前置定义）**
- spread 序列定义（canonical，避免方向混乱）：
  - `spread(t) = ln(P_short(t) / P_long(t))`
  - `P_short/P_long` 来自 event 中 **选定方向** 的两条腿（perp 用 `futures_price_close`）
- funding edge 定义（净成本模式）：
  - `edge(t) = fr_short(t) - fr_long(t)`（允许盈利为正，亏损为负）
  - 若 funding interval 缺失：用交易所默认值，并记录 `quality_flag=assumed_interval`
- spread 的 “收益/变化” 统一用差分：
  - `spread_ret_Δm = spread(t0) - spread(t0-Δ)`
  - `spread_diff(t) = spread(t) - spread(t-1m)`

**实现状态（已完成）**
- 事件写入：所有 Type B/C event 在写入时自动计算并保存 `features_agg.meta_last.factors_v2`。
- 回填：近 30 天 Type B/C event 已完成回填（`factors_v2` 全覆盖）。
- 质量元信息：`features_agg.meta_last.factors_v2_meta` 记录 `missing_rate/ffill_points/max_gap_sec/series_start/series_end/interval_assumed` 等。

**T1：实现 Core 因子（先 30~45 个）**
- 价差反转/趋势（全部基于 1min spread 序列；必要时并行计算 5min 版本做稳健性对照）：
  - `spread_ret_{1,5,15,30,60,240}m`：`spread(t0)-spread(t0-Δ)`
  - `spread_slope_{5,15,30,60,240}m`：OLS 斜率（x 为分钟索引 0..N-1，y=spread；输出“每分钟斜率”）
  - `spread_reversal_15m`：`sign(slope_5m) != sign(slope_30m)` 或 `spread_ret_5m * spread_ret_30m < 0`
  - `spread_crossings_{60,240}m`：`sign(spread - mean(spread))` 的符号翻转次数
  - `spread_vol_{15,30,60,240}m`：`std(spread_diff)`（用 1m 差分）
  - `spread_vol_ratio_30_over_240`：`spread_vol_30m / spread_vol_240m`（分母为 0 或缺失则置空）
  - `spread_drawdown_240m`：对 `cum_ret(t)=spread(t)-spread(t0-240m)` 做 `max_peak_to_trough` 回撤
  - `spread_skew_240m/spread_kurt_240m`：对 `spread_diff` 的偏度/峰度（样本不足置空）
- 传统指标（先少量，避免过拟合；全部基于 spread 序列）：
  - `spread_rsi_14`：对 `spread_diff` 计算 RSI（14 period）
  - `spread_macd_hist`：`EMA12(spread) - EMA26(spread) - EMA9(MACD)`（1min 口径）
  - `spread_boll_pct_b`：`(spread - (ma20-2σ)) / (4σ)`，超界可 >1 或 <0
- 资金费“净成本模式”（基于 1min funding_rate 序列）：
  - `funding_edge_short_minus_long(t)`：`edge(t)=fr_short(t)-fr_long(t)`
  - `funding_edge_change_{60,180,360}m`：`edge(t0)-edge(t0-Δ)`
  - `carry_net_cost_horizon_240m`：`edge(t0) * (240 / funding_interval_min)`（无 interval 时用默认并标记）
  - `funding_edge_vol_240m`：`std(edge)`（资金费稳定性）

**T2：扩展 Plus 因子（“银子越多越好”但要可控）**
- 多时间框动量：补齐 `spread_ret/slope` 在 90/120/180/360/720/1440/4320/10080/21600m 的版本。
- 更多反转/形态：
  - `exhaustion`：`abs(slope_5m) < abs(slope_30m)` 且 `spread_ret_5m` 与 `spread_ret_30m` 反向
  - `turning_point_score`：`-diff2(spread)` 在窗口内的 zscore（拐点强度）
  - `spread_zscore_{1h,4h,12h,3d,7d,15d}`：`(spread(t0)-mean)/std`
  - `hurst_proxy`：`log(std(spread_diff_2m)/std(spread_diff_1m)) / log(2)`（轻量 proxy）
- 更多资金费结构：
  - `funding_regime_quantile`：`edge(t0)` 在过去 30d/60d 分位（Q1..Q5）
  - `funding_edge_regime`：按分位或均值±σ 切分（low/mid/high），文字标签写入 `factors_v2_meta.funding_edge_regime_label`
  - `funding_edge_change_{1440,4320,10080,21600}m`：1/3/7/15d 变动
  - `time_to_next_funding_min`：`min(next_funding_time - decision_ts)`（仅 perp leg 可得）

**T3：回测评估与报告模板升级**
- horizon：主 240m；对照 60/480/1440m。
- 输出至少包含：
  - 每因子：覆盖率/缺失率、IC/IR、分位收益（Q5-Q1）
  - 分层：`quality_flag` 分层（ok vs sparse）+ 按“资金费 regime/波动 regime”分层
  - 稳健性：1min vs 5min 版本对照（同一因子两套粒度对比）

**T4（可选）：策略级事件回测**
- 用事件驱动模拟（entry@decision_ts，exit@tp/stop/time），手续费/滑点/资金费均参数化；用于回答“IC 好不等于可交易收益”的落地问题。

#### 4.1.7 第二版回测：Ridge + Logistic 训练链路（24h 为主）
目标：用 v2 因子预测 **综合收益**（价差收敛 + 资金费净收益），同时输出回归值与胜率，满足“训练后可快速上线”的要求。

**标签定义（默认）**
- `pnl_total = future_outcome.pnl`（当前已包含 `pnl_spread + pnl_funding`）
- 可选费用：`pnl_total -= 2 * fee_bps_per_leg / 10000`（训练脚本可配置）
- 分类标签：`win = (pnl_total > 0)`

**模型选择（已确认）**
- 回归：Ridge（L2 正则，稳定、可解释）
- 分类：Logistic（L2 正则，输出胜率）

**T1：构建训练数据集（落地脚本）**
- 来源：
  - `watch_signal_event.features_agg.meta_last.factors_v2`（因子）
  - `future_outcome.pnl`（24h 标签；可改 horizon）
- 过滤：
  - `factors_v2` 存在且可解析；可选 `factors_v2_meta.ok = true`
  - 缺失率过滤（默认 `max_missing<=0.2`）
- 时间切分：按 `start_ts` 排序，最近 N 天作为验证集
- 产出：
  - 训练矩阵（缺失率统计 + 中位数填充 + 标准化）
  - 可选 CSV（用于外部分析）

**T2：训练 Ridge + Logistic（落地脚本）**
- 训练脚本：`backtest/v2_ridge_logistic.py`
- 输出：
  - `reports/v2_ridge_logistic_model.json`（特征、缩放参数、系数）
  - `reports/v2_ridge_logistic_metrics.md`（训练/验证指标）
  - 多 horizon 训练会输出 `*_h{horizon}.json/.md` + `v2_ridge_logistic_summary.md`
- 评估指标：
  - 回归：MAE/RMSE/Corr/Sign-Acc
  - 分类：AUC/Logloss/Acc
  - 解释：在 metrics 中列出 Ridge/Logistic 的 Top 正/负/低权重特征（标准化后可比较）

**示例命令**
```
venv/bin/python backtest/v2_ridge_logistic.py \
  --days 60 \
  --valid-days 7 \
  --horizons 60,240,480,1440 \
  --signal-types B,C \
  --fee-bps-per-leg 0 \
  --out-dir reports
```

#### 4.1.8 第二版因子落库策略（先回测，后落库）
- v2 阶段优先“回测脚本内计算”，避免 PG 表膨胀。
- 当确认某些因子对 240m 的增量稳定后，再选择：
  - **开列落库**：少量核心因子（例如 `spread_ret_15m/spread_slope_30m/funding_edge/carry_net_cost_horizon`）
  - **meta jsonb 落库**：大量实验性指标（MACD/RSI/Bollinger 各参数版本），并在 `features_agg` 聚合压缩（首/末/均/极值）。

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

## 0.2.8 Watchlist 订单簿/BBO（8010）稳定性修复（2025-12-27）
- 现象：前端“买高卖低/卖高买低”与 8010 BBO 列偶发为空（尤其在重启后数分钟）。
- 根因：
  - `refresh_watchlist_orderbook_cache()` 的 sleep 误绑定到 `WATCHLIST_REFRESH_SECONDS`；当 watchlist 刷新被调得很慢（如 10min）且重启时线程先跑到 “无 active” 分支，会导致订单簿缓存长期不刷新，前端显示为空。
  - 8010 端偶发超时/卡顿时，FR_Monitor 会把 `monitor8010_bbo` 覆盖成 `{}`，导致前端 BBO 列瞬间变空。
- 已落地修复：
  - `config.py`：新增 `WATCHLIST_ORDERBOOK_REFRESH_SECONDS`（默认 10s），订单簿缓存刷新与 watchlist 刷新解耦。
  - `simple_app.py`：Type C 仍使用本地 REST 扫单订单簿（spot+perp）；Type B 使用 8010 perp L1 BBO，避免 Type C 因 8010 不提供 spot 而永远显示 `-`。
  - `simple_app.py`：若 8010 `/snapshot` 失败，则保留上一份 `monitor8010_bbo`（不让前端 BBO 列“清空”），并在 `/api/watchlist` 暴露 `monitor8010_error`。
