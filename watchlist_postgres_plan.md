# Watchlist 数据库（PostgreSQL）建设规划（结合现有页面/接口）

## 目标与约束
- 目的：记录 watchlist 的触发信号、执行/结果、回测特征输入，支撑因子迭代，并支撑现有前端页面的展示与图表。
- 约束：当前服务器已在跑 7 个交易所、约 800 个币种，CPU/磁盘紧张；必须关注写入开销、索引数量和存储占用。
- 优先级：先落地“触发信号 + 结果”两张主表；用精简序列支撑现有 watchlist 页面；再逐步丰富特征与回测标签。

## 预估数据量与资源占用
- 新约束：关注币种通常 <20 个；可接受 1~5 分钟落库一次（watchlist 及因子）。
- 量级重估（按 20 个符号，每 1 分钟）：20 * 60/h ≈ 1.2k 行/小时，≈ 28.8k 行/天；行尺寸 ~200B/行，则日增 ~6MB，7 天 ~42MB。
- 若 5 分钟落库：日增 ~1.2MB，基本可忽略。触发事件行数更低（<千级/天）。
- 结论：在 20 符号、1~5m 频率下，存储占用极小，保留期可拉长（raw 14~30 天，event/outcome 180 天），仍保持分区与周期清理以防膨胀。

## 与现有 watchlist 页面/接口的对应
- 页面：`templates/watchlist.html`（列表+指标）与 `templates/watchlist_charts.html`（12h 时序）。
- 接口：`/api/watchlist`（列表）、`/api/watchlist/metrics`（Type A 指标）、`/api/watchlist/series`（12h 图表）。
- 列表字段：Type A/B/C、资金费率、Spread 最新/均值/σ、区间(1h/12h)、斜率/穿越/Drift、跨所差价矩阵（订单簿扫单）、下一次资金费时间等。
- 图表需求：12h 分钟级价差（spot/futures）、15m 中线、基线、开/平仓点、资金费时间标注。
- 影响：数据库至少要支撑 (a) 5m~1m 粒度价差序列（仅 watchlist 符号），(b) Type A 指标所需窗口统计，(c) 触发/结果的回溯与标签；全量行情仍走现有高频通道，不进 PG。

## 最小可行表结构（迭代式上线，兼顾因子/IC）
1) `watch_signal_raw`（分钟级原始因子池）
   - 作用：完整保留 watchlist 逻辑每分钟或每次扫描的特征，不漏样本，供因子池/IC 计算。
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
- 事件表（event.features_agg）：保留首/末/均/极值的核心标量（同上），再附带 `pairs_top` 与 `funding_diff_top` 的首末快照，以便回溯。
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
- 事件窗口：`start_ts`=首次满足时间；`end_ts`=最后一次满足或冷静期结束；`duration_sec`=差值。
- 特征聚合：`features_agg` 记录首末值、均值、极值（spread、funding、volatility、range、slope、book_impact 等）。
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
- 运行状况（双写）：simple_app 以 venv 重启，`WATCHLIST_PG_ENABLED=1`。`watch_signal_raw` 持续写入；事件表仍 0 行（需更多连续触发或放宽 N=2/M=3 规则）。分区脚本手动运行成功。
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
- 运行验证：跑 24h 观察 `meta.factors` 覆盖度与磁盘增量，检查事件是否生成；必要时临时调事件归并 N=1 观测。
- 健康与回退：给 PG writer 增加写失败降级（文件/SQLite）与日志；启动 `watchlist_series_agg` 写入（5m 聚合）及 outcome worker（多 horizon）为回测/IC 产出标签。
