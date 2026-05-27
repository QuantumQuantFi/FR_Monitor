# Crowded Funding Roadmap

> **For Hermes:** Use this as the product/strategy roadmap for FR_Monitor before implementing new modules.  
> 更新时间：2026-05-10  
> 主线：资金费率 + 拥挤交易 + 控盘币事件。  
> 目标：找厚 alpha，不再追逐薄 IC。  
> 决策记录：`docs/STRATEGY_DECISION_2026-05-10_EVENT_FIRST_ALPHA.md`  
> Phase 1 历史事件池计划：`docs/PHASE1_HISTORICAL_EVENT_POOL_PLAN.md`

---

## 0. 战略判断

过去对 rank213、32b、154 以及 FR_Monitor 的研究说明一件事：

> 普通因子 IC 可以挖出来，但大多数边际很薄；厚 alpha 更可能来自异常事件、结构性错价和拥挤交易的被迫行为。

因此，FR_Monitor 下一阶段不再定位为“全市场因子挖掘器”，而应升级为：

# 拥挤交易与资金费率事件雷达

核心路径：

```text
事件发现 → 可成交验证 → funding/basis/拥挤度解释 → paper/live 审计 → 复用事件模板
```

而不是：

```text
海量因子 → IC/IR 排名 → 选薄 alpha → 继续调参
```

---

## 1. 三条方向的优先级

### P0：控盘币 / 榜单币事件雷达

**优先级：最高。**

目标：捕捉 PIPPIN、SIREN、币安人生这类“非正常交易状态”的币，识别其是否存在：

- perp premium；
- 极端 funding；
- 跨所价格脱节；
- 现货-perp 脱节；
- DEX-CEX 脱节；
- 高换手/高波动/疑似控盘出货或逼空。

理由：这类事件最可能有厚 alpha，但不能用普通 IC 框架捕捉。

---

### P1：CEX-DEX Funding/Basis Paper Engine

**优先级：高，但先 paper，不 live。**

目标：监控 Binance/OKX/Bybit/Bitget 与 Hyperliquid/Lighter/GRVT 等 perp 市场之间的：

- funding 差；
- basis 差；
- 可成交 bid/ask spread；
- 持有到 funding 或价差回归后的真实净收益。

理由：方向正确，但 DEX 执行、深度、延迟和 funding/index 机制很容易制造假机会。

---

### P2：事件条件下的因子研究

**优先级：中。**

目标：不是全市场挖 200 个因子，而是在 `event_universe` 内研究哪些条件决定事件后收益。

例如：

- funding 极端后，哪些会继续 squeeze，哪些会均值回归？
- 涨幅榜 + 高 funding，什么时候能 fade？
- perp-perp 价差，哪些交易所组合容易回归？
- spot-perp 结构是否比 perp-perp 更厚？

---

## 2. 事件分类 Taxonomy

新增统一概念：`event_universe`。

它不是全市场币种列表，而是“当前值得盯”的异常币池。

### E1：Funding Extreme

触发条件示例：

```text
abs(funding_rate_per_hour) 位于全市场 Top N
或 funding_rate_change 位于 Top N
或 next funding 前 expected carry 足够大
```

关键字段：

- `funding_rate`
- `funding_interval_hours`
- `funding_per_hour`
- `next_funding_time`
- `time_to_next_funding_min`
- `funding_percentile`
- `funding_change_1h/4h`

### E2：Cross-Exchange Perp Basis

触发条件示例：

```text
同一 symbol 在不同交易所 perp 的可成交价差 > threshold
```

方向：

```text
short = 高价 perp
long = 低价 perp
```

关键字段：

- `short_exchange`
- `long_exchange`
- `price_short_bid`
- `price_long_ask`
- `tradable_spread`
- `sweep_spread_50u/100u/500u`
- `net_carry_per_hour`

### E3：Spot-Perp Basis

触发条件示例：

```text
perp 明显高于 spot，且现货可买、perp 可空、盘口深度足够
```

方向：

```text
long spot
short perp
```

关键字段：

- `spot_exchange`
- `perp_exchange`
- `spot_ask`
- `perp_bid`
- `tradable_basis`
- `spot_balance_available`
- `spot_fee/slippage`

### E4：Controlled Coin / 榜单控盘事件

触发条件示例：

```text
涨幅榜/跌幅榜 Top N
+ volume zscore 高
+ funding/premium/spread 异常之一
```

关键字段：

- `return_15m/1h/4h/24h`
- `return_rank`
- `volume_zscore`
- `volume_acceleration`
- `premium_to_index`
- `funding_percentile`
- `exchange_dispersion`
- `tags`: `top_gainer`, `top_loser`, `high_funding`, `perp_premium`, `controlled_candidate`

### E5：DEX-CEX Dislocation

触发条件示例：

```text
DEX perp 与 CEX perp 的 bid/ask 或 funding 出现明显脱节
```

关键字段：

- `cex_exchange`
- `dex_exchange`
- `dex_snapshot_age_ms`
- `cex_bid/ask`
- `dex_bid/ask`
- `dex_depth_or_bbo_size`
- `net_carry_per_hour`
- `index_mark_diff`

---

## 3. 机会验证门控

每个 event 进入 paper/live 前，必须通过以下门控。

### 3.1 可成交门

禁止只用 last/mark。

优先级：

1. 按目标名义金额 sweep 后的成交价；
2. BBO bid/ask；
3. last/mark 仅展示和 fallback。

最低记录：

```text
entry_spread_last
entry_spread_bbo
entry_spread_sweep_50u
entry_spread_sweep_100u
orderbook_age_ms
depth_available
```

### 3.2 Funding 门

统一按小时归一：

```text
short_funding_per_hour = short_funding_rate / short_interval_h
long_funding_per_hour = long_funding_rate / long_interval_h
net_carry_per_hour = short_funding_per_hour - long_funding_per_hour
expected_carry_to_next = net_carry_per_hour * hours_to_next_funding
```

必须记录 interval，不能简单加总 daily funding。

### 3.3 拥挤门

优先使用真实 OI/long-short ratio；如果暂时没有，用 proxy：

```text
crowding_proxy = f(volume_zscore, funding_percentile, premium_to_index, return_rank, exchange_dispersion)
```

目标不是证明“价格会跌/会涨”，而是识别是否存在被迫交易的一边。

### 3.4 控盘风险门

控盘币禁止裸信号直接 live。必须打风险标签：

- `single_exchange_anomaly`
- `thin_book`
- `extreme_trend`
- `new_listing`
- `symbol_mapping_risk`
- `funding_interval_unknown`
- `dex_stale_book`

出现高危标签时，只允许展示/paper，不允许 live。

### 3.5 退出路径门

每个可交易机会必须提前定义退出：

- 价差回归 X%；
- 到下一次 funding 后退出；
- funding 反转退出；
- spread 扩大止损；
- 最大持仓时间；
- 交易所 API/盘口异常强平。

---

## 4. 分阶段实施计划

## Phase 0：收口与安全基线

**目标**：先让项目有清晰状态与主线，避免继续横向膨胀。

### Task 0.1：落地状态文档

**文件**：`docs/FR_MONITOR_STATE.md`

内容：

- 当前模块；
- 已完成能力；
- 已知风险；
- 可复用资产；
- live/paper 默认建议状态。

### Task 0.2：落地本路线图

**文件**：`docs/CROWDED_FUNDING_ROADMAP.md`

内容：

- 新主线；
- 事件 taxonomy；
- 门控规则；
- 分阶段计划；
- 不做事项。

### Task 0.3：冻结 live 扩张

在没有补齐审计前：

- 不扩大交易所；
- 不扩大 notional；
- 不打开 DEX-CEX live；
- 不让控盘币直接自动实盘。

---

## Phase 1：Controlled Coin Event Monitor MVP

**目标**：先把“值得盯的控盘/榜单币”抓出来。

### Task 1.1：新增配置

**建议文件**：`config.py`

新增：

```python
CONTROLLED_EVENT_CONFIG = {
    'enabled': True,
    'manual_symbols': 'PIPPIN,SIREN',
    'top_n': 50,
    'refresh_seconds': 60,
    'min_volume_zscore': 2.0,
    'min_abs_return_1h': 0.05,
    'min_abs_funding_per_hour': 0.0005,
    'min_exchange_dispersion': 0.005,
    'paper_only': True,
}
```

### Task 1.2：新增事件 universe 模块

**建议文件**：`event_universe.py`

职责：

- 读取当前 market snapshots / SQLite recent rows；
- 合并手动 watchlist；
- 计算 return/volume/funding/premium/spread proxy；
- 输出 `ControlledEvent` 列表。

初始 dataclass：

```python
@dataclass
class ControlledEvent:
    symbol: str
    event_ts: datetime
    score: float
    tags: List[str]
    return_1h: Optional[float]
    return_4h: Optional[float]
    volume_zscore: Optional[float]
    max_funding_per_hour: Optional[float]
    max_perp_premium: Optional[float]
    max_exchange_dispersion: Optional[float]
    best_candidate: Optional[Dict[str, Any]]
    risk_tags: List[str]
```

### Task 1.3：API 展示

**建议文件**：`simple_app.py`

新增：

```text
GET /api/events/controlled
```

返回：

```json
{
  "events": [...],
  "updated_at": "...",
  "config": {...}
}
```

### Task 1.4：最小页面/复用 watchlist 页面

优先简单，不做复杂 UI。能看到：

- symbol；
- score；
- tags；
- funding；
- spread；
- candidate direction；
- risk tags；
- last updated。

### Task 1.5：Paper signal 写入

暂不 live。先写 PG 或本地 JSON/SQLite：

```text
event_ts
symbol
event_type
tags
features
candidate_trade
risk_tags
paper_status
```

---

## Phase 2：CEX-DEX Funding/Basis Paper Engine

**目标**：验证 CEX/DEX funding-basis 是否真有净收益。

### Task 2.1：DEX snapshot 归一

统一 Binance/OKX/Bybit/Bitget/Hyperliquid/Lighter/GRVT 字段：

```text
exchange
symbol
bid
ask
bid_size
ask_size
mark_price
index_price
funding_rate
funding_interval_hours
next_funding_time
snapshot_age_ms
```

### Task 2.2：候选生成

规则：

```text
for each symbol:
  enumerate exchange pairs
  direction = short higher carry/high price, long lower carry/low price
  compute tradable spread and expected carry
```

### Task 2.3：Paper lifecycle

每个候选模拟：

- 入场；
- 持有 60m/240m/到 funding 后；
- 价差回归退出；
- 止损退出；
- 记录 fee/slippage/funding/basis 分解。

### Task 2.4：日报

每日输出：

```text
candidate_count
paper_trade_count
mean_net_pnl
median_net_pnl
win_rate
top symbols
failure reasons
best exchange pairs
```

---

## Phase 3：事件条件下的因子研究

**目标**：把 IC/IR 从“全市场薄因子”改成“事件后过滤器”。

### 研究 universe

仅使用：

- Controlled Coin event；
- Funding Extreme event；
- Cross-exchange basis event；
- Spot-perp basis event；
- DEX-CEX dislocation event。

### 第一批研究问题

1. Funding 极端后，做 opposite side 的收益分布；
2. 涨幅榜 + 高 funding，什么时候可以 fade；
3. perp-perp 价差哪些交易所组合最容易回归；
4. spot-perp 是否比 perp-perp 更厚；
5. DEX-CEX funding 差是否能覆盖 basis 风险。

### 输出格式

每个研究必须输出：

- 样本数量；
- OOS 时间切分；
- fee/slippage 后净收益；
- max adverse excursion；
- failure cases；
- 是否允许进入 paper/live。

---

## Phase 4：小额 live 白名单

**前置条件**：Phase 1/2 paper 至少跑满 2 周，并且审计闭环。

### Live 允许条件

必须同时满足：

- event score 高；
- sweep 后仍有正 edge；
- funding/basis 解释清楚；
- risk_tags 不含高危项；
- notional 小；
- 每 symbol 限一笔；
- 每日交易次数上限；
- 自动平仓路径可用；
- fee/funding/slippage 审计可用。

### Live 禁止条件

任一命中则禁止：

- 只有 last/mark，没有 BBO/sweep；
- 单交易所孤立异常；
- funding interval unknown；
- symbol mapping risk；
- DEX snapshot stale；
- orderbook unavailable；
- spot balance insufficient；
- 控盘币极端趋势中裸方向；
- 交易所处于 degraded/unhealthy。

---

## 5. 近期具体下一步

建议立即做：

1. **完成 Phase 0 文档落地**：本文件 + `FR_MONITOR_STATE.md`；
2. **实现 Phase 1 MVP**：`event_universe.py` + `/api/events/controlled`；
3. **先不动 live notional 和 DEX live**；
4. **把 PIPPIN/SIREN/币安人生这类手动 watchlist 接入事件池**；
5. **跑 3-7 天 event paper audit**，看哪些事件真的有可成交 edge。

---

## 6. 不做事项

为了避免重新陷入薄 alpha 泥潭，明确暂时不做：

1. 不做全市场 200 因子大炼丹；
2. 不扩大 live 交易规模；
3. 不把 DEX-CEX funding spread 直接实盘；
4. 不用 last price 判断可交易收益；
5. 不因 funding 高就裸空控盘币；
6. 不在 fee/slippage/funding 审计不完整时宣称策略有效。

---

## 7. 北极星指标

阶段性成功不以 IC 显著为唯一标准，而以：

1. **异常事件捕捉率**：热门控盘币能否及时进入 event universe；
2. **可成交 edge**：50U/100U/500U sweep 后是否仍有正 edge；
3. **paper 净收益**：扣 fee/slippage/funding/basis 后是否为正；
4. **审计闭环率**：每个 paper/live 信号是否能解释盈亏来源；
5. **低频高质量**：宁可每天 1-5 个高质量事件，不要 100 个薄信号。

---

## 8. 推荐命名

后续可以把这一套在代码中命名为：

```text
Crowded Funding Monitor
Controlled Event Universe
Funding Basis Paper Engine
```

中文内部叫：

```text
拥挤交易与资金费率事件雷达
```
