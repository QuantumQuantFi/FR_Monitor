# Phase 1 Historical Event Pool Plan

> 日期：2026-05-10  
> 决策背景：用户希望第一阶段优先使用可回测的历史数据构造事件池，而不是依赖实盘慢慢收集。  
> 关联决策：`docs/STRATEGY_DECISION_2026-05-10_EVENT_FIRST_ALPHA.md`

---

## 0. 一句话结论

Phase 1 不从实时交易开始，而是先做 **历史事件池回放**：

```text
历史数据 → 事件定义 → 事件样本表 → 未来收益/风险标签 → 找厚 alpha 模板
```

实时 event radar 暂时降级为后续部署形态；当前优先证明：历史上的异常事件里，哪些类型真的有可交易 edge。

---

## 1. 可用历史数据源

### 1.1 FR_Monitor 本地 SQLite

文件：`market_data.db`

已确认表：

- `price_data_1min`
  - 覆盖约：2026-04-10 到 2026-05-10；
  - 字段：symbol、exchange、spot/futures OHLC、funding_rate_avg、funding_interval_hours、next_funding_time、volume_24h_avg、data_points；
  - 用途：做 1m 级别事件发现、跨所 dispersion、spot/perp premium、短周期 forward return。

- `price_data`
  - 覆盖约：2026-05-03 到 2026-05-10；
  - 字段：symbol、exchange、spot_price、futures_price、funding_rate、funding_interval_hours、next_funding_time、mark/index、premium_percent、volume_24h；
  - 用途：更细的实时 snapshot 辅助，但量大，优先用 1min 聚合表。

### 1.2 Binance public archive / momentum 本地数据

目录示例：`clawd/jerry/momentum/data/binance_vision_rank154/data/futures/um/monthly/`

可用：

- futures klines；
- fundingRate；
- 多年历史。

用途：

- 做更长历史的 Binance 单所事件回测；
- funding extreme；
- top gainer / high volatility / new listing proxy；
- 事件后的 momentum vs mean reversion。

限制：

- 主要是 Binance 单交易所，不能直接验证跨所 basis；
- 盘口深度/BBO 不完整，交易成本需要保守假设。

---

## 2. 第一阶段建议事件池

优先做能从历史数据稳定重建的事件，不依赖主观新闻收集。

### E1：Top Gainer / Top Loser 事件

定义示例：

```text
return_1h 位于全市场 Top N 或 Bottom N
或 abs(return_4h) 位于 Top N
```

用途：

- 检查暴涨后是继续趋势还是均值回归；
- 捕捉控盘/榜单币 proxy；
- 后续叠加 funding/premium 判断拥挤程度。

### E2：Volume Shock 事件

定义示例：

```text
当前 volume_24h 或 proxy volume 相对过去窗口 zscore > threshold
```

用途：

- 标记异常关注度；
- 与 top gainer/funding extreme 联合使用。

### E3：Funding Extreme 事件

定义示例：

```text
abs(funding_rate / funding_interval_hours) 位于全市场 Top N
或 funding_per_hour 超过固定阈值
```

用途：

- 识别拥挤多/空；
- 检查高 funding 后 price return、funding-adjusted return、回撤风险。

注意：必须用 interval 归一，不能直接比较 1h/4h/8h funding rate。

### E4：Perp Premium / Spot-Perp Basis 事件

定义示例：

```text
(futures_price_close - spot_price_close) / spot_price_close 位于 Top N
```

用途：

- 检查 long spot + short perp 的结构可能；
- 区分趋势上涨和 perp 过热。

### E5：Cross-Exchange Dispersion 事件

定义示例：

```text
同一 symbol 同一分钟，不同 exchange 的 futures_price_close 最大/最小价差 > threshold
```

用途：

- 复盘 Type B perp-perp basis；
- 识别跨所价格脱节事件。

限制：历史表是 OHLC/last 聚合，不是 BBO/sweep，只能作为候选发现；真实可交易性要用保守成本折扣。

---

## 3. 不建议第一阶段优先做的事件

### DEX-CEX 历史事件

原因：历史 DEX BBO/depth 通常不完整，容易回测出假 edge。可以先作为后续扩展，不作为 Phase 1 起点。

### 新闻/叙事手动事件

例如“某 RWA 新闻发布后”。原因是采集新闻时间线成本高，作为新手起点太重。第一阶段用价格/volume/funding/premium 自动生成事件即可。

### 裸控盘币主观名单回测

可以作为人工检查样本，但不作为主事件定义。否则容易样本选择偏差：只挑记得的大涨币。

---

## 4. 推荐第一版事件池组合

第一版只做四类，避免复杂度爆炸：

1. `top_return_1h`：1h 涨跌幅榜；
2. `top_return_4h`：4h 涨跌幅榜；
3. `funding_extreme_per_hour`：资金费率按小时归一后的极端值；
4. `perp_basis_or_cross_exchange_dispersion`：spot-perp 或 perp-perp 脱节。

然后研究联合标签：

```text
top_gainer + high_funding
top_gainer + high_perp_premium
top_gainer + exchange_dispersion
funding_extreme + basis_dislocation
top_loser + negative_funding
```

---

## 5. 回测输出口径

每条事件生成一行 event sample：

```text
event_ts
symbol
event_type
tags
exchange_set
return_15m/1h/4h before event
funding_per_hour
spot_perp_basis
cross_exchange_dispersion
volume_proxy
future_return_15m/60m/240m/1440m
max_adverse_excursion
max_favorable_excursion
funding_adjusted_return
candidate_trade_direction
reject_reason
```

核心不是先训练模型，而是先看分布：

- 事件后继续涨/跌的概率；
- 均值回归概率；
- 最大逆向波动；
- 哪些组合标签收益最厚；
- 哪些只是看起来热闹但不可交易。

---

## 6. 第一阶段验收

Phase 1 历史事件池完成后，至少回答：

1. 哪类事件样本最多、最稳定；
2. 哪类事件 forward return 最厚；
3. 哪类事件 MAE 太大，不适合新手 live；
4. funding extreme 是趋势延续信号还是反转信号；
5. top gainer + high funding 是否比单独 top gainer 更有 edge；
6. spot-perp / cross-exchange dispersion 是否能筛出更安全的结构化机会；
7. 是否值得进入 paper/live event radar。

---

## 7. 新手执行原则

- 不追求一次覆盖所有市场；
- 先 Binance 长历史 + FR_Monitor 近 1 个月跨所数据；
- 先做事件样本表，不急着做复杂模型；
- 先看 15m/60m/240m/1440m forward distribution；
- 先用保守成本，不用乐观成交价；
- 只有事件模板稳定后，再接实时 radar。
