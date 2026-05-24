# Strategy Decision — Event-first Alpha 主线

> 日期：2026-05-10  
> 适用项目：`jerry/wlfi/FR_Monitor`  
> 结论：后续主线从“全市场薄因子/普通套利”收口为 **事件优先的 funding / basis / 控盘币机会发现与验证**。

---

## 1. 达成一致的一句话

当前阶段不再把主要精力放在“继续全市场挖大量 IC/IR 因子”，也不直接裸做控盘币 CTA，而是先建立：

```text
事件发现 → 可成交验证 → 结构化交易表达 → paper 审计 → 小额 live 白名单
```

核心目标是找到更厚的 alpha：

- 可能来自正在发生的事件；
- 也可能来自历史事件复盘；
- 但必须落到可交易、可审计、可复用的事件模板上。

---

## 2. 为什么做这个转向

过去对 rank213、32b、154 以及 FR_Monitor 的研究反馈比较一致：

1. 普通全市场因子可以挖出统计信号，但很多 alpha 太薄；
2. 部分策略存在未来函数或 live 可用性问题；
3. FR_Monitor 已经具备跨所行情、watchlist、orderbook 复核、小额 live 与审计基础，但过去价差/funding 实盘更接近盈亏平衡；
4. 继续扩大 broad factor mining 容易回到“IC 看起来有，扣费/滑点/OOS/live 后不够厚”的循环。

因此下一阶段的重点不是“更多因子”，而是“更好的事件池”。

---

## 3. 新主线定义

### 3.1 先找事件

事件可以包括：

- 控盘/榜单币：如 PIPPIN、SIREN、币安人生等；
- 涨幅榜/跌幅榜 Top；
- volume zscore 异常；
- funding rate per hour 极端；
- funding change 极端；
- perp premium 异常；
- 跨交易所 perp 价格 dispersion；
- spot-perp basis；
- CEX-DEX dislocation；
- 新币、B 股、RWA、热门叙事币。

### 3.2 再找结构化表达

事件进入 universe 后，优先寻找中性/半中性的表达方式：

1. 高价所 short + 低价所 long；
2. long spot + short perp；
3. CEX-DEX perp/basis；
4. funding carry + basis convergence；
5. 最后才考虑裸方向 CTA / fade / momentum。

原则：

> 用事件找厚度，用结构化交易控制死亡风险。

---

## 4. 优先级

### P0：Controlled Event Radar / 控盘事件雷达

目标：把值得盯的异常币及时抓出来。

最小输出字段：

- symbol；
- event_type / tags；
- return_15m / 1h / 4h / 24h；
- volume_zscore；
- funding_per_hour；
- perp_premium；
- exchange_dispersion；
- best_candidate_trade；
- risk_tags；
- paper_only。

### P1：CEX/DEX/Funding/Basis Paper Engine

目标：不急着 live，先 paper 记录真实净收益。

每个 candidate 必须记录：

- entry_spread_last；
- entry_spread_bbo；
- entry_spread_sweep_50u / 100u / 500u；
- orderbook_age_ms；
- fee estimate；
- funding interval；
- time_to_next_funding；
- basis PnL；
- funding PnL；
- slippage；
- reject / skip reason。

### P2：事件条件下的因子研究

只在 event_universe 内计算 IC/IR，不再做全市场大炼因子。

研究问题示例：

- 高 funding 后是继续 squeeze 还是均值回归；
- top gainer + high funding 什么时候可以 fade；
- 哪些交易所组合的 perp-perp basis 更容易回归；
- spot-perp 是否比 perp-perp 更厚；
- B 股/RWA/新币是否有独立 regime。

---

## 5. 当前明确不做

为了避免重新陷入薄 alpha 泥潭，近期明确不做：

1. 不做全市场 200 因子大炼丹作为主线；
2. 不扩大 live notional；
3. 不把 DEX-CEX funding spread 直接实盘；
4. 不用 last/mark price 判断可交易收益；
5. 不因 funding 高就裸空控盘币；
6. 不在 fee/slippage/funding 审计不完整时宣称策略有效。

---

## 6. 两周验证目标

接下来 14 天的目标不是证明一个大策略有效，而是回答：

> 控盘/榜单/拥挤事件里，是否存在 50U/100U 级别可成交、扣费后仍为正的 funding/basis 厚机会？

验收指标：

- 异常事件捕捉数量；
- 有结构化交易候选的比例；
- sweep 后仍有正 edge 的比例；
- paper net PnL 均值/中位数；
- top 机会来自哪类事件；
- 亏损/拒绝主要原因；
- 是否存在可复用事件模板。

---

## 7. 后续执行口径

以后讨论 FR_Monitor 下一步时，默认遵循本决策：

```text
先事件，后因子；
先可成交，后回测；
先 paper，后 live；
先结构化，后裸方向；
低频高质量优先于高频薄信号。
```
