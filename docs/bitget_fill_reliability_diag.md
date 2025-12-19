# Bitget 成交回填可靠性排查（avg_price / filled_qty）

## 背景
Live Trading 的 `realized_pnl_usdt` 依赖 `open/close` 两条腿的：
- `filled_qty`
- `avg_price`

其中 Bitget 的这两个值来自：
- 下单回执（place-order）通常只给 `orderId/clientOid/size/...`，不含最终成交均价。
- 因此系统会再调用 `/api/v2/mix/order/detail` 查询订单详情并回填成交信息。

当币种流动性较差或盘口剧烈波动时，Bitget 可能出现：
- 订单详情在短时间内先返回 **部分成交**（`filledQty>0` 但未终态）
- 随后才更新 `priceAvg/filledQty` 为最终结果

如果我们过早采样，就会导致：
- `avg_price/filled_qty` 与真实最终成交不一致
- 进而让 `realized_pnl_usdt` 偏小/偏大

## 本次改动（更稳健的 Bitget 回填）
文件：`trading/live_trading_manager.py`

对 Bitget 的 `_parse_fill_fields()` 做了增强：
- 轮询 `get_bitget_usdt_perp_order_detail()` 从 **3 次** 增加到 **8 次**（每次 sleep 0.4s，约 3 秒窗口）
- 不再“一旦 `filledQty>0` 就立刻停止”，而是：
  - 仅当订单进入终态（filled/cancelled/failed 等）才提前停止，或
  - `filledQty` 已经接近 `place-order` 回执里的 `size`（认为已基本完全成交）
- 当 `quoteVolume` 和 `filledQty` 都存在时，优先用 `quoteVolume/filledQty` 推导均价，避免字段歧义

目的：减少 “部分成交采样导致的均价/数量不可靠”。

## 如何验证（推荐在服务器上执行）
仓库提供了诊断脚本：`test_bitget_fill_reliability.py`

示例（用 ETH，20U，先开再平）：
```bash
./venv/bin/python test_bitget_fill_reliability.py --symbol ETH --notional 20 --sleep 0.8
```

脚本会输出：
- Bitget 订单簿扫盘估算（buy/sell）
- 下单返回的 `orderId/clientOid`
- 订单详情返回的 `priceAvg/filledQty/quoteVolume/state`
- 均价与订单簿估算值的偏差（diff）

期望（ETH 这种流动性好的标的）：
- `avg_open` 与 `orderbook buy_px_sweep` 偏差很小（通常 < 0.1%）
- `avg_close` 与 `orderbook sell_px_sweep` 偏差很小

如果仍出现极端偏差：
- 请把脚本输出粘贴出来（尤其是 detail 的字段），再继续定位是：
  - 下单时盘口瞬间跳变导致真实滑点
  - 还是 Bitget detail 字段解析/含义与预期不一致

