# Astro Funding 页面抓包与接口学习

本目录保存了 `https://funding-v2.astro-btc.xyz/?coin=SAPIEN` 的静态资源，便于离线分析和复用其中的指标获取逻辑。

## 包含内容
- `index.html`：入口 HTML。
- `index-BxgIuhub.js`、`RatePage-CooWzDvg.js`、`SpreadPage-Bj5xjxiA.js`、`utils-79LRmzOq.js`：核心前端逻辑。
- `scan_endpoints.py`：从上述 JS 中提取所有外部接口 URL 的小脚本。
- `endpoints.py`：Python 端点生成器，一键列出各交易所的查询方式及 URL。

运行 `python3 astro_funding_scraper/endpoints.py --symbol SAPIEN` 可列出针对某个币种的全部查询 URL；`scan_endpoints.py` 用于原始代码的正则提取。

## 快速使用

```bash
# 列出 SAPIEN 的所有端点
python3 astro_funding_scraper/endpoints.py --symbol SAPIEN

# 或扫原始 JS 的所有 URL（不填 symbol）
python3 astro_funding_scraper/scan_endpoints.py
```

## 端点一览（Python 方法 + 翻译）

`endpoints.py` 中的 `build_all(symbol)` 会生成以下查询方式，`pretty_print` 会格式化输出。传入的 `symbol` 不含 USDT 后缀（例如 `SAPIEN`），代码自动拼接成 `SAPIENUSDT`。

- **公共辅助**
  - 币种信息（市值/排名）：`https://astro-btc.xyz?action=mc&symbol=<COIN>`  
    作用：获取 `cmc_rank`、`market_cap`、`name`、`symbol` 等。
  - 代理入口：`https://funding.astro-btc.xyz/api/proxy`  
    使用：POST，头部需携带 `x-access-token: proxy-token-for-astro-pro`、`x-method`、`x-url`，请求体通常为 `{}`。默认直连，命中特定域名或直连失败时走代理。

- **Binance**
  - `fapi/v1/exchangeInfo`：校验交易对状态。
  - `fapi/v1/premiumIndex?symbol=<PAIR>`：资金费率/指数溢价。
  - `fapi/v1/fundingInfo`：资金费率上限/下限/周期。
  - `futures/data/openInterestHist?symbol=<PAIR>&period=15m&limit=1`：未平仓额（USD）。
  - `fapi/v1/ticker/24hr?symbol=<PAIR>`：24h 成交数据。
  - `fapi/v1/insuranceBalance`：风险金（USDT/USDC）。
  - `fapi/v1/constituents?symbol=<PAIR>`：指数成分。
  - `fapi/v1/fundingRate?symbol=<PAIR>&limit=72`：历史资金费率。

- **Bybit**
  - `v5/market/tickers?category=linear&symbol=<PAIR>`：行情/资金相关字段。
  - `v5/market/insurance?coin=USDT`：保险金。
  - `v5/market/index-price-components?indexName=<PAIR>`：指数成分。
  - `v5/market/funding/history?category=linear&symbol=<PAIR>&limit=72`：历史资金费率。

- **Gate**
  - `api.gateio.ws/api/v4/futures/usdt/tickers?contract=<COIN>_USDT`：行情/资金上限下限/未平仓/成交。
  - `apiw/v2/futures/common/index/breakdown?index=<COIN>_USDT`：指数成分。
  - `api.gateio.ws/api/v4/futures/usdt/funding_rate?contract=<COIN>_USDT&limit=72`：历史资金费率。

- **OKX**
  - `api/v5/public/funding-rate?instId=<COIN>-USDT-SWAP`：当前资金费率、未平仓。
  - `api/v5/public/funding-rate-history?instId=<COIN>-USDT-SWAP&limit=72`：历史资金费率。
  - `api/v5/public/mark-price?instId=<COIN>-USDT-SWAP`：标记价格。
  - `api/v5/market/index-tickers?instId=<COIN>-USDT`：指数价格。
  - `api/v5/market/index-components?index=<COIN>-USDT`：指数成分。

- **Bitget**
  - `api/v2/mix/market/tickers?productType=usdt-futures&symbol=<PAIR>`：行情 + 资金费率上下限。
  - `v1/kline/getMoreKlineDataV2?symbolId=<COIN>USDT_UMCBL...`：1m K 线（含自定义时间窗）。
  - `api/v2/mix/market/index-constituents?symbol=<COIN>USDT`：指数成分。
  - `api/v2/mix/market/history-fund-rate?productType=usdt-futures&symbol=<PAIR>&pageSize=72&pageNo=1`：历史资金费率。

- **HTX (Huobi)**
  - `linear-swap-api/v1/swap_funding_rate?contract_code=<COIN>-USDT`：当前资金费率。
  - `linear-swap-api/v1/swap_query_elements?contract_code=<COIN>-USDT`：资金费率上限/下限/周期。
  - `linear-swap-api/v1/swap_open_interest?contract_code=<COIN>-USDT`：未平仓。
  - `linear-swap-api/v1/swap_index?contract_code=<COIN>-USDT`：指数价。
  - `index/market/history/linear_swap_mark_price_kline?...`：标记价（1m）。
  - `linear-swap-api/v1/swap_historical_funding_rate?...`：历史资金费率。

- **Hyperliquid**
  - `https://api.hyperliquid.xyz/info`（POST，body: `{"type":"metaAndAssetCtxs"}`）：返回全量资产上下文，字段含 `oraclePx`、`markPx`、`funding`、`openInterest` 等。

- **指数成分汇总**
  - Binance：`fapi/v1/constituents?symbol=<COIN>USDT`
  - OKX：`market/index-components?index=<COIN>-USDT`
  - Bybit：`v5/market/index-price-components?indexName=<COIN>USDT`
  - Gate：`apiw/v2/futures/common/index/breakdown?index=<COIN>_USDT`
  - Bitget：`api/v2/mix/market/index-constituents?symbol=<COIN>USDT`

- **差价 & 历史差价（Spread 页面逻辑）**
  - 实时盘口（现货/合约，用于开/平差价计算）：
    - Binance：现货 `api/binance.com/api/v3/ticker/bookTicker?symbol=<COIN>USDT`；合约 `fapi.binance.com/fapi/v1/ticker/bookTicker?symbol=<COIN>USDT`
    - Bybit：现货 `v5/market/tickers?category=spot&symbol=<COIN>USDT`；合约 `v5/market/tickers?category=linear&symbol=<COIN>USDT`
    - OKX：现货 `market/ticker?instId=<COIN>-USDT`；合约 `market/ticker?instId=<COIN>-USDT-SWAP`
    - Gate：现货 `spot/tickers?currency_pair=<COIN>_USDT`；合约 `futures/usdt/tickers?contract=<COIN>_USDT`
    - Bitget：现货 `api/v2/spot/market/tickers?symbol=<COIN>USDT`；合约 `api/v2/mix/market/ticker?productType=USDT-FUTURES&symbol=<COIN>USDT`
    - HTX：现货 `market/detail/merged?symbol=<coin_lower>usdt`；合约 `linear-swap-ex/market/detail/merged?contract_code=<COIN>-USDT`
  - 历史差价（1m K 线，需自取数据后计算百分比差）：
    - Binance：现货 `api/v3/klines`；合约 `fapi/v1/klines`
    - Bybit：`v5/market/kline?category=spot|linear`
    - OKX：`market/history-candles?instId=<COIN>-USDT(-SWAP)&bar=1m`
    - Gate：现货 `spot/candlesticks?currency_pair=<COIN>_USDT`；合约 `futures/usdt/candlesticks?contract=<COIN>_USDT`
    - Bitget：现货 `spot/market/candles`；合约 `mix/market/candles`
    - HTX：现货 `market/history/kline?symbol=<coin_lower>usdt`；合约 `linear-swap-ex/market/history/kline?contract_code=<COIN>-USDT`

## Python 方法（核心代码片段）

```python
from astro_funding_scraper.endpoints import build_all, pretty_print

base = "SAPIEN"
pretty_print(base)           # 直接打印所有 URL
all_exchanges = build_all(base)
for ex in all_exchanges:
    print(ex.name, ex.format(base=base, pair=f"{base}USDT"))
```

## 说明
- 所有 URL 都是公开接口，前端默认直连；若直连命中特定域名或失败，可用代理 `https://funding.astro-btc.xyz/api/proxy` 转发（需要携带头部）。
- 币种参数一律不含 USDT 后缀，内部会自动拼接成 `<COIN>USDT`。
- 历史资金费率示例均拉取最近 72 条，可按需调整 `limit/pageSize`。

## 使用提示
- 页面通过 `localStorage` 保存登录 UID/Token（AES-GCM + SHA256）以及网络模式。默认“直连”，如需复刻代理逻辑可复用 `/api/proxy` 规则。
- SAPIEN 等币种的 watchlist 查询只需按上述接口拼接 `SAPIENUSDT` 即可，无需访问原站点。
