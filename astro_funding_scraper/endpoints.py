"""
Endpoint builder for the funding-v2.astro-btc.xyz page.

Given a coin symbol (e.g. "SAPIEN"), this module prints all REST endpoints
used by the original前端，用于离线复刻或调试。
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from typing import Dict, List


PROXY = "https://funding.astro-btc.xyz/api/proxy"
COIN_INFO = "https://astro-btc.xyz?action=mc&symbol={base}"


@dataclass
class ExchangeEndpoints:
    name: str
    endpoints: Dict[str, str] = field(default_factory=dict)

    def format(self, base: str, pair: str) -> Dict[str, str]:
        """Return endpoints with placeholders resolved."""
        base_lower = base.lower()
        return {
            k: v.format(base=base, pair=pair, base_lower=base_lower)
            for k, v in self.endpoints.items()
        }


def build_all(base: str) -> List[ExchangeEndpoints]:
    """
    Build endpoint lists for each supported exchange.

    Args:
        base: coin name without suffix, e.g. "SAPIEN".
    """
    pair = f"{base}USDT"

    exchanges = [
        ExchangeEndpoints(
            "Binance",
            {
                "exchange_info": "https://fapi.binance.com/fapi/v1/exchangeInfo",
                "premium_index": "https://fapi.binance.com/fapi/v1/premiumIndex?symbol={pair}",
                "funding_config": "https://fapi.binance.com/fapi/v1/fundingInfo",
                "open_interest": "https://fapi.binance.com/futures/data/openInterestHist?symbol={pair}&period=15m&limit=1",
                "ticker_24h": "https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={pair}",
                "insurance": "https://fapi.binance.com/fapi/v1/insuranceBalance",
                "constituents": "https://fapi.binance.com/fapi/v1/constituents?symbol={pair}",
                "funding_history": "https://fapi.binance.com/fapi/v1/fundingRate?symbol={pair}&limit=72",
                # 差价相关：盘口与1m K线
                "spread_spot_book": "https://api.binance.com/api/v3/ticker/bookTicker?symbol={pair}",
                "spread_futures_book": "https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={pair}",
                "spread_kline_spot": "https://api.binance.com/api/v3/klines?symbol={pair}&interval=1m&startTime=<ts>&endTime=<ts>&limit=1000",
                "spread_kline_futures": "https://fapi.binance.com/fapi/v1/klines?symbol={pair}&interval=1m&startTime=<ts>&endTime=<ts>&limit=1000",
            },
        ),
        ExchangeEndpoints(
            "Bybit",
            {
                "ticker": "https://api.bybit.com/v5/market/tickers?category=linear&symbol={pair}",
                "insurance": "https://api.bybit.com/v5/market/insurance?coin=USDT",
                "index_components": "https://api.bybit.com/v5/market/index-price-components?indexName={pair}",
                "funding_history": "https://api.bybit.com/v5/market/funding/history?category=linear&symbol={pair}&limit=72",
                "spread_spot_book": "https://api.bybit.com/v5/market/tickers?category=spot&symbol={pair}",
                "spread_futures_book": "https://api.bybit.com/v5/market/tickers?category=linear&symbol={pair}",
                "spread_kline_spot": "https://api.bybit.com/v5/market/kline?category=spot&symbol={pair}&interval=1&start=<ts>&end=<ts>&limit=200",
                "spread_kline_futures": "https://api.bybit.com/v5/market/kline?category=linear&symbol={pair}&interval=1&start=<ts>&end=<ts>&limit=200",
            },
        ),
        ExchangeEndpoints(
            "Gate",
            {
                "ticker": "https://api.gateio.ws/api/v4/futures/usdt/tickers?contract={base}_USDT",
                "index_components": "https://www.gate.com/apiw/v2/futures/common/index/breakdown?index={base}_USDT",
                "funding_history": "https://api.gateio.ws/api/v4/futures/usdt/funding_rate?contract={base}_USDT&limit=72",
                "spread_spot_book": "https://api.gateio.ws/api/v4/spot/tickers?currency_pair={base}_USDT",
                "spread_futures_book": "https://api.gateio.ws/api/v4/futures/usdt/tickers?contract={base}_USDT",
                "spread_kline_spot": "https://api.gateio.ws/api/v4/spot/candlesticks?currency_pair={base}_USDT&interval=1m&from=<ts>&to=<ts>",
                "spread_kline_futures": "https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={base}_USDT&interval=1m&from=<ts>&to=<ts>",
            },
        ),
        ExchangeEndpoints(
            "OKX",
            {
                "funding_rate": "https://www.okx.com/api/v5/public/funding-rate?instId={base}-USDT-SWAP",
                "funding_history": "https://www.okx.com/api/v5/public/funding-rate-history?instId={base}-USDT-SWAP&limit=72",
                "mark_price": "https://www.okx.com/api/v5/public/mark-price?instId={base}-USDT-SWAP",
                "index_tickers": "https://www.okx.com/api/v5/market/index-tickers?instId={base}-USDT",
                "open_interest_usd": "https://www.okx.com/api/v5/public/funding-rate?instId={base}-USDT-SWAP",
                "index_components": "https://www.okx.com/api/v5/market/index-components?index={base}-USDT",
                "spread_spot_book": "https://www.okx.com/api/v5/market/ticker?instId={base}-USDT",
                "spread_futures_book": "https://www.okx.com/api/v5/market/ticker?instId={base}-USDT-SWAP",
                "spread_kline_spot": "https://www.okx.com/api/v5/market/history-candles?instId={base}-USDT&bar=1m&limit=300&after=<ts>",
                "spread_kline_futures": "https://www.okx.com/api/v5/market/history-candles?instId={base}-USDT-SWAP&bar=1m&limit=300&after=<ts>",
            },
        ),
        ExchangeEndpoints(
            "Bitget",
            {
                "ticker": "https://api.bitget.com/api/v2/mix/market/tickers?productType=usdt-futures&symbol={pair}",
                "funding_cap_floor": "https://api.bitget.com/api/v2/mix/market/tickers?productType=usdt-futures&symbol={pair}",
                "kline": "https://www.bitgetapps.com/v1/kline/getMoreKlineDataV2?symbolId={base}USDT_UMCBL&kLineStep=1m&kLineType=4&startTime=<ts>&endTime=<ts>&limit=480",
                "index_components": "https://api.bitget.com/api/v2/mix/market/index-constituents?symbol={base}USDT",
                "funding_history": "https://api.bitget.com/api/v2/mix/market/history-fund-rate?productType=usdt-futures&symbol={pair}&pageSize=72&pageNo=1",
                "spread_spot_book": "https://api.bitget.com/api/v2/spot/market/tickers?symbol={pair}",
                "spread_futures_book": "https://api.bitget.com/api/v2/mix/market/ticker?productType=USDT-FUTURES&symbol={pair}",
                "spread_kline_spot": "https://api.bitget.com/api/v2/spot/market/candles?symbol={pair}&granularity=1min&startTime=<ts>&endTime=<ts>&limit=200",
                "spread_kline_futures": "https://api.bitget.com/api/v2/mix/market/candles?productType=USDT-FUTURES&symbol={pair}&granularity=1m&startTime=<ts>&endTime=<ts>&limit=200",
            },
        ),
        ExchangeEndpoints(
            "HTX",
            {
                "funding_rate": "https://api.hbdm.vn/linear-swap-api/v1/swap_funding_rate?contract_code={base}-USDT",
                "funding_config": "https://api.hbdm.com/linear-swap-api/v1/swap_query_elements?contract_code={base}-USDT",
                "open_interest": "https://api.hbdm.vn/linear-swap-api/v1/swap_open_interest?contract_code={base}-USDT",
                "index_price": "https://api.hbdm.vn/linear-swap-api/v1/swap_index?contract_code={base}-USDT",
                "mark_price": "https://api.hbdm.com/index/market/history/linear_swap_mark_price_kline?contract_code={base}-USDT&period=1min&size=1",
                "funding_history": "https://api.hbdm.com/linear-swap-api/v1/swap_historical_funding_rate?contract_code={base}-USDT&page_size=72&page_index=1",
                "spread_spot_book": "https://api.huobi.pro/market/detail/merged?symbol={base_lower}usdt",
                "spread_futures_book": "https://api.hbdm.vn/linear-swap-ex/market/detail/merged?contract_code={base}-USDT",
                "spread_kline_spot": "https://api.huobi.pro/market/history/kline?symbol={base_lower}usdt&period=1min&size=<limit>",
                "spread_kline_futures": "https://api.hbdm.vn/linear-swap-ex/market/history/kline?contract_code={base}-USDT&period=1min&size=<limit>",
            },
        ),
        ExchangeEndpoints(
            "Hyperliquid",
            {
                "meta_and_assets": "https://api.hyperliquid.xyz/info",  # post body: {"type":"metaAndAssetCtxs"}
            },
        ),
    ]

    return [ExchangeEndpoints("Common", {"coin_info": COIN_INFO})] + exchanges


def pretty_print(base: str) -> None:
    print(f"基础币种: {base}")
    print(f"交易对: {base}USDT")
    print(f"代理入口: {PROXY} (POST, 头部: x-access-token/x-method/x-url)")
    print(f"币种信息: {COIN_INFO.format(base=base)}")
    print("-" * 60)

    for ex in build_all(base):
        print(f"[{ex.name}]")
        for key, url in ex.format(base=base, pair=f"{base}USDT").items():
            print(f"  {key}: {url}")
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description="打印各交易所相关的资金费率/指数端点")
    parser.add_argument("--symbol", "-s", default="SAPIEN", help="币种，不含USDT后缀，默认 SAPIEN")
    args = parser.parse_args()

    base = args.symbol.strip().upper().replace("USDT", "")
    pretty_print(base)


if __name__ == "__main__":
    main()
