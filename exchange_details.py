import time
import requests
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import config


def _get_json(url: str, timeout: float = 4.0):
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def _fmt_pct(val):
    try:
        f = float(val)
        return f
    except Exception:
        return None


def _fmt_float(val):
    try:
        f = float(val)
        return f
    except Exception:
        return None


def _human_sum_insurance(data: Any) -> float:
    """Sum USDT/USDC marginBalance from Binance insuranceBalance response."""
    total = 0.0
    if isinstance(data, list):
        for item in data:
            assets = item.get("assets") if isinstance(item, dict) else None
            if not assets:
                continue
            for a in assets:
                asset = a.get("asset")
                if asset in ("USDT", "USDC"):
                    try:
                        total += float(a.get("marginBalance", 0))
                    except Exception:
                        continue
    return total or None


def _derive_interval_hours(history: List[Dict[str, Any]], key: str = "time") -> float:
    """
    依据资金费历史时间戳推断周期（毫秒差值转小时）。
    当缺少周期字段或交易所未显式返回时使用。
    """
    if not isinstance(history, list) or len(history) < 2:
        return None
    times = []
    for h in history:
        ts = h.get(key) if isinstance(h, dict) else None
        try:
            t = float(ts)
            # Gate 等部分接口返回秒，需要换算
            if t < 1e12:
                t *= 1000.0
            times.append(t)
        except Exception:
            continue
    if len(times) < 2:
        return None
    times = sorted(set(times))
    if len(times) < 2:
        return None
    # 取最近两个时间点的间隔
    diff_ms = times[-1] - times[-2]
    if diff_ms <= 0:
        return None
    hours = diff_ms / 3_600_000.0
    # 合理区间过滤，常见 1h / 4h / 8h
    if 0.5 <= hours <= 12:
        return round(hours, 3)
    return None


def fetch_binance(symbol: str):
    sym = symbol.upper() + "USDT"
    now = int(time.time() * 1000)
    premium = _get_json(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={sym}")
    fund_info = _get_json(f"https://fapi.binance.com/fapi/v1/fundingInfo?symbol={sym}")
    oi = _get_json(f"https://fapi.binance.com/futures/data/openInterestHist?symbol={sym}&period=5m&limit=1")
    ticker = _get_json(f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={sym}")
    insurance = _get_json("https://fapi.binance.com/fapi/v1/insuranceBalance")
    history = _get_json(f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={sym}&limit=200")
    components = _get_json(f"https://fapi.binance.com/fapi/v1/constituents?symbol={sym}")
    row = {
        "exchange": "binance",
        "symbol": symbol,
        "funding_interval_hours": None,
        "funding_rate": None,
        "funding_cap": None,
        "funding_floor": None,
        "index_diff": None,
        "open_interest": None,
        "risk_fund": None,
        "volume_quote": None,
        "timestamp": now,
        "mark_price": None,
        "index_price": None,
        "insurance_fund": None,
        "funding_history": [],
        "index_components": [],
    }
    if premium:
        fr = premium.get("lastFundingRate")
        row["funding_rate"] = _fmt_pct(fr)
        mark = _fmt_float(premium.get("markPrice"))
        idx = _fmt_float(premium.get("indexPrice"))
        row["mark_price"] = mark
        row["index_price"] = idx
        if mark and idx:
            base = idx or mark
            if base:
                row["index_diff"] = (mark - idx) / base
        next_ft = premium.get("nextFundingTime")
        row["next_funding_time"] = next_ft
    if fund_info and isinstance(fund_info, list) and fund_info:
        info = fund_info[0]
        row["funding_interval_hours"] = _fmt_float(info.get("fundingIntervalHours"))
        row["funding_cap"] = _fmt_pct(info.get("adjustedFundingRateCap"))
        row["funding_floor"] = _fmt_pct(info.get("adjustedFundingRateFloor"))
    if row["funding_interval_hours"] is None:
        row["funding_interval_hours"] = 8.0
    if oi and isinstance(oi, list) and oi:
        latest = oi[-1]
        val = latest.get("sumOpenInterestValue")
        row["open_interest"] = _fmt_float(val)
    if ticker:
        row["volume_quote"] = _fmt_float(ticker.get("quoteVolume"))
    if insurance:
        row["insurance_fund"] = _human_sum_insurance(insurance)
    if isinstance(history, list):
        row["funding_history"] = [
            {"time": h.get("fundingTime") or h.get("fundingTime"), "rate": _fmt_pct(h.get("fundingRate"))}
            for h in history if h.get("fundingRate") is not None
        ]
    if components and isinstance(components, dict):
        basket = components.get("basket") or []
        parsed = []
        for item in basket:
            symb = item.get("symbol")
            wt = _fmt_float(item.get("weightInQuote") or item.get("weightInUnits"))
            if symb:
                parsed.append({"symbol": symb, "weight": wt})
        row["index_components"] = parsed
    derived_interval = _derive_interval_hours(row.get("funding_history", []))
    if derived_interval:
        row["funding_interval_hours"] = derived_interval
    return row


def fetch_bybit(symbol: str):
    sym = symbol.upper() + "USDT"
    now = int(time.time() * 1000)
    ticker = _get_json(f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={sym}")
    insurance = _get_json("https://api.bybit.com/v5/market/insurance?coin=USDT")
    history = _get_json(f"https://api.bybit.com/v5/market/funding/history?category=linear&symbol={sym}&limit=200")
    components = _get_json(f"https://api.bybit.com/v5/market/index-price-components?indexName={sym}")
    row = {
        "exchange": "bybit",
        "symbol": symbol,
        "funding_interval_hours": 8.0,
        "funding_rate": None,
        "index_diff": None,
        "open_interest": None,
        "volume_quote": None,
        "timestamp": now,
        "mark_price": None,
        "index_price": None,
        "insurance_fund": None,
        "funding_history": [],
        "index_components": [],
    }
    try:
        if ticker and ticker.get("result", {}).get("list"):
            item = ticker["result"]["list"][0]
            row["funding_rate"] = _fmt_pct(item.get("fundingRate"))
            mark = _fmt_float(item.get("markPrice"))
            idx = _fmt_float(item.get("indexPrice"))
            row["mark_price"] = mark
            row["index_price"] = idx
            if mark and idx:
                base = idx or mark
                row["index_diff"] = (mark - idx) / base if base else None
            row["open_interest"] = _fmt_float(item.get("openInterest"))
            row["volume_quote"] = _fmt_float(item.get("turnover24h"))
            row["next_funding_time"] = item.get("nextFundingTime")
    except Exception:
        pass
    try:
        if insurance and insurance.get("result", {}).get("list"):
            total = 0.0
            for bucket in insurance["result"]["list"]:
                try:
                    total += float(bucket.get("balance", 0))
                except Exception:
                    continue
            row["insurance_fund"] = total or None
    except Exception:
        pass
    try:
        if history and history.get("result", {}).get("list"):
            row["funding_history"] = [
                {"time": h.get("fundingRateTimestamp"), "rate": _fmt_pct(h.get("fundingRate"))}
                for h in history["result"]["list"]
                if h.get("fundingRate") is not None
            ]
    except Exception:
        pass
    derived_interval = _derive_interval_hours(row.get("funding_history", []))
    if derived_interval:
        row["funding_interval_hours"] = derived_interval
    try:
        comps = components.get("result", {}).get("list", []) if components else []
        parsed = []
        for item in comps:
            symb = item.get("symbol")
            wt = _fmt_float(item.get("weight"))
            if symb:
                parsed.append({"symbol": symb, "weight": wt})
        row["index_components"] = parsed
    except Exception:
        pass
    return row


def fetch_gate(symbol: str):
    sym = symbol.upper() + "_USDT"
    now = int(time.time() * 1000)
    ticker = _get_json(f"https://api.gateio.ws/api/v4/futures/usdt/tickers?contract={sym}")
    stats = _get_json(f"https://api.gateio.ws/api/v4/futures/usdt/contract_stats?contract={sym}&limit=1")
    history = _get_json(f"https://api.gateio.ws/api/v4/futures/usdt/funding_rate?contract={sym}&limit=200")
    # Gate 未提供指数成分/保险金公开接口
    row = {
        "exchange": "gate",
        "symbol": symbol,
        "funding_interval_hours": 8.0,
        "funding_rate": None,
        "index_diff": None,
        "open_interest": None,
        "volume_quote": None,
        "timestamp": now,
        "mark_price": None,
        "index_price": None,
        "funding_history": [],
        "index_components": [],
    }
    try:
        if ticker and isinstance(ticker, list) and ticker:
            item = ticker[0]
            row["funding_rate"] = _fmt_pct(item.get("funding_rate"))
            mark = _fmt_float(item.get("mark_price"))
            idx = _fmt_float(item.get("index_price"))
            row["mark_price"] = mark
            row["index_price"] = idx
            if mark and idx:
                base = idx or mark
                row["index_diff"] = (mark - idx) / base if base else None
            row["open_interest"] = _fmt_float(item.get("total_size"))
            row["volume_quote"] = _fmt_float(item.get("volume_quote"))
        if stats and isinstance(stats, list) and stats:
            row["funding_interval_hours"] = _fmt_float(stats[0].get("funding_rate_indicative_interval"))
    except Exception:
        pass
    try:
        if isinstance(history, list):
            row["funding_history"] = [
                {"time": h.get("t") or h.get("timestamp"), "rate": _fmt_pct(h.get("r") or h.get("funding_rate"))}
                for h in history if h.get("r") or h.get("funding_rate") is not None
            ]
    except Exception:
        pass
    derived_interval = _derive_interval_hours(row.get("funding_history", []))
    if derived_interval:
        row["funding_interval_hours"] = derived_interval
    return row


def fetch_bitget(symbol: str):
    sym = symbol.upper() + "USDT"
    now = int(time.time() * 1000)
    ticker = _get_json(f"https://api.bitget.com/api/v2/mix/market/ticker?productType=usdt-futures&symbol={sym}")
    oi = _get_json(f"https://api.bitget.com/api/v2/mix/market/open-interest?productType=usdt-futures&symbol={sym}")
    fund = _get_json(f"https://api.bitget.com/api/v2/mix/market/current-fund-rate?productType=usdt-futures&symbol={sym}")
    history = _get_json(f"https://api.bitget.com/api/v2/mix/market/history-fundRate?productType=usdt-futures&symbol={sym}&pageSize=200")
    # Bitget 指数组成/保险金暂无公开接口，标记为空
    row = {
        "exchange": "bitget",
        "symbol": symbol,
        "funding_interval_hours": 8.0,
        "funding_rate": None,
        "index_diff": None,
        "open_interest": None,
        "volume_quote": None,
        "timestamp": now,
        "mark_price": None,
        "index_price": None,
        "funding_cap": None,
        "funding_floor": None,
        "funding_history": [],
        "index_components": [],
    }
    try:
        data = ticker.get("data") if ticker else None
        item = data[0] if isinstance(data, list) and data else data
        if item:
            row["funding_rate"] = _fmt_pct(item.get("fundingRate"))
            mark = _fmt_float(item.get("markPrice"))
            idx = _fmt_float(item.get("indexPrice"))
            row["mark_price"] = mark
            row["index_price"] = idx
            if mark and idx:
                base = idx or mark
                row["index_diff"] = (mark - idx) / base if base else None
            row["volume_quote"] = _fmt_float(item.get("quoteVolume"))
    except Exception:
        pass
    try:
        data = oi.get("data", {}) if oi else {}
        lst = data.get("openInterestList")
        if isinstance(lst, list) and lst:
            latest = lst[0]
            row["open_interest"] = _fmt_float(latest.get("value") or latest.get("size"))
    except Exception:
        pass
    try:
        data = fund.get("data") if fund else None
        item = data[0] if isinstance(data, list) and data else data
        if item:
            row["funding_rate"] = _fmt_pct(item.get("fundingRate"))
            row["funding_interval_hours"] = _fmt_float(item.get("fundingRateInterval")) or 4.0
            row["funding_cap"] = _fmt_pct(item.get("maxFundingRate"))
            row["funding_floor"] = _fmt_pct(item.get("minFundingRate"))
            row["next_funding_time"] = item.get("nextUpdate")
    except Exception:
        pass
    try:
        if history and history.get("data"):
            row["funding_history"] = [
                {
                    "time": h.get("timestamp") or h.get("fundingTime"),
                    "rate": _fmt_pct(h.get("fundingRate")),
                }
                for h in history["data"]
                if h.get("fundingRate") is not None
            ]
    except Exception:
        pass
    derived_interval = _derive_interval_hours(row.get("funding_history", []))
    if derived_interval:
        row["funding_interval_hours"] = derived_interval
    if row["funding_interval_hours"] is None:
        row["funding_interval_hours"] = 4.0
    return row


_LIGHTER_BASE = "https://mainnet.zklighter.elliot.ai/api/v1"
_LIGHTER_CACHE: Dict[str, Any] = {"ts": 0.0, "funding_rates": None, "exchange_stats": None}
_LIGHTER_CACHE_TTL = 10.0  # seconds


def _utc_next_hour_ms(now_ms: int) -> int:
    # Round up to next UTC hour boundary.
    hour_ms = 3_600_000
    return ((int(now_ms) // hour_ms) + 1) * hour_ms


def fetch_lighter(symbol: str):
    """
    Lighter perpetual public metrics.

    Notes
    - Lighter REST currently exposes:
      - /exchangeStats: 24h volume (quote/base), last trade price, etc. (bulk list)
      - /funding-rates: current funding rates (bulk list)
    - It does NOT expose (at least via these public endpoints) mark/index price, OI, insurance fund,
      or funding rate cap/floor. Those fields are returned as None to keep schema consistent.
    - Funding interval is treated as 1h (rule-based) and next funding time is aligned to next UTC hour.
      If Lighter later provides per-market schedule, replace this with the authoritative fields.
    """
    sym = symbol.upper()
    now_ms = int(time.time() * 1000)
    now_s = time.time()

    cached = _LIGHTER_CACHE if isinstance(_LIGHTER_CACHE, dict) else {}
    if now_s - float(cached.get("ts") or 0.0) > _LIGHTER_CACHE_TTL:
        cached["funding_rates"] = _get_json(f"{_LIGHTER_BASE}/funding-rates")
        cached["exchange_stats"] = _get_json(f"{_LIGHTER_BASE}/exchangeStats")
        cached["ts"] = now_s

    funding_rates = cached.get("funding_rates")
    exchange_stats = cached.get("exchange_stats")

    row = {
        "exchange": "lighter",
        "symbol": symbol,
        "funding_interval_hours": 1.0,
        "funding_rate": None,
        "funding_cap": None,
        "funding_floor": None,
        "index_diff": None,
        "open_interest": None,
        "risk_fund": None,
        "volume_quote": None,
        "timestamp": now_ms,
        "mark_price": None,
        "index_price": None,
        "insurance_fund": None,
        "funding_history": [],
        "index_components": [],
        "next_funding_time": _utc_next_hour_ms(now_ms),
    }

    try:
        if isinstance(funding_rates, dict):
            for entry in funding_rates.get("funding_rates") or []:
                if not isinstance(entry, dict):
                    continue
                if str(entry.get("symbol") or "").upper() != sym:
                    continue
                row["funding_rate"] = _fmt_pct(entry.get("rate"))
                break
    except Exception:
        pass

    try:
        if isinstance(exchange_stats, dict):
            for entry in exchange_stats.get("order_book_stats") or []:
                if not isinstance(entry, dict):
                    continue
                if str(entry.get("symbol") or "").upper() != sym:
                    continue
                # Use daily quote volume as "24h额" proxy.
                row["volume_quote"] = _fmt_float(entry.get("daily_quote_token_volume"))
                # Lighter does not publish mark/index in this endpoint; keep None.
                break
    except Exception:
        pass

    return row


def _ns_to_ms(value: Any) -> float:
    try:
        v = float(value)
    except Exception:
        return None
    # Heuristic: GRVT returns unix ns (~1e18)
    if v > 1e15:
        return v / 1_000_000.0
    if v > 1e12:
        return v
    return v * 1000.0


def _post_json(url: str, payload: Dict[str, Any], timeout: float = 4.0):
    try:
        resp = requests.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def fetch_grvt(symbol: str):
    base = config.GRVT_REST_BASE_URL.rstrip("/") or "https://market-data.grvt.io"
    sym_u = symbol.upper()
    inst = f"{sym_u}_USDT_Perp"
    now = int(time.time() * 1000)

    ticker = _post_json(f"{base}/full/v1/ticker", {"instrument": inst})
    instrument = _post_json(f"{base}/full/v1/instrument", {"instrument": inst})
    history = _post_json(f"{base}/full/v1/funding", {"instrument": inst, "limit": 200})

    row = {
        "exchange": "grvt",
        "symbol": symbol,
        "funding_interval_hours": None,
        "funding_rate": None,
        "funding_cap": None,
        "funding_floor": None,
        "index_diff": None,
        "open_interest": None,
        "risk_fund": None,
        "volume_quote": None,
        "timestamp": now,
        "mark_price": None,
        "index_price": None,
        "insurance_fund": None,
        "funding_history": [],
        "index_components": [],
        "next_funding_time": None,
    }

    inst_row = instrument.get("result") if isinstance(instrument, dict) else None
    if isinstance(inst_row, dict):
        try:
            row["funding_interval_hours"] = _fmt_float(inst_row.get("funding_interval_hours"))
        except Exception:
            pass
        try:
            cap = _fmt_float(inst_row.get("adjusted_funding_rate_cap"))
            floor = _fmt_float(inst_row.get("adjusted_funding_rate_floor"))
            # GRVT expresses funding in percentage points; normalize to decimal.
            row["funding_cap"] = (cap / 100.0) if cap is not None else None
            row["funding_floor"] = (floor / 100.0) if floor is not None else None
        except Exception:
            pass

    tick_row = ticker.get("result") if isinstance(ticker, dict) else None
    if isinstance(tick_row, dict):
        try:
            mark = _fmt_float(tick_row.get("mark_price"))
            idx = _fmt_float(tick_row.get("index_price"))
            row["mark_price"] = mark
            row["index_price"] = idx
            if mark and idx:
                base_px = idx or mark
                row["index_diff"] = (mark - idx) / base_px if base_px else None
        except Exception:
            pass
        try:
            fr_pp = _fmt_float(tick_row.get("funding_rate") or tick_row.get("funding_rate_8h_curr"))
            row["funding_rate"] = (fr_pp / 100.0) if fr_pp is not None else None
        except Exception:
            pass
        try:
            oi = _fmt_float(tick_row.get("open_interest"))
            row["open_interest"] = oi
        except Exception:
            pass
        try:
            buy_q = _fmt_float(tick_row.get("buy_volume_24h_q")) or 0.0
            sell_q = _fmt_float(tick_row.get("sell_volume_24h_q")) or 0.0
            row["volume_quote"] = (buy_q + sell_q) or None
        except Exception:
            pass
        try:
            row["next_funding_time"] = _ns_to_ms(tick_row.get("next_funding_time"))
        except Exception:
            pass

    if isinstance(history, dict):
        items = history.get("result") or []
        if isinstance(items, list):
            parsed = []
            for h in items:
                if not isinstance(h, dict):
                    continue
                ft_ms = _ns_to_ms(h.get("funding_time"))
                fr_pp = _fmt_float(h.get("funding_rate"))
                parsed.append(
                    {
                        "time": ft_ms,
                        "rate": (fr_pp / 100.0) if fr_pp is not None else None,
                        "mark_price": _fmt_float(h.get("mark_price")),
                    }
                )
            row["funding_history"] = [x for x in parsed if x.get("rate") is not None]

    derived_interval = _derive_interval_hours(row.get("funding_history", []))
    if derived_interval:
        row["funding_interval_hours"] = derived_interval
    if row.get("funding_interval_hours") is None:
        row["funding_interval_hours"] = 8.0
    return row


def fetch_okx(symbol: str):
    sym = symbol.upper() + "-USDT-SWAP"
    uly = symbol.upper() + "-USDT"
    now = int(time.time() * 1000)
    ticker = _get_json(f"https://www.okx.com/api/v5/market/ticker?instId={sym}")
    oi_public = _get_json(f"https://www.okx.com/api/v5/public/open-interest?instId={sym}")
    funding_now = _get_json(f"https://www.okx.com/api/v5/public/funding-rate?instId={sym}")
    mark_price = _get_json(f"https://www.okx.com/api/v5/public/mark-price?instType=SWAP&instId={sym}")
    index_px = _get_json(f"https://www.okx.com/api/v5/market/index-tickers?quoteCcy=USDT&instId={uly}")
    insurance = _get_json(f"https://www.okx.com/api/v5/public/insurance-fund?instType=SWAP&uly={uly}")
    history = _get_json(f"https://www.okx.com/api/v5/public/funding-rate-history?instId={sym}&limit=200")
    components = _get_json(f"https://www.okx.com/api/v5/market/index-components?index={uly}")
    row = {
        "exchange": "okx",
        "symbol": symbol,
        "funding_interval_hours": None,
        "funding_rate": None,
        "index_diff": None,
        "open_interest": None,
        "volume_quote": None,
        "timestamp": now,
        "mark_price": None,
        "index_price": None,
        "insurance_fund": None,
        "funding_cap": None,
        "funding_floor": None,
        "funding_history": [],
        "index_components": [],
    }
    try:
        if ticker and ticker.get("data"):
            item = ticker["data"][0]
            row["open_interest"] = _fmt_float(item.get("oi"))
            row["volume_quote"] = _fmt_float(item.get("volCcy24h"))
    except Exception:
        pass
    try:
        if oi_public and oi_public.get("data"):
            oi_item = oi_public["data"][0]
            row["open_interest"] = row["open_interest"] or _fmt_float(
                oi_item.get("oiValue") or oi_item.get("oiUsd") or oi_item.get("oi")
            )
    except Exception:
        pass
    try:
        if funding_now and funding_now.get("data"):
            item = funding_now["data"][0]
            row["funding_rate"] = _fmt_pct(item.get("fundingRate"))
            row["funding_cap"] = _fmt_pct(item.get("maxFundingRate"))
            row["funding_floor"] = _fmt_pct(item.get("minFundingRate"))
            row["next_funding_time"] = item.get("nextFundingTime") or item.get("fundingTime")
            row["funding_interval_hours"] = 4.0
    except Exception:
        pass
    if row["funding_interval_hours"] is None:
        row["funding_interval_hours"] = 4.0
    try:
        if mark_price and mark_price.get("data"):
            mp = mark_price["data"][0]
            row["mark_price"] = _fmt_float(mp.get("markPx"))
    except Exception:
        pass
    try:
        if index_px and index_px.get("data"):
            idx_item = index_px["data"][0]
            idx_val = _fmt_float(idx_item.get("idxPx"))
            row["index_price"] = idx_val
    except Exception:
        pass
    try:
        if row.get("mark_price") and row.get("index_price"):
            base = row["index_price"] or row["mark_price"]
            if base:
                row["index_diff"] = (row["mark_price"] - row["index_price"]) / base
    except Exception:
        pass
    try:
        if insurance and insurance.get("data"):
            total = 0.0
            for bucket in insurance["data"]:
                details = bucket.get("details") or []
                for det in details:
                    try:
                        total += float(det.get("balance", 0))
                    except Exception:
                        continue
            row["insurance_fund"] = total or None
    except Exception:
        pass
    try:
        if history and history.get("data"):
            row["funding_history"] = [
                {"time": h.get("fundingTime"), "rate": _fmt_pct(h.get("realizedRate"))}
                for h in history["data"]
                if h.get("realizedRate") is not None
            ]
    except Exception:
        pass
    derived_interval = _derive_interval_hours(row.get("funding_history", []))
    if derived_interval:
        row["funding_interval_hours"] = derived_interval
    try:
        comps = components.get("data", []) if components else []
        parsed = []
        for block in comps:
            items = block.get("components") or []
            for item in items:
                symb = item.get("component")
                wt = _fmt_float(item.get("weight"))
                if symb:
                    parsed.append({"symbol": symb, "weight": wt})
        row["index_components"] = parsed
    except Exception:
        pass
    return row


def fetch_htx(symbol: str):
    """
    Huobi/HTX USDT 永续合约公开接口。
    """
    contract = symbol.upper() + "-USDT"
    now = int(time.time() * 1000)
    funding = _get_json(f"https://api.hbdm.com/linear-swap-api/v1/swap_funding_rate?contract_code={contract}")
    oi = _get_json(f"https://api.hbdm.com/linear-swap-api/v1/swap_open_interest?contract_code={contract}")
    ticker = _get_json(f"https://api.hbdm.com/linear-swap-ex/market/detail/merged?contract_code={contract}")
    history = _get_json(
        f"https://api.hbdm.com/linear-swap-api/v1/swap_historical_funding_rate?contract_code={contract}"
        "&page_size=20&page_index=1"
    )
    row = {
        "exchange": "htx",
        "symbol": symbol,
        "funding_interval_hours": 8.0,
        "funding_rate": None,
        "index_diff": None,
        "open_interest": None,
        "volume_quote": None,
        "timestamp": now,
        "mark_price": None,
        "index_price": None,
        "funding_cap": None,
        "funding_floor": None,
        "funding_history": [],
        "index_components": [],
    }
    try:
        data = funding.get("data") if funding else None
        if data:
            row["funding_rate"] = _fmt_pct(data.get("funding_rate"))
            row["funding_interval_hours"] = 8.0  # HTX 8h 结算
            row["next_funding_time"] = data.get("next_funding_time")
            row["funding_cap"] = _fmt_pct(data.get("max_funding_rate"))
            row["funding_floor"] = _fmt_pct(data.get("min_funding_rate"))
            prem = _fmt_pct(data.get("estimated_rate"))
            if prem is not None and row["funding_rate"] is None:
                row["funding_rate"] = prem
    except Exception:
        pass
    try:
        if oi and oi.get("data"):
            latest = oi["data"][0]
            row["open_interest"] = _fmt_float(latest.get("value") or latest.get("volume"))
    except Exception:
        pass
    try:
        if ticker and ticker.get("tick"):
            t = ticker["tick"]
            row["mark_price"] = _fmt_float(t.get("close"))
            row["volume_quote"] = _fmt_float(t.get("trade_turnover"))
    except Exception:
        pass
    try:
        if history and history.get("data", {}).get("data"):
            lst = history["data"]["data"]
            row["funding_history"] = [
                {"time": item.get("funding_time"), "rate": _fmt_pct(item.get("funding_rate"))}
                for item in lst
                if item.get("funding_rate") is not None
            ]
    except Exception:
        pass
    derived_interval = _derive_interval_hours(row.get("funding_history", []))
    if derived_interval:
        row["funding_interval_hours"] = derived_interval
    return row


FETCHERS = {
    "binance": fetch_binance,
    "bybit": fetch_bybit,
    "gate": fetch_gate,
    "bitget": fetch_bitget,
    "lighter": fetch_lighter,
    "grvt": fetch_grvt,
    "okx": fetch_okx,
    "htx": fetch_htx,
}


_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_TTL = 30  # seconds
_FETCH_TIMEOUT = 6.0  # 单个交易所拉取的超时时间，避免阻塞主请求
_EXECUTOR = ThreadPoolExecutor(max_workers=12)


def fetch_exchange_details(symbols: List[str]):
    """
    返回 {symbol: [rows]}，每行包含资金费、周期、指数差、OI、24h 额等公开接口数据。
    """
    result = {}
    now = time.time()
    for sym in symbols:
        cache_key = sym.upper()
        cached = _CACHE.get(cache_key)
        if cached and now - cached["ts"] < _CACHE_TTL:
            result[sym] = cached["rows"]
            continue
        rows = []
        futures = {}
        for ex, fn in FETCHERS.items():
            try:
                futures[ex] = _EXECUTOR.submit(fn, sym)
            except Exception:
                continue
        for ex, fut in futures.items():
            try:
                res = fut.result(timeout=_FETCH_TIMEOUT)
                if res:
                    rows.append(res)
            except Exception:
                continue
        # 如果本次全部失败，回退使用旧缓存，避免前端空数据
        if not rows and cached:
            rows = cached["rows"]
        result[sym] = rows
        _CACHE[cache_key] = {"ts": time.time(), "rows": rows}
    return result
