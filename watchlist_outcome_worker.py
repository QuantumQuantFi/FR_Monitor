from __future__ import annotations

"""
Outcome 计算 worker（单次执行）
 - 周期性执行（建议 cron/循环，每 5~10 分钟一次）。
 - 扫描 watch_signal_event，针对未生成 outcome 的 horizon 计算 PnL。
 - 数据源优先：SQLite price_data_1min（现有行情聚合）。
 - 轻量设计：每次最多处理 MAX_TASKS 条 event-horizon，避免占用过多资源。
"""

import argparse
import logging
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import psycopg
import requests

import config

HORIZONS_MIN = [15, 30, 60, 240, 480, 1440, 2880, 5760]  # 15m,30m,1h,4h,8h,24h,48h,96h
MAX_TASKS = 1000  # 每次运行最多处理的 event-horizon 组合，防止扫全表
MAX_REST_CALLS = 50  # 每轮允许的 REST 调用上限，防止过载


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Snapshot:
    ts: datetime
    spot: Optional[float]
    perp: Optional[float]
    funding_rate: Optional[float]
    funding_interval_hours: Optional[float]
    next_funding_time: Optional[datetime]
    leg_a: Optional[Dict[str, Any]] = None
    leg_b: Optional[Dict[str, Any]] = None


def load_event_tasks(conn_pg) -> List[Dict[str, Any]]:
    """
    返回需要计算 outcome 的事件，包含双腿信息。
    规则：事件 start_ts 已超过最小 horizon，且至少有一个“已到期”的 horizon 尚未写入 outcome。
    """
    min_horizon = min(HORIZONS_MIN)
    sql = """
    WITH need AS (
        SELECT e.id, e.exchange, e.symbol, e.start_ts,
               e.leg_a_exchange, e.leg_a_symbol, e.leg_a_kind, e.leg_a_price_first, e.leg_a_price_last, e.leg_a_funding_rate_first, e.leg_a_funding_rate_last,
               e.leg_b_exchange, e.leg_b_symbol, e.leg_b_kind, e.leg_b_price_first, e.leg_b_price_last, e.leg_b_funding_rate_first, e.leg_b_funding_rate_last
        FROM watchlist.watch_signal_event e
        WHERE e.start_ts <= now() - make_interval(mins := %s)
          AND e.leg_a_exchange IS NOT NULL
          AND e.leg_b_exchange IS NOT NULL
    )
    SELECT n.*
    FROM need n
    WHERE EXISTS (
        SELECT 1 FROM unnest(%s::int[]) h
        WHERE n.start_ts + make_interval(mins := h) <= now()
          AND NOT EXISTS (
              SELECT 1 FROM watchlist.future_outcome o
               WHERE o.event_id = n.id AND o.horizon_min = h
          )
    )
    ORDER BY n.start_ts
    LIMIT %s;
    """
    rows = conn_pg.execute(sql, (min_horizon, HORIZONS_MIN, MAX_TASKS)).fetchall()
    out = []
    cols = [desc.name for desc in conn_pg.description] if hasattr(conn_pg, "description") else None
    for r in rows:
        if cols:
            out.append(dict(zip(cols, r)))
        else:
            out.append(
                {
                    "id": r[0],
                    "exchange": r[1],
                    "symbol": r[2],
                    "start_ts": r[3],
                    "leg_a_exchange": r[4],
                    "leg_a_symbol": r[5],
                    "leg_a_kind": r[6],
                    "leg_a_price_first": r[7],
                    "leg_a_price_last": r[8],
                    "leg_a_funding_rate_first": r[9],
                    "leg_a_funding_rate_last": r[10],
                    "leg_b_exchange": r[11],
                    "leg_b_symbol": r[12],
                    "leg_b_kind": r[13],
                    "leg_b_price_first": r[14],
                    "leg_b_price_last": r[15],
                    "leg_b_funding_rate_first": r[16],
                    "leg_b_funding_rate_last": r[17],
                }
            )
    return out


def _parse_ts(ts_str: Optional[str]) -> Optional[datetime]:
    if not ts_str:
        return None
    try:
        ts = datetime.fromisoformat(ts_str)
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _sqlite_ts(dt: datetime) -> str:
    """
    格式化为 SQLite 文本时间戳（与 price_data_1min 中的存储一致，形如 '2025-12-09 10:37:00'）。
    - 统一使用 UTC，无时区后缀，避免字符串比较因 'T'/'+' 混入导致过滤失效。
    """
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def load_snapshot(sqlite_conn: sqlite3.Connection, exchange: str, symbol: str, target_ts: datetime) -> Optional[Snapshot]:
    """
    取目标时间点之前最近的一条 1min K（聚合数据）。
    """
    cursor = sqlite_conn.execute(
        """
        SELECT timestamp, spot_price_close, futures_price_close, funding_rate_avg, funding_interval_hours, next_funding_time
        FROM price_data_1min
        WHERE exchange = ? AND symbol = ? AND timestamp <= ?
        ORDER BY timestamp DESC
        LIMIT 1;
        """,
        (exchange, symbol, _sqlite_ts(target_ts)),
    )
    row = cursor.fetchone()
    if not row:
        return None
    ts = _parse_ts(row[0])
    if ts is None:
        return None
    return Snapshot(
        ts=ts,
        spot=row[1],
        perp=row[2],
        funding_rate=row[3],
        funding_interval_hours=row[4],
        next_funding_time=_parse_ts(row[5]),
        leg_a=None,
        leg_b=None,
    )


def load_spread_series(sqlite_conn: sqlite3.Connection, exchange: str, symbol: str, start: datetime, end: datetime) -> List[Tuple[datetime, float]]:
    cursor = sqlite_conn.execute(
        """
        SELECT timestamp, spot_price_close, futures_price_close
        FROM price_data_1min
        WHERE exchange = ? AND symbol = ? AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC;
        """,
        (exchange, symbol, _sqlite_ts(start), _sqlite_ts(end)),
    )
    out = []
    for ts_str, spot, perp in cursor.fetchall():
        if spot and perp:
            ts = datetime.fromisoformat(ts_str)
            ts = ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
            if spot != 0:
                out.append((ts, (spot - perp) / spot))
    return out


def load_leg_price_series(
    sqlite_conn: sqlite3.Connection, exchange: str, symbol: str, kind: str, start: datetime, end: datetime
) -> Dict[datetime, float]:
    """
    返回 ts -> price 的映射，按 kind 选择 spot 或 perp 收盘价。
    """
    cursor = sqlite_conn.execute(
        """
        SELECT timestamp, spot_price_close, futures_price_close
        FROM price_data_1min
        WHERE exchange = ? AND symbol = ? AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC;
        """,
        (exchange, symbol, _sqlite_ts(start), _sqlite_ts(end)),
    )
    out: Dict[datetime, float] = {}
    for ts_str, spot, perp in cursor.fetchall():
        ts = _parse_ts(ts_str)
        if ts is None:
            continue
        price = spot if kind == "spot" else perp
        if price:
            out[ts] = price
    return out


def load_funding_series(
    sqlite_conn: sqlite3.Connection, exchange: str, symbol: str, start: datetime, end: datetime
) -> List[Tuple[datetime, Optional[float], Optional[float], Optional[datetime]]]:
    """
    返回 (ts, funding_rate_avg, funding_interval_hours, next_funding_time)
    """
    cursor = sqlite_conn.execute(
        """
        SELECT timestamp, funding_rate_avg, funding_interval_hours, next_funding_time
        FROM price_data_1min
        WHERE exchange = ? AND symbol = ? AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC;
        """,
        (exchange, symbol, _sqlite_ts(start), _sqlite_ts(end)),
    )
    out = []
    for ts_str, fr, interval_h, nft in cursor.fetchall():
        ts = _parse_ts(ts_str)
        if ts is None:
            continue
        out.append((ts, fr, interval_h, _parse_ts(nft)))
    return out


class FundingHistoryFetcher:
    """
    轻量 REST funding 历史获取。实现 Binance / OKX / Bybit / Bitget，带缓存与频控。
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "watchlist-outcome-worker/1.0"})
        self._cache: Dict[Tuple[str, str, int, int], List[Tuple[datetime, float]]] = {}
        self._calls = 0

    def fetch(self, exchange: str, symbol: str, start_ts: datetime, end_ts: datetime) -> List[Tuple[datetime, float]]:
        if self._calls >= MAX_REST_CALLS:
            return []
        key = (exchange, symbol, int(start_ts.timestamp()), int(end_ts.timestamp()))
        if key in self._cache:
            return self._cache[key]
        out: List[Tuple[datetime, float]] = []
        try:
            if exchange.lower() == "binance":
                out = self._fetch_binance(symbol, start_ts, end_ts)
            elif exchange.lower() == "okx":
                out = self._fetch_okx(symbol, start_ts, end_ts)
            elif exchange.lower() == "bybit":
                out = self._fetch_bybit(symbol, start_ts, end_ts)
            elif exchange.lower() == "bitget":
                out = self._fetch_bitget(symbol, start_ts, end_ts)
            # 其他交易所可按需扩展
        except Exception:
            out = []
        self._cache[key] = out
        if out:
            self._calls += 1
        return out

    def _fetch_binance(self, symbol: str, start_ts: datetime, end_ts: datetime) -> List[Tuple[datetime, float]]:
        # Binance USDT 合约符号通常为 <base>USDT
        sym = symbol.upper()
        if not sym.endswith("USDT"):
            sym = f"{sym}USDT"
        out: List[Tuple[datetime, float]] = []
        cursor_start = int(start_ts.timestamp() * 1000)
        # NOTE: Binance has occasional WAF/403 when using very large limits or startTime+endTime together.
        # Use startTime-only pagination with modest limit and stop when reaching end_ts.
        while self._calls < MAX_REST_CALLS:
            params = {"symbol": sym, "startTime": cursor_start, "limit": 200}
            resp = self.session.get("https://fapi.binance.com/fapi/v1/fundingRate", params=params, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break
            chunk: List[Tuple[datetime, float]] = []
            last_ts_ms = None
            reached_end = False
            for item in data:
                try:
                    ts_ms = int(item["fundingTime"])
                    ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    if ts > end_ts:
                        reached_end = True
                        break
                    if ts < start_ts or ts > end_ts:
                        continue
                    rate = float(item["fundingRate"])
                    chunk.append((ts, rate))
                    last_ts_ms = ts_ms
                except Exception:
                    continue
            out.extend(chunk)
            if reached_end or last_ts_ms is None or len(data) < 200:
                break
            cursor_start = last_ts_ms + 1
            self._calls += 1
        return out

    def _fetch_okx(self, symbol: str, start_ts: datetime, end_ts: datetime) -> List[Tuple[datetime, float]]:
        inst_id = f"{symbol.upper()}-USDT-SWAP"
        out: List[Tuple[datetime, float]] = []

        def _call(before: Optional[int], after: Optional[int]) -> List[Tuple[datetime, float]]:
            params = {
                "instId": inst_id,
                "limit": 100,
            }
            if before:
                params["before"] = before
            if after:
                params["after"] = after
            resp = self.session.get("https://www.okx.com/api/v5/public/funding-rate-history", params=params, timeout=5)
            resp.raise_for_status()
            data = resp.json().get("data") or []
            out_local: List[Tuple[datetime, float]] = []
            for item in data:
                try:
                    ts = datetime.fromtimestamp(int(item.get("fundingTime")) / 1000, tz=timezone.utc)
                    rate_raw = item.get("realizedRate") or item.get("fundingRate")
                    rate = float(rate_raw)
                    if start_ts <= ts <= end_ts:
                        out_local.append((ts, rate))
                except Exception:
                    continue
            return out_local

        # 一次带 before/after 查询
        before = int(end_ts.timestamp() * 1000)
        after = int(start_ts.timestamp() * 1000)
        # OKX 接口返回倒序，以 after 作为游标翻页
        while before > after and self._calls < MAX_REST_CALLS:
            chunk = _call(before, after)
            out.extend(chunk)
            self._calls += 1
            if not chunk:
                break
            min_ts = min(ts for ts, _ in chunk)
            before = int((min_ts - timedelta(milliseconds=1)).timestamp() * 1000)
            if len(chunk) < 100:
                break
        # 若仍为空，再做一次不带窗口获取最新一页
        if not out:
            out.extend(_call(None, None))
        return out

    def _fetch_bybit(self, symbol: str, start_ts: datetime, end_ts: datetime) -> List[Tuple[datetime, float]]:
        sym = f"{symbol.upper()}USDT"
        out: List[Tuple[datetime, float]] = []
        cursor = None
        calls = 0
        while calls < 5 and self._calls < MAX_REST_CALLS:
            params = {
                "category": "linear",
                "symbol": sym,
                "start": int(start_ts.timestamp() * 1000),
                "end": int(end_ts.timestamp() * 1000),
                "limit": 200,
            }
            if cursor:
                params["cursor"] = cursor
            resp = self.session.get("https://api.bybit.com/v5/market/funding/history", params=params, timeout=5)
            resp.raise_for_status()
            result = resp.json().get("result", {}) or {}
            list_data = result.get("list") or []
            for item in list_data:
                try:
                    ts = datetime.fromtimestamp(int(item["fundingRateTimestamp"]) / 1000, tz=timezone.utc)
                    rate = float(item["fundingRate"])
                    if start_ts <= ts <= end_ts:
                        out.append((ts, rate))
                except Exception:
                    continue
            cursor = result.get("nextPageCursor")
            calls += 1
            self._calls += 1
            if not cursor or not list_data:
                break
        return out

    def _fetch_bitget(self, symbol: str, start_ts: datetime, end_ts: datetime) -> List[Tuple[datetime, float]]:
        inst_id = f"{symbol.upper()}USDT"
        def _query(params: Dict[str, object]) -> List[Tuple[datetime, float]]:
            resp = self.session.get("https://api.bitget.com/api/v2/mix/market/history-fund-rate", params=params, timeout=5)
            resp.raise_for_status()
            data = resp.json().get("data") or []
            out_local: List[Tuple[datetime, float]] = []
            for item in data:
                try:
                    ts_raw = item.get("fundingTime") or item.get("timestamp")
                    ts = datetime.fromtimestamp(int(ts_raw) / 1000, tz=timezone.utc)
                    rate = float(item.get("fundingRate"))
                    if start_ts <= ts <= end_ts:
                        out_local.append((ts, rate))
                except Exception:
                    continue
            return out_local

        params = {
            "symbol": inst_id,
            "productType": "umcbl",  # USDT 本位永续
            "startTime": int(start_ts.timestamp() * 1000),
            "endTime": int(end_ts.timestamp() * 1000),
            "limit": 1000,
        }
        out: List[Tuple[datetime, float]] = []
        try:
            out.extend(_query(params))
        except Exception:
            pass
        # 若窗口或 instId 查询无结果，再尝试不带时间/不带 _UMCBL 的符号
        if not out:
            try:
                out.extend(_query({"symbol": inst_id, "productType": "umcbl", "limit": 200}))
            except Exception:
                pass
        if not out:
            plain_sym = f"{symbol.upper()}USDT"
            try:
                out.extend(_query({"symbol": plain_sym, "productType": "umcbl", "limit": 200}))
            except Exception:
                pass
        # 仍为空，尝试旧版 v1 接口兜底并分页
        if not out:
            page_no = 1
            while page_no <= 5 and self._calls < MAX_REST_CALLS:
                try:
                    params_v1 = {"symbol": inst_id, "pageSize": 100, "pageNo": page_no}
                    resp3 = self.session.get("https://api.bitget.com/api/mix/v1/market/history-fundRate", params=params_v1, timeout=5)
                    resp3.raise_for_status()
                    data3 = resp3.json().get("data") or []
                    if not data3:
                        break
                    for item in data3:
                        try:
                            ts = datetime.fromtimestamp(int(item["fundingTime"]) / 1000, tz=timezone.utc)
                            rate = float(item["fundingRate"])
                            if start_ts <= ts <= end_ts:
                                out.append((ts, rate))
                        except Exception:
                            continue
                    page_no += 1
                    self._calls += 1
                except Exception:
                    break
        return out


def _build_funding_schedule(
    series: List[Tuple[datetime, Optional[float], Optional[float], Optional[datetime]]],
    start_ts: datetime,
    end_ts: datetime,
) -> Tuple[Optional[float], List[Tuple[datetime, float]]]:
    """
    根据 funding 序列（含 next_funding_time/interval）推导结算时间点，并按结算点前最近的资金费率累加（离散，不做均值）。
    结算点来源：
      - 优先使用序列中的 next_funding_time（每条 1min 数据携带的“下次资金费时间”）。
      - 在一个结算点之后，用“结算点之后第一条非空 next_funding_time”来确定下一个结算点；若缺失，则用最近的 interval_h 推算。
    返回 (funding_change, used_points)，funding_change 为 sum(rate)（假设 rate 已是该次结算应计比例）。
    """
    if not series:
        return None, []
    # 序列按时间升序
    series_sorted = sorted(series, key=lambda x: x[0])
    used_points: List[Tuple[datetime, float]] = []

    def _find_next_ft(after_ts: datetime, fallback_interval: float) -> Optional[datetime]:
        for ts, _, _, nft in series_sorted:
            if nft and nft > after_ts:
                return nft
        # fallback: 用最近的 interval 推算
        return after_ts + timedelta(hours=fallback_interval)

    # 默认 interval（用最后一个非空）
    interval_h = None
    for _, _, ih, _ in reversed(series_sorted):
        if ih and ih > 0:
            interval_h = ih
            break
    interval_h = interval_h or 8.0

    # 首个结算点：选第一个在 start_ts 之后的 next_funding_time，若无则 start+interval
    next_ft = None
    for _, _, _, nft in series_sorted:
        if nft and nft >= start_ts:
            next_ft = nft
            break
    if next_ft is None:
        next_ft = start_ts + timedelta(hours=interval_h)

    while next_ft <= end_ts + timedelta(seconds=1):
        # 结算点前最近的资金费率
        rate = None
        for ts, fr, _, _ in reversed(series_sorted):
            if ts <= next_ft and fr is not None:
                rate = fr
                break
        if rate is not None:
            used_points.append((next_ft, rate))
        # 找下一个结算点：结算点之后第一条非空 nft，缺失则 interval 推算
        nxt = _find_next_ft(next_ft, interval_h)
        if not nxt:
            break
        next_ft = nxt

    if not used_points:
        return None, []

    # 每个结算点直接累加 rate（假设 rate 为当期应收/付比例，不再按 interval 折算）
    funding_change = sum(rate for _, rate in used_points)
    return funding_change, used_points


def _leg_from_event(row: Dict[str, Any], prefix: str) -> Optional[Dict[str, Any]]:
    ex = row.get(f"{prefix}_exchange")
    if not ex:
        return None
    return {
        "exchange": ex,
        "symbol": row.get(f"{prefix}_symbol"),
        "kind": row.get(f"{prefix}_kind"),
        "price_first": row.get(f"{prefix}_price_first"),
        "price_last": row.get(f"{prefix}_price_last"),
        "funding_first": row.get(f"{prefix}_funding_rate_first"),
        "funding_last": row.get(f"{prefix}_funding_rate_last"),
    }


def _funding_stats(series: List[Tuple[datetime, float]]) -> Dict[str, Any]:
    if not series:
        return {}
    vals = [v for _, v in series]
    mean = sum(vals) / len(vals)
    var = sum((v - mean) ** 2 for v in vals) / len(vals) if vals else 0
    std = var ** 0.5
    last_ts, last_val = series[-1]
    momentum = vals[-1] - vals[0] if len(vals) >= 2 else None
    return {
        "n": len(vals),
        "mean": mean,
        "std": std,
        "min": min(vals),
        "max": max(vals),
        "last_ts": last_ts.isoformat(),
        "last": last_val,
        "momentum": momentum,
    }


def compute_outcome(
    sqlite_conn: sqlite3.Connection,
    event_row: Dict[str, Any],
    horizon_min: int,
    rest_fetcher: Optional[FundingHistoryFetcher] = None,
) -> Optional[Dict[str, object]]:
    leg_a = _leg_from_event(event_row, "leg_a")
    leg_b = _leg_from_event(event_row, "leg_b")
    if not leg_a or not leg_b:
        return None
    exchange = event_row.get("exchange")
    symbol = event_row.get("symbol")
    start_ts: datetime = event_row.get("start_ts")
    end_ts = start_ts + timedelta(minutes=horizon_min)
    # 起终点价：按腿类型取 spot/perp
    snap_a_start = load_snapshot(sqlite_conn, leg_a["exchange"], leg_a["symbol"], start_ts)
    snap_b_start = load_snapshot(sqlite_conn, leg_b["exchange"], leg_b["symbol"], start_ts)
    snap_a_end = load_snapshot(sqlite_conn, leg_a["exchange"], leg_a["symbol"], end_ts)
    snap_b_end = load_snapshot(sqlite_conn, leg_b["exchange"], leg_b["symbol"], end_ts)

    def leg_price(kind: str, snap: Optional[Snapshot]) -> Optional[float]:
        if not snap:
            return None
        if kind == "spot":
            return snap.spot
        return snap.perp

    a_start_price = leg_price(leg_a.get("kind"), snap_a_start)
    b_start_price = leg_price(leg_b.get("kind"), snap_b_start)
    a_end_price = leg_price(leg_a.get("kind"), snap_a_end)
    b_end_price = leg_price(leg_b.get("kind"), snap_b_end)
    if None in (a_start_price, b_start_price, a_end_price, b_end_price):
        return None

    def rel_spread(a_price: float, b_price: float) -> float:
        return (a_price - b_price) / a_price if a_price else 0.0

    spread_start = rel_spread(a_start_price, b_start_price)
    spread_end = rel_spread(a_end_price, b_end_price)

    # 构造 spread 时间序列（对齐两腿时间）
    series_a = load_leg_price_series(sqlite_conn, leg_a["exchange"], leg_a["symbol"], leg_a["kind"], start_ts, end_ts)
    series_b = load_leg_price_series(sqlite_conn, leg_b["exchange"], leg_b["symbol"], leg_b["kind"], start_ts, end_ts)
    spread_series: List[Tuple[datetime, float]] = []
    for ts, pa in series_a.items():
        pb = series_b.get(ts)
        if pb:
            spread_series.append((ts, rel_spread(pa, pb)))
    spread_series.sort(key=lambda x: x[0])

    max_dd = None
    vol = None
    if spread_series:
        peak = spread_series[0][1]
        dd = 0.0
        for _, v in spread_series:
            peak = max(peak, v)
            dd = min(dd, v - peak)
        max_dd = dd
        vals = [v for _, v in spread_series]
        if len(vals) > 1:
            mean = sum(vals) / len(vals)
            var = sum((v - mean) ** 2 for v in vals) / (len(vals) - 1)
            vol = math.sqrt(var)

    funding_change_total = 0.0
    funding_applied: Dict[str, Any] = {}
    funding_hist: Dict[str, Any] = {}
    legs_info = [("a", leg_a), ("b", leg_b)]
    for label, leg in legs_info:
        if leg.get("kind") != "perp":
            continue
        fc = None
        used_points: List[Tuple[datetime, float]] = []
        if rest_fetcher:
            rest_points = rest_fetcher.fetch(leg["exchange"], leg["symbol"], start_ts, end_ts)
            if rest_points:
                fc = sum(rate for _, rate in rest_points)
                used_points = rest_points
        if fc is None:
            funding_series = load_funding_series(sqlite_conn, leg["exchange"], leg["symbol"], start_ts, end_ts)
            fc, used_points = _build_funding_schedule(funding_series, start_ts, end_ts)
        if fc is not None:
            funding_change_total += fc
        funding_applied[label] = {
            "exchange": leg["exchange"],
            "symbol": leg["symbol"],
            "used_points": [(ts.isoformat(), rate) for ts, rate in used_points],
            "source": "rest" if rest_fetcher and used_points else "local",
        }
        if used_points:
            # 仅保留近 50 条，计算基础统计 + 动量，便于事件层持久化
            tail = used_points[-50:]
            funding_hist[label] = {
                "stats": _funding_stats(tail),
                "tail": [(ts.isoformat(), rate) for ts, rate in tail],
            }

    pnl_spread = spread_end - spread_start
    pnl_funding = funding_change_total
    pnl_total = pnl_spread + pnl_funding

    return {
        "horizon_min": horizon_min,
        "spread_change": pnl_spread,
        "funding_change": funding_change_total if funding_applied else None,
        "pnl": pnl_total,
        "max_drawdown": max_dd,
        "volatility": vol,
        "funding_applied": funding_applied or None,
        "funding_hist": funding_hist or None,
    }


def upsert_outcome(conn_pg, event_id: int, outcome: Dict[str, object]) -> None:
    try:
        from psycopg.types.json import Json  # type: ignore
    except Exception:  # pragma: no cover
        Json = None
    params = {**outcome, "event_id": event_id}
    if Json and outcome.get("funding_applied") is not None:
        params["funding_applied"] = Json(outcome["funding_applied"])
    conn_pg.execute(
        """
        INSERT INTO watchlist.future_outcome
          (event_id, horizon_min, pnl, spread_change, funding_change, max_drawdown, volatility, funding_applied)
        VALUES (%(event_id)s, %(horizon_min)s, %(pnl)s, %(spread_change)s, %(funding_change)s, %(max_drawdown)s, %(volatility)s, %(funding_applied)s)
        ON CONFLICT (event_id, horizon_min)
        DO UPDATE SET
          pnl = EXCLUDED.pnl,
          spread_change = EXCLUDED.spread_change,
          funding_change = EXCLUDED.funding_change,
          max_drawdown = EXCLUDED.max_drawdown,
          volatility = EXCLUDED.volatility,
          funding_applied = EXCLUDED.funding_applied;
        """,
        params,
    )


def update_event_funding_hist(conn_pg, event_id: int, funding_hist: Optional[Dict[str, Any]]) -> None:
    """将 funding_hist 写回事件 features_agg，便于回测/特征使用。"""
    if not funding_hist:
        return
    try:
        from psycopg.types.json import Json  # type: ignore
    except Exception:  # pragma: no cover
        Json = None
    payload = funding_hist
    if Json:
        payload = Json(funding_hist)
    conn_pg.execute(
        """
        UPDATE watchlist.watch_signal_event
           SET features_agg = COALESCE(features_agg, '{}'::jsonb) || jsonb_build_object('funding_hist', %s)
         WHERE id = %s;
        """,
        (payload, event_id),
    )


def ensure_unique_index(conn_pg) -> None:
    conn_pg.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes WHERE schemaname = 'watchlist' AND indexname = 'future_outcome_event_horizon_idx'
            ) THEN
                CREATE UNIQUE INDEX future_outcome_event_horizon_idx
                    ON watchlist.future_outcome(event_id, horizon_min);
            END IF;
        END$$;
        """
    )


def main(loop_once: bool = True) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("outcome_worker")
    pg_dsn = config.WATCHLIST_PG_CONFIG["dsn"]
    sqlite_path = config.WATCHLIST_CONFIG.get("db_path", "market_data.db")
    fetcher = FundingHistoryFetcher()
    with sqlite3.connect(sqlite_path) as sqlite_conn, psycopg.connect(pg_dsn, autocommit=True) as pg_conn:
        ensure_unique_index(pg_conn)
        tasks = load_event_tasks(pg_conn)
        if not tasks:
            logger.info("no pending events")
            return
        now_ts = _utcnow()
        processed = 0
        for row in tasks:
            missing = [
                h
                for h in HORIZONS_MIN
                if row["start_ts"] + timedelta(minutes=h) <= now_ts
                and not pg_conn.execute(
                    "SELECT 1 FROM watchlist.future_outcome WHERE event_id=%s AND horizon_min=%s",
                    (row["id"], h),
                ).fetchone()
            ]
            for h in missing:
                out = compute_outcome(sqlite_conn, row, h, rest_fetcher=fetcher)
                if not out:
                    continue
                upsert_outcome(pg_conn, row["id"], out)
                update_event_funding_hist(pg_conn, row["id"], out.get("funding_hist"))
                processed += 1
            if processed >= MAX_TASKS:
                break
        logger.info("processed outcomes: %s", processed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute future outcomes for watchlist events")
    parser.add_argument("--loop-once", action="store_true", help="run once and exit (default)")
    args = parser.parse_args()
    main(loop_once=args.loop_once)
