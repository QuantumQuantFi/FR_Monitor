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
MAX_REST_CALLS = 500  # 每轮允许的 REST 调用上限，防止过载


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
        SELECT e.id, e.exchange, e.symbol, e.signal_type, e.start_ts,
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
                    "signal_type": r[3],
                    "start_ts": r[4],
                    "leg_a_exchange": r[5],
                    "leg_a_symbol": r[6],
                    "leg_a_kind": r[7],
                    "leg_a_price_first": r[8],
                    "leg_a_price_last": r[9],
                    "leg_a_funding_rate_first": r[10],
                    "leg_a_funding_rate_last": r[11],
                    "leg_b_exchange": r[12],
                    "leg_b_symbol": r[13],
                    "leg_b_kind": r[14],
                    "leg_b_price_first": r[15],
                    "leg_b_price_last": r[16],
                    "leg_b_funding_rate_first": r[17],
                    "leg_b_funding_rate_last": r[18],
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
        # Cache by day bucket to reuse results across many events in the same day.
        # key=(exchange, symbol, day_bucket_start_ts)
        self._cache_day: Dict[Tuple[str, str, int], List[Tuple[datetime, float]]] = {}
        self._calls = 0

    def fetch(self, exchange: str, symbol: str, start_ts: datetime, end_ts: datetime) -> List[Tuple[datetime, float]]:
        if self._calls >= MAX_REST_CALLS:
            return []
        ex = exchange.lower()
        start_ts = start_ts.astimezone(timezone.utc)
        end_ts = end_ts.astimezone(timezone.utc)

        def _day_floor(dt: datetime) -> datetime:
            return dt.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        out: List[Tuple[datetime, float]] = []
        day = _day_floor(start_ts)
        end_day = _day_floor(end_ts)
        while day <= end_day and self._calls < MAX_REST_CALLS:
            key = (ex, symbol, int(day.timestamp()))
            if key not in self._cache_day:
                day_start = day
                day_end = day + timedelta(days=1)
                # Widen the query start slightly so we don't miss boundary points, then filter back to the day range.
                query_start = day_start - timedelta(seconds=1)
                query_end = day_end
                points: List[Tuple[datetime, float]] = []
                try:
                    if ex == "binance":
                        points = self._fetch_binance(symbol, query_start, query_end)
                    elif ex == "okx":
                        points = self._fetch_okx(symbol, query_start, query_end)
                    elif ex == "bybit":
                        points = self._fetch_bybit(symbol, query_start, query_end)
                    elif ex == "bitget":
                        points = self._fetch_bitget(symbol, query_start, query_end)
                except Exception:
                    points = []
                # Keep only points within [day_start, day_end)
                self._cache_day[key] = [(ts, rate) for ts, rate in points if day_start <= ts < day_end]

            for ts, rate in self._cache_day.get(key) or []:
                if start_ts < ts <= end_ts:
                    out.append((ts, rate))
            day = day + timedelta(days=1)

        out.sort(key=lambda x: x[0])
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
                    # Only include points in (start_ts, end_ts]
                    if ts <= start_ts or ts > end_ts:
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
                    if start_ts < ts <= end_ts:
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
                    if start_ts < ts <= end_ts:
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
                    if start_ts < ts <= end_ts:
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
                            if start_ts < ts <= end_ts:
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

    # 首个结算点：选第一个在 start_ts 之后的 next_funding_time（严格大于），若无则 start+interval
    next_ft = None
    for _, _, _, nft in series_sorted:
        if nft and nft > start_ts:
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
    signal_type = str(event_row.get("signal_type") or "").strip()
    start_ts: datetime = event_row.get("start_ts")
    end_ts = start_ts + timedelta(minutes=horizon_min)

    def leg_price(kind: str, snap: Optional[Snapshot]) -> Optional[float]:
        if not snap:
            return None
        if kind == "spot":
            return snap.spot
        return snap.perp

    # 起终点价：按腿类型取 spot/perp（用于确定 long/short 方向与 label）
    snap_a_start = load_snapshot(sqlite_conn, leg_a["exchange"], leg_a["symbol"], start_ts)
    snap_b_start = load_snapshot(sqlite_conn, leg_b["exchange"], leg_b["symbol"], start_ts)
    snap_a_end = load_snapshot(sqlite_conn, leg_a["exchange"], leg_a["symbol"], end_ts)
    snap_b_end = load_snapshot(sqlite_conn, leg_b["exchange"], leg_b["symbol"], end_ts)

    a_start_price = leg_price(leg_a.get("kind"), snap_a_start)
    b_start_price = leg_price(leg_b.get("kind"), snap_b_start)
    a_end_price = leg_price(leg_a.get("kind"), snap_a_end)
    b_end_price = leg_price(leg_b.get("kind"), snap_b_end)
    if None in (a_start_price, b_start_price, a_end_price, b_end_price):
        return None

    # Canonical trade definition:
    # - Always produce a deterministic "long leg" and "short leg".
    # - Spread PnL uses log ratio: pnl_spread = log(short/long)_start - log(short/long)_end.
    # - Funding PnL applies position sign: long pays +rate, short receives +rate.
    legs = {
        "a": {
            "label": "a",
            "exchange": leg_a["exchange"],
            "symbol": leg_a["symbol"],
            "kind": leg_a["kind"],
            "funding_rate_first": leg_a.get("funding_first"),
            "price_start": float(a_start_price),
            "price_end": float(a_end_price),
        },
        "b": {
            "label": "b",
            "exchange": leg_b["exchange"],
            "symbol": leg_b["symbol"],
            "kind": leg_b["kind"],
            "funding_rate_first": leg_b.get("funding_first"),
            "price_start": float(b_start_price),
            "price_end": float(b_end_price),
        },
    }

    def _log_ratio(short_p: float, long_p: float) -> float:
        return math.log(short_p / long_p)

    def _choose_long_short() -> Tuple[Dict[str, Any], Dict[str, Any], str, Dict[str, Any]]:
        a = legs["a"]
        b = legs["b"]
        meta: Dict[str, Any] = {}
        position_mode = "price"
        if signal_type == "C":
            # spot vs perp，Type C 语义：现货低于永续 -> long spot, short perp
            if a["kind"] == "spot" and b["kind"] == "perp":
                return a, b, "type_c_spot_long_perp_short", meta
            if b["kind"] == "spot" and a["kind"] == "perp":
                return b, a, "type_c_spot_long_perp_short", meta
        if signal_type == "A":
            # Binance spot/perp：优先按“收资金费”方向定义持仓（为资金费累计回测服务）。
            # funding > 0: longs pay shorts -> short perp long spot
            # funding < 0: shorts pay longs -> long perp short spot（需要可做空现货/借币；仅用于回测口径）
            spot = a if a["kind"] == "spot" else (b if b["kind"] == "spot" else None)
            perp = a if a["kind"] == "perp" else (b if b["kind"] == "perp" else None)
            fr = None
            if perp:
                try:
                    fr = float(perp.get("funding_rate_first")) if perp.get("funding_rate_first") is not None else None
                except Exception:
                    fr = None
            if spot and perp and fr is not None:
                position_mode = "receive_funding"
                if fr >= 0:
                    return spot, perp, "type_a_receive_funding_short_perp_long_spot", meta
                meta["assumption_spot_short_allowed"] = True
                return perp, spot, "type_a_receive_funding_long_perp_short_spot", meta
            # fallback: treat like Type C (long spot short perp if available)
            if spot and perp:
                return spot, perp, "type_a_default_spot_long_perp_short", meta
        if signal_type == "B":
            # perp-perp：short 高价腿，long 低价腿（价差收敛为正收益）
            if a["price_start"] >= b["price_start"]:
                return b, a, "type_b_long_low_short_high", meta
            return a, b, "type_b_long_low_short_high", meta

        # Generic fallback: long low, short high
        if a["price_start"] >= b["price_start"]:
            return b, a, f"fallback_long_low_short_high_{position_mode}", meta
        return a, b, f"fallback_long_low_short_high_{position_mode}", meta

    long_leg, short_leg, position_rule, position_meta = _choose_long_short()

    spread_metric_start = _log_ratio(short_leg["price_start"], long_leg["price_start"])
    spread_metric_end = _log_ratio(short_leg["price_end"], long_leg["price_end"])
    pnl_spread = spread_metric_start - spread_metric_end

    # Spread PnL path for risk metrics (align timestamps across legs)
    long_series = load_leg_price_series(
        sqlite_conn, long_leg["exchange"], long_leg["symbol"], long_leg["kind"], start_ts, end_ts
    )
    short_series = load_leg_price_series(
        sqlite_conn, short_leg["exchange"], short_leg["symbol"], short_leg["kind"], start_ts, end_ts
    )
    pnl_series: List[Tuple[datetime, float]] = []
    for ts, short_p in short_series.items():
        long_p = long_series.get(ts)
        if long_p and short_p and long_p > 0 and short_p > 0:
            metric_t = _log_ratio(float(short_p), float(long_p))
            pnl_t = spread_metric_start - metric_t
            pnl_series.append((ts, pnl_t))
    pnl_series.sort(key=lambda x: x[0])

    max_dd = None
    vol = None
    if pnl_series:
        peak = pnl_series[0][1]
        dd = 0.0
        for _, v in pnl_series:
            peak = max(peak, v)
            dd = min(dd, v - peak)
        max_dd = dd
        vals = [v for _, v in pnl_series]
        if len(vals) > 1:
            mean = sum(vals) / len(vals)
            var = sum((v - mean) ** 2 for v in vals) / (len(vals) - 1)
            vol = math.sqrt(var)

    # Funding: apply position sign (long pays +rate, short receives +rate).
    # pos_sign: +1 for long leg, -1 for short leg.
    funding_pnl_total = 0.0
    funding_applied: Dict[str, Any] = {}
    funding_hist: Dict[str, Any] = {}

    def _pos_sign(leg: Dict[str, Any]) -> int:
        if leg["label"] == long_leg["label"]:
            return 1
        if leg["label"] == short_leg["label"]:
            return -1
        return 0

    legs_info = [("a", leg_a), ("b", leg_b)]
    for label, leg in legs_info:
        kind = leg.get("kind")
        if kind != "perp":
            continue
        used_points: List[Tuple[datetime, float]] = []
        raw_sum_rate: Optional[float] = None
        if rest_fetcher:
            used_points = rest_fetcher.fetch(leg["exchange"], leg["symbol"], start_ts, end_ts)
            if used_points:
                raw_sum_rate = sum(rate for _, rate in used_points)
        if raw_sum_rate is None:
            funding_series = load_funding_series(sqlite_conn, leg["exchange"], leg["symbol"], start_ts, end_ts)
            raw_sum_rate, used_points = _build_funding_schedule(funding_series, start_ts, end_ts)

        pos = _pos_sign(legs[label])
        pnl_leg = None
        if raw_sum_rate is not None and pos != 0:
            pnl_leg = -float(pos) * float(raw_sum_rate)
            funding_pnl_total += pnl_leg

        funding_applied[label] = {
            "exchange": leg["exchange"],
            "symbol": leg["symbol"],
            "position": "long" if pos == 1 else ("short" if pos == -1 else "flat"),
            "pos_sign": pos,
            "raw_sum_rate": raw_sum_rate,
            "pnl": pnl_leg,
            "used_points": [(ts.isoformat(), rate) for ts, rate in used_points],
            "source": "rest" if rest_fetcher and used_points else "local",
        }
        if used_points:
            tail = used_points[-50:]
            funding_hist[label] = {
                "stats": _funding_stats(tail),
                "tail": [(ts.isoformat(), rate) for ts, rate in tail],
            }

    pnl_total = pnl_spread + (funding_pnl_total if funding_applied else 0.0)

    label = {
        "canonical": True,
        "signal_type": signal_type,
        "position_rule": position_rule,
        **(position_meta or {}),
        "spread_metric": "log(short_price/long_price)",
        "long_leg": {k: long_leg[k] for k in ("label", "exchange", "symbol", "kind")},
        "short_leg": {k: short_leg[k] for k in ("label", "exchange", "symbol", "kind")},
        "funding_rate_long_first": (
            float(long_leg.get("funding_rate_first")) if long_leg.get("kind") == "perp" and long_leg.get("funding_rate_first") is not None else 0.0
        ),
        "funding_rate_short_first": (
            float(short_leg.get("funding_rate_first")) if short_leg.get("kind") == "perp" and short_leg.get("funding_rate_first") is not None else 0.0
        ),
        "funding_edge_short_minus_long_first": (
            (
                float(short_leg.get("funding_rate_first")) if short_leg.get("kind") == "perp" and short_leg.get("funding_rate_first") is not None else 0.0
            )
            - (
                float(long_leg.get("funding_rate_first")) if long_leg.get("kind") == "perp" and long_leg.get("funding_rate_first") is not None else 0.0
            )
        ),
        "spread_metric_start": spread_metric_start,
        "spread_metric_end": spread_metric_end,
        "spread_metric_change": (spread_metric_end - spread_metric_start),
        "pnl_spread": pnl_spread,
        "pnl_funding": funding_pnl_total if funding_applied else None,
        "pnl_total": pnl_total,
    }

    return {
        "horizon_min": horizon_min,
        # Canonical: positive means the chosen spread converged in our favor.
        "spread_change": pnl_spread,
        # Canonical: signed funding contribution for the chosen position.
        "funding_change": funding_pnl_total if funding_applied else None,
        "pnl": pnl_total,
        "max_drawdown": max_dd,
        "volatility": vol,
        "funding_applied": funding_applied or None,
        "funding_hist": funding_hist or None,
        "label": label,
    }


def upsert_outcome(conn_pg, event_id: int, outcome: Dict[str, object]) -> None:
    try:
        from psycopg.types.json import Json  # type: ignore
    except Exception:  # pragma: no cover
        Json = None
    params = {**outcome, "event_id": event_id}
    if Json and outcome.get("funding_applied") is not None:
        params["funding_applied"] = Json(outcome["funding_applied"])
    if Json and outcome.get("label") is not None:
        params["label"] = Json(outcome["label"])
    conn_pg.execute(
        """
        INSERT INTO watchlist.future_outcome
          (event_id, horizon_min, pnl, spread_change, funding_change, max_drawdown, volatility, funding_applied, label)
        VALUES (%(event_id)s, %(horizon_min)s, %(pnl)s, %(spread_change)s, %(funding_change)s, %(max_drawdown)s, %(volatility)s, %(funding_applied)s, %(label)s)
        ON CONFLICT (event_id, horizon_min)
        DO UPDATE SET
          pnl = EXCLUDED.pnl,
          spread_change = EXCLUDED.spread_change,
          funding_change = EXCLUDED.funding_change,
          max_drawdown = EXCLUDED.max_drawdown,
          volatility = EXCLUDED.volatility,
          funding_applied = EXCLUDED.funding_applied,
          label = EXCLUDED.label;
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

    def _parse_iso(text: str) -> datetime:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _load_events_in_range(conn_pg, since: datetime, until: datetime, limit: int) -> List[Dict[str, Any]]:
        sql = """
        SELECT
            e.id, e.exchange, e.symbol, e.signal_type, e.start_ts,
            e.leg_a_exchange, e.leg_a_symbol, e.leg_a_kind, e.leg_a_price_first, e.leg_a_price_last, e.leg_a_funding_rate_first, e.leg_a_funding_rate_last,
            e.leg_b_exchange, e.leg_b_symbol, e.leg_b_kind, e.leg_b_price_first, e.leg_b_price_last, e.leg_b_funding_rate_first, e.leg_b_funding_rate_last
        FROM watchlist.watch_signal_event e
        WHERE e.start_ts >= %(since)s
          AND e.start_ts < %(until)s
          AND e.leg_a_exchange IS NOT NULL
          AND e.leg_b_exchange IS NOT NULL
        ORDER BY e.start_ts DESC
        LIMIT %(limit)s;
        """
        rows = conn_pg.execute(sql, {"since": since, "until": until, "limit": int(limit)}).fetchall()
        cols = [desc.name for desc in conn_pg.description] if hasattr(conn_pg, "description") else None
        out: List[Dict[str, Any]] = []
        for r in rows:
            if cols:
                out.append(dict(zip(cols, r)))
            else:
                out.append(
                    {
                        "id": r[0],
                        "exchange": r[1],
                        "symbol": r[2],
                        "signal_type": r[3],
                        "start_ts": r[4],
                        "leg_a_exchange": r[5],
                        "leg_a_symbol": r[6],
                        "leg_a_kind": r[7],
                        "leg_a_price_first": r[8],
                        "leg_a_price_last": r[9],
                        "leg_a_funding_rate_first": r[10],
                        "leg_a_funding_rate_last": r[11],
                        "leg_b_exchange": r[12],
                        "leg_b_symbol": r[13],
                        "leg_b_kind": r[14],
                        "leg_b_price_first": r[15],
                        "leg_b_price_last": r[16],
                        "leg_b_funding_rate_first": r[17],
                        "leg_b_funding_rate_last": r[18],
                    }
                )
        return out

    fetcher: Optional[FundingHistoryFetcher] = FundingHistoryFetcher()

    with sqlite3.connect(sqlite_path) as sqlite_conn, psycopg.connect(pg_dsn, autocommit=True) as pg_conn:
        ensure_unique_index(pg_conn)
        now_ts = _utcnow()
        max_tasks = int(getattr(loop_once, "max_tasks", MAX_TASKS))  # type: ignore[attr-defined]
        processed = 0

        # Recompute mode: overwrite existing outcomes in a time window (for backfill/canonicalization fixes).
        recompute_since = getattr(loop_once, "recompute_since", None)  # type: ignore[attr-defined]
        if recompute_since:
            since = _parse_iso(str(recompute_since))
            until_raw = getattr(loop_once, "recompute_until", None)  # type: ignore[attr-defined]
            until = _parse_iso(str(until_raw)) if until_raw else now_ts
            horizons_raw = getattr(loop_once, "recompute_horizons", "")  # type: ignore[attr-defined]
            if horizons_raw:
                if str(horizons_raw).strip().lower() == "all":
                    horizons = list(HORIZONS_MIN)
                else:
                    horizons = [int(x.strip()) for x in str(horizons_raw).split(",") if x.strip()]
            else:
                horizons = list(HORIZONS_MIN)
            limit = int(getattr(loop_once, "recompute_limit", 5000))  # type: ignore[attr-defined]
            no_rest = bool(getattr(loop_once, "no_rest", False))  # type: ignore[attr-defined]
            fetcher = None if no_rest else fetcher
            events = _load_events_in_range(pg_conn, since, until, limit=limit)
            if not events:
                logger.info("no events to recompute")
                return
            for row in events:
                for h in horizons:
                    if row["start_ts"] + timedelta(minutes=h) > now_ts:
                        continue
                    out = compute_outcome(sqlite_conn, row, h, rest_fetcher=fetcher)
                    if not out:
                        continue
                    upsert_outcome(pg_conn, row["id"], out)
                    update_event_funding_hist(pg_conn, row["id"], out.get("funding_hist"))
                    processed += 1
                    if processed >= max_tasks:
                        break
                if processed >= max_tasks:
                    break
            logger.info("recomputed outcomes: %s", processed)
            return

        # Default mode: only compute missing outcomes for matured events.
        tasks = load_event_tasks(pg_conn)
        if not tasks:
            logger.info("no pending events")
            return
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
            if processed >= max_tasks:
                break
        logger.info("processed outcomes: %s", processed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute future outcomes for watchlist events")
    parser.add_argument("--loop-once", action="store_true", help="run once and exit (default)")
    parser.add_argument("--recompute-since", type=str, default="", help="ISO8601 UTC, recompute outcomes in window")
    parser.add_argument("--recompute-until", type=str, default="", help="ISO8601 UTC, exclusive end (default now)")
    parser.add_argument("--recompute-horizons", type=str, default="", help="Comma list minutes or 'all' (default all)")
    parser.add_argument("--recompute-limit", type=int, default=5000, help="Max events to scan in recompute mode")
    parser.add_argument("--max-tasks", type=int, default=MAX_TASKS, help="Max event-horizon computations per run")
    parser.add_argument("--no-rest", action="store_true", help="Disable REST funding history (use local fallback only)")
    args = parser.parse_args()
    # Hack: keep backward compatible main() signature; pass args as loop_once container.
    main(loop_once=args)
