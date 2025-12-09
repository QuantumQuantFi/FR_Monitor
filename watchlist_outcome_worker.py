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
from typing import Dict, List, Optional, Sequence, Tuple

import psycopg
import requests

import config

HORIZONS_MIN = [15, 30, 60, 240, 480, 1440, 2880, 5760]  # 15m,30m,1h,4h,8h,24h,48h,96h
MAX_TASKS = 200  # 每次运行最多处理的 event-horizon 组合，防止扫全表
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


def load_event_tasks(conn_pg) -> List[Tuple[int, str, str, datetime]]:
    """
    返回需要计算 outcome 的事件 (id, exchange, symbol, start_ts)。
    规则：事件 start_ts 已超过最小 horizon，且某些 horizon 尚未写入 outcome。
    """
    min_horizon = min(HORIZONS_MIN)
    sql = """
    WITH need AS (
        SELECT e.id, e.exchange, e.symbol, e.start_ts
        FROM watchlist.watch_signal_event e
        WHERE e.start_ts <= now() - make_interval(mins := %s)
    )
    SELECT n.id, n.exchange, n.symbol, n.start_ts
    FROM need n
    WHERE EXISTS (
        SELECT 1 FROM unnest(%s::int[]) h
        WHERE NOT EXISTS (
            SELECT 1 FROM watchlist.future_outcome o
            WHERE o.event_id = n.id AND o.horizon_min = h
        )
    )
    ORDER BY n.start_ts
    LIMIT %s;
    """
    rows = conn_pg.execute(sql, (min_horizon, HORIZONS_MIN, MAX_TASKS)).fetchall()
    return [(r[0], r[1], r[2], r[3]) for r in rows]


def _parse_ts(ts_str: Optional[str]) -> Optional[datetime]:
    if not ts_str:
        return None
    try:
        ts = datetime.fromisoformat(ts_str)
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    except Exception:
        return None


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
        (exchange, symbol, target_ts.isoformat()),
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
    )


def load_spread_series(sqlite_conn: sqlite3.Connection, exchange: str, symbol: str, start: datetime, end: datetime) -> List[Tuple[datetime, float]]:
    cursor = sqlite_conn.execute(
        """
        SELECT timestamp, spot_price_close, futures_price_close
        FROM price_data_1min
        WHERE exchange = ? AND symbol = ? AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC;
        """,
        (exchange, symbol, start.isoformat(), end.isoformat()),
    )
    out = []
    for ts_str, spot, perp in cursor.fetchall():
        if spot and perp:
            ts = datetime.fromisoformat(ts_str)
            ts = ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
            if spot != 0:
                out.append((ts, (spot - perp) / spot))
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
        (exchange, symbol, start.isoformat(), end.isoformat()),
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
    轻量 REST funding 历史获取。当前只实现 Binance，其他交易所返回空。
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
        params = {
            "symbol": sym,
            "startTime": int(start_ts.timestamp() * 1000),
            "endTime": int(end_ts.timestamp() * 1000),
            "limit": 1000,
        }
        resp = self.session.get("https://fapi.binance.com/fapi/v1/fundingRate", params=params, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        out: List[Tuple[datetime, float]] = []
        for item in data:
            try:
                ts = datetime.fromtimestamp(item["fundingTime"] / 1000, tz=timezone.utc)
                rate = float(item["fundingRate"])
                out.append((ts, rate))
            except Exception:
                continue
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


def compute_outcome(
    sqlite_conn: sqlite3.Connection,
    exchange: str,
    symbol: str,
    start_ts: datetime,
    horizon_min: int,
    rest_fetcher: Optional[FundingHistoryFetcher] = None,
) -> Optional[Dict[str, object]]:
    end_ts = start_ts + timedelta(minutes=horizon_min)
    snap_start = load_snapshot(sqlite_conn, exchange, symbol, start_ts)
    snap_end = load_snapshot(sqlite_conn, exchange, symbol, end_ts)
    if not snap_start or not snap_end or not snap_start.spot or not snap_start.perp or not snap_end.spot or not snap_end.perp:
        return None
    spread_start = (snap_start.spot - snap_start.perp) / snap_start.spot if snap_start.spot else None
    spread_end = (snap_end.spot - snap_end.perp) / snap_end.spot if snap_end.spot else None
    if spread_start is None or spread_end is None:
        return None

    spread_series = load_spread_series(sqlite_conn, exchange, symbol, snap_start.ts, snap_end.ts)
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

    funding_change = None
    used_points: List[Tuple[datetime, float]] = []
    # 优先尝试 REST funding 历史
    if rest_fetcher:
        rest_points = rest_fetcher.fetch(exchange, symbol, snap_start.ts, snap_end.ts)
        if rest_points:
            funding_change = sum(rate for _, rate in rest_points)
            used_points = rest_points
    if funding_change is None:
        # 回退：本地 1min funding_rate + next_funding_time 推导
        funding_series = load_funding_series(sqlite_conn, exchange, symbol, snap_start.ts, snap_end.ts)
        funding_change, used_points = _build_funding_schedule(funding_series, snap_start.ts, snap_end.ts)

    pnl_spread = spread_end - spread_start
    pnl_funding = funding_change if funding_change is not None else 0.0
    pnl_total = pnl_spread + pnl_funding

    return {
        "horizon_min": horizon_min,
        "spread_change": pnl_spread,
        "funding_change": funding_change,
        "pnl": pnl_total,
        "max_drawdown": max_dd,
        "volatility": vol,
    }


def upsert_outcome(conn_pg, event_id: int, outcome: Dict[str, object]) -> None:
    conn_pg.execute(
        """
        INSERT INTO watchlist.future_outcome
          (event_id, horizon_min, pnl, spread_change, funding_change, max_drawdown, volatility)
        VALUES (%(event_id)s, %(horizon_min)s, %(pnl)s, %(spread_change)s, %(funding_change)s, %(max_drawdown)s, %(volatility)s)
        ON CONFLICT (event_id, horizon_min)
        DO UPDATE SET
          pnl = EXCLUDED.pnl,
          spread_change = EXCLUDED.spread_change,
          funding_change = EXCLUDED.funding_change,
          max_drawdown = EXCLUDED.max_drawdown,
          volatility = EXCLUDED.volatility;
        """,
        {**outcome, "event_id": event_id},
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
        processed = 0
        for event_id, exchange, symbol, start_ts in tasks:
            missing = [h for h in HORIZONS_MIN if not pg_conn.execute(
                "SELECT 1 FROM watchlist.future_outcome WHERE event_id=%s AND horizon_min=%s",
                (event_id, h),
            ).fetchone()]
            for h in missing:
                out = compute_outcome(sqlite_conn, exchange, symbol, start_ts, h, rest_fetcher=fetcher)
                if not out:
                    continue
                upsert_outcome(pg_conn, event_id, out)
                processed += 1
            if processed >= MAX_TASKS:
                break
        logger.info("processed outcomes: %s", processed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute future outcomes for watchlist events")
    parser.add_argument("--loop-once", action="store_true", help="run once and exit (default)")
    args = parser.parse_args()
    main(loop_once=args.loop_once)
