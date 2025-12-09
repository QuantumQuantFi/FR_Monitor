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

import config

HORIZONS_MIN = [15, 30, 60, 240, 480, 1440, 2880, 5760]  # 15m,30m,1h,4h,8h,24h,48h,96h
MAX_TASKS = 200  # 每次运行最多处理的 event-horizon 组合，防止扫全表


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Snapshot:
    ts: datetime
    spot: Optional[float]
    perp: Optional[float]
    funding_rate: Optional[float]
    funding_interval_hours: Optional[float]


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


def load_snapshot(sqlite_conn: sqlite3.Connection, exchange: str, symbol: str, target_ts: datetime) -> Optional[Snapshot]:
    """
    取目标时间点之前最近的一条 1min K（聚合数据）。
    """
    cursor = sqlite_conn.execute(
        """
        SELECT timestamp, spot_price_close, futures_price_close, funding_rate_avg, funding_interval_hours
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
    ts = datetime.fromisoformat(row[0])
    return Snapshot(
        ts=ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts,
        spot=row[1],
        perp=row[2],
        funding_rate=row[3],
        funding_interval_hours=row[4],
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


def load_funding_series(sqlite_conn: sqlite3.Connection, exchange: str, symbol: str, start: datetime, end: datetime) -> List[Tuple[datetime, float, float]]:
    """
    返回 (ts, funding_rate_avg, funding_interval_hours)
    """
    cursor = sqlite_conn.execute(
        """
        SELECT timestamp, funding_rate_avg, funding_interval_hours
        FROM price_data_1min
        WHERE exchange = ? AND symbol = ? AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC;
        """,
        (exchange, symbol, start.isoformat(), end.isoformat()),
    )
    out = []
    for ts_str, fr, interval_h in cursor.fetchall():
        ts = datetime.fromisoformat(ts_str)
        ts = ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
        out.append((ts, fr, interval_h))
    return out


def compute_outcome(
    sqlite_conn: sqlite3.Connection,
    exchange: str,
    symbol: str,
    start_ts: datetime,
    horizon_min: int,
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

    # 资金费：简单估算，按期间均值 * (horizon_hours / interval_hours)
    funding_series = load_funding_series(sqlite_conn, exchange, symbol, snap_start.ts, snap_end.ts)
    funding_change = None
    if funding_series:
        rates = [fr for _, fr, _ in funding_series if fr is not None]
        if rates:
            avg_rate = sum(rates) / len(rates)
            interval_h = None
            for _, _, ih in reversed(funding_series):
                if ih and ih > 0:
                    interval_h = ih
                    break
            interval_h = interval_h or 8.0
            funding_change = avg_rate * (horizon_min / 60.0) / interval_h

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
                out = compute_outcome(sqlite_conn, exchange, symbol, start_ts, h)
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
