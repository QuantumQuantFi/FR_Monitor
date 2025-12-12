#!/usr/bin/env python3
"""
Backfill missing B/C (exchange=multi) metrics columns in Postgres watch_signal_raw
by recomputing from SQLite price_data_1min using the two stored legs.

This fills only NULL metric fields (it does not overwrite existing non-NULL values):
- range_1h
- range_12h
- volatility
- slope_3m
- drift_ratio

Notes:
- Raw rows have sub-minute timestamps; this script floors to minute for SQLite alignment.
- The spread series uses abs spread (|a-b|/min(a,b)) to match Type B/C trigger semantics.

Usage:
  venv/bin/python scripts/backfill_watchlist_raw_metrics_from_sqlite.py \
      --pg-dsn "postgresql://..." --sqlite-path market_data.db --days 7
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import os
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

import psycopg

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import watchlist_metrics as wm  # noqa: E402


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _floor_minute(ts: datetime) -> datetime:
    if ts.tzinfo:
        ts = ts.astimezone(timezone.utc)
    return ts.replace(second=0, microsecond=0)


def _sqlite_ts(ts: datetime) -> str:
    ts = ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return ts.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _load_leg_series(
    cursor: sqlite3.Cursor,
    exchange: str,
    symbol: str,
    kind: str,
    start_ts: datetime,
    end_ts: datetime,
) -> Dict[datetime, Tuple[float, Optional[float], Optional[float], Optional[datetime]]]:
    out: Dict[datetime, Tuple[float, Optional[float], Optional[float], Optional[datetime]]] = {}
    start_s = _sqlite_ts(start_ts)
    end_s = _sqlite_ts(end_ts)
    cursor.execute(
        """
        SELECT timestamp, spot_price_close, futures_price_close,
               funding_rate_avg, funding_interval_hours, next_funding_time
        FROM price_data_1min
        WHERE exchange=? AND symbol=? AND timestamp>=? AND timestamp<=?
        ORDER BY timestamp ASC;
        """,
        (exchange, symbol, start_s, end_s),
    )
    for ts_raw, spot, fut, fr, interval_h, next_ft in cursor.fetchall():
        ts = wm._parse_ts(ts_raw)
        if ts is None:
            continue
        price = spot if kind == "spot" else fut
        if price is None or price <= 0:
            continue
        fr_val = float(fr) if fr not in (None, 0, "") else None
        interval_val = float(interval_h) if interval_h not in (None, 0, "") else None
        next_val = wm._parse_ts(next_ft)
        out[ts] = (float(price), fr_val, interval_val, next_val)
    return out


def _build_spread_points(
    series_a: Dict[datetime, Tuple[float, Optional[float], Optional[float], Optional[datetime]]],
    series_b: Dict[datetime, Tuple[float, Optional[float], Optional[float], Optional[datetime]]],
) -> List[wm.SpreadPoint]:
    points: List[wm.SpreadPoint] = []
    # As-of join on union timestamps to handle per-exchange minute gaps.
    # Only emit points when both legs have a recent value (avoid carrying too stale quotes).
    max_stale = timedelta(minutes=5)
    ts_union = sorted(set(series_a.keys()) | set(series_b.keys()))
    last_a: Optional[Tuple[datetime, float, Optional[float], Optional[float], Optional[datetime]]] = None
    last_b: Optional[Tuple[datetime, float, Optional[float], Optional[float], Optional[datetime]]] = None
    for ts in ts_union:
        if ts in series_a:
            pa, fr_a, interval_a, next_a = series_a[ts]
            last_a = (ts, pa, fr_a, interval_a, next_a)
        if ts in series_b:
            pb, fr_b, interval_b, next_b = series_b[ts]
            last_b = (ts, pb, fr_b, interval_b, next_b)
        if not last_a or not last_b:
            continue
        tsa, pa, fr_a, interval_a, next_a = last_a
        tsb, pb, fr_b, interval_b, next_b = last_b
        if (ts - tsa) > max_stale or (ts - tsb) > max_stale:
            continue
        interval_hint = None
        for iv in (interval_a, interval_b):
            if iv and iv > 0:
                interval_hint = iv if interval_hint is None else min(interval_hint, iv)
        next_candidates = [n for n in (next_a, next_b) if n]
        next_hint = min(next_candidates) if next_candidates else None
        funding_hint = fr_a if fr_a is not None else fr_b
        points.append(
            wm.SpreadPoint(
                ts=ts,
                spot=pa,
                futures=pb,
                funding_rate=funding_hint,
                funding_interval_hours=interval_hint,
                next_funding_time=next_hint,
                abs_spread=True,
            )
        )
    return points


@dataclass(frozen=True)
class RawRowKey:
    leg_a_exchange: str
    leg_a_symbol: str
    leg_a_kind: str
    leg_b_exchange: str
    leg_b_symbol: str
    leg_b_kind: str


def iter_target_rows(pg_conn, since: datetime, limit: Optional[int] = None) -> Iterable[Dict[str, Any]]:
    sql = """
    SELECT id, ts, signal_type,
           leg_a_exchange, leg_a_symbol, leg_a_kind,
           leg_b_exchange, leg_b_symbol, leg_b_kind,
           range_1h, range_12h, volatility, slope_3m, drift_ratio
    FROM watchlist.watch_signal_raw
    WHERE exchange='multi'
      AND ts >= %s
      AND leg_a_exchange IS NOT NULL AND leg_b_exchange IS NOT NULL
      AND (
            range_1h IS NULL
         OR range_12h IS NULL
         OR volatility IS NULL
         OR slope_3m IS NULL
         OR drift_ratio IS NULL
      )
    ORDER BY ts ASC
    """
    if limit:
        sql += " LIMIT %s"
        cur = pg_conn.execute(sql, (since, limit))
    else:
        cur = pg_conn.execute(sql, (since,))
    cols = [d.name for d in cur.description]
    for r in cur.fetchall():
        yield dict(zip(cols, r))


def compute_metrics_series(points: List[wm.SpreadPoint], cfg: Dict[str, Any]) -> Dict[datetime, Dict[str, Optional[float]]]:
    """
    Compute a subset of metrics for each point timestamp.
    Returns mapping: ts -> {'range_1h','range_12h','volatility','slope_3m','drift_ratio'}
    """
    out: Dict[datetime, Dict[str, Optional[float]]] = {}
    if not points:
        return out

    window_minutes = int(cfg.get("window_minutes", 60))
    slope_minutes = int(cfg.get("slope_minutes", 3))
    crossing_window_h = float(cfg.get("crossing_window_hours", 3.0))
    crossing_mid_minutes = int(cfg.get("crossing_mid_minutes", 30))
    range_short_h = float(cfg.get("range_hours_short", 1))
    range_long_h = float(cfg.get("range_hours_long", 12))

    used_points: List[wm.SpreadPoint] = []
    spreads: List[float] = []
    times: List[datetime] = []
    for p in points:
        v = p.spread_rel
        if v is None:
            continue
        used_points.append(p)
        spreads.append(float(v))
        times.append(p.ts)
    if not spreads:
        return out

    # rolling mean/std window
    win: Deque[float] = deque()
    win_sum = 0.0
    win_sumsq = 0.0

    # range windows excluding funding minutes (time-based expiry)
    short_max: Deque[Tuple[int, float]] = deque()
    short_min: Deque[Tuple[int, float]] = deque()
    long_max: Deque[Tuple[int, float]] = deque()
    long_min: Deque[Tuple[int, float]] = deque()

    def push_monotonic(maxdq: Deque[Tuple[int, float]], mindq: Deque[Tuple[int, float]], idx: int, val: float) -> None:
        while maxdq and maxdq[-1][1] < val:
            maxdq.pop()
        maxdq.append((idx, val))
        while mindq and mindq[-1][1] > val:
            mindq.pop()
        mindq.append((idx, val))

    def expire(dq: Deque[Tuple[int, float]], cutoff: datetime) -> None:
        while dq and times[dq[0][0]] < cutoff:
            dq.popleft()

    for i, (ts, val) in enumerate(zip(times, spreads)):
        # update mean/std window
        win.append(val)
        win_sum += val
        win_sumsq += val * val
        if len(win) > window_minutes:
            old = win.popleft()
            win_sum -= old
            win_sumsq -= old * old
        spread_mean = None
        spread_std = None
        if win:
            spread_mean = win_sum / len(win)
            var = (win_sumsq / len(win)) - (spread_mean * spread_mean)
            spread_std = (var ** 0.5) if var > 0 else 0.0

        # update range windows (exclude funding minutes)
        pt = used_points[i]
        is_funding = wm._is_funding_minute(pt.ts, pt.funding_interval_hours, pt.next_funding_time)
        if not is_funding:
            push_monotonic(short_max, short_min, i, val)
            push_monotonic(long_max, long_min, i, val)

        cutoff_short = ts - timedelta(hours=range_short_h)
        cutoff_long = ts - timedelta(hours=range_long_h)
        expire(short_max, cutoff_short)
        expire(short_min, cutoff_short)
        expire(long_max, cutoff_long)
        expire(long_min, cutoff_long)

        range_1h = None
        if short_max and short_min:
            range_1h = short_max[0][1] - short_min[0][1]
        range_12h = None
        if long_max and long_min:
            range_12h = long_max[0][1] - long_min[0][1]

        # recent slope (same semantics as compute_metrics_for_symbol)
        slope_3m = None
        j0 = max(0, i - slope_minutes)
        recent = spreads[j0 : i + 1]
        if len(recent) >= 2:
            slope_3m = (recent[-1] - recent[0]) / slope_minutes

        # drift_ratio: based on crossing_window midline and current range_1h
        drift_ratio = None
        cutoff_cross = ts - timedelta(hours=crossing_window_h)
        # find earliest index within cutoff (linear scan backwards small window)
        k = i
        while k > 0 and times[k - 1] >= cutoff_cross:
            k -= 1
        crossing_series = spreads[k : i + 1]
        if crossing_series and range_1h and range_1h > 0:
            mid = wm._midline(crossing_series, crossing_mid_minutes)
            mid_last = mid[-1] if mid else None
            if mid_last is not None:
                drift_ratio = abs(crossing_series[-1] - mid_last) / range_1h

        out[ts] = {
            "range_1h": range_1h,
            "range_12h": range_12h,
            "volatility": spread_std,
            "slope_3m": slope_3m,
            "drift_ratio": drift_ratio,
        }

    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill multi raw metrics from SQLite")
    parser.add_argument("--pg-dsn", required=True)
    parser.add_argument("--sqlite-path", default="market_data.db")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--batch", type=int, default=2000)
    parser.add_argument("--log-every", type=int, default=25, help="log every N leg pairs")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger("backfill_raw_metrics")

    since = _utcnow() - timedelta(days=max(1, args.days))
    limit = args.limit or None
    cfg = dict(wm.WATCHLIST_METRICS_CONFIG)

    with psycopg.connect(args.pg_dsn, autocommit=True) as pg_conn, sqlite3.connect(
        args.sqlite_path, timeout=30.0
    ) as sqlite_conn:
        sqlite_cur = sqlite_conn.cursor()

        rows = list(iter_target_rows(pg_conn, since, limit=limit))
        if not rows:
            logger.info("no target raw rows")
            return

        grouped: Dict[RawRowKey, List[Dict[str, Any]]] = defaultdict(list)
        for r in rows:
            key = RawRowKey(
                leg_a_exchange=str(r["leg_a_exchange"]).lower(),
                leg_a_symbol=str(r["leg_a_symbol"]).upper(),
                leg_a_kind=str(r["leg_a_kind"]).lower(),
                leg_b_exchange=str(r["leg_b_exchange"]).lower(),
                leg_b_symbol=str(r["leg_b_symbol"]).upper(),
                leg_b_kind=str(r["leg_b_kind"]).lower(),
            )
            grouped[key].append(r)

        logger.info("loaded %s rows into %s leg pairs", len(rows), len(grouped))

        updated = 0
        pair_idx = 0
        pending_updates: List[Dict[str, Any]] = []

        for key, group_rows in grouped.items():
            pair_idx += 1
            ts_list = [_floor_minute(r["ts"]) for r in group_rows]
            min_ts = min(ts_list)
            max_ts = max(ts_list)
            start_ts = min_ts - timedelta(hours=float(cfg.get("range_hours_long", 12)))
            end_ts = max_ts

            series_a = _load_leg_series(sqlite_cur, key.leg_a_exchange, key.leg_a_symbol, key.leg_a_kind, start_ts, end_ts)
            series_b = _load_leg_series(sqlite_cur, key.leg_b_exchange, key.leg_b_symbol, key.leg_b_kind, start_ts, end_ts)
            points = _build_spread_points(series_a, series_b)
            if not points:
                continue

            metrics_by_ts = compute_metrics_series(points, cfg)
            for r, ts_min in zip(group_rows, ts_list):
                metrics = metrics_by_ts.get(ts_min)
                if not metrics:
                    continue
                pending_updates.append(
                    {
                        "id": r["id"],
                        "ts": r["ts"],
                        "range_1h": metrics.get("range_1h"),
                        "range_12h": metrics.get("range_12h"),
                        "volatility": metrics.get("volatility"),
                        "slope_3m": metrics.get("slope_3m"),
                        "drift_ratio": metrics.get("drift_ratio"),
                    }
                )

            if pending_updates and (len(pending_updates) >= args.batch):
                updated += _flush_pg_updates(pg_conn, pending_updates)
                pending_updates.clear()

            if args.log_every and pair_idx % args.log_every == 0:
                logger.info("processed pairs=%s/%s updated_rows=%s", pair_idx, len(grouped), updated)

        if pending_updates:
            updated += _flush_pg_updates(pg_conn, pending_updates)

        logger.info("done. total_rows=%s updated_rows=%s pairs=%s", len(rows), updated, len(grouped))


def _flush_pg_updates(pg_conn, updates: List[Dict[str, Any]]) -> int:
    if not updates:
        return 0
    sql = """
    UPDATE watchlist.watch_signal_raw
       SET range_1h = COALESCE(range_1h, %(range_1h)s),
           range_12h = COALESCE(range_12h, %(range_12h)s),
           volatility = COALESCE(volatility, %(volatility)s),
           slope_3m = COALESCE(slope_3m, %(slope_3m)s),
           drift_ratio = COALESCE(drift_ratio, %(drift_ratio)s)
     WHERE ts=%(ts)s AND id=%(id)s
    """
    with pg_conn.cursor() as cur:
        cur.executemany(sql, updates)
    return len(updates)


if __name__ == "__main__":
    main()
