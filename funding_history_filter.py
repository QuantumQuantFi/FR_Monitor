from __future__ import annotations

import math
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from funding_utils import derive_funding_interval_hours


def _parse_ts(ts_str: Any) -> Optional[datetime]:
    if not ts_str:
        return None
    if isinstance(ts_str, datetime):
        return ts_str if ts_str.tzinfo else ts_str.replace(tzinfo=timezone.utc)
    try:
        ts = datetime.fromisoformat(str(ts_str))
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _sqlite_ts(dt: datetime) -> str:
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def _sign_changes(values: List[float], *, dead_band_bp: float = 1.0) -> int:
    last = None
    changes = 0
    dead_band = float(dead_band_bp) / 10000.0
    for v in values:
        if v is None:
            continue
        if abs(v) < dead_band:
            continue
        s = 1 if v > 0 else -1
        if last is None:
            last = s
            continue
        if s != last:
            changes += 1
            last = s
    return changes


def _hourly_series(
    conn: sqlite3.Connection,
    *,
    exchange: str,
    symbol: str,
    start: datetime,
    end: datetime,
) -> List[Tuple[datetime, float, Optional[float]]]:
    rows: List[Tuple[datetime, float, Optional[float]]] = []
    cur = conn.execute(
        """
        SELECT timestamp, funding_rate_avg, funding_interval_hours
        FROM price_data_1min
        WHERE exchange = ? AND symbol = ? AND timestamp >= ? AND timestamp <= ?
          AND funding_rate_avg IS NOT NULL
        ORDER BY timestamp ASC;
        """,
        (exchange, symbol, _sqlite_ts(start), _sqlite_ts(end)),
    )
    buckets: Dict[datetime, Dict[str, List[float]]] = {}
    for ts_str, fr, iv in cur.fetchall():
        ts = _parse_ts(ts_str)
        if ts is None:
            continue
        try:
            fr_val = float(fr)
        except Exception:
            continue
        iv_val: Optional[float] = None
        if iv not in (None, ""):
            try:
                iv_val = float(iv)
            except Exception:
                iv_val = None
        hour = ts.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
        bucket = buckets.setdefault(hour, {"fr": [], "iv": []})
        bucket["fr"].append(fr_val)
        if iv_val is not None:
            bucket["iv"].append(iv_val)
    for hour in sorted(buckets.keys()):
        frs = buckets[hour]["fr"]
        if not frs:
            continue
        fr_mean = sum(frs) / len(frs)
        ivs = buckets[hour]["iv"]
        iv_mean = (sum(ivs) / len(ivs)) if ivs else None
        rows.append((hour, float(fr_mean), iv_mean))
    return rows


def _leg_metrics(rows: List[Tuple[datetime, float, Optional[float]]], *, dead_band_bp: float) -> Optional[Dict[str, Any]]:
    vals = [fr for _ts, fr, _iv in rows if fr is not None]
    if not vals:
        return None
    mean_abs = sum(abs(v) for v in vals) / len(vals)
    min_v = min(vals)
    max_v = max(vals)
    mean = sum(vals) / len(vals)
    var = sum((v - mean) ** 2 for v in vals) / len(vals)
    std = math.sqrt(var)
    return {
        "count": len(vals),
        "mean_abs_bp": mean_abs * 10000.0,
        "range_bp": (max_v - min_v) * 10000.0,
        "std_bp": std * 10000.0,
        "sign_changes": _sign_changes(vals, dead_band_bp=dead_band_bp),
    }


def _net_metrics(
    *,
    long_rows: List[Tuple[datetime, float, Optional[float]]],
    short_rows: List[Tuple[datetime, float, Optional[float]]],
    long_ex: str,
    short_ex: str,
    dead_band_bp: float,
) -> Optional[Dict[str, Any]]:
    long_map = {ts: (fr, iv) for ts, fr, iv in long_rows if fr is not None}
    short_map = {ts: (fr, iv) for ts, fr, iv in short_rows if fr is not None}
    common = sorted(set(long_map).intersection(short_map))
    if not common:
        return None
    net_vals: List[float] = []
    for ts in common:
        fr_l, iv_l = long_map[ts]
        fr_s, iv_s = short_map[ts]
        iv_l_h = derive_funding_interval_hours(str(long_ex), iv_l, fallback=True)
        iv_s_h = derive_funding_interval_hours(str(short_ex), iv_s, fallback=True)
        if not iv_l_h or not iv_s_h:
            continue
        net_vals.append((float(fr_s) / float(iv_s_h)) - (float(fr_l) / float(iv_l_h)))
    if not net_vals:
        return None
    mean = sum(net_vals) / len(net_vals)
    min_v = min(net_vals)
    max_v = max(net_vals)
    var = sum((v - mean) ** 2 for v in net_vals) / len(net_vals)
    std = math.sqrt(var)
    return {
        "count": len(net_vals),
        "net_mean_loss_4h_bp": max(0.0, -mean * 4.0) * 10000.0,
        "net_range_bp": (max_v - min_v) * 10000.0,
        "net_std_bp": std * 10000.0,
        "net_sign_changes": _sign_changes(net_vals, dead_band_bp=dead_band_bp),
    }


def evaluate_type_b_funding_history(
    *,
    db_path: str,
    symbol: str,
    long_ex: str,
    short_ex: str,
    end_ts: Optional[datetime] = None,
    window_hours: float = 48.0,
    min_hours: int = 12,
    dead_band_bp: float = 1.0,
    thresholds: Optional[Dict[str, float]] = None,
    allow_insufficient: bool = True,
    cache: Optional[Dict[Tuple[str, str, str], Dict[str, Any]]] = None,
    cache_ttl_seconds: float = 300.0,
) -> Tuple[bool, Dict[str, Any]]:
    now = end_ts or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    window_h = float(window_hours or 0.0)
    start = now - timedelta(hours=window_h)
    end = now
    key_base = now.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat()

    def _load_series(ex: str) -> List[Tuple[datetime, float, Optional[float]]]:
        if cache is None:
            conn = sqlite3.connect(db_path, timeout=15.0)
            try:
                return _hourly_series(conn, exchange=ex, symbol=symbol, start=start, end=end)
            finally:
                conn.close()
        cache_key = (str(ex), str(symbol), key_base)
        cached = cache.get(cache_key)
        if cached and cached.get("expires_at") and cached["expires_at"] >= datetime.now(timezone.utc).timestamp():
            return list(cached.get("series") or [])
        conn = sqlite3.connect(db_path, timeout=15.0)
        try:
            series = _hourly_series(conn, exchange=ex, symbol=symbol, start=start, end=end)
        finally:
            conn.close()
        cache[cache_key] = {
            "expires_at": datetime.now(timezone.utc).timestamp() + float(cache_ttl_seconds),
            "series": list(series),
        }
        return series

    long_series = _load_series(long_ex)
    short_series = _load_series(short_ex)
    long_metrics = _leg_metrics(long_series, dead_band_bp=dead_band_bp)
    short_metrics = _leg_metrics(short_series, dead_band_bp=dead_band_bp)
    net_metrics = _net_metrics(
        long_rows=long_series,
        short_rows=short_series,
        long_ex=long_ex,
        short_ex=short_ex,
        dead_band_bp=dead_band_bp,
    )

    th = thresholds or {}
    limit_range = float(th.get("range_bp", 120.0))
    limit_std = float(th.get("std_bp", 22.0))
    limit_mean_abs = float(th.get("mean_abs_bp", 35.0))
    limit_sign = int(th.get("sign_changes", 5))
    limit_net_mean = float(th.get("net_mean_loss_4h_bp", 20.0))
    limit_net_range = float(th.get("net_range_bp", 60.0))
    limit_net_sign = int(th.get("net_sign_changes", 10))

    reasons: List[str] = []
    insufficient = False

    def _check_leg(tag: str, metrics: Optional[Dict[str, Any]]) -> bool:
        nonlocal insufficient
        if not metrics or int(metrics.get("count") or 0) < int(min_hours):
            insufficient = True
            return True
        ok = True
        if float(metrics.get("range_bp") or 0.0) > limit_range:
            reasons.append(f"{tag}:range")
            ok = False
        if float(metrics.get("std_bp") or 0.0) > limit_std:
            reasons.append(f"{tag}:std")
            ok = False
        if float(metrics.get("mean_abs_bp") or 0.0) > limit_mean_abs:
            reasons.append(f"{tag}:mean_abs")
            ok = False
        if int(metrics.get("sign_changes") or 0) > limit_sign:
            reasons.append(f"{tag}:sign")
            ok = False
        return ok

    def _check_net(metrics: Optional[Dict[str, Any]]) -> bool:
        nonlocal insufficient
        if not metrics or int(metrics.get("count") or 0) < int(min_hours):
            insufficient = True
            return True
        ok = True
        if float(metrics.get("net_mean_loss_4h_bp") or 0.0) > limit_net_mean:
            reasons.append("net:mean_loss")
            ok = False
        if float(metrics.get("net_range_bp") or 0.0) > limit_net_range:
            reasons.append("net:range")
            ok = False
        if int(metrics.get("net_sign_changes") or 0) > limit_net_sign:
            reasons.append("net:sign")
            ok = False
        return ok

    ok = _check_leg("long", long_metrics) and _check_leg("short", short_metrics) and _check_net(net_metrics)
    if insufficient and allow_insufficient:
        ok = True
    elif insufficient and not allow_insufficient:
        ok = False
        reasons.append("insufficient_data")

    info = {
        "ok": bool(ok),
        "insufficient_data": bool(insufficient),
        "window_hours": window_h,
        "min_hours": int(min_hours),
        "dead_band_bp": float(dead_band_bp),
        "long_exchange": str(long_ex),
        "short_exchange": str(short_ex),
        "long": long_metrics,
        "short": short_metrics,
        "net": net_metrics,
        "thresholds": {
            "range_bp": limit_range,
            "std_bp": limit_std,
            "mean_abs_bp": limit_mean_abs,
            "sign_changes": limit_sign,
            "net_mean_loss_4h_bp": limit_net_mean,
            "net_range_bp": limit_net_range,
            "net_sign_changes": limit_net_sign,
        },
        "reasons": reasons,
    }
    return bool(ok), info
