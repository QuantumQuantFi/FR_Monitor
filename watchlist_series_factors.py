from __future__ import annotations

import math
import sqlite3
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_LOOKBACK_MIN = 240
DEFAULT_ASOF_TOL_SEC = 90
DEFAULT_FFILL_BARS = 2
DEFAULT_MIN_VALID_RATIO = 0.8
DEFAULT_MAX_BACKFILL_BARS = 2

DEFAULT_FUNDING_INTERVAL_HOURS = {
    "binance": 8.0,
    "okx": 8.0,
    "bybit": 8.0,
    "bitget": 8.0,
    "gate": 8.0,
    "htx": 8.0,
    "grvt": 8.0,
    "hyperliquid": 1.0,
    "lighter": 1.0,
}


class SeriesCache:
    def __init__(self, max_entries: int = 128) -> None:
        self.max_entries = max_entries
        self._data: OrderedDict[Tuple[str, str, str], Dict[str, Any]] = OrderedDict()

    def get(self, key: Tuple[str, str, str]) -> Optional[Dict[str, Any]]:
        entry = self._data.get(key)
        if entry is not None:
            self._data.move_to_end(key)
        return entry

    def put(self, key: Tuple[str, str, str], entry: Dict[str, Any]) -> None:
        self._data[key] = entry
        self._data.move_to_end(key)
        while len(self._data) > self.max_entries:
            self._data.popitem(last=False)


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


def _asof_nearest(
    series: List[Tuple[datetime, float, Optional[float], Optional[float], Optional[datetime]]],
    target: datetime,
    tol_sec: int,
    ts_list: Optional[List[datetime]] = None,
) -> Optional[Tuple[float, Optional[float], Optional[float], Optional[datetime], float]]:
    if not series:
        return None
    from bisect import bisect_left

    if ts_list is None:
        ts_list = [t for (t, *_rest) in series]
    i = bisect_left(ts_list, target)
    candidates = []
    if 0 <= i < len(series):
        candidates.append(series[i])
    if i - 1 >= 0:
        candidates.append(series[i - 1])
    best = None
    best_gap = None
    for t, price, fr, iv, nft in candidates:
        gap = abs((t - target).total_seconds())
        if best is None or gap < best_gap:
            best = (price, fr, iv, nft)
            best_gap = gap
    if best is None or best_gap is None:
        return None
    if best_gap > tol_sec:
        return None
    return best[0], best[1], best[2], best[3], float(best_gap)


def _load_leg_series_1min(
    conn: sqlite3.Connection,
    exchange: str,
    symbol: str,
    kind: str,
    start: datetime,
    end: datetime,
) -> List[Tuple[datetime, float, Optional[float], Optional[float], Optional[datetime]]]:
    out: List[Tuple[datetime, float, Optional[float], Optional[float], Optional[datetime]]] = []
    cursor = conn.execute(
        """
        SELECT timestamp, spot_price_close, futures_price_close,
               funding_rate_avg, funding_interval_hours, next_funding_time
        FROM price_data_1min
        WHERE exchange = ? AND symbol = ? AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC;
        """,
        (exchange, symbol, _sqlite_ts(start), _sqlite_ts(end)),
    )
    for ts_str, spot, perp, fr, iv_h, nft in cursor.fetchall():
        ts = _parse_ts(ts_str)
        if ts is None:
            continue
        price = spot if kind == "spot" else perp
        if price is None or price <= 0:
            continue
        fr_val = None
        if kind == "spot":
            fr_val = 0.0
        elif fr not in (None, "", 0):
            try:
                fr_val = float(fr)
            except Exception:
                fr_val = None
        interval_val = None
        if iv_h not in (None, "", 0):
            try:
                interval_val = float(iv_h)
            except Exception:
                interval_val = None
        next_val = _parse_ts(nft)
        out.append((ts, float(price), fr_val, interval_val, next_val))
    return out


def _slice_series(
    series: List[Tuple[datetime, float, Optional[float], Optional[float], Optional[datetime]]],
    ts_list: List[datetime],
    start: datetime,
    end: datetime,
) -> List[Tuple[datetime, float, Optional[float], Optional[float], Optional[datetime]]]:
    if not series:
        return []
    from bisect import bisect_left, bisect_right

    lo = bisect_left(ts_list, start)
    hi = bisect_right(ts_list, end)
    return series[lo:hi]


def _merge_series(
    base: List[Tuple[datetime, float, Optional[float], Optional[float], Optional[datetime]]],
    addon: List[Tuple[datetime, float, Optional[float], Optional[float], Optional[datetime]]],
) -> List[Tuple[datetime, float, Optional[float], Optional[float], Optional[datetime]]]:
    if not base:
        return addon
    if not addon:
        return base
    merged: Dict[datetime, Tuple[datetime, float, Optional[float], Optional[float], Optional[datetime]]] = {
        t: (t, p, fr, iv, nft) for t, p, fr, iv, nft in base
    }
    for row in addon:
        merged[row[0]] = row
    return sorted(merged.values(), key=lambda x: x[0])


def _load_leg_series_cached(
    conn: sqlite3.Connection,
    exchange: str,
    symbol: str,
    kind: str,
    start: datetime,
    end: datetime,
    cache: Optional[SeriesCache],
) -> List[Tuple[datetime, float, Optional[float], Optional[float], Optional[datetime]]]:
    if cache is None:
        return _load_leg_series_1min(conn, exchange, symbol, kind, start, end)

    key = (exchange, symbol, kind)
    entry = cache.get(key)
    if entry is None:
        series = _load_leg_series_1min(conn, exchange, symbol, kind, start, end)
        cache.put(
            key,
            {
                "start": start,
                "end": end,
                "series": series,
                "ts_list": [t for t, *_ in series],
            },
        )
        return series

    series = entry["series"]
    c_start = entry["start"]
    c_end = entry["end"]
    ts_list = entry["ts_list"]

    if start >= c_start and end <= c_end:
        return _slice_series(series, ts_list, start, end)

    if start < c_start:
        head = _load_leg_series_1min(conn, exchange, symbol, kind, start, c_start)
        series = _merge_series(series, head)
        c_start = start
    if end > c_end:
        tail = _load_leg_series_1min(conn, exchange, symbol, kind, c_end, end)
        series = _merge_series(series, tail)
        c_end = end

    ts_list = [t for t, *_ in series]
    entry.update({"start": c_start, "end": c_end, "series": series, "ts_list": ts_list})
    cache.put(key, entry)
    return _slice_series(series, ts_list, start, end)


def _align_series(
    series: List[Tuple[datetime, float, Optional[float], Optional[float], Optional[datetime]]],
    grid: List[datetime],
    tol_sec: int,
    ffill_bars: int,
) -> Tuple[
    List[Optional[Tuple[float, Optional[float], Optional[float], Optional[datetime]]]],
    Dict[str, Any],
]:
    aligned: List[Optional[Tuple[float, Optional[float], Optional[float], Optional[datetime]]]] = []
    missing = 0
    ffill = 0
    max_gap = 0.0
    ts_list = [t for (t, *_rest) in series] if series else []
    last_valid: Optional[Tuple[float, Optional[float], Optional[float], Optional[datetime]]] = None
    last_age = 0
    for t in grid:
        pick = _asof_nearest(series, t, tol_sec, ts_list)
        if pick:
            price, fr, iv, nft, gap = pick
            max_gap = max(max_gap, gap)
            last_valid = (price, fr, iv, nft)
            last_age = 0
            aligned.append(last_valid)
            continue
        if last_valid is not None and last_age < ffill_bars:
            last_age += 1
            ffill += 1
            aligned.append(last_valid)
            continue
        missing += 1
        last_valid = None
        last_age = 0
        aligned.append(None)
    meta = {
        "total_points": len(grid),
        "missing_points": missing,
        "ffill_points": ffill,
        "missing_rate": (missing / len(grid)) if grid else None,
        "max_gap_sec": max_gap,
        "tol_sec": tol_sec,
        "ffill_bars": ffill_bars,
    }
    return aligned, meta


def _rolling_mean_std(values: List[float]) -> Tuple[Optional[float], Optional[float]]:
    if not values:
        return None, None
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / len(values)
    return mean, math.sqrt(var)


def _std(values: List[float]) -> Optional[float]:
    if not values:
        return None
    _, std = _rolling_mean_std(values)
    return std


def _skew_kurt(values: List[float]) -> Tuple[Optional[float], Optional[float]]:
    mean, std = _rolling_mean_std(values)
    if mean is None or std is None or std == 0:
        return None, None
    n = len(values)
    skew = sum(((v - mean) / std) ** 3 for v in values) / n
    kurt = sum(((v - mean) / std) ** 4 for v in values) / n
    return skew, kurt


def _ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < period or period <= 0:
        return None
    alpha = 2.0 / (period + 1)
    ema_val = values[0]
    for v in values[1:]:
        ema_val = alpha * v + (1 - alpha) * ema_val
    return ema_val


def _macd_hist(values: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Optional[float]:
    if len(values) < slow or fast <= 0 or slow <= 0 or signal <= 0:
        return None
    ema_fast = values[0]
    ema_slow = values[0]
    alpha_fast = 2.0 / (fast + 1)
    alpha_slow = 2.0 / (slow + 1)
    macd_series: List[float] = []
    for v in values[1:]:
        ema_fast = alpha_fast * v + (1 - alpha_fast) * ema_fast
        ema_slow = alpha_slow * v + (1 - alpha_slow) * ema_slow
        macd_series.append(ema_fast - ema_slow)
    if len(macd_series) < signal:
        return None
    signal_val = _ema(macd_series, signal)
    if signal_val is None:
        return None
    return macd_series[-1] - signal_val


def _ols_slope(values: List[float]) -> Optional[float]:
    n = len(values)
    if n < 2:
        return None
    xs = list(range(n))
    sum_x = sum(xs)
    sum_y = sum(values)
    sum_xx = sum(x * x for x in xs)
    sum_xy = sum(x * y for x, y in zip(xs, values))
    denom = n * sum_xx - sum_x * sum_x
    if denom == 0:
        return None
    return (n * sum_xy - sum_x * sum_y) / denom


def _count_crossings(values: List[float]) -> Optional[int]:
    if not values:
        return None
    mean = sum(values) / len(values)
    prev_sign = None
    crossings = 0
    for v in values:
        sign = 1 if v - mean >= 0 else -1
        if prev_sign is not None and sign != prev_sign:
            crossings += 1
        prev_sign = sign
    return crossings


def _drawdown(values: List[float]) -> Optional[float]:
    if not values:
        return None
    peak = values[0]
    max_dd = 0.0
    for v in values:
        if v > peak:
            peak = v
        dd = peak - v
        if dd > max_dd:
            max_dd = dd
    return max_dd


def _rsi(values: List[float], period: int = 14) -> Optional[float]:
    if len(values) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(-period, 0):
        diff = values[i] - values[i - 1]
        if diff >= 0:
            gains += diff
        else:
            losses -= diff
    if gains + losses == 0:
        return None
    rs = gains / losses if losses != 0 else float("inf")
    return 100.0 - (100.0 / (1.0 + rs))


def _boll_pct_b(values: List[float], period: int = 20) -> Optional[float]:
    if len(values) < period:
        return None
    window = values[-period:]
    mean, std = _rolling_mean_std(window)
    if mean is None or std is None:
        return None
    lower = mean - 2 * std
    upper = mean + 2 * std
    if upper == lower:
        return None
    return (values[-1] - lower) / (upper - lower)


def _collect_window(values: List[Optional[float]], window: int) -> Optional[List[float]]:
    if len(values) < window:
        return None
    sub = values[-window:]
    if any(v is None for v in sub):
        return None
    return [float(v) for v in sub]  # type: ignore[arg-type]


def _collect_diff(values: List[Optional[float]], window: int) -> Optional[List[float]]:
    sub = _collect_window(values, window + 1)
    if not sub:
        return None
    return [sub[i] - sub[i - 1] for i in range(1, len(sub))]


def _min_valid_count(window: int, ratio: float) -> int:
    return max(2, int(math.ceil(window * ratio)))


def _collect_window_allow_missing(
    values: List[Optional[float]], window: int, min_valid_ratio: float
) -> Optional[List[float]]:
    if len(values) < window:
        return None
    sub = values[-window:]
    valid = [float(v) for v in sub if v is not None]
    if len(valid) < _min_valid_count(window, min_valid_ratio):
        return None
    return valid


def _collect_window_xy(
    values: List[Optional[float]], window: int, min_valid_ratio: float
) -> Optional[Tuple[List[float], List[float]]]:
    if len(values) < window:
        return None
    sub = values[-window:]
    xs: List[float] = []
    ys: List[float] = []
    for i, v in enumerate(sub):
        if v is None:
            continue
        xs.append(float(i))
        ys.append(float(v))
    if len(ys) < _min_valid_count(window, min_valid_ratio):
        return None
    return xs, ys


def _collect_diff_allow_missing(
    values: List[Optional[float]], window: int, min_valid_ratio: float
) -> Optional[List[float]]:
    if len(values) < window + 1:
        return None
    sub = values[-(window + 1) :]
    diffs: List[float] = []
    prev = None
    for v in sub:
        if v is None:
            prev = None
            continue
        if prev is not None:
            diffs.append(float(v) - float(prev))
        prev = v
    if len(diffs) < _min_valid_count(window - 1, min_valid_ratio):
        return None
    return diffs


def _ols_slope_xy(xs: List[float], ys: List[float]) -> Optional[float]:
    n = len(xs)
    if n < 2:
        return None
    sum_x = sum(xs)
    sum_y = sum(ys)
    sum_xx = sum(x * x for x in xs)
    sum_xy = sum(x * y for x, y in zip(xs, ys))
    denom = n * sum_xx - sum_x * sum_x
    if denom == 0:
        return None
    return (n * sum_xy - sum_x * sum_y) / denom


def _value_at(values: List[Optional[float]], idx: int, max_backfill: int) -> Optional[float]:
    n = len(values)
    pos = idx if idx >= 0 else n + idx
    if pos < 0 or pos >= n:
        return None
    if values[pos] is not None:
        return float(values[pos])  # type: ignore[return-value]
    for step in range(1, max_backfill + 1):
        p = pos + step
        if p < n and values[p] is not None:
            return float(values[p])  # type: ignore[return-value]
    for step in range(1, max_backfill + 1):
        p = pos - step
        if p >= 0 and values[p] is not None:
            return float(values[p])  # type: ignore[return-value]
    return None


def _zscore(values: List[float]) -> Optional[float]:
    if len(values) < 2:
        return None
    mean, std = _rolling_mean_std(values)
    if mean is None or std is None or std == 0:
        return None
    return (values[-1] - mean) / std


def _percentile_rank(values: List[float], value: float) -> Optional[float]:
    if not values:
        return None
    sorted_vals = sorted(values)
    count = sum(1 for v in sorted_vals if v <= value)
    return count / len(sorted_vals)


def compute_event_series_factors(
    sqlite_path: str,
    event_ts: datetime,
    short_leg: Dict[str, Any],
    long_leg: Dict[str, Any],
    *,
    lookback_min: int = DEFAULT_LOOKBACK_MIN,
    tol_sec: int = DEFAULT_ASOF_TOL_SEC,
    ffill_bars: int = DEFAULT_FFILL_BARS,
    min_valid_ratio: float = DEFAULT_MIN_VALID_RATIO,
    max_backfill_bars: int = DEFAULT_MAX_BACKFILL_BARS,
    series_cache: Optional[SeriesCache] = None,
) -> Tuple[Dict[str, Optional[float]], Dict[str, Any]]:
    if event_ts.tzinfo is None:
        event_ts = event_ts.replace(tzinfo=timezone.utc)
    start_ts = event_ts - timedelta(minutes=lookback_min)
    grid = [start_ts + timedelta(minutes=i) for i in range(lookback_min + 1)]

    meta: Dict[str, Any] = {
        "series_start": start_ts.isoformat(),
        "series_end": event_ts.isoformat(),
        "lookback_min": lookback_min,
    }

    with sqlite3.connect(sqlite_path, timeout=15.0) as conn:
        s_short = _load_leg_series_cached(
            conn,
            str(short_leg.get("exchange") or ""),
            str(short_leg.get("symbol") or ""),
            str(short_leg.get("kind") or "perp"),
            start_ts,
            event_ts,
            series_cache,
        )
        s_long = _load_leg_series_cached(
            conn,
            str(long_leg.get("exchange") or ""),
            str(long_leg.get("symbol") or ""),
            str(long_leg.get("kind") or "perp"),
            start_ts,
            event_ts,
            series_cache,
        )

    aligned_short, meta_short = _align_series(s_short, grid, tol_sec, ffill_bars)
    aligned_long, meta_long = _align_series(s_long, grid, tol_sec, ffill_bars)
    meta["short_leg"] = {
        "exchange": short_leg.get("exchange"),
        "symbol": short_leg.get("symbol"),
        "kind": short_leg.get("kind"),
    }
    meta["long_leg"] = {
        "exchange": long_leg.get("exchange"),
        "symbol": long_leg.get("symbol"),
        "kind": long_leg.get("kind"),
    }
    meta["short_quality"] = meta_short
    meta["long_quality"] = meta_long

    spread_series: List[Optional[float]] = []
    edge_series: List[Optional[float]] = []
    for i in range(len(grid)):
        s = aligned_short[i]
        l = aligned_long[i]
        if s is None or l is None:
            spread_series.append(None)
            edge_series.append(None)
            continue
        s_price, s_fr, s_iv, s_nft = s
        l_price, l_fr, l_iv, l_nft = l
        if s_price <= 0 or l_price <= 0:
            spread_series.append(None)
            edge_series.append(None)
            continue
        spread_series.append(math.log(s_price / l_price))
        if s_fr is None or l_fr is None:
            edge_series.append(None)
        else:
            edge_series.append(float(s_fr) - float(l_fr))

    def _series_at(values: List[Optional[float]], offset_min: int) -> Optional[float]:
        idx = -(offset_min + 1)
        if abs(idx) > len(values):
            return None
        v0 = _value_at(values, -1, max_backfill_bars)
        v1 = _value_at(values, idx, max_backfill_bars)
        if v0 is None or v1 is None:
            return None
        return float(v0) - float(v1)

    factors: Dict[str, Optional[float]] = {}

    for delta in (1, 5, 15, 30, 60, 240):
        factors[f"spread_ret_{delta}m"] = _series_at(spread_series, delta)

    for window in (5, 15, 30, 60, 240):
        window_xy = _collect_window_xy(spread_series, window + 1, min_valid_ratio)
        if window_xy:
            xs, ys = window_xy
            factors[f"spread_slope_{window}m"] = _ols_slope_xy(xs, ys)
        else:
            factors[f"spread_slope_{window}m"] = None

    slope_5 = factors.get("spread_slope_5m")
    slope_30 = factors.get("spread_slope_30m")
    ret_5 = factors.get("spread_ret_5m")
    ret_30 = factors.get("spread_ret_30m")
    reversal = None
    if slope_5 is not None and slope_30 is not None:
        reversal = 1.0 if (slope_5 >= 0) != (slope_30 >= 0) else 0.0
    elif ret_5 is not None and ret_30 is not None:
        reversal = 1.0 if (ret_5 * ret_30) < 0 else 0.0
    factors["spread_reversal_15m"] = reversal

    for window in (60, 240):
        window_vals = _collect_window_allow_missing(spread_series, window + 1, min_valid_ratio)
        factors[f"spread_crossings_{window}m"] = _count_crossings(window_vals) if window_vals else None

    for window in (15, 30, 60, 240):
        diffs = _collect_diff_allow_missing(spread_series, window, min_valid_ratio)
        factors[f"spread_vol_{window}m"] = _std(diffs) if diffs else None

    vol_30 = factors.get("spread_vol_30m")
    vol_240 = factors.get("spread_vol_240m")
    factors["spread_vol_ratio_30_over_240"] = (vol_30 / vol_240) if vol_30 and vol_240 else None

    window_vals_240 = _collect_window_allow_missing(spread_series, 241, min_valid_ratio)
    factors["spread_drawdown_240m"] = _drawdown(window_vals_240) if window_vals_240 else None
    diffs_240 = _collect_diff_allow_missing(spread_series, 240, min_valid_ratio)
    skew_240, kurt_240 = _skew_kurt(diffs_240) if diffs_240 else (None, None)
    factors["spread_skew_240m"] = skew_240
    factors["spread_kurt_240m"] = kurt_240

    rsi_vals = _collect_window_allow_missing(spread_series, 15, min_valid_ratio)
    factors["spread_rsi_14"] = _rsi(rsi_vals) if rsi_vals else None

    macd_vals = _collect_window_allow_missing(spread_series, 60, min_valid_ratio)  # enough for EMA26
    factors["spread_macd_hist"] = _macd_hist(macd_vals) if macd_vals else None

    factors["spread_boll_pct_b"] = _boll_pct_b(_collect_window_allow_missing(spread_series, 20, min_valid_ratio) or [])

    for delta in (60, 180, 360, 1440, 4320, 10080, 21600):
        factors[f"funding_edge_change_{delta}m"] = _series_at(edge_series, delta)

    edge_240 = _collect_window(edge_series, 241)
    if edge_240:
        edge_vals = [v for v in edge_240 if v is not None]
    else:
        edge_vals = []
    factors["funding_edge_vol_240m"] = _std(edge_vals) if edge_vals else None

    edge_now = edge_series[-1] if edge_series else None
    factors["funding_edge_short_minus_long"] = float(edge_now) if edge_now is not None else None

    interval_min = None
    interval_assumed = False
    for iv in (short_leg.get("funding_interval_hours"), long_leg.get("funding_interval_hours")):
        if iv and float(iv) > 0:
            interval_min = float(iv) * 60.0 if interval_min is None else min(interval_min, float(iv) * 60.0)
    if interval_min is None:
        ex = str(short_leg.get("exchange") or "").lower()
        interval_h = DEFAULT_FUNDING_INTERVAL_HOURS.get(ex)
        if interval_h:
            interval_min = interval_h * 60.0
            interval_assumed = True
    if edge_now is not None and interval_min:
        factors["carry_net_cost_horizon_240m"] = float(edge_now) * (240.0 / float(interval_min))
    else:
        factors["carry_net_cost_horizon_240m"] = None
    meta["interval_assumed"] = interval_assumed

    # T2: multi-timeframe momentum
    for delta in (90, 120, 180, 360, 720, 1440, 4320, 10080, 21600):
        factors[f"spread_ret_{delta}m"] = _series_at(spread_series, delta)

    for window in (90, 120, 180, 360, 720, 1440, 4320, 10080, 21600):
        window_xy = _collect_window_xy(spread_series, window + 1, min_valid_ratio)
        if window_xy:
            xs, ys = window_xy
            factors[f"spread_slope_{window}m"] = _ols_slope_xy(xs, ys)
        else:
            factors[f"spread_slope_{window}m"] = None

    # T2: exhaustion / turning point
    exhaustion = None
    if slope_5 is not None and slope_30 is not None and ret_5 is not None and ret_30 is not None:
        if abs(slope_5) < abs(slope_30) and (ret_5 * ret_30) < 0:
            exhaustion = 1.0
        else:
            exhaustion = 0.0
    factors["exhaustion"] = exhaustion

    tp_window = _collect_window_allow_missing(spread_series, 61, min_valid_ratio)
    if tp_window and len(tp_window) >= 3:
        second_diff = []
        for i in range(2, len(tp_window)):
            second_diff.append(tp_window[i] - 2 * tp_window[i - 1] + tp_window[i - 2])
        factors["turning_point_score"] = _zscore(second_diff) if second_diff else None
    else:
        factors["turning_point_score"] = None

    # T2: zscore windows
    for minutes, label in ((60, "1h"), (240, "4h"), (720, "12h"), (4320, "3d"), (10080, "7d"), (21600, "15d")):
        window_vals = _collect_window_allow_missing(spread_series, minutes + 1, min_valid_ratio)
        factors[f"spread_zscore_{label}"] = _zscore(window_vals) if window_vals else None

    # T2: hurst proxy
    hurst_vals = _collect_window_allow_missing(spread_series, 241, min_valid_ratio)
    if hurst_vals and len(hurst_vals) >= 3:
        diff1 = [hurst_vals[i] - hurst_vals[i - 1] for i in range(1, len(hurst_vals))]
        diff2 = [hurst_vals[i] - hurst_vals[i - 2] for i in range(2, len(hurst_vals))]
        std1 = _std(diff1) if diff1 else None
        std2 = _std(diff2) if diff2 else None
        if std1 and std2 and std1 > 0 and std2 > 0:
            factors["hurst_proxy"] = math.log(std2 / std1) / math.log(2.0)
        else:
            factors["hurst_proxy"] = None
    else:
        factors["hurst_proxy"] = None

    # T2: funding regimes
    edge_vals = [v for v in edge_series if v is not None]
    if edge_now is not None and edge_vals:
        factors["funding_regime_quantile"] = _percentile_rank(edge_vals, float(edge_now))
    else:
        factors["funding_regime_quantile"] = None

    regime_label = None
    regime_val = None
    if edge_now is not None and edge_vals:
        mean_edge, std_edge = _rolling_mean_std(edge_vals)
        if mean_edge is not None and std_edge is not None and std_edge > 0:
            edge_change_60 = factors.get("funding_edge_change_60m")
            if edge_change_60 is not None and abs(edge_change_60) > 2 * std_edge:
                regime_val = 2.0 if edge_change_60 > 0 else -2.0
                regime_label = "jump"
            elif edge_now > mean_edge + std_edge:
                regime_val = 1.0
                regime_label = "high"
            elif edge_now < mean_edge - std_edge:
                regime_val = -1.0
                regime_label = "low"
            else:
                regime_val = 0.0
                regime_label = "mid"
    factors["funding_edge_regime"] = regime_val
    meta["funding_edge_regime_label"] = regime_label

    # T2: time to next funding
    def _next_time(val: Any) -> Optional[datetime]:
        if isinstance(val, datetime):
            return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
        if isinstance(val, str):
            try:
                ts = datetime.fromisoformat(val)
                return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
            except Exception:
                return None
        return None

    next_candidates = []
    for leg in (short_leg, long_leg):
        if leg.get("kind") != "perp":
            continue
        nt = _next_time(leg.get("next_funding_time"))
        if nt:
            next_candidates.append(nt)
    if next_candidates:
        next_ts = min(next_candidates)
        factors["time_to_next_funding_min"] = (next_ts - event_ts).total_seconds() / 60.0
    else:
        factors["time_to_next_funding_min"] = None

    # Drop non-finite values
    for k, v in list(factors.items()):
        if v is None:
            continue
        if isinstance(v, (int, float)) and (math.isinf(v) or math.isnan(v)):
            factors[k] = None

    return factors, meta
