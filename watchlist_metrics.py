import sqlite3
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from config import WATCHLIST_METRICS_CONFIG


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def _time_to_next_hour(now: datetime) -> float:
    """返回距离下一个整点的分钟数。"""
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    return (next_hour - now).total_seconds() / 60.0


def _is_funding_minute(ts: datetime) -> bool:
    """
    Binance 资金费率结算在整点，排除整点前后 1 分钟的数据点。
    过滤 minute in {59, 0, 1}，防止异常尖点影响统计。
    """
    return ts.minute in {59, 0, 1}


@dataclass
class SpreadPoint:
    ts: datetime
    spot: float
    futures: float

    @property
    def spread_rel(self) -> Optional[float]:
        if self.spot:
            return (self.spot - self.futures) / self.spot
        return None


@dataclass
class SpreadMetrics:
    last_spread: Optional[float]
    spread_mean: Optional[float]
    spread_std: Optional[float]
    baseline_rel: Optional[float]
    recent_slope: Optional[float]
    volatility: Optional[float]
    range_short: Optional[float]
    range_long: Optional[float]
    crossings_1h: int
    drift_ratio: Optional[float]
    entry_condition1: bool
    entry_condition2: bool
    take_profit_trigger: bool
    stop_loss_trigger: bool
    funding_exit_window: bool
    hits_above_limit: int
    limit_threshold: Optional[float]
    details: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["timestamp"] = _now_utc().isoformat()
        return payload


def _rolling_mean_std(values: List[float]) -> Tuple[Optional[float], Optional[float]]:
    n = len(values)
    if n == 0:
        return None, None
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n
    std = var ** 0.5
    return mean, std


def _range(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return max(values) - min(values)


def _crossing_count(series: List[float], midline: List[float]) -> int:
    """统计序列与中线的穿越次数（符号变化次数）。"""
    count = 0
    for i in range(1, len(series)):
        if midline[i] is None or midline[i - 1] is None:
            continue
        a = series[i] - midline[i]
        b = series[i - 1] - midline[i - 1]
        if a == 0:
            continue
        if a * b < 0:
            count += 1
    return count


def _midline(series: List[float], window: int) -> List[Optional[float]]:
    """简单滑动平均中线。"""
    out: List[Optional[float]] = []
    acc: List[float] = []
    for v in series:
        if v is None:
            acc.append(0.0)
        else:
            acc.append(v)
        if len(acc) > window:
            acc.pop(0)
        out.append(sum(acc) / len(acc))
    return out


def fetch_spread_series(
    db_path: str,
    symbols: List[str],
    *,
    hours: int = 6,
) -> Dict[str, List[SpreadPoint]]:
    """
    从 sqlite 抽取分钟级现货/永续价格，返回按 symbol 分组的 SpreadPoint 列表（升序）。
    仅使用 Binance 作为 watchlist 数据来源。
    """
    cutoff_sql = f"-{int(hours)} hours"
    results: Dict[str, List[SpreadPoint]] = {s: [] for s in symbols}
    try:
        conn = sqlite3.connect(db_path, timeout=15.0)
        cursor = conn.cursor()
        for symbol in symbols:
            cursor.execute(
                """
                SELECT timestamp, spot_price_close, futures_price_close
                FROM price_data_1min
                WHERE symbol = ? AND exchange = 'binance'
                  AND timestamp >= datetime('now', ?)
                ORDER BY timestamp ASC
                """,
                (symbol, cutoff_sql),
            )
            rows = cursor.fetchall()
            for ts_raw, spot, fut in rows:
                ts = _parse_ts(ts_raw)
                if ts is None or spot is None or fut is None:
                    continue
                if spot <= 0 or fut <= 0:
                    continue
                results[symbol].append(SpreadPoint(ts=ts, spot=float(spot), futures=float(fut)))
    except Exception as exc:
        # 失败时返回空，调用方应处理
        print(f"fetch_spread_series failed: {exc}")
    return results


def compute_metrics_for_symbol(points: List[SpreadPoint]) -> SpreadMetrics:
    """
    针对单个 symbol 计算价差相关指标，仅做监控输出，不做交易动作。
    """
    cfg = WATCHLIST_METRICS_CONFIG
    window_minutes = int(cfg.get("window_minutes", 60))
    slope_minutes = int(cfg.get("slope_minutes", 3))
    midline_minutes = int(cfg.get("midline_minutes", 15))
    range_short_h = float(cfg.get("range_hours_short", 1))
    range_long_h = float(cfg.get("range_hours_long", 6))
    drift_ratio_max = float(cfg.get("drift_ratio_max", 0.3))
    spread_abs_baseline = float(cfg.get("spread_abs_baseline", 0.01))

    if not points:
        return SpreadMetrics(
            last_spread=None,
            spread_mean=None,
            spread_std=None,
            baseline_rel=None,
            recent_slope=None,
            volatility=None,
            range_short=None,
            range_long=None,
            crossings_1h=0,
            drift_ratio=None,
            entry_condition1=False,
            entry_condition2=False,
            take_profit_trigger=False,
            stop_loss_trigger=False,
            funding_exit_window=False,
            hits_above_limit=0,
            limit_threshold=None,
            details={"reason": "no_data"},
        )

    spreads_rel: List[Optional[float]] = [p.spread_rel for p in points]
    times = [p.ts for p in points]
    last_spread = spreads_rel[-1]

    # 滚动窗口（基于分钟点数）
    window_values = [v for v in spreads_rel[-window_minutes:] if v is not None]
    spread_mean, spread_std = _rolling_mean_std(window_values)
    baseline_rel = None
    if spread_mean is not None and spread_std is not None:
        baseline_rel = spread_mean - 1.5 * spread_std

    # 斜率：最近 slope_minutes 内的均值差 / 窗口长度
    recent_slope = None
    if len(spreads_rel) >= slope_minutes + 1:
        recent_window = [v for v in spreads_rel[-slope_minutes - 1 :] if v is not None]
        if len(recent_window) >= 2:
            recent_slope = (recent_window[-1] - recent_window[0]) / slope_minutes

    volatility = spread_std  # 与 spread_std 一致，语义强调波动率

    # 区间与过滤资金费率分钟
    def _filter_range(hours: float) -> List[float]:
        cutoff = times[-1] - timedelta(hours=hours)
        vals: List[float] = []
        for ts, val in zip(times, spreads_rel):
            if val is None or ts < cutoff:
                continue
            if _is_funding_minute(ts):
                continue
            vals.append(val)
        return vals

    range_short_vals = _filter_range(range_short_h)
    range_long_vals = _filter_range(range_long_h)
    range_short = _range(range_short_vals)
    range_long = _range(range_long_vals)

    # 中线与穿越次数（1h内，15m中线）
    one_hour_cutoff = times[-1] - timedelta(hours=1)
    one_hour_series = [v for ts, v in zip(times, spreads_rel) if v is not None and ts >= one_hour_cutoff]
    midline_series = _midline(one_hour_series, midline_minutes) if one_hour_series else []
    crossings = _crossing_count(one_hour_series, midline_series) if one_hour_series else 0
    drift_ratio = None
    if one_hour_series:
        mid_last = midline_series[-1] if midline_series else None
        if mid_last is not None and range_short:
            drift_ratio = abs(one_hour_series[-1] - mid_last) / range_short if range_short else None

    # 触发条件（入场逻辑的计算结果，不执行动作）
    entry_condition1 = (
        last_spread is not None
        and baseline_rel is not None
        and last_spread < baseline_rel
        and last_spread < spread_abs_baseline
        and (recent_slope is not None and recent_slope < 0)
    )

    entry_condition2 = (
        (volatility or 0) >= float(cfg.get("volatility_threshold", 0.0))
        and (range_short or 0) >= float(cfg.get("range_threshold_short", 0.0))
        and (range_long or 0) >= float(cfg.get("range_threshold_long", 0.0))
        and crossings >= int(cfg.get("crossing_min_count", 0))
        and (drift_ratio is not None and drift_ratio <= drift_ratio_max)
    )

    # 平仓指标
    now = _now_utc()
    minutes_to_next_hour = _time_to_next_hour(now)
    time_factor = max(0.2, min(1.0, minutes_to_next_hour / 60.0))
    limit_threshold = None
    hits_above_limit = 0
    take_profit_trigger = False
    stop_loss_trigger = False

    if spread_mean is not None and spread_std is not None:
        limit_threshold = spread_mean + float(cfg.get("take_profit_multiplier", 0.5)) * time_factor * spread_std
        lookback_values = [v for v in window_values if v is not None]
        hits_above_limit = sum(1 for v in lookback_values if v >= limit_threshold)
        take_profit_trigger = hits_above_limit >= 3 and (recent_slope is not None and recent_slope < 0)
        stop_loss_trigger = last_spread is not None and last_spread <= -(limit_threshold) - float(
            cfg.get("stop_loss_buffer", 0.005)
        )

    funding_exit_window = minutes_to_next_hour <= float(cfg.get("funding_exit_minutes", 5))

    details = {
        "minutes_to_next_funding": minutes_to_next_hour,
        "time_factor": time_factor,
        "midline_last": midline_series[-1] if midline_series else None,
    }

    return SpreadMetrics(
        last_spread=last_spread,
        spread_mean=spread_mean,
        spread_std=spread_std,
        baseline_rel=baseline_rel,
        recent_slope=recent_slope,
        volatility=volatility,
        range_short=range_short,
        range_long=range_long,
        crossings_1h=crossings,
        drift_ratio=drift_ratio,
        entry_condition1=entry_condition1,
        entry_condition2=entry_condition2,
        take_profit_trigger=take_profit_trigger,
        stop_loss_trigger=stop_loss_trigger,
        funding_exit_window=funding_exit_window,
        hits_above_limit=hits_above_limit,
        limit_threshold=limit_threshold,
        details=details,
    )


def compute_metrics_for_symbols(db_path: str, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    批量计算 watchlist active 符号的指标，返回 {symbol: metrics_dict}
    """
    series_map = fetch_spread_series(db_path, symbols, hours=int(WATCHLIST_METRICS_CONFIG.get("range_hours_long", 6)))
    out: Dict[str, Dict[str, Any]] = {}
    for symbol, points in series_map.items():
        metrics = compute_metrics_for_symbol(points)
        out[symbol] = metrics.to_dict()
    return out


def compute_series_with_signals(db_path: str, symbols: List[str]) -> Dict[str, Any]:
    """
    生成 6h 价差序列及信号点，便于前端画图。仅用于可视化，不做交易决策。
    """
    cfg = WATCHLIST_METRICS_CONFIG
    window_minutes = int(cfg.get("window_minutes", 60))
    slope_minutes = int(cfg.get("slope_minutes", 3))
    midline_minutes = int(cfg.get("midline_minutes", 15))
    range_short_h = float(cfg.get("range_hours_short", 1))
    range_long_h = float(cfg.get("range_hours_long", 6))
    spread_abs_baseline = float(cfg.get("spread_abs_baseline", 0.01))
    drift_ratio_max = float(cfg.get("drift_ratio_max", 0.3))

    series_map = fetch_spread_series(db_path, symbols, hours=int(range_long_h))
    out: Dict[str, Any] = {}

    for symbol, points in series_map.items():
        cutoff = _now_utc() - timedelta(hours=range_long_h)
        # 显式截取到最近窗口，防止时区误差导致序列无限增长；再做上限裁剪保护前端
        points = [p for p in points if p.ts >= cutoff]
        if len(points) > 1000:
            points = points[-1000:]
        if not points:
            out[symbol] = {"points": [], "midline": [], "baseline": [], "entry_signals": [], "exit_signals": []}
            continue

        spreads = [p.spread_rel for p in points]
        times = [p.ts for p in points]

        midline_series: List[Optional[float]] = []
        acc_mid: List[float] = []
        for v in spreads:
            acc_mid.append(0.0 if v is None else v)
            if len(acc_mid) > midline_minutes:
                acc_mid.pop(0)
            midline_series.append(sum(acc_mid) / len(acc_mid))

        baseline_series: List[Optional[float]] = []
        entry_signals: List[Dict[str, Any]] = []
        exit_signals: List[Dict[str, Any]] = []

        for i, (ts, val) in enumerate(zip(times, spreads)):
            # 滚动窗口（按索引近似分钟）
            start_idx = max(0, i - window_minutes + 1)
            window_vals = [v for v in spreads[start_idx : i + 1] if v is not None]
            mean, std = _rolling_mean_std(window_vals)
            baseline = None
            if mean is not None and std is not None:
                baseline = mean - 1.5 * std
            baseline_series.append(baseline)

            # 斜率
            slope_val = None
            if i - slope_minutes >= 0 and spreads[i] is not None and spreads[i - slope_minutes] is not None:
                slope_val = (spreads[i] - spreads[i - slope_minutes]) / slope_minutes

            # range 过滤资金费分钟
            cutoff_short = ts - timedelta(hours=range_short_h)
            cutoff_long = ts - timedelta(hours=range_long_h)
            range_short_vals = [
                v for t, v in zip(times, spreads) if v is not None and t >= cutoff_short and not _is_funding_minute(t)
            ]
            range_long_vals = [
                v for t, v in zip(times, spreads) if v is not None and t >= cutoff_long and not _is_funding_minute(t)
            ]
            range_short = _range(range_short_vals)
            range_long = _range(range_long_vals)

            # crossings in last 1h
            one_hour_cutoff = ts - timedelta(hours=1)
            one_hour_series = [
                v for t, v in zip(times, spreads) if v is not None and t >= one_hour_cutoff
            ]
            one_hour_mid = [m for t, m in zip(times, midline_series) if t >= one_hour_cutoff]
            crossings = _crossing_count(one_hour_series, one_hour_mid) if one_hour_series else 0
            drift_ratio = None
            if one_hour_series and range_short:
                mid_last = one_hour_mid[-1] if one_hour_mid else None
                if mid_last is not None and range_short:
                    drift_ratio = abs(one_hour_series[-1] - mid_last) / range_short

            # 条件
            entry_condition1 = (
                val is not None
                and baseline is not None
                and val < baseline
                and val < spread_abs_baseline
                and (slope_val is not None and slope_val < 0)
            )
            entry_condition2 = (
                (std or 0) >= float(cfg.get("volatility_threshold", 0.0))
                and (range_short or 0) >= float(cfg.get("range_threshold_short", 0.0))
                and (range_long or 0) >= float(cfg.get("range_threshold_long", 0.0))
                and crossings >= int(cfg.get("crossing_min_count", 0))
                and (drift_ratio is not None and drift_ratio <= drift_ratio_max)
            )

            # 平仓信号
            minutes_to_next_hour = _time_to_next_hour(ts)
            time_factor = max(0.2, min(1.0, minutes_to_next_hour / 60.0))
            limit_threshold = None
            take_profit_trigger = False
            stop_loss_trigger = False
            if mean is not None and std is not None:
                limit_threshold = mean + float(cfg.get("take_profit_multiplier", 0.5)) * time_factor * std
                hits_above = sum(1 for v in window_vals if v is not None and v >= limit_threshold)
                take_profit_trigger = hits_above >= 3 and (slope_val is not None and slope_val < 0)
                stop_loss_trigger = val is not None and val <= -(limit_threshold) - float(
                    cfg.get("stop_loss_buffer", 0.005)
                )
            funding_exit_window = minutes_to_next_hour <= float(cfg.get("funding_exit_minutes", 5))

            if entry_condition1 and entry_condition2:
                entry_signals.append({"t": ts.isoformat(), "v": val})
            if take_profit_trigger:
                exit_signals.append({"t": ts.isoformat(), "v": val, "type": "tp"})
            if stop_loss_trigger:
                exit_signals.append({"t": ts.isoformat(), "v": val, "type": "sl"})
            if funding_exit_window:
                exit_signals.append({"t": ts.isoformat(), "v": val, "type": "funding"})

        out[symbol] = {
            "points": [{"t": t.isoformat(), "v": v} for t, v in zip(times, spreads) if v is not None],
            "midline": [{"t": t.isoformat(), "v": m} for t, m in zip(times, midline_series) if m is not None],
            "baseline": [{"t": t.isoformat(), "v": b} for t, b in zip(times, baseline_series) if b is not None],
            "entry_signals": entry_signals,
            "exit_signals": exit_signals,
        }

    return out
