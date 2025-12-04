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


def _minutes_to_next_funding(ts: datetime, next_ft: Optional[datetime], interval_hours: Optional[float]) -> Optional[float]:
    """优先使用下一次资金费时间，否则用周期近似；无法推断时返回 None。"""
    if next_ft:
        return (next_ft - ts).total_seconds() / 60.0
    if interval_hours and interval_hours > 0:
        interval_min = interval_hours * 60.0
        minute_in_day = ts.hour * 60.0 + ts.minute + ts.second / 60.0
        next_minute = ((int(minute_in_day / interval_min) + 1) * interval_min)
        return next_minute - minute_in_day
    return None


def _is_funding_minute(ts: datetime, interval_hours: Optional[float] = None, ref_time: Optional[datetime] = None) -> bool:
    """
    判断是否处于资金费率结算附近。
    优先使用下一次结算时间 ref_time（通常由交易所返回），否则根据 interval_hours 近似判断。
    """
    if ref_time:
        return abs((ts - ref_time).total_seconds()) <= 90
    if interval_hours and interval_hours > 0:
        # 粗略判断：以整点为锚点的周期（1h/2h/4h/8h）
        if ts.minute in {59, 0, 1}:
            try:
                return (ts.hour % int(round(interval_hours))) == 0
            except ZeroDivisionError:
                return False
    # 默认仍然排除整点附近的异常点
    return ts.minute in {59, 0, 1}


@dataclass
class SpreadPoint:
    ts: datetime
    spot: float
    futures: float
    funding_rate: Optional[float] = None
    funding_interval_hours: Optional[float] = None
    next_funding_time: Optional[datetime] = None

    @property
    def spread_rel(self) -> Optional[float]:
        if self.spot:
            return (self.spot - self.futures) / self.spot
        return None


@dataclass
class SpreadMetrics:
    last_spot_price: Optional[float]
    last_futures_price: Optional[float]
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


def _recent_values_within(
    times: List[datetime],
    values: List[Optional[float]],
    now_ts: datetime,
    window_minutes: int,
) -> List[float]:
    cutoff = now_ts - timedelta(minutes=window_minutes)
    out: List[float] = []
    for t, v in zip(times, values):
        if v is None:
            continue
        if t < cutoff or t > now_ts:
            continue
        out.append(v)
    return out


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
                SELECT timestamp, spot_price_close, futures_price_close,
                       funding_rate_avg, funding_interval_hours, next_funding_time
                FROM price_data_1min
                WHERE symbol = ? AND exchange = 'binance'
                  AND timestamp >= datetime('now', ?)
                ORDER BY timestamp ASC
                """,
                (symbol, cutoff_sql),
            )
            rows = cursor.fetchall()
            for ts_raw, spot, fut, fr, interval_hours, next_ft in rows:
                ts = _parse_ts(ts_raw)
                if ts is None or spot is None or fut is None:
                    continue
                if spot <= 0 or fut <= 0:
                    continue
                results[symbol].append(
                    SpreadPoint(
                        ts=ts,
                        spot=float(spot),
                        futures=float(fut),
                        funding_rate=float(fr) if fr is not None else None,
                        funding_interval_hours=float(interval_hours) if interval_hours is not None else None,
                        next_funding_time=_parse_ts(next_ft),
                    )
                )
    except Exception as exc:
        # 失败时返回空，调用方应处理
        print(f"fetch_spread_series failed: {exc}")
    return results


def compute_metrics_for_symbol(points: List[SpreadPoint]) -> SpreadMetrics:
    """
    针对单个 symbol 计算价差相关指标，仅做监控输出，不做交易动作。
    """
    cfg = WATCHLIST_METRICS_CONFIG
    tp_window_minutes = 20          # 局部峰值窗口
    tp_long_window_minutes = 120    # 止盈长窗
    cooldown_minutes = 10
    window_minutes = int(cfg.get("window_minutes", 60))
    slope_minutes = int(cfg.get("slope_minutes", 3))
    midline_minutes = int(cfg.get("midline_minutes", 15))
    crossing_window_h = float(cfg.get("crossing_window_hours", 3.0))
    crossing_mid_minutes = int(cfg.get("crossing_mid_minutes", 30))
    range_short_h = float(cfg.get("range_hours_short", 1))
    range_long_h = float(cfg.get("range_hours_long", 6))
    drift_ratio_max = float(cfg.get("drift_ratio_max", 0.3))
    spread_abs_baseline = float(cfg.get("spread_abs_baseline", 0.01))

    if not points:
        return SpreadMetrics(
            last_spot_price=None,
            last_futures_price=None,
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
    last_spot_price = points[-1].spot if points else None
    last_futures_price = points[-1].futures if points else None

    # 滚动窗口（基于分钟点数）
    window_values = [v for v in spreads_rel[-window_minutes:] if v is not None]
    if not window_values:
        # 兼容跨所数据点较稀疏的情况，退化到全量可用数据
        window_values = [v for v in spreads_rel if v is not None][-window_minutes:]
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
        for pt in points:
            val = pt.spread_rel
            if val is None or pt.ts < cutoff:
                continue
            if _is_funding_minute(pt.ts, pt.funding_interval_hours, pt.next_funding_time):
                continue
            vals.append(val)
        return vals

    range_short_vals = _filter_range(range_short_h)
    range_long_vals = _filter_range(range_long_h)
    if not range_short_vals:
        range_short_vals = [v for v in spreads_rel if v is not None][-max(1, int(range_short_h * 60)) :]
    if not range_long_vals:
        range_long_vals = [v for v in spreads_rel if v is not None][-max(1, int(range_long_h * 60)) :]
    range_short = _range(range_short_vals)
    range_long = _range(range_long_vals)

    # 中线与穿越次数（3h窗口，30m中线）
    crossing_cutoff = times[-1] - timedelta(hours=crossing_window_h)
    crossing_series = [v for ts, v in zip(times, spreads_rel) if v is not None and ts >= crossing_cutoff]
    if not crossing_series:
        crossing_series = [v for v in spreads_rel if v is not None][-crossing_mid_minutes * 2 :]
    midline_series = _midline(crossing_series, crossing_mid_minutes) if crossing_series else []
    crossings = _crossing_count(crossing_series, midline_series) if crossing_series else 0
    drift_ratio = None
    if crossing_series:
        mid_last = midline_series[-1] if midline_series else None
        if mid_last is not None and range_short:
            drift_ratio = abs(crossing_series[-1] - mid_last) / range_short if range_short else None

    # 触发条件（入场逻辑的计算结果，不执行动作）
    entry_condition1 = (
        last_spread is not None
        and baseline_rel is not None
        and last_spread < baseline_rel
        and last_spread < spread_abs_baseline
        and (recent_slope is not None and recent_slope <= 0)
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
    interval_hours_hint = points[-1].funding_interval_hours if points else None
    next_ft = points[-1].next_funding_time if points else None
    minutes_to_funding = None
    if next_ft:
        minutes_to_funding = (next_ft - now).total_seconds() / 60.0
    # 对于 4h/8h 资金费，阈值保持 1.0；仅 1h 周期按小时缩放
    if interval_hours_hint and interval_hours_hint >= 4:
        time_factor = 1.0
    else:
        time_factor = max(0.6, min(1.0, minutes_to_next_hour / 60.0))
    limit_threshold = None
    hits_above_limit = 0
    take_profit_trigger = False
    stop_loss_trigger = False

    if spread_mean is not None and spread_std is not None:
        long_window_vals = _recent_values_within(times, spreads_rel, times[-1], tp_long_window_minutes)
        long_mean, long_std = _rolling_mean_std(long_window_vals)
        if long_mean is not None and long_std is not None:
            limit_threshold = long_mean + float(cfg.get("take_profit_multiplier", 1.2)) * long_std
            # 局部峰值确认：当前为近 20 分钟最高且开始回落（斜率<0）
            recent_peak_vals = _recent_values_within(times, spreads_rel, times[-1], tp_window_minutes)
            current_val = spreads_rel[-1]
            is_local_peak = False
            if current_val is not None and recent_peak_vals:
                is_local_peak = current_val >= max(recent_peak_vals)
            take_profit_trigger = (
                current_val is not None
                and current_val >= limit_threshold
                and is_local_peak
                and (recent_slope is not None and recent_slope < 0)
            )
            # 资费前提前落袋：距离资费 <30 分钟且价差高于短窗均值+0.5σ
            if minutes_to_funding is not None and minutes_to_funding <= 30:
                pre_funding_threshold = spread_mean + 0.5 * spread_std
                if current_val is not None and pre_funding_threshold is not None:
                    take_profit_trigger = take_profit_trigger or (current_val >= pre_funding_threshold)

        # 止损：连续 2 个点跌破 -(threshold + buffer) 且斜率向下
        stop_loss_threshold = -(limit_threshold) - float(cfg.get("stop_loss_buffer", 0.005))
        recent_pair = [v for v in spreads_rel[-2:] if v is not None]
        stop_loss_trigger = (
            len(recent_pair) == 2 and all(v <= stop_loss_threshold for v in recent_pair) and (recent_slope or 0) < 0
        )

    # 资金费离场：依据真实 funding 时间，无法推断则回退到整点
    minutes_to_funding = _minutes_to_next_funding(now, next_ft, interval_hours_hint)
    funding_exit_window = False
    if minutes_to_funding is not None:
        funding_exit_window = 0 <= minutes_to_funding <= float(cfg.get("funding_exit_minutes", 5))
    else:
        funding_exit_window = minutes_to_next_hour <= float(cfg.get("funding_exit_minutes", 5))

    details = {
        "minutes_to_next_funding": minutes_to_next_hour,
        "time_factor": time_factor,
        "midline_last": midline_series[-1] if midline_series else None,
    }

    return SpreadMetrics(
        last_spot_price=last_spot_price,
        last_futures_price=last_futures_price,
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
    tp_window_minutes = 20
    tp_long_window_minutes = 120
    crossing_window_h = float(cfg.get("crossing_window_hours", 3.0))
    crossing_mid_minutes = int(cfg.get("crossing_mid_minutes", 30))
    cooldown_minutes = 10
    sl_consecutive = 2
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
        if len(points) > 1500:
            points = points[-1500:]
        if not points:
            out[symbol] = {
                "points": [],
                "midline": [],
                "baseline": [],
                "entry_signals": [],
                "exit_signals": [],
                "spot": [],
                "futures": [],
                "funding_times": [],
                "funding_interval_hours": None,
            }
            continue

        spreads = [p.spread_rel for p in points]
        times = [p.ts for p in points]
        spot_prices = [p.spot for p in points]
        futures_prices = [p.futures for p in points]

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

        funding_times: List[str] = []

        last_tp_ts: Optional[datetime] = None
        last_sl_ts: Optional[datetime] = None
        last_funding_ts: Optional[datetime] = None

        for i, (ts, val, pt) in enumerate(zip(times, spreads, points)):
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
                p.spread_rel
                for p in points
                if p.spread_rel is not None
                and p.ts >= cutoff_short
                and not _is_funding_minute(p.ts, p.funding_interval_hours, p.next_funding_time)
            ]
            range_long_vals = [
                p.spread_rel
                for p in points
                if p.spread_rel is not None
                and p.ts >= cutoff_long
                and not _is_funding_minute(p.ts, p.funding_interval_hours, p.next_funding_time)
            ]
            range_short = _range(range_short_vals)
            range_long = _range(range_long_vals)

            # crossings in last 3h with 30m midline
            crossing_cutoff = ts - timedelta(hours=crossing_window_h)
            crossing_series = [v for t, v in zip(times, spreads) if v is not None and t >= crossing_cutoff]
            crossing_mid = _midline(crossing_series, crossing_mid_minutes) if crossing_series else []
            crossings = _crossing_count(crossing_series, crossing_mid) if crossing_series else 0
            drift_ratio = None
            if crossing_series and range_short:
                mid_last = crossing_mid[-1] if crossing_mid else None
                if mid_last is not None and range_short:
                    drift_ratio = abs(crossing_series[-1] - mid_last) / range_short

            # 条件
            entry_condition1 = (
                val is not None
                and baseline is not None
                and val < baseline
                and val < spread_abs_baseline
                and (slope_val is not None and slope_val <= 0)
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
            interval_hours_hint = pt.funding_interval_hours
            next_ft = pt.next_funding_time
            minutes_to_funding = _minutes_to_next_funding(ts, next_ft, interval_hours_hint)
            if interval_hours_hint and interval_hours_hint >= 4:
                time_factor = 1.0
            else:
                time_factor = max(0.6, min(1.0, minutes_to_next_hour / 60.0))
            limit_threshold = None
            take_profit_trigger = False
            stop_loss_trigger = False
            if mean is not None and std is not None:
                long_vals = _recent_values_within(times, spreads, ts, tp_long_window_minutes)
                long_mean, long_std = _rolling_mean_std(long_vals)
                if long_mean is not None and long_std is not None:
                    limit_threshold = long_mean + float(cfg.get("take_profit_multiplier", 1.2)) * long_std
                    peak_vals = _recent_values_within(times, spreads, ts, tp_window_minutes)
                    current_val = spreads[i]
                    is_local_peak = False
                    if current_val is not None and peak_vals:
                        is_local_peak = current_val >= max(peak_vals)
                    take_profit_trigger = (
                        current_val is not None
                        and current_val >= limit_threshold
                        and is_local_peak
                        and (slope_val is not None and slope_val < 0)
                    )
                    if minutes_to_funding is not None and minutes_to_funding <= 30:
                        pf_threshold = mean + 0.5 * std
                        if current_val is not None and pf_threshold is not None:
                            take_profit_trigger = take_profit_trigger or (current_val >= pf_threshold)

                sl_threshold = -(limit_threshold) - float(cfg.get("stop_loss_buffer", 0.005))
                recent_vals = [v for v in spreads[i - sl_consecutive + 1 : i + 1] if v is not None]
                stop_loss_trigger = (
                    len(recent_vals) == sl_consecutive and all(v <= sl_threshold for v in recent_vals) and (slope_val or 0) < 0
                )
            funding_exit_window = False
            if minutes_to_funding is not None:
                funding_exit_window = 0 <= minutes_to_funding <= float(cfg.get("funding_exit_minutes", 5))
            else:
                funding_exit_window = minutes_to_next_hour <= float(cfg.get("funding_exit_minutes", 5))

            if entry_condition1 and entry_condition2:
                entry_signals.append({"t": ts.isoformat(), "v": val})
            if take_profit_trigger:
                if not last_tp_ts or (ts - last_tp_ts) >= timedelta(minutes=max(cooldown_minutes, 15)):
                    exit_signals.append({"t": ts.isoformat(), "v": val, "type": "tp"})
                    last_tp_ts = ts
            if stop_loss_trigger:
                if not last_sl_ts or (ts - last_sl_ts) >= timedelta(minutes=cooldown_minutes):
                    exit_signals.append({"t": ts.isoformat(), "v": val, "type": "sl"})
                    last_sl_ts = ts
            if funding_exit_window:
                min_gap = max(10, int((interval_hours_hint or 1) * 20))
                if not last_funding_ts or (ts - last_funding_ts) >= timedelta(minutes=min_gap):
                    exit_signals.append({"t": ts.isoformat(), "v": val, "type": "funding"})
                    last_funding_ts = ts

            if pt.next_funding_time:
                funding_times.append(pt.next_funding_time.isoformat())

        out[symbol] = {
            "points": [{"t": t.isoformat(), "v": v} for t, v in zip(times, spreads) if v is not None],
            "midline": [{"t": t.isoformat(), "v": m} for t, m in zip(times, midline_series) if m is not None],
            "baseline": [{"t": t.isoformat(), "v": b} for t, b in zip(times, baseline_series) if b is not None],
            "spot": [{"t": t.isoformat(), "v": v} for t, v in zip(times, spot_prices) if v is not None],
            "futures": [{"t": t.isoformat(), "v": v} for t, v in zip(times, futures_prices) if v is not None],
            "entry_signals": entry_signals,
            "exit_signals": exit_signals,
            "funding_times": sorted(list({ft for ft in funding_times})),
            "funding_interval_hours": points[-1].funding_interval_hours if points else None,
            "entry_type": "A",
        }

    return out


def _series_from_points(points: List[SpreadPoint], entry_type: str = "A") -> Dict[str, Any]:
    """
    复用 compute_series_with_signals 的逻辑，但直接使用给定的 SpreadPoint 列表（用于 B/C）。
    """
    cfg = WATCHLIST_METRICS_CONFIG
    tp_window_minutes = 20
    tp_long_window_minutes = 120
    crossing_window_h = float(cfg.get("crossing_window_hours", 3.0))
    crossing_mid_minutes = int(cfg.get("crossing_mid_minutes", 30))
    cooldown_minutes = 10
    sl_consecutive = 2
    window_minutes = int(cfg.get("window_minutes", 60))
    slope_minutes = int(cfg.get("slope_minutes", 3))
    midline_minutes = int(cfg.get("midline_minutes", 15))
    range_short_h = float(cfg.get("range_hours_short", 1))
    range_long_h = float(cfg.get("range_hours_long", 6))
    spread_abs_baseline = float(cfg.get("spread_abs_baseline", 0.01))
    drift_ratio_max = float(cfg.get("drift_ratio_max", 0.3))

    cutoff = _now_utc() - timedelta(hours=range_long_h)
    points = [p for p in points if p.ts >= cutoff]
    if len(points) > 1500:
        points = points[-1500:]
    if not points:
        return {
            "points": [],
            "midline": [],
            "baseline": [],
            "entry_signals": [],
            "exit_signals": [],
            "spot": [],
            "futures": [],
            "funding_times": [],
            "funding_interval_hours": None,
            "entry_type": entry_type,
        }

    spreads = [p.spread_rel for p in points]
    times = [p.ts for p in points]
    spot_prices = [p.spot for p in points]
    futures_prices = [p.futures for p in points]

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
    funding_times: List[str] = []
    last_tp_ts: Optional[datetime] = None
    last_sl_ts: Optional[datetime] = None
    last_funding_ts: Optional[datetime] = None

    for i, (ts, val, pt) in enumerate(zip(times, spreads, points)):
        start_idx = max(0, i - window_minutes + 1)
        window_vals = [v for v in spreads[start_idx : i + 1] if v is not None]
        mean, std = _rolling_mean_std(window_vals)
        baseline = None
        if mean is not None and std is not None:
            baseline = mean - 1.5 * std
        baseline_series.append(baseline)

        slope_val = None
        if i - slope_minutes >= 0 and spreads[i] is not None and spreads[i - slope_minutes] is not None:
            slope_val = (spreads[i] - spreads[i - slope_minutes]) / slope_minutes

        cutoff_short = ts - timedelta(hours=range_short_h)
        cutoff_long = ts - timedelta(hours=range_long_h)
        range_short_vals = [
            p.spread_rel
            for p in points
            if p.spread_rel is not None
            and p.ts >= cutoff_short
            and not _is_funding_minute(p.ts, p.funding_interval_hours, p.next_funding_time)
        ]
        range_long_vals = [
            p.spread_rel
            for p in points
            if p.spread_rel is not None
            and p.ts >= cutoff_long
            and not _is_funding_minute(p.ts, p.funding_interval_hours, p.next_funding_time)
        ]
        range_short = _range(range_short_vals)
        range_long = _range(range_long_vals)

        crossing_cutoff = ts - timedelta(hours=crossing_window_h)
        crossing_series = [v for t, v in zip(times, spreads) if v is not None and t >= crossing_cutoff]
        crossing_mid = _midline(crossing_series, crossing_mid_minutes) if crossing_series else []
        crossings = _crossing_count(crossing_series, crossing_mid) if crossing_series else 0
        drift_ratio = None
        if crossing_series and range_short:
            mid_last = crossing_mid[-1] if crossing_mid else None
            if mid_last is not None and range_short:
                drift_ratio = abs(crossing_series[-1] - mid_last) / range_short

        entry_condition1 = (
            val is not None
            and baseline is not None
            and val < baseline
            and val < spread_abs_baseline
            and (slope_val is not None and slope_val <= 0)
        )
        entry_condition2 = (
            (std or 0) >= float(cfg.get("volatility_threshold", 0.0))
            and (range_short or 0) >= float(cfg.get("range_threshold_short", 0.0))
            and (range_long or 0) >= float(cfg.get("range_threshold_long", 0.0))
            and crossings >= int(cfg.get("crossing_min_count", 0))
            and (drift_ratio is not None and drift_ratio <= drift_ratio_max)
        )

        minutes_to_next_hour = _time_to_next_hour(ts)
        interval_hours_hint = pt.funding_interval_hours
        next_ft = pt.next_funding_time
        minutes_to_funding = _minutes_to_next_funding(ts, next_ft, interval_hours_hint)
        if interval_hours_hint and interval_hours_hint >= 4:
            time_factor = 1.0
        else:
            time_factor = max(0.6, min(1.0, minutes_to_next_hour / 60.0))
        limit_threshold = None
        take_profit_trigger = False
        stop_loss_trigger = False
        if mean is not None and std is not None:
            long_vals = _recent_values_within(times, spreads, ts, tp_long_window_minutes)
            long_mean, long_std = _rolling_mean_std(long_vals)
            if long_mean is not None and long_std is not None:
                limit_threshold = long_mean + float(cfg.get("take_profit_multiplier", 1.2)) * long_std
                peak_vals = _recent_values_within(times, spreads, ts, tp_window_minutes)
                current_val = spreads[i]
                is_local_peak = False
                if current_val is not None and peak_vals:
                    is_local_peak = current_val >= max(peak_vals)
                take_profit_trigger = (
                    current_val is not None
                    and current_val >= limit_threshold
                    and is_local_peak
                    and (slope_val is not None and slope_val < 0)
                )
                if minutes_to_funding is not None and minutes_to_funding <= 30:
                    pf_threshold = mean + 0.5 * std
                    if current_val is not None and pf_threshold is not None:
                        take_profit_trigger = take_profit_trigger or (current_val >= pf_threshold)

            sl_threshold = -(limit_threshold) - float(cfg.get("stop_loss_buffer", 0.005))
            recent_vals = [v for v in spreads[i - sl_consecutive + 1 : i + 1] if v is not None]
            stop_loss_trigger = (
                len(recent_vals) == sl_consecutive and all(v <= sl_threshold for v in recent_vals) and (slope_val or 0) < 0
            )
        funding_exit_window = False
        if minutes_to_funding is not None:
            funding_exit_window = 0 <= minutes_to_funding <= float(cfg.get("funding_exit_minutes", 5))
        else:
            funding_exit_window = minutes_to_next_hour <= float(cfg.get("funding_exit_minutes", 5))

        if entry_condition1 and entry_condition2:
            entry_signals.append({"t": ts.isoformat(), "v": val})
        if take_profit_trigger:
            if not last_tp_ts or (ts - last_tp_ts) >= timedelta(minutes=max(cooldown_minutes, 15)):
                exit_signals.append({"t": ts.isoformat(), "v": val, "type": "tp"})
                last_tp_ts = ts
        if stop_loss_trigger:
            if not last_sl_ts or (ts - last_sl_ts) >= timedelta(minutes=cooldown_minutes):
                exit_signals.append({"t": ts.isoformat(), "v": val, "type": "sl"})
                last_sl_ts = ts
        if funding_exit_window:
            min_gap = max(10, int((interval_hours_hint or 1) * 20))
            if not last_funding_ts or (ts - last_funding_ts) >= timedelta(minutes=min_gap):
                exit_signals.append({"t": ts.isoformat(), "v": val, "type": "funding"})
                last_funding_ts = ts

        if pt.next_funding_time:
            funding_times.append(pt.next_funding_time.isoformat())

    return {
        "points": [{"t": t.isoformat(), "v": v} for t, v in zip(times, spreads) if v is not None],
        "midline": [{"t": t.isoformat(), "v": m} for t, m in zip(times, midline_series) if m is not None],
        "baseline": [{"t": t.isoformat(), "v": b} for t, b in zip(times, baseline_series) if b is not None],
        "spot": [{"t": t.isoformat(), "v": v} for t, v in zip(times, spot_prices) if v is not None],
        "futures": [{"t": t.isoformat(), "v": v} for t, v in zip(times, futures_prices) if v is not None],
        "entry_signals": entry_signals,
        "exit_signals": exit_signals,
        "funding_times": sorted(list({ft for ft in funding_times})),
        "funding_interval_hours": points[-1].funding_interval_hours if points else None,
        "entry_type": entry_type,
    }


def _fetch_price_map(
    db_path: str,
    symbol: str,
    exchange: str,
    *,
    use_futures: bool,
    hours: int = 12,
    limit: int = 2000,
) -> Dict[str, float]:
    """按 timestamp -> price 返回价格映射，方便做交集，仅取最近窗口以降低 IO。"""
    field = "futures_price_close" if use_futures else "spot_price_close"
    out: Dict[str, float] = {}
    try:
        conn = sqlite3.connect(db_path, timeout=15.0)
        cursor = conn.cursor()
        cutoff_sql = f"-{int(hours)} hours"
        cursor.execute(
            f"""
            SELECT timestamp, {field}
            FROM price_data_1min
            WHERE symbol = ? AND exchange = ? AND timestamp >= datetime('now', ?)
            ORDER BY timestamp ASC
            LIMIT ?
            """,
            (symbol, exchange, cutoff_sql, limit),
        )
        for ts_raw, price in cursor.fetchall():
            if price is None or price <= 0:
                continue
            out[ts_raw] = float(price)
    except Exception as exc:
        print(f"_fetch_price_map failed {symbol} {exchange}: {exc}")
    return out


def compute_pair_spread_series(db_path: str, entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    为 Type B/C 生成跨交易所价差序列（简化版，不含信号）。
    Type B: futures vs futures；Type C: spot vs futures。
    """
    out: Dict[str, Any] = {}
    window_hours = 12
    for entry in entries:
        symbol = entry.get("symbol")
        etype = entry.get("entry_type")
        trigger = entry.get("trigger_details") or {}
        if etype == "B":
            pair = trigger.get("pair") or []
            if len(pair) != 2:
                continue
            a_ex, b_ex = pair
            price_a = _fetch_price_map(db_path, symbol, a_ex, use_futures=True, hours=window_hours)
            price_b = _fetch_price_map(db_path, symbol, b_ex, use_futures=True, hours=window_hours)
            common_ts = sorted(set(price_a.keys()) & set(price_b.keys()))
            points = []
            for ts in common_ts:
                base = min(price_a[ts], price_b[ts])
                if not base:
                    continue
                spread = abs(price_a[ts] - price_b[ts]) / base
                points.append({"t": ts, "v": spread})
            out[symbol] = {
                "entry_type": "B",
                "pair_exchanges": pair,
                "points": points,
                "price_a": [{"t": ts, "v": price_a[ts]} for ts in common_ts],
                "price_b": [{"t": ts, "v": price_b[ts]} for ts in common_ts],
            }
        elif etype == "C":
            spot_ex = trigger.get("spot_exchange")
            fut_ex = trigger.get("futures_exchange")
            if not spot_ex or not fut_ex:
                continue
            spot_prices = _fetch_price_map(db_path, symbol, spot_ex, use_futures=False, hours=window_hours)
            fut_prices = _fetch_price_map(db_path, symbol, fut_ex, use_futures=True, hours=window_hours)
            common_ts = sorted(set(spot_prices.keys()) & set(fut_prices.keys()))
            points = []
            for ts in common_ts:
                base = spot_prices[ts]
                if not base:
                    continue
                spread = (fut_prices[ts] - base) / base
                if spread is None:
                    continue
                points.append({"t": ts, "v": spread})
            out[symbol] = {
                "entry_type": "C",
                "pair_exchanges": [spot_ex, fut_ex],
                "points": points,
                "price_spot": [{"t": ts, "v": spot_prices[ts]} for ts in common_ts],
                "price_futures": [{"t": ts, "v": fut_prices[ts]} for ts in common_ts],
            }
    return out


def _build_cross_points(
    db_path: str,
    entry: Dict[str, Any],
    *,
    hours: int = 12,
    limit: int = 2000,
) -> List[SpreadPoint]:
    """
    将 Type B/C 的跨所价差转换为 SpreadPoint 序列，便于复用 Type A 的指标计算。
    现货视为“资金费率=0 且只能做多”的永续，将低价作为 spot，高价作为 futures，保持与 Type A 相同的方向（价差多为负数）。
    """
    etype = entry.get("entry_type")
    trigger = entry.get("trigger_details") or {}
    symbol = entry.get("symbol")
    points: List[SpreadPoint] = []

    def _parse_time(ts_raw: str) -> Optional[datetime]:
        try:
            return datetime.fromisoformat(ts_raw).replace(tzinfo=timezone.utc)
        except Exception:
            return None

    if etype == "B":
        pair = trigger.get("pair") or []
        if len(pair) != 2:
            return points
        a_ex, b_ex = pair
        price_a = _fetch_price_map(db_path, symbol, a_ex, use_futures=True, hours=hours, limit=limit)
        price_b = _fetch_price_map(db_path, symbol, b_ex, use_futures=True, hours=hours, limit=limit)
        common_ts = sorted(set(price_a.keys()) & set(price_b.keys()))
        funding_map = trigger.get("funding") or {}
        for ts_raw in common_ts:
            ts = _parse_time(ts_raw)
            if not ts:
                continue
            pa = price_a.get(ts_raw)
            pb = price_b.get(ts_raw)
            if not pa or not pb:
                continue
            # 低价视为“spot”，高价视为“futures”，保持 Type A 的 spread 定义
            if pa <= pb:
                spot_price, fut_price = pa, pb
                fr = funding_map.get(b_ex)
            else:
                spot_price, fut_price = pb, pa
                fr = funding_map.get(a_ex)
            points.append(
                SpreadPoint(
                    ts=ts,
                    spot=spot_price,
                    futures=fut_price,
                    funding_rate=float(fr) if fr is not None else None,
                    funding_interval_hours=None,
                    next_funding_time=None,
                )
            )
    elif etype == "C":
        spot_ex = trigger.get("spot_exchange")
        fut_ex = trigger.get("futures_exchange")
        if not spot_ex or not fut_ex:
            return points
        spot_prices = _fetch_price_map(db_path, symbol, spot_ex, use_futures=False, hours=hours, limit=limit)
        fut_prices = _fetch_price_map(db_path, symbol, fut_ex, use_futures=True, hours=hours, limit=limit)
        common_ts = sorted(set(spot_prices.keys()) & set(fut_prices.keys()))
        funding_map = trigger.get("funding") or {}
        for ts_raw in common_ts:
            ts = _parse_time(ts_raw)
            if not ts:
                continue
            sp = spot_prices.get(ts_raw)
            fp = fut_prices.get(ts_raw)
            if not sp or not fp:
                continue
            # 若极端情况下现货高于永续，仍以低价作为“spot”保持方向一致
            if sp <= fp:
                spot_price, fut_price = sp, fp
                fr = funding_map.get(fut_ex)
            else:
                spot_price, fut_price = fp, sp
                fr = funding_map.get(fut_ex)
            points.append(
                SpreadPoint(
                    ts=ts,
                    spot=spot_price,
                    futures=fut_price,
                    funding_rate=float(fr) if fr is not None else None,
                    funding_interval_hours=None,
                    next_funding_time=None,
                )
            )
    return points


def compute_metrics_for_entries(db_path: str, entries: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    统一计算 Type A/B/C 的价差指标，均复用 Type A 的 Spread 指标逻辑：
    - Type A：Binance 现货 vs 永续（原有逻辑）
    - Type B：任意两家永续，高价视为 futures，低价视为“现货”
    - Type C：现货 vs 任一期货，若现货反向高于期货，仍将低价视作“现货”以保持方向一致
    """
    metrics: Dict[str, Dict[str, Any]] = {}
    type_a_symbols = [e["symbol"] for e in entries if e.get("entry_type") == "A"]
    cross_entries = [e for e in entries if e.get("entry_type") in ("B", "C")]
    series_map: Dict[str, List[SpreadPoint]] = {}
    if type_a_symbols:
        series_map.update(
            fetch_spread_series(
                db_path,
                type_a_symbols,
                hours=int(WATCHLIST_METRICS_CONFIG.get("range_hours_long", 6)),
            )
        )
    # 构建跨所的 SpreadPoint 列表
    for entry in cross_entries:
        sym = entry.get("symbol")
        series_map[sym] = _build_cross_points(db_path, entry)

    for sym, points in series_map.items():
        metrics[sym] = compute_metrics_for_symbol(points).to_dict()
    return metrics


def compute_series_for_entries(db_path: str, entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    统一生成 Type A/B/C 的图表序列：
    - Type A：复用原有 compute_series_with_signals
    - Type B/C：使用跨所 SpreadPoint（低价=“spot”、高价=“futures”）复用同样的指标与曲线
    """
    out: Dict[str, Any] = {}
    type_a_symbols = [e["symbol"] for e in entries if e.get("entry_type") == "A"]
    cross_entries = [e for e in entries if e.get("entry_type") in ("B", "C")]
    range_long_h = int(WATCHLIST_METRICS_CONFIG.get("range_hours_long", 6))
    if type_a_symbols:
        out.update(compute_series_with_signals(db_path, type_a_symbols))
    for entry in cross_entries:
        sym = entry.get("symbol")
        points = _build_cross_points(db_path, entry, hours=range_long_h)
        series = _series_from_points(points, entry_type=entry.get("entry_type") or "B")
        trigger = entry.get("trigger_details") or {}
        if entry.get("entry_type") == "B":
            series["pair_exchanges"] = trigger.get("pair") or []
        elif entry.get("entry_type") == "C":
            series["pair_exchanges"] = [trigger.get("spot_exchange"), trigger.get("futures_exchange")]
        out[sym] = series
    return out
