from __future__ import annotations

import argparse
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import psycopg

DEFAULT_PG_DSN = os.environ.get("WATCHLIST_PG_DSN") or "postgresql://wl_reader:wl_reader_A3f9xB2@127.0.0.1:5432/watchlist"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(text: str) -> datetime:
    # Accept ISO8601 with/without timezone; treat naive as UTC.
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _bucket_id(ts: datetime, bucket_minutes: int) -> int:
    sec = int(ts.timestamp())
    step = bucket_minutes * 60
    return sec // step


def _pearson_corr(x: np.ndarray, y: np.ndarray) -> Optional[float]:
    if x.size < 2 or y.size < 2:
        return None
    x0 = x - x.mean()
    y0 = y - y.mean()
    denom = float(np.sqrt(np.sum(x0 * x0) * np.sum(y0 * y0)))
    if denom <= 0:
        return None
    return float(np.sum(x0 * y0) / denom)


def _rankdata(vals: Sequence[float]) -> np.ndarray:
    """
    Average ranks for ties, ranks start at 1.
    Implemented without scipy to keep deps minimal.
    """
    n = len(vals)
    order = sorted(range(n), key=lambda i: vals[i])
    ranks = [0.0] * n
    i = 0
    while i < n:
        j = i
        vi = vals[order[i]]
        while j + 1 < n and vals[order[j + 1]] == vi:
            j += 1
        avg_rank = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg_rank
        i = j + 1
    return np.asarray(ranks, dtype=float)


def _spearman_corr(x: Sequence[float], y: Sequence[float]) -> Optional[float]:
    if len(x) < 2 or len(y) < 2:
        return None
    rx = _rankdata(x)
    ry = _rankdata(y)
    return _pearson_corr(rx, ry)


@dataclass
class Row:
    event_id: int
    start_ts: datetime
    signal_type: str
    symbol: str
    exchange: str
    leg_a_kind: Optional[str]
    leg_a_price_first: Optional[float]
    leg_a_funding_rate_first: Optional[float]
    leg_a_next_funding_time: Optional[datetime]
    leg_a_funding_interval_hours: Optional[float]
    leg_b_kind: Optional[str]
    leg_b_price_first: Optional[float]
    leg_b_funding_rate_first: Optional[float]
    leg_b_next_funding_time: Optional[datetime]
    leg_b_funding_interval_hours: Optional[float]
    raw: Dict[str, Any]
    pnl: float
    spread_change: Optional[float]
    funding_change: Optional[float]


def _dt_or_none(val: Any) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.astimezone(timezone.utc) if val.tzinfo else val.replace(tzinfo=timezone.utc)
    return None


def load_rows(
    conn: psycopg.Connection,
    *,
    since: datetime,
    until: datetime,
    horizon_min: int,
    signal_types: Sequence[str],
    require_position_rule: bool = False,
) -> List[Row]:
    sql = """
    SELECT
      e.id AS event_id,
      e.start_ts,
      e.signal_type,
      e.symbol,
      e.exchange,
      e.leg_a_kind,
      e.leg_a_price_first,
      e.leg_a_funding_rate_first,
      e.leg_a_next_funding_time,
      e.leg_a_funding_interval_hours,
      e.leg_b_kind,
      e.leg_b_price_first,
      e.leg_b_funding_rate_first,
      e.leg_b_next_funding_time,
      e.leg_b_funding_interval_hours,
      r.spread_rel,
      r.range_1h,
      r.range_12h,
      r.volatility,
      r.slope_3m,
      r.crossings_1h,
      r.drift_ratio,
      r.best_buy_high_sell_low,
      r.best_sell_high_buy_low,
      r.funding_diff_max,
      r.premium_index_diff,
      o.pnl,
      o.spread_change,
      o.funding_change
    FROM watchlist.watch_signal_event e
    JOIN watchlist.future_outcome o
      ON o.event_id = e.id AND o.horizon_min = %(horizon_min)s
    LEFT JOIN LATERAL (
      SELECT
        spread_rel,
        range_1h,
        range_12h,
        volatility,
        slope_3m,
        crossings_1h,
        drift_ratio,
        best_buy_high_sell_low,
        best_sell_high_buy_low,
        funding_diff_max,
        premium_index_diff
      FROM watchlist.watch_signal_raw r
      WHERE r.ts = e.start_ts
        AND r.exchange = e.exchange
        AND r.symbol = e.symbol
        AND r.signal_type = e.signal_type
      ORDER BY r.id DESC
      LIMIT 1
    ) r ON TRUE
    WHERE e.start_ts >= %(since)s
      AND e.start_ts < %(until)s
      AND e.signal_type = ANY(%(signal_types)s::char(1)[])
      AND o.pnl IS NOT NULL
      AND (%(require_position_rule)s IS FALSE OR (o.label ? 'position_rule'));
    """
    params = {
        "since": since,
        "until": until,
        "horizon_min": horizon_min,
        "signal_types": list(signal_types),
        "require_position_rule": bool(require_position_rule),
    }
    rows = []
    with conn.cursor() as cur:
        cur.execute(sql, params)
        for r in cur.fetchall():
            raw = {
                "spread_rel": r[15],
                "range_1h": r[16],
                "range_12h": r[17],
                "volatility": r[18],
                "slope_3m": r[19],
                "crossings_1h": r[20],
                "drift_ratio": r[21],
                "best_buy_high_sell_low": r[22],
                "best_sell_high_buy_low": r[23],
                "funding_diff_max": r[24],
                "premium_index_diff": r[25],
            }
            rows.append(
                Row(
                    event_id=int(r[0]),
                    start_ts=_dt_or_none(r[1]) or since,
                    signal_type=str(r[2]).strip(),
                    symbol=str(r[3]),
                    exchange=str(r[4]),
                    leg_a_kind=r[5],
                    leg_a_price_first=r[6],
                    leg_a_funding_rate_first=r[7],
                    leg_a_next_funding_time=_dt_or_none(r[8]),
                    leg_a_funding_interval_hours=r[9],
                    leg_b_kind=r[10],
                    leg_b_price_first=r[11],
                    leg_b_funding_rate_first=r[12],
                    leg_b_next_funding_time=_dt_or_none(r[13]),
                    leg_b_funding_interval_hours=r[14],
                    raw=raw,
                    pnl=float(r[26]),
                    spread_change=r[27],
                    funding_change=r[28],
                )
            )
    return rows


def _time_to_next_funding_min(start_ts: datetime, next_ft: Optional[datetime]) -> Optional[float]:
    if not next_ft:
        return None
    dt = (next_ft - start_ts).total_seconds() / 60.0
    if dt < -1e-6:
        return None
    return dt


def build_factors(row: Row) -> Dict[str, Optional[float]]:
    a_price = row.leg_a_price_first
    b_price = row.leg_b_price_first
    a_fr = row.leg_a_funding_rate_first
    b_fr = row.leg_b_funding_rate_first

    factors: Dict[str, Optional[float]] = {}
    factors["raw_spread_rel"] = row.raw.get("spread_rel")
    factors["raw_volatility"] = row.raw.get("volatility")
    factors["raw_range_1h"] = row.raw.get("range_1h")
    factors["raw_range_12h"] = row.raw.get("range_12h")
    factors["raw_slope_3m"] = row.raw.get("slope_3m")
    factors["raw_crossings_1h"] = row.raw.get("crossings_1h")
    factors["raw_drift_ratio"] = row.raw.get("drift_ratio")
    factors["raw_best_buy_high_sell_low"] = row.raw.get("best_buy_high_sell_low")
    factors["raw_best_sell_high_buy_low"] = row.raw.get("best_sell_high_buy_low")
    factors["raw_funding_diff_max"] = row.raw.get("funding_diff_max")
    factors["raw_premium_index_diff"] = row.raw.get("premium_index_diff")

    # Canonical long/short definition (must match watchlist_outcome_worker.compute_outcome).
    # - Spread metric: log(short_price/long_price)
    # - Funding edge: long pays +rate, short receives +rate.
    def _choose_long_short() -> Tuple[str, str, Dict[str, Any]]:
        meta: Dict[str, Any] = {}
        sig = (row.signal_type or "").strip().upper()
        legs = {
            "a": {"kind": row.leg_a_kind, "price": a_price, "funding": a_fr},
            "b": {"kind": row.leg_b_kind, "price": b_price, "funding": b_fr},
        }
        if sig == "C":
            if legs["a"]["kind"] == "spot" and legs["b"]["kind"] == "perp":
                return "a", "b", {"position_rule": "type_c_spot_long_perp_short"}
            if legs["b"]["kind"] == "spot" and legs["a"]["kind"] == "perp":
                return "b", "a", {"position_rule": "type_c_spot_long_perp_short"}
        if sig == "A":
            # Prefer "receive funding" direction when perp funding is known.
            spot = "a" if legs["a"]["kind"] == "spot" else ("b" if legs["b"]["kind"] == "spot" else None)
            perp = "a" if legs["a"]["kind"] == "perp" else ("b" if legs["b"]["kind"] == "perp" else None)
            if spot and perp and legs[perp]["funding"] is not None:
                fr = float(legs[perp]["funding"])
                if fr >= 0:
                    return spot, perp, {"position_rule": "type_a_receive_funding_short_perp_long_spot"}
                meta["assumption_spot_short_allowed"] = True
                return perp, spot, {"position_rule": "type_a_receive_funding_long_perp_short_spot", **meta}
            if spot and perp:
                return spot, perp, {"position_rule": "type_a_default_spot_long_perp_short"}
        if sig == "B" and a_price and b_price:
            if float(a_price) >= float(b_price):
                return "b", "a", {"position_rule": "type_b_long_low_short_high"}
            return "a", "b", {"position_rule": "type_b_long_low_short_high"}

        # Fallback: long low, short high
        if a_price and b_price and float(a_price) >= float(b_price):
            return "b", "a", {"position_rule": "fallback_long_low_short_high"}
        return "a", "b", {"position_rule": "fallback_long_low_short_high"}

    long_k, short_k, pos_meta = _choose_long_short()
    if pos_meta.get("assumption_spot_short_allowed"):
        factors["assumption_spot_short_allowed"] = 1.0

    def _leg_price(key: str) -> Optional[float]:
        return float(a_price) if key == "a" and a_price else (float(b_price) if key == "b" and b_price else None)

    def _leg_funding(key: str) -> float:
        fr = a_fr if key == "a" else b_fr
        return float(fr) if fr is not None else 0.0

    long_price = _leg_price(long_k)
    short_price = _leg_price(short_k)
    if long_price and short_price and long_price > 0 and short_price > 0:
        factors["spread_log_short_over_long"] = math.log(short_price / long_price)
    else:
        factors["spread_log_short_over_long"] = None

    # Funding edge per settlement (normalized notional=1): short receives +rate; long pays +rate.
    # spot legs have funding=0.
    long_kind = row.leg_a_kind if long_k == "a" else row.leg_b_kind
    short_kind = row.leg_a_kind if short_k == "a" else row.leg_b_kind
    long_funding = _leg_funding(long_k) if long_kind == "perp" else 0.0
    short_funding = _leg_funding(short_k) if short_kind == "perp" else 0.0
    factors["funding_rate_long"] = long_funding if long_kind == "perp" else None
    factors["funding_rate_short"] = short_funding if short_kind == "perp" else None
    factors["funding_edge_short_minus_long"] = (short_funding - long_funding) if (long_kind == "perp" or short_kind == "perp") else None

    if a_price and b_price and a_price > 0 and b_price > 0:
        factors["legs_rel_spread_a_minus_b_over_a"] = (a_price - b_price) / a_price
        base = min(a_price, b_price)
        factors["legs_abs_spread_over_min"] = abs(a_price - b_price) / base if base else None
    else:
        factors["legs_rel_spread_a_minus_b_over_a"] = None
        factors["legs_abs_spread_over_min"] = None

    factors["funding_rate_a"] = float(a_fr) if a_fr is not None else None
    factors["funding_rate_b"] = float(b_fr) if b_fr is not None else None
    if a_fr is not None and b_fr is not None:
        factors["funding_rate_diff_a_minus_b"] = float(a_fr) - float(b_fr)
        factors["funding_rate_max_abs"] = max(abs(float(a_fr)), abs(float(b_fr)))
    else:
        factors["funding_rate_diff_a_minus_b"] = None
        factors["funding_rate_max_abs"] = None

    # Funding schedule features (perp legs only)
    tta = _time_to_next_funding_min(row.start_ts, row.leg_a_next_funding_time) if row.leg_a_kind == "perp" else None
    ttb = _time_to_next_funding_min(row.start_ts, row.leg_b_next_funding_time) if row.leg_b_kind == "perp" else None
    factors["time_to_next_funding_a_min"] = tta
    factors["time_to_next_funding_b_min"] = ttb
    factors["time_to_next_funding_min"] = min(v for v in (tta, ttb) if v is not None) if (tta is not None or ttb is not None) else None
    factors["funding_interval_a_h"] = float(row.leg_a_funding_interval_hours) if row.leg_a_funding_interval_hours else None
    factors["funding_interval_b_h"] = float(row.leg_b_funding_interval_hours) if row.leg_b_funding_interval_hours else None

    # Trade-level schedule features: prefer the perp leg(s) involved in the canonical trade.
    def _leg_ttn(key: str) -> Optional[float]:
        if key == "a":
            return tta
        return ttb

    long_ttn = _leg_ttn(long_k) if (long_kind == "perp") else None
    short_ttn = _leg_ttn(short_k) if (short_kind == "perp") else None
    factors["time_to_next_funding_trade_min"] = min(
        v for v in (long_ttn, short_ttn) if v is not None
    ) if (long_ttn is not None or short_ttn is not None) else None
    return factors


def _iter_factor_ic(
    rows: Sequence[Row],
    *,
    bucket_minutes: int,
    min_bucket_n: int,
    label_getter: Callable[[Row], Optional[float]],
) -> Dict[str, List[float]]:
    # bucket -> factor -> [(x,y)]
    buckets: Dict[int, Dict[str, List[Tuple[float, float]]]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        y = label_getter(r)
        if y is None or not math.isfinite(float(y)):
            continue
        bid = _bucket_id(r.start_ts, bucket_minutes)
        factors = build_factors(r)
        for name, x in factors.items():
            if x is None:
                continue
            xf = float(x)
            if not math.isfinite(xf):
                continue
            buckets[bid][name].append((xf, float(y)))

    ic_by_factor: Dict[str, List[float]] = defaultdict(list)
    for _, factor_map in buckets.items():
        for name, pairs in factor_map.items():
            if len(pairs) < min_bucket_n:
                continue
            xs = [p[0] for p in pairs]
            ys = [p[1] for p in pairs]
            ic = _spearman_corr(xs, ys)
            if ic is None or not math.isfinite(ic):
                continue
            ic_by_factor[name].append(float(ic))
    return dict(ic_by_factor)


def _summarize(ic_by_factor: Dict[str, List[float]]) -> List[Dict[str, Any]]:
    out = []
    for name, ics in ic_by_factor.items():
        if not ics:
            continue
        arr = np.asarray(ics, dtype=float)
        mean = float(arr.mean())
        std = float(arr.std(ddof=1)) if arr.size >= 2 else 0.0
        ir = (mean / std) if std > 0 else None
        t_stat = (mean / (std / math.sqrt(arr.size))) if (std > 0 and arr.size > 1) else None
        out.append(
            {
                "factor": name,
                "n_buckets": int(arr.size),
                "ic_mean": mean,
                "ic_std": std,
                "ir": ir,
                "t": t_stat,
            }
        )
    out.sort(key=lambda r: (abs(r["ic_mean"]), r["n_buckets"]), reverse=True)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Compute factor IC/IR from PG watch_signal_event + future_outcome")
    parser.add_argument("--dsn", type=str, default=DEFAULT_PG_DSN, help="Postgres DSN (or WATCHLIST_PG_DSN env)")
    parser.add_argument("--since", type=str, default="", help="ISO8601 UTC, e.g. 2025-12-10T00:00:00+00:00")
    parser.add_argument("--until", type=str, default="", help="ISO8601 UTC, exclusive end")
    parser.add_argument("--days", type=float, default=7.0, help="Used when --since is not provided")
    parser.add_argument("--horizon-min", type=int, default=240, help="Outcome horizon (minutes)")
    parser.add_argument("--signal-types", type=str, default="B,C", help="Comma-separated, e.g. A,B,C")
    parser.add_argument("--bucket-min", type=int, default=60, help="Time bucket (minutes) for cross-sectional IC")
    parser.add_argument("--min-bucket-n", type=int, default=10, help="Min samples per bucket for IC")
    parser.add_argument(
        "--require-position-rule",
        action="store_true",
        help="Only include outcomes computed by canonical PnL code (future_outcome.label ? 'position_rule')",
    )
    parser.add_argument(
        "--label",
        type=str,
        default="pnl",
        choices=["pnl", "spread_change", "spread_change_canon", "funding_change"],
        help="Label field from future_outcome",
    )
    args = parser.parse_args()

    now = _utcnow()
    if args.since:
        since = _parse_dt(args.since)
    else:
        since = now - timedelta(days=float(args.days))
    until = _parse_dt(args.until) if args.until else now
    signal_types = [s.strip().upper() for s in str(args.signal_types).split(",") if s.strip()]
    horizon_min = int(args.horizon_min)

    label_getter: Callable[[Row], Optional[float]]
    if args.label == "pnl":
        label_getter = lambda r: r.pnl
    elif args.label == "spread_change":
        label_getter = lambda r: r.spread_change
    elif args.label == "spread_change_canon":
        def _canon(r: Row) -> Optional[float]:
            sc = r.spread_change
            if sc is None:
                return None
            # For Type B (perp-perp), current spread_change sign depends on leg ordering.
            # Canonicalize so "spread narrowing" -> positive label.
            if r.leg_a_kind == "perp" and r.leg_b_kind == "perp" and r.leg_a_price_first and r.leg_b_price_first:
                if r.leg_a_price_first > r.leg_b_price_first:
                    return -float(sc)
            return float(sc)

        label_getter = _canon
    else:
        label_getter = lambda r: r.funding_change

    with psycopg.connect(args.dsn) as conn:
        rows = load_rows(
            conn,
            since=since,
            until=until,
            horizon_min=horizon_min,
            signal_types=signal_types,
            require_position_rule=bool(args.require_position_rule),
        )

    print(f"rows: {len(rows)}  horizon_min={horizon_min}  since={since.isoformat()}  until={until.isoformat()}")
    if not rows:
        return 0

    ic_by_factor = _iter_factor_ic(
        rows,
        bucket_minutes=int(args.bucket_min),
        min_bucket_n=int(args.min_bucket_n),
        label_getter=label_getter,
    )
    summary = _summarize(ic_by_factor)
    if not summary:
        print("no IC results (try lowering --min-bucket-n or increasing time window)")
        return 0

    print("factor\tn_buckets\tic_mean\tic_std\tir\tt")
    for r in summary:
        ir = "" if r["ir"] is None else f"{r['ir']:.4f}"
        t = "" if r["t"] is None else f"{r['t']:.2f}"
        print(f"{r['factor']}\t{r['n_buckets']}\t{r['ic_mean']:.6f}\t{r['ic_std']:.6f}\t{ir}\t{t}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
