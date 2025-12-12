from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import psycopg

# Allow running from repo root via `python backtest/...`.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backtest.pg_event_ic_ir import DEFAULT_PG_DSN, Row, _parse_dt, _spearman_corr, _utcnow, build_factors, load_rows


@dataclass
class MetricSummary:
    factor: str
    n_buckets: int
    ic_mean: float
    ic_std: float
    ir: Optional[float]
    t: Optional[float]
    q_spread_mean: Optional[float]
    q_spread_std: Optional[float]
    q_spread_t: Optional[float]
    pooled_spearman: Optional[float]
    pooled_n: int
    non_null_n: int
    missing_rate: float


def _bucket_id(ts: datetime, bucket_minutes: int) -> int:
    sec = int(ts.timestamp())
    step = bucket_minutes * 60
    return sec // step


def _as_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        return None
    if not math.isfinite(v):
        return None
    return v


def _quantile_spread_per_bucket(
    pairs: Sequence[Tuple[float, float]],
    *,
    quantiles: int,
) -> Optional[float]:
    if len(pairs) < max(2 * quantiles, 2):
        return None
    pairs_sorted = sorted(pairs, key=lambda p: p[0])
    n = len(pairs_sorted)
    qn = n // quantiles
    if qn <= 0:
        return None
    low = pairs_sorted[:qn]
    high = pairs_sorted[-qn:]
    if not low or not high:
        return None
    low_mean = float(np.mean([p[1] for p in low]))
    high_mean = float(np.mean([p[1] for p in high]))
    return high_mean - low_mean


def _summarize_series(values: Sequence[float]) -> Tuple[float, float, Optional[float]]:
    if not values:
        return 0.0, 0.0, None
    arr = np.asarray(values, dtype=float)
    mean = float(arr.mean())
    std = float(arr.std(ddof=1)) if arr.size >= 2 else 0.0
    t = (mean / (std / math.sqrt(arr.size))) if (std > 0 and arr.size > 1) else None
    return mean, std, t


def _compute_bucket_metrics(
    rows: Sequence[Row],
    *,
    label_getter: Callable[[Row], Optional[float]],
    bucket_minutes: int,
    min_bucket_n: int,
    quantiles: int,
    dedup: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Returns per-factor metrics:
      {
        factor: {
          "ics": [...],
          "q_spreads": [...],
          "pooled_pairs": [(x,y), ...],
          "non_null_n": int,
          "pooled_n": int,
        }
      }
    """

    # bucket -> list[Row]
    buckets: Dict[int, List[Row]] = {}
    for r in rows:
        bid = _bucket_id(r.start_ts, bucket_minutes)
        buckets.setdefault(bid, []).append(r)

    factor_to_ics: Dict[str, List[float]] = {}
    factor_to_qs: Dict[str, List[float]] = {}
    factor_to_pairs: Dict[str, List[Tuple[float, float]]] = {}
    factor_to_non_null: Dict[str, int] = {}

    for bid, bucket_rows in buckets.items():
        # Dedup per (exchange,symbol,signal_type) inside the bucket.
        if dedup != "none":
            grouped: Dict[Tuple[str, str, str], List[Row]] = {}
            for r in bucket_rows:
                grouped.setdefault((r.exchange, r.symbol, r.signal_type), []).append(r)
            bucket_rows2: List[Row] = []
            for _, rs in grouped.items():
                if dedup == "first":
                    bucket_rows2.append(sorted(rs, key=lambda x: x.start_ts)[0])
                    continue
                # max_abs_spread
                best: Optional[Tuple[float, Row]] = None
                for r in rs:
                    f = build_factors(r)
                    score = _as_float(f.get("spread_log_short_over_long"))
                    if score is None:
                        score = _as_float(f.get("raw_spread_rel"))
                    score_abs = abs(score) if score is not None else -1.0
                    if best is None or score_abs > best[0]:
                        best = (score_abs, r)
                if best is not None:
                    bucket_rows2.append(best[1])
            bucket_rows = bucket_rows2

        # Build factor pairs for this bucket
        per_factor_pairs: Dict[str, List[Tuple[float, float]]] = {}
        for r in bucket_rows:
            y = label_getter(r)
            yv = _as_float(y)
            if yv is None:
                continue
            factors = build_factors(r)
            for name, x in factors.items():
                xv = _as_float(x)
                if xv is None:
                    continue
                per_factor_pairs.setdefault(name, []).append((xv, yv))
                factor_to_pairs.setdefault(name, []).append((xv, yv))
                factor_to_non_null[name] = factor_to_non_null.get(name, 0) + 1

        for name, pairs in per_factor_pairs.items():
            if len(pairs) < min_bucket_n:
                continue
            xs = [p[0] for p in pairs]
            ys = [p[1] for p in pairs]
            ic = _spearman_corr(xs, ys)
            if ic is None or not math.isfinite(ic):
                continue
            factor_to_ics.setdefault(name, []).append(float(ic))
            q_spread = _quantile_spread_per_bucket(pairs, quantiles=quantiles)
            if q_spread is not None and math.isfinite(q_spread):
                factor_to_qs.setdefault(name, []).append(float(q_spread))

    out: Dict[str, Dict[str, Any]] = {}
    for name, pairs in factor_to_pairs.items():
        pooled_n = len(pairs)
        pooled_s = None
        if pooled_n >= 2:
            pooled_s = _spearman_corr([p[0] for p in pairs], [p[1] for p in pairs])
        out[name] = {
            "ics": factor_to_ics.get(name, []),
            "q_spreads": factor_to_qs.get(name, []),
            "pooled_spearman": pooled_s,
            "pooled_n": pooled_n,
            "non_null_n": int(factor_to_non_null.get(name, 0)),
        }
    return out


def _factor_missingness(rows: Sequence[Row]) -> Dict[str, Dict[str, Any]]:
    total = len(rows)
    counts: Dict[str, int] = {}
    for r in rows:
        f = build_factors(r)
        for name, x in f.items():
            if _as_float(x) is None:
                continue
            counts[name] = counts.get(name, 0) + 1
    out: Dict[str, Dict[str, Any]] = {}
    for name, nn in counts.items():
        miss_rate = 1.0 - (nn / total) if total else 1.0
        out[name] = {"non_null_n": nn, "missing_rate": miss_rate}
    return out


def _write_csv(path: Path, rows: Sequence[MetricSummary]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "factor",
                "n_buckets",
                "ic_mean",
                "ic_std",
                "ir",
                "t",
                "q_spread_mean",
                "q_spread_std",
                "q_spread_t",
                "pooled_spearman",
                "pooled_n",
                "non_null_n",
                "missing_rate",
            ]
        )
        for r in rows:
            w.writerow(
                [
                    r.factor,
                    r.n_buckets,
                    f"{r.ic_mean:.8f}",
                    f"{r.ic_std:.8f}",
                    "" if r.ir is None else f"{r.ir:.8f}",
                    "" if r.t is None else f"{r.t:.4f}",
                    "" if r.q_spread_mean is None else f"{r.q_spread_mean:.8f}",
                    "" if r.q_spread_std is None else f"{r.q_spread_std:.8f}",
                    "" if r.q_spread_t is None else f"{r.q_spread_t:.4f}",
                    "" if r.pooled_spearman is None else f"{r.pooled_spearman:.8f}",
                    r.pooled_n,
                    r.non_null_n,
                    f"{r.missing_rate:.6f}",
                ]
            )


def _md_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for r in rows:
        lines.append("| " + " | ".join(r) + " |")
    return "\n".join(lines)


def _compute_report_for_horizon(
    conn: psycopg.Connection,
    *,
    since: datetime,
    until: datetime,
    horizon_min: int,
    signal_types: Sequence[str],
    label: str,
    bucket_min: int,
    min_bucket_n: int,
    quantiles: int,
    require_position_rule: bool,
    dedup: str,
) -> Tuple[List[MetricSummary], Dict[str, Any]]:
    label_getter: Callable[[Row], Optional[float]]
    if label == "pnl":
        label_getter = lambda r: r.pnl
    elif label == "spread_change":
        label_getter = lambda r: r.spread_change
    elif label == "funding_change":
        label_getter = lambda r: r.funding_change
    else:
        raise ValueError(f"unknown label: {label}")

    rows = load_rows(
        conn,
        since=since,
        until=until,
        horizon_min=horizon_min,
        signal_types=signal_types,
        require_position_rule=require_position_rule,
    )
    meta: Dict[str, Any] = {
        "horizon_min": horizon_min,
        "rows": len(rows),
        "since": since.isoformat(),
        "until": until.isoformat(),
        "signal_types": list(signal_types),
        "label": label,
        "bucket_min": bucket_min,
        "min_bucket_n": min_bucket_n,
        "quantiles": quantiles,
        "require_position_rule": require_position_rule,
        "dedup": dedup,
    }
    if not rows:
        return [], meta

    missingness = _factor_missingness(rows)
    bucket_metrics = _compute_bucket_metrics(
        rows,
        label_getter=label_getter,
        bucket_minutes=bucket_min,
        min_bucket_n=min_bucket_n,
        quantiles=quantiles,
        dedup=dedup,
    )

    out: List[MetricSummary] = []
    total = len(rows)
    for factor, m in bucket_metrics.items():
        ics: List[float] = list(m.get("ics") or [])
        if not ics:
            continue
        ic_mean, ic_std, ic_t = _summarize_series(ics)
        ir = (ic_mean / ic_std) if ic_std > 0 else None

        q_spreads = list(m.get("q_spreads") or [])
        if q_spreads:
            q_mean, q_std, q_t = _summarize_series(q_spreads)
        else:
            q_mean, q_std, q_t = None, None, None

        pooled_s = m.get("pooled_spearman")
        pooled_n = int(m.get("pooled_n") or 0)
        non_null_n = int(m.get("non_null_n") or 0)
        miss_rate = missingness.get(factor, {}).get("missing_rate")
        if miss_rate is None:
            miss_rate = 1.0 - (non_null_n / total) if total else 1.0

        out.append(
            MetricSummary(
                factor=factor,
                n_buckets=len(ics),
                ic_mean=ic_mean,
                ic_std=ic_std,
                ir=ir,
                t=ic_t,
                q_spread_mean=q_mean,
                q_spread_std=q_std,
                q_spread_t=q_t,
                pooled_spearman=float(pooled_s) if pooled_s is not None else None,
                pooled_n=pooled_n,
                non_null_n=non_null_n,
                missing_rate=float(miss_rate),
            )
        )

    out.sort(key=lambda r: (abs(r.ic_mean), r.n_buckets, r.pooled_n), reverse=True)
    return out, meta


def _load_available_horizons(conn: psycopg.Connection, *, since: datetime, until: datetime) -> List[int]:
    sql = """
    SELECT DISTINCT o.horizon_min
    FROM watchlist.future_outcome o
    JOIN watchlist.watch_signal_event e ON e.id = o.event_id
    WHERE e.start_ts >= %(since)s AND e.start_ts < %(until)s
    ORDER BY o.horizon_min ASC;
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"since": since, "until": until})
        return [int(r[0]) for r in cur.fetchall()]


def main() -> int:
    p = argparse.ArgumentParser(description="Generate first IC/IR backtest report (PG watch_signal_event + future_outcome)")
    p.add_argument("--dsn", type=str, default=DEFAULT_PG_DSN, help="Postgres DSN (or WATCHLIST_PG_DSN env)")
    p.add_argument("--since", type=str, default="", help="ISO8601 UTC, inclusive start")
    p.add_argument("--until", type=str, default="", help="ISO8601 UTC, exclusive end")
    p.add_argument("--days", type=float, default=7.0, help="Used when --since is not provided")
    p.add_argument("--horizons", type=str, default="240,480,1440", help="Comma-separated minutes, or 'all'")
    p.add_argument("--signal-types", type=str, default="A,B,C", help="Comma-separated, e.g. B,C")
    p.add_argument("--label", type=str, default="pnl", choices=["pnl", "spread_change", "funding_change"])
    p.add_argument("--bucket-min", type=int, default=60, help="Time bucket for cross-sectional IC")
    p.add_argument("--min-bucket-n", type=int, default=10, help="Min samples per bucket")
    p.add_argument("--quantiles", type=int, default=5, help="Quantiles for Q(high)-Q(low) spread")
    p.add_argument("--require-position-rule", action="store_true", help="Only include canonical outcomes (label ? position_rule)")
    p.add_argument("--dedup", type=str, default="max_abs_spread", choices=["none", "first", "max_abs_spread"])
    p.add_argument("--out-dir", type=str, default="reports", help="Output directory")
    p.add_argument("--name", type=str, default="", help="Report base name (without extension)")
    p.add_argument("--top-k", type=int, default=30, help="Top factors to show per horizon in md")
    p.add_argument(
        "--breakdown",
        type=str,
        default="signal_type",
        choices=["none", "signal_type"],
        help="Optional breakdown section in report",
    )
    args = p.parse_args()

    now = _utcnow()
    if args.since:
        since = _parse_dt(args.since)
    else:
        since = now - timedelta(days=float(args.days))
    until = _parse_dt(args.until) if args.until else now

    signal_types = [s.strip().upper() for s in str(args.signal_types).split(",") if s.strip()]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with psycopg.connect(args.dsn) as conn:
        if str(args.horizons).strip().lower() == "all":
            horizons = _load_available_horizons(conn, since=since, until=until)
        else:
            horizons = [int(x.strip()) for x in str(args.horizons).split(",") if x.strip()]

        all_meta: Dict[str, Any] = {
            "generated_at": now.isoformat(),
            "dsn": args.dsn,
            "since": since.isoformat(),
            "until": until.isoformat(),
            "signal_types": signal_types,
            "label": args.label,
            "bucket_min": int(args.bucket_min),
            "min_bucket_n": int(args.min_bucket_n),
            "quantiles": int(args.quantiles),
            "require_position_rule": bool(args.require_position_rule),
            "dedup": args.dedup,
            "horizons": horizons,
        }

        horizon_results: Dict[int, List[MetricSummary]] = {}
        horizon_meta: Dict[int, Dict[str, Any]] = {}
        for h in horizons:
            res, meta = _compute_report_for_horizon(
                conn,
                since=since,
                until=until,
                horizon_min=h,
                signal_types=signal_types,
                label=args.label,
                bucket_min=int(args.bucket_min),
                min_bucket_n=int(args.min_bucket_n),
                quantiles=int(args.quantiles),
                require_position_rule=bool(args.require_position_rule),
                dedup=args.dedup,
            )
            horizon_results[h] = res
            horizon_meta[h] = meta

            csv_path = out_dir / f"first_backtest_ic_ir_h{h}m.csv"
            _write_csv(csv_path, res)

        breakdown_results: Dict[str, Dict[int, List[MetricSummary]]] = {}
        if args.breakdown == "signal_type":
            for st in signal_types:
                per_h: Dict[int, List[MetricSummary]] = {}
                for h in horizons:
                    res, _meta = _compute_report_for_horizon(
                        conn,
                        since=since,
                        until=until,
                        horizon_min=h,
                        signal_types=[st],
                        label=args.label,
                        bucket_min=int(args.bucket_min),
                        min_bucket_n=int(args.min_bucket_n),
                        quantiles=int(args.quantiles),
                        require_position_rule=bool(args.require_position_rule),
                        dedup=args.dedup,
                    )
                    per_h[h] = res
                breakdown_results[st] = per_h

    base = args.name.strip() or f"first_backtest_ic_ir_{since.date().isoformat()}_{until.date().isoformat()}"
    md_path = out_dir / f"{base}.md"
    json_path = out_dir / f"{base}.json"

    # Write report JSON (for audit & future automation).
    with json_path.open("w", encoding="utf-8") as f:
        json.dump({"meta": all_meta, "by_horizon": horizon_meta}, f, ensure_ascii=False, indent=2)

    # Markdown report
    md: List[str] = []
    md.append(f"# 首次 IC/IR 回测报告（label={args.label}）")
    md.append("")
    md.append("## 运行参数（可复现）")
    md.append("")
    cmd = (
        f"venv/bin/python backtest/first_backtest_report.py "
        f"--since {since.isoformat()} --until {until.isoformat()} "
        f"--horizons {','.join(str(h) for h in horizons)} "
        f"--signal-types {','.join(signal_types)} "
        f"--label {args.label} --bucket-min {int(args.bucket_min)} --min-bucket-n {int(args.min_bucket_n)} "
        f"--quantiles {int(args.quantiles)} --dedup {args.dedup} "
        f"{'--require-position-rule ' if args.require_position_rule else ''}"
        f"--out-dir {out_dir}"
    ).strip()
    md.append(f"- 命令：`{cmd}`")
    md.append(f"- DSN：`{args.dsn}`")
    md.append("")

    md.append("## 数据集口径（摘要）")
    md.append("")
    md.append("- 决策时点：`watch_signal_event.start_ts`")
    md.append("- 因子快照：`watch_signal_raw.ts = event.start_ts`（避免未来信息泄露）")
    md.append("- 标签：`future_outcome.{pnl|spread_change|funding_change}`（当前默认用 `pnl`）")
    md.append("- 横截面 IC：按 `bucket-min` 分桶后，在桶内对 (factor,label) 做 Spearman 相关")
    md.append("- IR：`mean(IC_t)/std(IC_t)`；并输出 t-stat（`mean/(std/sqrt(n))`）")
    md.append("- 分位收益：每桶按因子分位分组，输出 `Q(high)-Q(low)` 的均值/std/t")
    md.append(f"- 数据集抽取参考：`scripts/watchlist_event_factor_outcome.sql`")
    md.append("")

    md.append("## Horizon 结果概览")
    md.append("")
    overview_rows: List[List[str]] = []
    for h in horizons:
        meta = horizon_meta.get(h) or {}
        n_rows = int(meta.get("rows") or 0)
        n_factors = len(horizon_results.get(h) or [])
        overview_rows.append(
            [
                str(h),
                str(n_rows),
                str(n_factors),
                "是" if args.require_position_rule else "否",
            ]
        )
    md.append(_md_table(["horizon_min", "样本行数", "可用因子数(有IC)", "仅 canonical outcome"], overview_rows))
    md.append("")

    for h in horizons:
        res = horizon_results.get(h) or []
        meta = horizon_meta.get(h) or {}
        md.append(f"## Horizon={h}m（样本={int(meta.get('rows') or 0)}）Top{int(args.top_k)}")
        md.append("")
        if not res:
            md.append("- 数据不足，无法计算有效 IC（尝试扩大窗口或降低 `--min-bucket-n`）。")
            md.append("")
            continue
        top = res[: int(args.top_k)]
        rows_md: List[List[str]] = []
        for r in top:
            rows_md.append(
                [
                    r.factor,
                    str(r.n_buckets),
                    f"{r.ic_mean:.4f}",
                    f"{r.ir:.3f}" if r.ir is not None else "",
                    f"{r.t:.2f}" if r.t is not None else "",
                    f"{r.q_spread_mean:.6f}" if r.q_spread_mean is not None else "",
                    f"{r.q_spread_t:.2f}" if r.q_spread_t is not None else "",
                    f"{r.pooled_spearman:.4f}" if r.pooled_spearman is not None else "",
                    str(r.pooled_n),
                    f"{(1.0 - r.missing_rate) * 100.0:.1f}%",
                ]
            )
        md.append(
            _md_table(
                [
                    "factor",
                    "n_buckets",
                    "IC_mean",
                    "IR",
                    "t(IC)",
                    "Qhigh-Qlow_mean",
                    "t(Q)",
                    "Spearman(pooled)",
                    "pooled_n",
                    "覆盖率",
                ],
                rows_md,
            )
        )
        md.append("")

    if args.breakdown == "signal_type":
        md.append("## 分组结果（按 signal_type）")
        md.append("")
        md.append("- 说明：仅展示每个 signal_type 在各 horizon 的 Top10（按 |IC_mean| 排序）。")
        md.append("")
        for st in signal_types:
            md.append(f"### signal_type={st}")
            md.append("")
            for h in horizons:
                res = (breakdown_results.get(st, {}) or {}).get(h) or []
                if not res:
                    md.append(f"- horizon={h}m：数据不足")
                    continue
                top = res[:10]
                md_rows: List[List[str]] = []
                for r in top:
                    md_rows.append(
                        [
                            str(h),
                            r.factor,
                            str(r.n_buckets),
                            f"{r.ic_mean:.4f}",
                            f"{r.ir:.3f}" if r.ir is not None else "",
                            f"{r.t:.2f}" if r.t is not None else "",
                            str(r.pooled_n),
                        ]
                    )
                md.append(_md_table(["horizon", "factor", "n_buckets", "IC_mean", "IR", "t(IC)", "pooled_n"], md_rows))
                md.append("")

    md.append("## 输出文件")
    md.append("")
    md.append(f"- 报告：`{md_path}`")
    md.append(f"- 元数据：`{json_path}`")
    md.append(f"- CSV：`{out_dir}/first_backtest_ic_ir_h{{horizon}}m.csv`")
    md.append("")

    md_path.write_text("\n".join(md), encoding="utf-8")
    print(str(md_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
