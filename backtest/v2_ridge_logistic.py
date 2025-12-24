from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import psycopg
from psycopg import rows

DEFAULT_PG_DSN = os.environ.get("WATCHLIST_PG_DSN") or "postgresql://wl_reader:wl_reader_A3f9xB2@127.0.0.1:5432/watchlist"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(text: str) -> datetime:
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        out = float(val)
    except Exception:
        return None
    if math.isfinite(out):
        return out
    return None


@dataclass
class EventRow:
    event_id: int
    start_ts: datetime
    signal_type: str
    symbol: str
    exchange: str
    factors: Dict[str, Optional[float]]
    factors_meta: Dict[str, Any]
    pnl: float
    spread_change: Optional[float]
    funding_change: Optional[float]


def _iter_rows(
    conn: psycopg.Connection,
    *,
    since: datetime,
    until: datetime,
    horizon_min: int,
    signal_types: Sequence[str],
    require_meta_ok: bool,
) -> Iterable[EventRow]:
    sql = """
    SELECT
      e.id AS event_id,
      e.start_ts,
      e.signal_type,
      e.symbol,
      e.exchange,
      e.features_agg->'meta_last'->'factors_v2' AS factors_v2,
      e.features_agg->'meta_last'->'factors_v2_meta' AS factors_v2_meta,
      o.pnl,
      o.spread_change,
      o.funding_change
    FROM watchlist.watch_signal_event e
    JOIN watchlist.future_outcome o
      ON o.event_id = e.id AND o.horizon_min = %(horizon_min)s
    WHERE e.start_ts >= %(since)s
      AND e.start_ts < %(until)s
      AND e.signal_type = ANY(%(signal_types)s::char(1)[])
      AND o.pnl IS NOT NULL
      AND e.features_agg ? 'meta_last';
    """
    params = {
        "since": since,
        "until": until,
        "horizon_min": horizon_min,
        "signal_types": list(signal_types),
    }
    with conn.cursor() as cur:
        cur.execute(sql, params)
        for r in cur.fetchall():
            factors = r["factors_v2"] if isinstance(r["factors_v2"], dict) else None
            if not factors:
                continue
            meta = r["factors_v2_meta"] if isinstance(r["factors_v2_meta"], dict) else {}
            if require_meta_ok and meta and meta.get("ok") is False:
                continue
            row = EventRow(
                event_id=int(r["event_id"]),
                start_ts=r["start_ts"].astimezone(timezone.utc) if r["start_ts"].tzinfo else r["start_ts"].replace(tzinfo=timezone.utc),
                signal_type=str(r["signal_type"]).strip(),
                symbol=str(r["symbol"]),
                exchange=str(r["exchange"]),
                factors={k: _safe_float(v) for k, v in factors.items()},
                factors_meta=meta,
                pnl=float(r["pnl"]),
                spread_change=_safe_float(r["spread_change"]),
                funding_change=_safe_float(r["funding_change"]),
            )
            yield row


def _build_feature_table(
    rows: Sequence[EventRow],
    *,
    max_missing: float,
) -> Tuple[List[str], np.ndarray, np.ndarray, np.ndarray, List[EventRow], Dict[str, float]]:
    feature_set = set()
    for row in rows:
        feature_set.update(row.factors.keys())
    features = sorted(feature_set)

    n = len(rows)
    m = len(features)
    x = np.full((n, m), np.nan, dtype=float)
    y = np.zeros(n, dtype=float)
    y_spread = np.zeros(n, dtype=float)
    y_funding = np.zeros(n, dtype=float)

    for i, row in enumerate(rows):
        y[i] = row.pnl
        y_spread[i] = row.spread_change if row.spread_change is not None else np.nan
        y_funding[i] = row.funding_change if row.funding_change is not None else np.nan
        for j, key in enumerate(features):
            val = row.factors.get(key)
            if val is None:
                continue
            x[i, j] = val

    missing_rate = np.mean(np.isnan(x), axis=0)
    keep_mask = missing_rate <= max_missing
    kept_features = [f for f, keep in zip(features, keep_mask) if keep]
    x = x[:, keep_mask]
    missing_rate_map = {f: float(r) for f, r in zip(kept_features, missing_rate[keep_mask])}
    return kept_features, x, y, y_spread, rows, missing_rate_map


def _time_split(rows: Sequence[EventRow], x: np.ndarray, y: np.ndarray, y_spread: np.ndarray, *, valid_days: int) -> Dict[str, Any]:
    idx_sorted = sorted(range(len(rows)), key=lambda i: rows[i].start_ts)
    rows_sorted = [rows[i] for i in idx_sorted]
    x_sorted = x[idx_sorted]
    y_sorted = y[idx_sorted]
    y_spread_sorted = y_spread[idx_sorted]
    if not rows_sorted:
        return {}
    cutoff_ts = rows_sorted[-1].start_ts - timedelta(days=valid_days)
    train_idx = [i for i, r in enumerate(rows_sorted) if r.start_ts < cutoff_ts]
    valid_idx = [i for i, r in enumerate(rows_sorted) if r.start_ts >= cutoff_ts]
    if not train_idx:
        train_idx = list(range(len(rows_sorted)))
        valid_idx = []
    return {
        "rows": rows_sorted,
        "x": x_sorted,
        "y": y_sorted,
        "y_spread": y_spread_sorted,
        "train_idx": np.asarray(train_idx, dtype=int),
        "valid_idx": np.asarray(valid_idx, dtype=int),
    }


def _impute_and_scale(x: np.ndarray, train_idx: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    x_train = x[train_idx]
    med = np.nanmedian(x_train, axis=0)
    mean = np.nanmean(x_train, axis=0)
    std = np.nanstd(x_train, axis=0)
    std[std == 0] = 1.0
    x_imputed = np.where(np.isnan(x), med, x)
    x_scaled = (x_imputed - mean) / std
    return x_scaled, med, mean, std


def _ridge_fit(x: np.ndarray, y: np.ndarray, alpha: float) -> np.ndarray:
    n, m = x.shape
    x_aug = np.hstack([x, np.ones((n, 1))])
    eye = np.eye(m + 1, dtype=float)
    eye[-1, -1] = 0.0  # do not regularize intercept
    xtx = x_aug.T @ x_aug
    w = np.linalg.solve(xtx + alpha * eye, x_aug.T @ y)
    return w


def _sigmoid(z: np.ndarray) -> np.ndarray:
    z = np.clip(z, -50.0, 50.0)
    return 1.0 / (1.0 + np.exp(-z))


def _logistic_fit(
    x: np.ndarray,
    y: np.ndarray,
    *,
    alpha: float,
    lr: float,
    max_iter: int,
) -> np.ndarray:
    n, m = x.shape
    x_aug = np.hstack([x, np.ones((n, 1))])
    w = np.zeros(m + 1, dtype=float)
    for _ in range(max_iter):
        z = x_aug @ w
        p = _sigmoid(z)
        grad = (x_aug.T @ (p - y)) / float(n)
        grad[:-1] += alpha * w[:-1]
        w -= lr * grad
    return w


def _predict_linear(x: np.ndarray, w: np.ndarray) -> np.ndarray:
    x_aug = np.hstack([x, np.ones((x.shape[0], 1))])
    return x_aug @ w


def _predict_prob(x: np.ndarray, w: np.ndarray) -> np.ndarray:
    x_aug = np.hstack([x, np.ones((x.shape[0], 1))])
    return _sigmoid(x_aug @ w)


def _auc_score(y_true: np.ndarray, y_prob: np.ndarray) -> Optional[float]:
    if y_true.size < 2:
        return None
    order = np.argsort(y_prob)
    y = y_true[order]
    n_pos = float(np.sum(y == 1))
    n_neg = float(np.sum(y == 0))
    if n_pos == 0 or n_neg == 0:
        return None
    rank_sum = 0.0
    for idx, val in enumerate(y, start=1):
        if val == 1:
            rank_sum += idx
    auc = (rank_sum - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)
    return float(auc)


def _reg_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    if y_true.size == 0:
        return {}
    diff = y_pred - y_true
    mae = float(np.mean(np.abs(diff)))
    rmse = float(np.sqrt(np.mean(diff * diff)))
    corr = np.corrcoef(y_true, y_pred)[0, 1] if y_true.size > 1 else 0.0
    sign_acc = float(np.mean(np.sign(y_true) == np.sign(y_pred)))
    return {"mae": mae, "rmse": rmse, "corr": float(corr), "sign_acc": sign_acc}


def _clf_metrics(y_true: np.ndarray, y_prob: np.ndarray) -> Dict[str, float]:
    if y_true.size == 0:
        return {}
    y_pred = (y_prob >= 0.5).astype(float)
    acc = float(np.mean(y_pred == y_true))
    logloss = float(-np.mean(y_true * np.log(y_prob + 1e-12) + (1 - y_true) * np.log(1 - y_prob + 1e-12)))
    auc = _auc_score(y_true, y_prob)
    return {"acc": acc, "logloss": logloss, "auc": float(auc) if auc is not None else float("nan")}


def _rank_features(features: List[str], weights: np.ndarray, top_n: int = 12) -> Dict[str, List[Tuple[str, float]]]:
    pairs = list(zip(features, weights.tolist()))
    pairs.sort(key=lambda x: x[1], reverse=True)
    pos = pairs[:top_n]
    neg = list(reversed(pairs[-top_n:]))
    abs_sorted = sorted(pairs, key=lambda x: abs(x[1]))
    low = abs_sorted[:top_n]
    return {"positive": pos, "negative": neg, "low": low}


def _write_feature_block(f, title: str, ranking: Dict[str, List[Tuple[str, float]]]) -> None:
    f.write(f"## {title}\n")
    f.write("### Top positive (increase predicted)\n")
    for name, val in ranking["positive"]:
        f.write(f"- {name}: {val:.6f}\n")
    f.write("\n### Top negative (decrease predicted)\n")
    for name, val in ranking["negative"]:
        f.write(f"- {name}: {val:.6f}\n")
    f.write("\n### Low impact (abs weight small)\n")
    for name, val in ranking["low"]:
        f.write(f"- {name}: {val:.6f}\n")
    f.write("\n")


def _train_one(
    *,
    dsn: str,
    since: datetime,
    until: datetime,
    horizon_min: int,
    signal_types: Sequence[str],
    max_missing: float,
    fee_bps_per_leg: float,
    ridge_alpha: float,
    logreg_alpha: float,
    logreg_lr: float,
    logreg_iter: int,
    valid_days: int,
    require_meta_ok: bool,
    out_dir: str,
    dump_csv: bool,
) -> Dict[str, Any]:
    with psycopg.connect(dsn, row_factory=rows.dict_row) as conn:
        rows_list = list(
            _iter_rows(
                conn,
                since=since,
                until=until,
                horizon_min=horizon_min,
                signal_types=signal_types,
                require_meta_ok=bool(require_meta_ok),
            )
        )

    if not rows_list:
        return {"horizon_min": horizon_min, "ok": False, "reason": "no rows"}

    fee_total = fee_bps_per_leg / 10000.0 * 2.0
    for row in rows_list:
        row.pnl = row.pnl - fee_total

    features, x_raw, y_raw, y_spread, rows_list, missing_rate = _build_feature_table(
        rows_list, max_missing=max_missing
    )
    split = _time_split(rows_list, x_raw, y_raw, y_spread, valid_days=valid_days)
    if not split:
        return {"horizon_min": horizon_min, "ok": False, "reason": "split failed"}

    x_scaled, med, mean, std = _impute_and_scale(split["x"], split["train_idx"])
    y = split["y"]
    y_cls = (y > 0).astype(float)

    train_idx = split["train_idx"]
    valid_idx = split["valid_idx"]

    ridge_w = _ridge_fit(x_scaled[train_idx], y[train_idx], ridge_alpha)
    logreg_w = _logistic_fit(
        x_scaled[train_idx],
        y_cls[train_idx],
        alpha=logreg_alpha,
        lr=logreg_lr,
        max_iter=logreg_iter,
    )

    train_pred = _predict_linear(x_scaled[train_idx], ridge_w)
    train_prob = _predict_prob(x_scaled[train_idx], logreg_w)
    train_reg = _reg_metrics(y[train_idx], train_pred)
    train_clf = _clf_metrics(y_cls[train_idx], train_prob)

    valid_reg: Dict[str, float] = {}
    valid_clf: Dict[str, float] = {}
    if valid_idx.size > 0:
        valid_pred = _predict_linear(x_scaled[valid_idx], ridge_w)
        valid_prob = _predict_prob(x_scaled[valid_idx], logreg_w)
        valid_reg = _reg_metrics(y[valid_idx], valid_pred)
        valid_clf = _clf_metrics(y_cls[valid_idx], valid_prob)

    model_payload = {
        "generated_at": _utcnow().isoformat(),
        "horizon_min": horizon_min,
        "signal_types": list(signal_types),
        "days": int((until - since).days),
        "valid_days": valid_days,
        "fee_bps_per_leg": fee_bps_per_leg,
        "features": features,
        "missing_rate": missing_rate,
        "scaler": {
            "median": med.tolist(),
            "mean": mean.tolist(),
            "std": std.tolist(),
        },
        "ridge": {"alpha": ridge_alpha, "coef": ridge_w[:-1].tolist(), "intercept": float(ridge_w[-1])},
        "logistic": {
            "alpha": logreg_alpha,
            "lr": logreg_lr,
            "iter": logreg_iter,
            "coef": logreg_w[:-1].tolist(),
            "intercept": float(logreg_w[-1]),
        },
        "metrics": {
            "train": {"reg": train_reg, "clf": train_clf, "samples": int(train_idx.size)},
            "valid": {"reg": valid_reg, "clf": valid_clf, "samples": int(valid_idx.size)},
        },
    }

    model_path = os.path.join(out_dir, f"v2_ridge_logistic_model_h{horizon_min}.json")
    with open(model_path, "w", encoding="utf-8") as f:
        json.dump(model_payload, f, indent=2, sort_keys=True)

    metrics_path = os.path.join(out_dir, f"v2_ridge_logistic_metrics_h{horizon_min}.md")
    ridge_rank = _rank_features(features, np.asarray(ridge_w[:-1]), top_n=12)
    log_rank = _rank_features(features, np.asarray(logreg_w[:-1]), top_n=12)
    with open(metrics_path, "w", encoding="utf-8") as f:
        f.write("# V2 Ridge + Logistic Metrics\n\n")
        f.write(f"- generated_at: {model_payload['generated_at']}\n")
        f.write(f"- horizon_min: {horizon_min}\n")
        f.write(f"- signal_types: {','.join(signal_types)}\n")
        f.write(f"- days: {model_payload['days']}\n")
        f.write(f"- valid_days: {model_payload['valid_days']}\n")
        f.write(f"- fee_bps_per_leg: {fee_bps_per_leg}\n")
        f.write(f"- features: {len(features)}\n")
        f.write(f"- samples: {len(rows_list)}\n\n")
        f.write("## Train\n")
        f.write(f"- reg: {train_reg}\n")
        f.write(f"- clf: {train_clf}\n\n")
        f.write("## Valid\n")
        f.write(f"- reg: {valid_reg}\n")
        f.write(f"- clf: {valid_clf}\n\n")
        f.write("## Interpret\n")
        f.write("说明：已对特征做标准化，因此系数可比较。正系数=预测收益/胜率上升，负系数=下降。\n\n")
        _write_feature_block(f, "Ridge (pnl_total)", ridge_rank)
        _write_feature_block(f, "Logistic (win_prob)", log_rank)

    if dump_csv:
        csv_path = os.path.join(out_dir, f"v2_ridge_logistic_dataset_h{horizon_min}.csv")
        with open(csv_path, "w", encoding="utf-8") as f:
            header = ["event_id", "start_ts", "signal_type", "symbol", "exchange", "pnl_total"]
            header.extend(features)
            f.write(",".join(header) + "\n")
            for i, row in enumerate(split["rows"]):
                vals = [str(row.event_id), row.start_ts.isoformat(), row.signal_type, row.symbol, row.exchange, f"{y[i]:.10f}"]
                feats = x_scaled[i]
                vals.extend("" if math.isnan(v) else f"{v:.10f}" for v in feats)
                f.write(",".join(vals) + "\n")

    return {
        "horizon_min": horizon_min,
        "ok": True,
        "model_path": model_path,
        "metrics_path": metrics_path,
        "train": train_reg,
        "valid": valid_reg,
        "train_clf": train_clf,
        "valid_clf": valid_clf,
        "samples": len(rows_list),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Train v2 Ridge + Logistic models on watchlist factors_v2.")
    parser.add_argument("--dsn", default=DEFAULT_PG_DSN)
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--valid-days", type=int, default=7)
    parser.add_argument("--horizon-min", type=int, default=1440)
    parser.add_argument("--horizons", default="")
    parser.add_argument("--signal-types", default="B,C")
    parser.add_argument("--max-missing", type=float, default=0.2)
    parser.add_argument("--fee-bps-per-leg", type=float, default=0.0)
    parser.add_argument("--ridge-alpha", type=float, default=1.0)
    parser.add_argument("--logreg-alpha", type=float, default=0.1)
    parser.add_argument("--logreg-lr", type=float, default=0.1)
    parser.add_argument("--logreg-iter", type=int, default=500)
    parser.add_argument("--require-meta-ok", action="store_true")
    parser.add_argument("--out-dir", default="reports")
    parser.add_argument("--dump-csv", action="store_true")
    args = parser.parse_args()

    until = _utcnow()
    since = until - timedelta(days=args.days)
    signal_types = [s.strip().upper() for s in args.signal_types.split(",") if s.strip()]
    out_dir = os.path.abspath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    horizons: List[int] = []
    if args.horizons.strip():
        for part in args.horizons.split(","):
            part = part.strip()
            if not part:
                continue
            horizons.append(int(part))
    if not horizons:
        horizons = [int(args.horizon_min)]

    results = []
    for horizon in horizons:
        res = _train_one(
            dsn=args.dsn,
            since=since,
            until=until,
            horizon_min=horizon,
            signal_types=signal_types,
            max_missing=args.max_missing,
            fee_bps_per_leg=args.fee_bps_per_leg,
            ridge_alpha=args.ridge_alpha,
            logreg_alpha=args.logreg_alpha,
            logreg_lr=args.logreg_lr,
            logreg_iter=args.logreg_iter,
        require_meta_ok=bool(args.require_meta_ok),
        out_dir=out_dir,
        dump_csv=bool(args.dump_csv),
        valid_days=args.valid_days,
    )
        results.append(res)
        if res.get("ok"):
            print(f"h{horizon}: model={res['model_path']} metrics={res['metrics_path']}")
        else:
            print(f"h{horizon}: {res.get('reason')}")

    summary_path = os.path.join(out_dir, "v2_ridge_logistic_summary.md")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("# V2 Ridge + Logistic Summary\n\n")
        f.write(f"- generated_at: {_utcnow().isoformat()}\n")
        f.write(f"- horizons: {', '.join(str(h) for h in horizons)}\n")
        f.write(f"- signal_types: {','.join(signal_types)}\n")
        f.write(f"- days: {args.days}\n")
        f.write(f"- valid_days: {args.valid_days}\n")
        f.write(f"- fee_bps_per_leg: {args.fee_bps_per_leg}\n\n")
        for res in results:
            f.write(f"## Horizon {res['horizon_min']}\n")
            if not res.get("ok"):
                f.write(f"- status: failed ({res.get('reason')})\n\n")
                continue
            f.write(f"- samples: {res['samples']}\n")
            f.write(f"- train reg: {res['train']}\n")
            f.write(f"- train clf: {res['train_clf']}\n")
            f.write(f"- valid reg: {res['valid']}\n")
            f.write(f"- valid clf: {res['valid_clf']}\n")
            f.write(f"- metrics: {res['metrics_path']}\n")
            f.write(f"- model: {res['model_path']}\n\n")

    print(f"saved summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
