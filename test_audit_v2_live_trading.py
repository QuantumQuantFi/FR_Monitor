from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    import psycopg
except Exception:
    psycopg = None  # type: ignore

try:
    from psycopg.rows import dict_row  # type: ignore
except Exception:
    dict_row = None  # type: ignore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
    except Exception:
        return None
    if f != f:  # NaN
        return None
    return f


def _safe_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("true", "t", "1", "yes", "y"):
        return True
    if s in ("false", "f", "0", "no", "n"):
        return False
    return None


@dataclass
class Thresholds:
    v1_pnl: float
    v1_prob: float
    v2_pnl_240: float
    v2_prob_240: float
    v2_pnl_1440: float
    v2_prob_1440: float
    v2_max_missing: float


def _load_thresholds() -> Thresholds:
    import config as _cfg  # local import

    lt = getattr(_cfg, "LIVE_TRADING_CONFIG", {}) or {}
    v2cfg = getattr(_cfg, "WATCHLIST_V2_PRED_CONFIG", {}) or {}
    return Thresholds(
        v1_pnl=float(lt.get("pnl_threshold") or 0.0085),
        v1_prob=float(lt.get("win_prob_threshold") or 0.85),
        v2_pnl_240=float(lt.get("v2_pnl_threshold_240") or (lt.get("pnl_threshold") or 0.0085)),
        v2_prob_240=float(lt.get("v2_win_prob_threshold_240") or (lt.get("win_prob_threshold") or 0.85)),
        v2_pnl_1440=float(lt.get("v2_pnl_threshold_1440") or (lt.get("pnl_threshold") or 0.0085)),
        v2_prob_1440=float(lt.get("v2_win_prob_threshold_1440") or (lt.get("win_prob_threshold") or 0.85)),
        v2_max_missing=float(v2cfg.get("max_missing_ratio") or 0.2),
    )


def _pick_choice(passing: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not passing:
        return None
    passing.sort(
        key=lambda c: (
            float(c.get("pnl_hat") or -1e9),
            float(c.get("win_prob") or -1e9),
            -int(c.get("horizon_min") or 10**9),
        ),
        reverse=True,
    )
    return passing[0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit v2 pipeline + live trading decisions from Postgres.")
    parser.add_argument("--dsn", default=None, help="Postgres DSN (default: WATCHLIST_PG_CONFIG['dsn'])")
    parser.add_argument("--hours", type=float, default=8.0, help="Lookback hours (default: 8)")
    parser.add_argument("--limit", type=int, default=5000, help="Max events to scan (default: 5000)")
    parser.add_argument("--samples", type=int, default=5, help="Print up to N sample event_ids per category (default: 5)")
    parser.add_argument("--dump_csv", default=None, help="Optional CSV path to dump per-event audit rows")
    args = parser.parse_args()

    if psycopg is None:
        raise SystemExit("psycopg not installed. Run: python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt")

    import config as _cfg  # local import

    dsn = str(args.dsn or (_cfg.WATCHLIST_PG_CONFIG.get("dsn") if hasattr(_cfg, "WATCHLIST_PG_CONFIG") else "") or "").strip()
    if not dsn:
        raise SystemExit("missing dsn: pass --dsn or set WATCHLIST_PG_DSN")

    thr = _load_thresholds()
    since = _utcnow() - timedelta(hours=float(args.hours))

    conn_kwargs: Dict[str, Any] = {"autocommit": True}
    if dict_row is not None:
        conn_kwargs["row_factory"] = dict_row
    with psycopg.connect(dsn, **conn_kwargs) as conn:
        rows = conn.execute(
            """
            WITH base AS (
              SELECT
                e.id AS event_id,
                e.symbol,
                e.signal_type,
                e.start_ts,
                e.end_ts,
                COALESCE(
                  (e.features_agg #>> '{meta_last,orderbook_validation,ts}')::timestamptz,
                  (e.features_agg #>> '{meta_last,pred_v2_meta,ts}')::timestamptz,
                  (e.features_agg #>> '{meta_last,factors_v2_meta,ts}')::timestamptz,
                  e.end_ts,
                  e.start_ts
                ) AS decision_ts,
                COALESCE(
                  (e.features_agg #>> '{meta_last,pnl_regression_ob,pred,240,pnl_hat}')::double precision,
                  (e.features_agg #>> '{meta_last,pnl_regression,pred,240,pnl_hat}')::double precision
                ) AS v1_pnl_hat_240,
                COALESCE(
                  (e.features_agg #>> '{meta_last,pnl_regression_ob,pred,240,win_prob}')::double precision,
                  (e.features_agg #>> '{meta_last,pnl_regression,pred,240,win_prob}')::double precision
                ) AS v1_win_prob_240,
                (e.features_agg #>> '{meta_last,pred_v2,240,pnl_hat}')::double precision AS v2_pnl_hat_240,
                (e.features_agg #>> '{meta_last,pred_v2,240,win_prob}')::double precision AS v2_win_prob_240,
                (e.features_agg #>> '{meta_last,pred_v2,240,missing_rate}')::double precision AS v2_missing_240,
                (e.features_agg #>> '{meta_last,pred_v2,240,ok}')::boolean AS v2_ok_240,
                (e.features_agg #>> '{meta_last,pred_v2,1440,pnl_hat}')::double precision AS v2_pnl_hat_1440,
                (e.features_agg #>> '{meta_last,pred_v2,1440,win_prob}')::double precision AS v2_win_prob_1440,
                (e.features_agg #>> '{meta_last,pred_v2,1440,missing_rate}')::double precision AS v2_missing_1440,
                (e.features_agg #>> '{meta_last,pred_v2,1440,ok}')::boolean AS v2_ok_1440,
                s.id AS signal_id,
                s.status AS signal_status,
                s.pred_source,
                s.reason AS signal_reason,
                s.created_at AS signal_created_at
              FROM watchlist.watch_signal_event e
              LEFT JOIN watchlist.live_trade_signal s
                ON s.event_id = e.id
              WHERE e.signal_type IN ('B','C')
                AND COALESCE(e.end_ts, e.start_ts) >= %s
              ORDER BY COALESCE(e.end_ts, e.start_ts) DESC
              LIMIT %s
            )
            SELECT * FROM base;
            """,
            (since, int(args.limit)),
        ).fetchall()

    total = len(rows)
    if not total:
        print(f"no events found in last {args.hours}h")
        return 0

    chosen_counter: Counter[str] = Counter()
    pass_counter: Counter[str] = Counter()
    signal_counter: Counter[str] = Counter()
    signal_reason_counter: Counter[str] = Counter()
    pass_combo_counter: Counter[str] = Counter()
    live_vs_would_counter: Counter[str] = Counter()

    sample_events: Dict[str, List[Dict[str, Any]]] = {"v1_only": [], "v2_only": [], "both": [], "none": []}

    delays: List[Tuple[float, Dict[str, Any]]] = []

    dump_rows: List[Dict[str, Any]] = []

    for r in rows:
        decision_ts = r.get("decision_ts")
        start_ts = r.get("start_ts")
        end_ts = r.get("end_ts")
        if isinstance(decision_ts, datetime) and isinstance(start_ts, datetime):
            delays.append(((decision_ts - start_ts).total_seconds(), r))

        passing: List[Dict[str, Any]] = []

        v1_pnl = _safe_float(r.get("v1_pnl_hat_240"))
        v1_prob = _safe_float(r.get("v1_win_prob_240"))
        v1_pass = False
        if v1_pnl is not None and v1_prob is not None and v1_pnl >= thr.v1_pnl and v1_prob >= thr.v1_prob:
            passing.append({"source": "v1_240", "horizon_min": 240, "pnl_hat": v1_pnl, "win_prob": v1_prob})
            pass_counter["v1_240"] += 1
            v1_pass = True

        v2_ok_240 = _safe_bool(r.get("v2_ok_240"))
        v2_miss_240 = _safe_float(r.get("v2_missing_240"))
        v2_pnl_240 = _safe_float(r.get("v2_pnl_hat_240"))
        v2_prob_240 = _safe_float(r.get("v2_win_prob_240"))
        miss_ok_240 = v2_miss_240 is None or v2_miss_240 <= thr.v2_max_missing
        v2_240_pass = False
        if (v2_ok_240 is not False) and miss_ok_240 and v2_pnl_240 is not None and v2_prob_240 is not None:
            if v2_pnl_240 >= thr.v2_pnl_240 and v2_prob_240 >= thr.v2_prob_240:
                passing.append({"source": "v2_240", "horizon_min": 240, "pnl_hat": v2_pnl_240, "win_prob": v2_prob_240})
                pass_counter["v2_240"] += 1
                v2_240_pass = True

        v2_ok_1440 = _safe_bool(r.get("v2_ok_1440"))
        v2_miss_1440 = _safe_float(r.get("v2_missing_1440"))
        v2_pnl_1440 = _safe_float(r.get("v2_pnl_hat_1440"))
        v2_prob_1440 = _safe_float(r.get("v2_win_prob_1440"))
        miss_ok_1440 = v2_miss_1440 is None or v2_miss_1440 <= thr.v2_max_missing
        v2_1440_pass = False
        if (v2_ok_1440 is not False) and miss_ok_1440 and v2_pnl_1440 is not None and v2_prob_1440 is not None:
            if v2_pnl_1440 >= thr.v2_pnl_1440 and v2_prob_1440 >= thr.v2_prob_1440:
                passing.append({"source": "v2_1440", "horizon_min": 1440, "pnl_hat": v2_pnl_1440, "win_prob": v2_prob_1440})
                pass_counter["v2_1440"] += 1
                v2_1440_pass = True

        choice = _pick_choice(passing)
        if choice:
            chosen_counter[str(choice["source"])] += 1
        else:
            chosen_counter["none"] += 1

        any_v2_pass = bool(v2_240_pass or v2_1440_pass)
        if v1_pass and not any_v2_pass:
            combo = "v1_only"
        elif (not v1_pass) and any_v2_pass:
            combo = "v2_only"
        elif v1_pass and any_v2_pass:
            combo = "both"
        else:
            combo = "none"
        pass_combo_counter[combo] += 1
        if len(sample_events[combo]) < int(args.samples):
            sample_events[combo].append(r)

        pred_source = str(r.get("pred_source") or "")
        if pred_source:
            signal_counter[pred_source] += 1
        status = str(r.get("signal_status") or "")
        if status:
            signal_counter[f"status:{status}"] += 1
        reason = str(r.get("signal_reason") or "")
        if reason:
            signal_reason_counter[reason] += 1

        if pred_source:
            would = str(choice["source"]) if choice else "none"
            live_vs_would_counter[f"live:{pred_source}|would:{would}"] += 1

        if args.dump_csv:
            dump_rows.append(
                {
                    "event_id": r.get("event_id"),
                    "symbol": r.get("symbol"),
                    "signal_type": r.get("signal_type"),
                    "start_ts": r.get("start_ts"),
                    "decision_ts": r.get("decision_ts"),
                    "v1_pnl_hat_240": v1_pnl,
                    "v1_win_prob_240": v1_prob,
                    "v1_pass": v1_pass,
                    "v2_pnl_hat_240": v2_pnl_240,
                    "v2_win_prob_240": v2_prob_240,
                    "v2_missing_240": v2_miss_240,
                    "v2_ok_240": v2_ok_240,
                    "v2_240_pass": v2_240_pass,
                    "v2_pnl_hat_1440": v2_pnl_1440,
                    "v2_win_prob_1440": v2_prob_1440,
                    "v2_missing_1440": v2_miss_1440,
                    "v2_ok_1440": v2_ok_1440,
                    "v2_1440_pass": v2_1440_pass,
                    "pass_combo": combo,
                    "would_choose": (choice or {}).get("source") if choice else None,
                    "live_signal_id": r.get("signal_id"),
                    "live_status": r.get("signal_status"),
                    "live_pred_source": pred_source or None,
                    "live_reason": r.get("signal_reason"),
                }
            )

    print(f"lookback_hours={args.hours} events={total}")
    print(
        "thresholds:",
        f"v1(pnl>={thr.v1_pnl:.4f},prob>={thr.v1_prob:.2f})",
        f"v2_240(pnl>={thr.v2_pnl_240:.4f},prob>={thr.v2_prob_240:.2f})",
        f"v2_1440(pnl>={thr.v2_pnl_1440:.4f},prob>={thr.v2_prob_1440:.2f})",
        f"max_missing={thr.v2_max_missing:.2f}",
    )
    print("pass_counts:", dict(pass_counter))
    print("pass_combo_counts:", dict(pass_combo_counter))
    print("would_choose_counts:", dict(chosen_counter))
    print("live_trade_signal_counts:", dict(signal_counter))
    if live_vs_would_counter:
        print("live_vs_would_choose_top:", live_vs_would_counter.most_common(12))
    if signal_reason_counter:
        top_reasons = signal_reason_counter.most_common(10)
        print("top_skipped_reasons:", top_reasons)

    if int(args.samples) > 0:
        for key in ("v1_only", "v2_only", "both", "none"):
            evs = sample_events.get(key) or []
            if not evs:
                continue
            print(f"samples_{key}:")
            for rr in evs:
                print(
                    f"  event_id={rr.get('event_id')} sym={rr.get('symbol')} type={rr.get('signal_type')} "
                    f"live_pred={rr.get('pred_source') or '-'} live_status={rr.get('signal_status') or '-'} "
                    f"v1=({rr.get('v1_pnl_hat_240')},{rr.get('v1_win_prob_240')}) "
                    f"v2_240=({rr.get('v2_pnl_hat_240')},{rr.get('v2_win_prob_240')},miss={rr.get('v2_missing_240')},ok={rr.get('v2_ok_240')}) "
                    f"v2_1440=({rr.get('v2_pnl_hat_1440')},{rr.get('v2_win_prob_1440')},miss={rr.get('v2_missing_1440')},ok={rr.get('v2_ok_1440')})"
                )

    delays.sort(key=lambda x: x[0], reverse=True)
    top = delays[:5]
    if top:
        print("top_decision_delay_sec (decision_ts - start_ts):")
        for sec, r in top:
            print(
                f"  {sec:8.1f}s  sym={r.get('symbol')} type={r.get('signal_type')} event_id={r.get('event_id')} start_ts={r.get('start_ts')} decision_ts={r.get('decision_ts')}"
            )

    if args.dump_csv:
        import csv

        path = str(args.dump_csv)
        if dump_rows:
            fieldnames = list(dump_rows[0].keys())
            with open(path, "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                for row in dump_rows:
                    w.writerow(row)
            print(f"dumped_csv={path} rows={len(dump_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
