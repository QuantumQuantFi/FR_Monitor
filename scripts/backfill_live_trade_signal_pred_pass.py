from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb  # type: ignore
except Exception:
    psycopg = None  # type: ignore
    dict_row = None  # type: ignore
    Jsonb = None  # type: ignore


def main() -> int:
    # Ensure repo root is importable when running as `python scripts/...`.
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    parser = argparse.ArgumentParser(description="Backfill live_trade_signal.payload.pred_pass for historical rows.")
    parser.add_argument("--dsn", default=None, help="Postgres DSN (default: WATCHLIST_PG_CONFIG['dsn'])")
    parser.add_argument("--days", type=int, default=30, help="Lookback days for signals (default: 30)")
    parser.add_argument("--limit", type=int, default=5000, help="Max rows to update (default: 5000)")
    parser.add_argument("--dry_run", action="store_true", help="Only print counts; don't update")
    args = parser.parse_args()

    if psycopg is None:
        raise SystemExit("psycopg not installed. Activate venv and pip install -r requirements.txt")

    import config as _cfg  # local import
    from trading.live_trading_manager import LiveTradingConfig, LiveTradingManager  # local import

    dsn = str(args.dsn or (_cfg.WATCHLIST_PG_CONFIG.get("dsn") if hasattr(_cfg, "WATCHLIST_PG_CONFIG") else "") or "").strip()
    if not dsn:
        raise SystemExit("missing dsn: pass --dsn or set WATCHLIST_PG_DSN")

    lt = getattr(_cfg, "LIVE_TRADING_CONFIG", {}) or {}
    mgr = LiveTradingManager(
        LiveTradingConfig(
            enabled=False,
            dsn=dsn,
            allowed_exchanges=tuple(),
            horizon_min=int(lt.get("horizon_min") or 240),
            pnl_threshold=float(lt.get("pnl_threshold") or 0.0085),
            win_prob_threshold=float(lt.get("win_prob_threshold") or 0.85),
            v2_enabled=bool(lt.get("v2_enabled", True)),
            v2_pnl_threshold_240=float(lt.get("v2_pnl_threshold_240") or (lt.get("pnl_threshold") or 0.0085)),
            v2_win_prob_threshold_240=float(lt.get("v2_win_prob_threshold_240") or (lt.get("win_prob_threshold") or 0.85)),
            v2_pnl_threshold_1440=float(lt.get("v2_pnl_threshold_1440") or (lt.get("pnl_threshold") or 0.0085)),
            v2_win_prob_threshold_1440=float(lt.get("v2_win_prob_threshold_1440") or (lt.get("win_prob_threshold") or 0.85)),
        )
    )

    conn_kwargs: Dict[str, Any] = {"autocommit": True}
    if dict_row is not None:
        conn_kwargs["row_factory"] = dict_row
    with psycopg.connect(dsn, **conn_kwargs) as conn:
        rows = conn.execute(
            """
            WITH base AS (
              SELECT
                s.id AS signal_id,
                s.event_id,
                COALESCE(
                  (e.features_agg #>> '{meta_last,pnl_regression_ob,pred,240,pnl_hat}')::double precision,
                  (e.features_agg #>> '{meta_last,pnl_regression,pred,240,pnl_hat}')::double precision
                ) AS pnl_hat_240,
                COALESCE(
                  (e.features_agg #>> '{meta_last,pnl_regression_ob,pred,240,win_prob}')::double precision,
                  (e.features_agg #>> '{meta_last,pnl_regression,pred,240,win_prob}')::double precision
                ) AS win_prob_240,
                (e.features_agg #>> '{meta_last,pred_v2,240,pnl_hat}')::double precision AS v2_pnl_hat_240,
                (e.features_agg #>> '{meta_last,pred_v2,240,win_prob}')::double precision AS v2_win_prob_240,
                CASE
                  WHEN (e.features_agg #>> '{meta_last,pred_v2,240,error}') IS NOT NULL THEN FALSE
                  WHEN (e.features_agg #>> '{meta_last,pred_v2,240,missing_rate}') IS NULL
                    THEN (e.features_agg #>> '{meta_last,pred_v2,240,ok}')::boolean
                  ELSE (
                    (e.features_agg #>> '{meta_last,pred_v2,240,missing_rate}')::double precision
                    <= COALESCE(
                      (e.features_agg #>> '{meta_last,pred_v2_meta,max_missing_ratio}')::double precision,
                      0.2
                    )
                  )
                END AS v2_ok_240,
                (e.features_agg #>> '{meta_last,pred_v2,1440,pnl_hat}')::double precision AS v2_pnl_hat_1440,
                (e.features_agg #>> '{meta_last,pred_v2,1440,win_prob}')::double precision AS v2_win_prob_1440,
                CASE
                  WHEN (e.features_agg #>> '{meta_last,pred_v2,1440,error}') IS NOT NULL THEN FALSE
                  WHEN (e.features_agg #>> '{meta_last,pred_v2,1440,missing_rate}') IS NULL
                    THEN (e.features_agg #>> '{meta_last,pred_v2,1440,ok}')::boolean
                  ELSE (
                    (e.features_agg #>> '{meta_last,pred_v2,1440,missing_rate}')::double precision
                    <= COALESCE(
                      (e.features_agg #>> '{meta_last,pred_v2_meta,max_missing_ratio}')::double precision,
                      0.2
                    )
                  )
                END AS v2_ok_1440
              FROM watchlist.live_trade_signal s
              JOIN watchlist.watch_signal_event e
                ON e.id = s.event_id
              WHERE s.created_at >= now() - make_interval(days := %s)
                AND (s.payload #> '{pred_pass}') IS NULL
              ORDER BY s.id DESC
              LIMIT %s
            )
            SELECT * FROM base;
            """,
            (int(args.days), int(args.limit)),
        ).fetchall()

        total = len(rows)
        if not total:
            print("no rows to backfill")
            return 0
        print(f"to_backfill={total} dry_run={bool(args.dry_run)}")
        if args.dry_run:
            return 0

        updated = 0
        for r in rows:
            event = {
                "pnl_hat_240": r.get("pnl_hat_240"),
                "win_prob_240": r.get("win_prob_240"),
                "v2_pnl_hat_240": r.get("v2_pnl_hat_240"),
                "v2_win_prob_240": r.get("v2_win_prob_240"),
                "v2_ok_240": r.get("v2_ok_240"),
                "v2_pnl_hat_1440": r.get("v2_pnl_hat_1440"),
                "v2_win_prob_1440": r.get("v2_win_prob_1440"),
                "v2_ok_1440": r.get("v2_ok_1440"),
            }
            pred_pass = mgr._pred_pass_summary(event)
            signal_id = int(r.get("signal_id") or 0)
            if not signal_id:
                continue
            val: Any = pred_pass
            if Jsonb is not None:
                val = Jsonb(pred_pass)
            conn.execute(
                """
                UPDATE watchlist.live_trade_signal
                   SET payload = COALESCE(payload, '{}'::jsonb) || jsonb_build_object('pred_pass', %s::jsonb),
                       updated_at = now()
                 WHERE id = %s;
                """,
                (val, signal_id),
            )
            updated += 1
        print(f"updated={updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
