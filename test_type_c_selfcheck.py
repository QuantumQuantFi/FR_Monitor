#!/usr/bin/env python3
import json
import time
from typing import Any, Dict, List, Optional, Tuple

import config

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

from trading.live_trading_manager import LiveTradingConfig, LiveTradingManager, TradeExecutionError


PREFER_SYMBOLS = ("ETH", "BTC", "SOL", "BNB", "XRP")


def _parse_obj(val: Any) -> Dict[str, Any]:
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _build_config() -> LiveTradingConfig:
    cfg = config.LIVE_TRADING_CONFIG
    spot_notional = float(cfg.get("spot_per_leg_notional_usdt") or 0.0)
    return LiveTradingConfig(
        enabled=True,
        dsn=str(config.WATCHLIST_PG_CONFIG.get("dsn")),
        allowed_exchanges=tuple(
            x.strip().lower() for x in str(cfg.get("allowed_exchanges") or "").split(",") if x.strip()
        )
        or ("binance", "bybit"),
        horizon_min=int(cfg.get("horizon_min", 240)),
        pnl_threshold=float(cfg.get("pnl_threshold", 0.012)),
        win_prob_threshold=float(cfg.get("win_prob_threshold", 0.93)),
        type_c_pnl_threshold=float(cfg.get("type_c_pnl_threshold", 0.012)),
        type_c_win_prob_threshold=float(cfg.get("type_c_win_prob_threshold", 0.93)),
        spot_trading_enabled=bool(cfg.get("spot_trading_enabled", False)),
        spot_allowed_exchanges=tuple(
            x.strip().lower() for x in str(cfg.get("spot_allowed_exchanges") or "").split(",") if x.strip()
        )
        or ("okx", "binance", "bybit", "bitget"),
        spot_per_leg_notional_usdt=spot_notional if spot_notional > 0 else None,
        v2_enabled=bool(cfg.get("v2_enabled", True)),
        v2_pnl_threshold_240=float(cfg.get("v2_pnl_threshold_240", 0.012)),
        v2_win_prob_threshold_240=float(cfg.get("v2_win_prob_threshold_240", 0.92)),
        v2_pnl_threshold_1440=float(cfg.get("v2_pnl_threshold_1440", 0.014)),
        v2_win_prob_threshold_1440=float(cfg.get("v2_win_prob_threshold_1440", 0.93)),
        max_concurrent_trades=int(cfg.get("max_concurrent_trades", 10)),
        scan_interval_seconds=float(cfg.get("scan_interval_seconds", 20.0)),
        monitor_interval_seconds=float(cfg.get("monitor_interval_seconds", 60.0)),
        take_profit_ratio=float(cfg.get("take_profit_ratio", 0.8)),
        orderbook_confirm_samples=int(cfg.get("orderbook_confirm_samples", 3)),
        orderbook_confirm_sleep_seconds=float(cfg.get("orderbook_confirm_sleep_seconds", 0.7)),
        max_hold_days=int(cfg.get("max_hold_days", 7)),
        stop_loss_total_pnl_pct=float(cfg.get("stop_loss_total_pnl_pct", 0.01)),
        stop_loss_funding_per_hour_pct=float(cfg.get("stop_loss_funding_per_hour_pct", 0.003)),
        max_abs_funding=float(cfg.get("max_abs_funding", 0.001)),
        close_retry_cooldown_seconds=float(cfg.get("close_retry_cooldown_seconds", 120.0)),
        scale_in_enabled=bool(cfg.get("scale_in_enabled", True)),
        scale_in_max_entries=int(cfg.get("scale_in_max_entries", 4)),
        scale_in_trigger_mult=float(cfg.get("scale_in_trigger_mult", 1.5)),
        scale_in_min_interval_minutes=int(cfg.get("scale_in_min_interval_minutes", 30)),
        scale_in_signal_max_age_minutes=int(cfg.get("scale_in_signal_max_age_minutes", 30)),
        scale_in_max_total_notional_usdt=(
            float(cfg.get("scale_in_max_total_notional_usdt"))
            if float(cfg.get("scale_in_max_total_notional_usdt") or 0.0) > 0
            else None
        ),
        event_lookback_minutes=180,
        per_leg_notional_usdt=float(cfg.get("per_leg_notional_usdt", 50.0)),
        candidate_limit=int(cfg.get("candidate_limit", 50)),
        per_symbol_top_k=int(cfg.get("per_symbol_top_k", 3)),
        max_symbols_per_scan=int(cfg.get("max_symbols_per_scan", 8)),
        kick_driven=bool(cfg.get("kick_driven", True)),
    )


def _pick_candidate(
    mgr: LiveTradingManager, conn, candidates: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    def _pref_rank(sym: str) -> int:
        try:
            return PREFER_SYMBOLS.index(sym)
        except ValueError:
            return len(PREFER_SYMBOLS) + 1

    ordered = sorted(
        (c for c in candidates if str(c.get("signal_type") or "").upper() == "C"),
        key=lambda r: (_pref_rank(str(r.get("symbol") or "").upper()), -(r.get("pnl_hat_240") or 0.0)),
    )
    for ev in ordered:
        symbol = str(ev.get("symbol") or "").upper()
        if not symbol:
            continue
        active = conn.execute(
            """
            SELECT 1 FROM watchlist.live_trade_signal
             WHERE symbol=%s AND status IN ('opening','open','closing')
             LIMIT 1;
            """,
            (symbol,),
        ).fetchone()
        if active:
            continue
        return ev
    return None


def main() -> None:
    if psycopg is None:
        print("psycopg not installed; cannot run self-check.")
        return

    cfg = _build_config()
    mgr = LiveTradingManager(cfg)

    conn_kwargs = {"autocommit": True}
    if dict_row:
        conn_kwargs["row_factory"] = dict_row

    with psycopg.connect(cfg.dsn, **conn_kwargs) as conn:
        candidates = mgr._fetch_candidates(conn)
        if not candidates:
            print("No live-trading candidates found.")
            return

        ordered = []
        primary = _pick_candidate(mgr, conn, candidates)
        if primary:
            ordered.append(primary)
        ordered.extend([c for c in candidates if c not in ordered])

        event = None
        reval = None
        confirm = None
        spot_ex = None
        perp_ex = None

        for ev in ordered[:20]:
            if str(ev.get("signal_type") or "").upper() != "C":
                continue
            symbol = str(ev.get("symbol") or "").upper()
            if not symbol:
                continue
            pred_choice = mgr._pick_pred_choice(ev)
            if not pred_choice:
                continue
            pred_choice = dict(pred_choice)
            pred_choice["thr_pnl"] = float(getattr(cfg, "type_c_pnl_threshold", cfg.pnl_threshold))
            pred_choice["thr_prob"] = float(getattr(cfg, "type_c_win_prob_threshold", cfg.win_prob_threshold))

            spot_ex, perp_ex, ex_source = mgr._resolve_type_c_exchanges(ev)
            if not (spot_ex and perp_ex):
                continue
            if not (mgr._supported_spot_exchange(spot_ex) and mgr._supported_exchange(perp_ex)):
                continue

            factors = _parse_obj(ev.get("factors") or {})
            if not factors:
                continue

            reval = mgr._revalidate_with_orderbook_type_c(
                symbol=symbol,
                spot_exchange=str(spot_ex),
                perp_exchange=str(perp_ex),
                base_factors=factors,
                pred_choice=pred_choice,
            )
            if not reval or not reval.get("ok"):
                reason = (reval or {}).get("reason") if isinstance(reval, dict) else "no_reval"
                print(f"Skip {symbol}: revalidate {reason}")
                continue

            confirm = mgr._confirm_open_signal_with_orderbook_type_c(
                symbol=symbol,
                spot_exchange=str(spot_ex),
                perp_exchange=str(perp_ex),
                base_factors=factors,
                initial_reval=reval if isinstance(reval, dict) else None,
                pred_choice=pred_choice,
            )
            if not confirm.get("ok"):
                print(f"Skip {symbol}: confirm {confirm.get('reason')}")
                continue

            event = ev
            break

        if not (event and reval and confirm and spot_ex and perp_ex):
            print("No Type C candidate passed orderbook revalidation/confirm.")
            return

        symbol = str(event.get("symbol") or "").upper()
        pred_choice = mgr._pick_pred_choice(event)
        pred_choice = dict(pred_choice or {})
        pred_choice["thr_pnl"] = float(getattr(cfg, "type_c_pnl_threshold", cfg.pnl_threshold))
        pred_choice["thr_prob"] = float(getattr(cfg, "type_c_win_prob_threshold", cfg.win_prob_threshold))

        client_base = f"selfC{int(event['id'])}O{int(time.time())}"
        payload = {
            "event": {
                "id": int(event["id"]),
                "start_ts": str(event.get("start_ts")),
                "symbol": symbol,
                "signal_type": event.get("signal_type"),
                "trigger_details": event.get("trigger_details"),
            },
            "pred_choice": pred_choice,
            "orderbook_revalidation": {
                "pnl_hat": reval.get("pnl_hat"),
                "win_prob": reval.get("win_prob"),
                "pnl_hat_eval": reval.get("pnl_hat_eval"),
                "win_prob_eval": reval.get("win_prob_eval"),
                "pred_source": reval.get("pred_source"),
                "pred_horizon_min": reval.get("pred_horizon_min"),
            },
            "orderbook_confirm_open": confirm,
            "self_check": True,
        }

        signal_id = mgr._insert_signal(
            conn,
            event=event,
            leg_long_exchange=str(confirm.get("long_exchange") or spot_ex),
            leg_short_exchange=str(confirm.get("short_exchange") or perp_ex),
            pnl_hat=float(pred_choice.get("pnl_hat") or 0.0),
            win_prob=float(pred_choice.get("win_prob") or 0.0),
            pnl_hat_ob=float(reval.get("pnl_hat_eval") or 0.0),
            win_prob_ob=float(reval.get("win_prob_eval") or 0.0),
            horizon_min=int(pred_choice.get("horizon_min") or cfg.horizon_min),
            pred_source=str(pred_choice.get("source") or "v1_240"),
            payload=payload,
            status="opening",
            reason=None,
            client_order_id_base=client_base,
        )
        if not signal_id:
            print("Failed to insert live_trade_signal; aborting.")
            return

        try:
            mgr._open_trade(
                conn,
                signal_id,
                symbol,
                str(confirm.get("long_exchange") or spot_ex),
                str(confirm.get("short_exchange") or perp_ex),
                client_base,
                pnl_hat_ob=float(reval.get("pnl_hat_eval") or reval.get("pnl_hat") or 0.0),
                signal_type="C",
            )
            mgr._update_signal_status(conn, signal_id, "open")
        except TradeExecutionError as exc:
            mgr._record_error(
                conn,
                signal_id=signal_id,
                stage="selfcheck_open_trade",
                error_type="TradeExecutionError",
                message=str(exc),
                context={"symbol": symbol, "spot_ex": spot_ex, "perp_ex": perp_ex},
            )
            mgr._update_signal_status(conn, signal_id, "failed", reason=str(exc))
            print(f"Open failed: {exc}")
            return

        qty_long, qty_short = mgr._load_open_quantities(conn, signal_id, symbol)
        if qty_long is None or qty_short is None:
            qty_long = mgr._get_exchange_position_size(str(spot_ex), symbol, market_type="spot") or 0.0
            qty_short = abs(mgr._get_exchange_position_size(str(perp_ex), symbol, market_type="perp") or 0.0)

        mgr._attempt_close(
            conn,
            signal_row={
                "id": signal_id,
                "status": "open",
                "signal_type": "C",
                "symbol": symbol,
                "leg_long_exchange": str(spot_ex),
                "leg_short_exchange": str(perp_ex),
                "close_requested_at": None,
            },
            close_reason="self_check",
            close_pnl_spread=0.0,
            qty_long=float(qty_long or 0.0),
            qty_short=float(qty_short or 0.0),
        )

        print(
            f"Type C self-check done: signal_id={signal_id} symbol={symbol} "
            f"spot={spot_ex} perp={perp_ex}"
        )


if __name__ == "__main__":
    main()
