from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import logging
import math
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    psycopg = None  # type: ignore
    dict_row = None  # type: ignore
    Jsonb = None  # type: ignore

from orderbook_utils import fetch_orderbook_prices, fetch_orderbook_prices_for_quantity
from watchlist_pnl_regression_model import predict_bc
from trading.trade_executor import (
    TradeExecutionError,
    execute_perp_market_order,
    place_binance_perp_market_order,
    get_binance_perp_positions,
    get_binance_perp_order,
    get_bybit_linear_order,
    get_bybit_linear_positions,
    get_bitget_usdt_perp_order_detail,
    get_bitget_usdt_perp_positions,
    get_hyperliquid_perp_positions,
    get_okx_swap_positions,
    place_bitget_usdt_perp_market_order,
    place_bybit_linear_market_order,
    place_hyperliquid_perp_market_order,
    get_okx_swap_order,
    place_okx_swap_market_order,
    set_binance_perp_leverage,
    set_bybit_linear_leverage,
    set_bitget_usdt_perp_leverage,
    set_okx_swap_leverage,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _jsonb(val: Any) -> Any:
    if val is None:
        return None
    if Jsonb is not None:
        return Jsonb(val)
    return json.dumps(val, ensure_ascii=False)


@dataclass(frozen=True)
class LiveTradingConfig:
    enabled: bool
    dsn: str
    allowed_exchanges: Tuple[str, ...] = ("binance", "bybit", "okx")
    horizon_min: int = 240
    pnl_threshold: float = 0.013
    win_prob_threshold: float = 0.94
    max_concurrent_trades: int = 10
    scan_interval_seconds: float = 20.0
    event_lookback_minutes: int = 30
    per_leg_notional_usdt: float = 20.0
    orderbook_market_type: str = "perp"
    candidate_limit: int = 200
    per_symbol_top_k: int = 3
    monitor_interval_seconds: float = 60.0
    take_profit_ratio: float = 0.8
    orderbook_confirm_samples: int = 3
    orderbook_confirm_sleep_seconds: float = 0.7
    max_hold_days: int = 7
    close_retry_cooldown_seconds: float = 120.0


class LiveTradingManager:
    """
    Minimal live trading loop (Phase 1):
    - Read Type B candidates from watchlist.watch_signal_event (PG).
    - Apply thresholds (pnl_hat/win_prob at horizon=240).
    - Revalidate using orderbook sweep prices with per-leg notional.
    - Enforce: max 10 concurrent, one active trade per symbol.
    - Execute 2-leg perp market orders (long low, short high).
    - Record signals/orders/errors in PG tables under schema=watchlist.
    """

    def __init__(self, config: LiveTradingConfig):
        self.config = config
        self.logger = logging.getLogger("live_trading")
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._symbol_locks: Dict[str, threading.Lock] = {}
        self._symbol_locks_lock = threading.Lock()

    def start(self) -> None:
        if not self.config.enabled:
            self.logger.info("live trading disabled; skip start")
            return
        if psycopg is None:
            self.logger.error("psycopg not installed; live trading cannot start")
            return
        if self._thread:
            return
        self.ensure_schema()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_loop, name="live-trading", daemon=True)
        self._thread.start()
        self.logger.info(
            "live trading started horizon=%sm pnl>%.4f win>%.3f max=%s",
            self.config.horizon_min,
            self.config.pnl_threshold,
            self.config.win_prob_threshold,
            self.config.max_concurrent_trades,
        )

    def stop(self) -> None:
        if not self._thread:
            return
        self._stop.set()
        self._thread.join(timeout=5)
        self._thread = None

    def manual_force_close(self, signal_id: int) -> Dict[str, Any]:
        """One-click manual flatten for a signal.

        - Query both exchanges' current positions for the signal symbol.
        - Submit reduce-only market orders to flatten any residual exposure.
        - Update signal status to closing/closed accordingly and record orders/errors in PG.
        """
        if psycopg is None:
            return {"ok": False, "error": "psycopg not installed"}
        sid = int(signal_id or 0)
        if sid <= 0:
            return {"ok": False, "error": "invalid signal_id"}

        conn_kwargs: Dict[str, Any] = {"autocommit": True}
        if dict_row:
            conn_kwargs["row_factory"] = dict_row

        with psycopg.connect(self.config.dsn, **conn_kwargs) as conn:
            row = conn.execute(
                "SELECT * FROM watchlist.live_trade_signal WHERE id=%s LIMIT 1;",
                (sid,),
            ).fetchone()
            if not row or not isinstance(row, dict):
                return {"ok": False, "error": "signal not found", "signal_id": sid}

            symbol = str(row.get("symbol") or "").upper()
            long_ex = str(row.get("leg_long_exchange") or "").lower()
            short_ex = str(row.get("leg_short_exchange") or "").lower()
            status = str(row.get("status") or "").lower()

            if not symbol or not long_ex or not short_ex:
                return {
                    "ok": False,
                    "error": "signal missing symbol/exchanges",
                    "signal_id": sid,
                    "symbol": symbol,
                    "long_ex": long_ex,
                    "short_ex": short_ex,
                }

            lock = self._get_symbol_lock(symbol)
            with lock:
                # Mark closing to prevent concurrent monitor/open logic from racing.
                conn.execute(
                    """
                    UPDATE watchlist.live_trade_signal
                       SET status='closing',
                           close_reason=COALESCE(close_reason, 'manual'),
                           close_requested_at=now(),
                           close_pnl_spread=COALESCE(close_pnl_spread, last_pnl_spread),
                           updated_at=now()
                     WHERE id=%s
                       AND status <> 'closed';
                    """,
                    (sid,),
                )

                # Query positions concurrently.
                with ThreadPoolExecutor(max_workers=2) as pool:
                    fut_a = pool.submit(self._get_exchange_position_size, long_ex, symbol)
                    fut_b = pool.submit(self._get_exchange_position_size, short_ex, symbol)
                    pos_a = fut_a.result()
                    pos_b = fut_b.result()

                before = {"long_exchange": pos_a, "short_exchange": pos_b}

                # Place reduce-only close orders for any non-flat exposure.
                orders: List[Dict[str, Any]] = []
                close_base = f"wl{sid}M{int(time.time())}"
                for leg_name, ex, size, client_suffix in (
                    ("long", long_ex, pos_a, "L"),
                    ("short", short_ex, pos_b, "S"),
                ):
                    if size is None:
                        self._record_error(
                            conn,
                            signal_id=sid,
                            stage="manual_close_position_query",
                            error_type="PositionQueryError",
                            message="position query failed or returned None",
                            context={"exchange": ex, "symbol": symbol, "leg": leg_name},
                        )
                        continue
                    if abs(float(size)) <= 1e-9:
                        continue

                    pos_leg = "long" if float(size) > 0 else "short"
                    qty = abs(float(size))
                    client_id = f"{close_base}-{client_suffix}"
                    try:
                        resp = self._place_close_order(
                            exchange=ex,
                            symbol=symbol,
                            position_leg=pos_leg,
                            quantity=qty,
                            client_order_id=client_id,
                        )
                        fill = self._parse_fill_fields(ex, symbol, resp)
                        order_param = _jsonb(resp) if resp is not None else None
                        side = "short" if pos_leg == "long" else "long"
                        conn.execute(
                            """
                            INSERT INTO watchlist.live_trade_order(
                                signal_id, action, leg, exchange, side, market_type, notional_usdt, quantity,
                                filled_qty, avg_price, cum_quote, exchange_order_id,
                                client_order_id, submitted_at, order_resp, status
                            )
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                            """,
                            (
                                sid,
                                "close",
                                leg_name,
                                ex,
                                side,
                                "perp",
                                None,
                                str(qty),
                                float(fill.get("filled_qty")) if fill.get("filled_qty") is not None else None,
                                float(fill.get("avg_price")) if fill.get("avg_price") is not None else None,
                                float(fill.get("cum_quote")) if fill.get("cum_quote") is not None else None,
                                str(fill.get("exchange_order_id")) if fill.get("exchange_order_id") is not None else None,
                                client_id,
                                _utcnow(),
                                order_param,
                                str(fill.get("status") or "submitted"),
                            ),
                        )
                        orders.append(
                            {
                                "leg": leg_name,
                                "exchange": ex,
                                "position_leg_closed": pos_leg,
                                "quantity": qty,
                                "client_order_id": client_id,
                                "fill": fill,
                                "raw": resp,
                            }
                        )
                    except Exception as exc:
                        self._record_error(
                            conn,
                            signal_id=sid,
                            stage="manual_close_order",
                            error_type=type(exc).__name__,
                            message=str(exc),
                            context={"exchange": ex, "symbol": symbol, "leg": leg_name, "pos_leg": pos_leg, "qty": qty},
                        )

                # Wait a short time for positions to flatten, then update status if flat.
                after = {"long_exchange": None, "short_exchange": None}
                flat = None
                for _ in range(6):
                    time.sleep(0.6)
                    with ThreadPoolExecutor(max_workers=2) as pool:
                        fut_a2 = pool.submit(self._get_exchange_position_size, long_ex, symbol)
                        fut_b2 = pool.submit(self._get_exchange_position_size, short_ex, symbol)
                        a2 = fut_a2.result()
                        b2 = fut_b2.result()
                    after = {"long_exchange": a2, "short_exchange": b2}
                    if a2 is not None and b2 is not None and abs(float(a2)) <= 1e-9 and abs(float(b2)) <= 1e-9:
                        flat = True
                        break
                    flat = False

                if flat:
                    self._update_signal_status(conn, sid, "closed")

                return {
                    "ok": True,
                    "signal_id": sid,
                    "symbol": symbol,
                    "status_before": status,
                    "positions_before": before,
                    "orders": orders,
                    "positions_after": after,
                    "closed": bool(flat),
                }

    def _get_symbol_lock(self, symbol: str) -> threading.Lock:
        key = (symbol or "").upper()
        with self._symbol_locks_lock:
            lock = self._symbol_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._symbol_locks[key] = lock
            return lock

    def _run_loop(self) -> None:
        last_monitor_ts = 0.0
        while not self._stop.wait(timeout=self.config.scan_interval_seconds):
            try:
                self.process_once()
            except Exception as exc:  # pragma: no cover - safety net
                self.logger.exception("live trading loop error: %s", exc)
            try:
                now = time.time()
                if now - last_monitor_ts >= float(self.config.monitor_interval_seconds):
                    self.monitor_open_trades_once()
                    last_monitor_ts = now
            except Exception as exc:  # pragma: no cover - safety net
                self.logger.exception("live trading monitor error: %s", exc)

    def ensure_schema(self) -> None:
        if psycopg is None:
            return
        ddl = """
        CREATE TABLE IF NOT EXISTS watchlist.live_trade_signal (
          id bigserial PRIMARY KEY,
          created_at timestamptz NOT NULL DEFAULT now(),
          updated_at timestamptz NOT NULL DEFAULT now(),
          event_id bigint NOT NULL,
          symbol text NOT NULL,
          signal_type char(1) NOT NULL,
          horizon_min int NOT NULL,
          pnl_hat double precision,
          win_prob double precision,
          pnl_hat_ob double precision,
          win_prob_ob double precision,
          leg_long_exchange text,
          leg_short_exchange text,
          status text NOT NULL DEFAULT 'new',
          reason text,
          payload jsonb,
          client_order_id_base text,
          opened_at timestamptz,
          closed_at timestamptz,
          close_reason text,
          entry_spread_metric double precision,
          take_profit_pnl double precision,
          force_close_at timestamptz,
          close_requested_at timestamptz,
          last_check_at timestamptz,
          last_spread_metric double precision,
          last_pnl_spread double precision,
          close_pnl_spread double precision
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_live_trade_signal_event
          ON watchlist.live_trade_signal(event_id);
        CREATE INDEX IF NOT EXISTS idx_live_trade_signal_status
          ON watchlist.live_trade_signal(status, created_at DESC);

        -- One active trade per symbol (opening/open/closing).
        CREATE UNIQUE INDEX IF NOT EXISTS idx_live_trade_signal_symbol_active
          ON watchlist.live_trade_signal(symbol)
          WHERE status IN ('opening','open','closing');

        CREATE TABLE IF NOT EXISTS watchlist.live_trade_order (
          id bigserial PRIMARY KEY,
          created_at timestamptz NOT NULL DEFAULT now(),
          signal_id bigint NOT NULL REFERENCES watchlist.live_trade_signal(id) ON DELETE CASCADE,
          action text NOT NULL DEFAULT 'open',
          leg text NOT NULL,
          exchange text NOT NULL,
          side text NOT NULL,
          market_type text NOT NULL DEFAULT 'perp',
          notional_usdt double precision,
          quantity text,
          filled_qty double precision,
          avg_price double precision,
          cum_quote double precision,
          exchange_order_id text,
          client_order_id text,
          submitted_at timestamptz,
          order_resp jsonb,
          status text NOT NULL DEFAULT 'submitted'
        );
        CREATE INDEX IF NOT EXISTS idx_live_trade_order_signal
          ON watchlist.live_trade_order(signal_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS watchlist.live_trade_spread_sample (
          id bigserial PRIMARY KEY,
          ts timestamptz NOT NULL DEFAULT now(),
          signal_id bigint NOT NULL REFERENCES watchlist.live_trade_signal(id) ON DELETE CASCADE,
          symbol text NOT NULL,
          long_exchange text,
          short_exchange text,
          long_sell_px double precision,
          short_buy_px double precision,
          spread_metric double precision,
          pnl_spread double precision,
          pnl_hat_ob double precision,
          take_profit_pnl double precision,
          decision text,
          context jsonb
        );
        CREATE INDEX IF NOT EXISTS idx_live_trade_spread_sample_signal
          ON watchlist.live_trade_spread_sample(signal_id, ts DESC);

        CREATE TABLE IF NOT EXISTS watchlist.live_trade_error (
          id bigserial PRIMARY KEY,
          ts timestamptz NOT NULL DEFAULT now(),
          signal_id bigint REFERENCES watchlist.live_trade_signal(id) ON DELETE SET NULL,
          stage text NOT NULL,
          error_type text,
          message text,
          context jsonb
        );
        CREATE INDEX IF NOT EXISTS idx_live_trade_error_ts
          ON watchlist.live_trade_error(ts DESC);

        -- Backward-compatible schema upgrades (safe no-op when already present).
        ALTER TABLE watchlist.live_trade_signal
          ADD COLUMN IF NOT EXISTS entry_spread_metric double precision;
        ALTER TABLE watchlist.live_trade_signal
          ADD COLUMN IF NOT EXISTS take_profit_pnl double precision;
        ALTER TABLE watchlist.live_trade_signal
          ADD COLUMN IF NOT EXISTS force_close_at timestamptz;
        ALTER TABLE watchlist.live_trade_signal
          ADD COLUMN IF NOT EXISTS close_requested_at timestamptz;
        ALTER TABLE watchlist.live_trade_signal
          ADD COLUMN IF NOT EXISTS last_check_at timestamptz;
        ALTER TABLE watchlist.live_trade_signal
          ADD COLUMN IF NOT EXISTS last_spread_metric double precision;
        ALTER TABLE watchlist.live_trade_signal
          ADD COLUMN IF NOT EXISTS last_pnl_spread double precision;
        ALTER TABLE watchlist.live_trade_signal
          ADD COLUMN IF NOT EXISTS close_pnl_spread double precision;

        ALTER TABLE watchlist.live_trade_order
          ADD COLUMN IF NOT EXISTS action text NOT NULL DEFAULT 'open';
        ALTER TABLE watchlist.live_trade_order
          ADD COLUMN IF NOT EXISTS filled_qty double precision;
        ALTER TABLE watchlist.live_trade_order
          ADD COLUMN IF NOT EXISTS avg_price double precision;
        ALTER TABLE watchlist.live_trade_order
          ADD COLUMN IF NOT EXISTS cum_quote double precision;
        ALTER TABLE watchlist.live_trade_order
          ADD COLUMN IF NOT EXISTS exchange_order_id text;
        """
        with psycopg.connect(self.config.dsn, autocommit=True) as conn:
            conn.execute(ddl)
            try:
                self._apply_runtime_config_migrations(conn)
            except Exception as exc:
                self.logger.warning("apply runtime config migrations failed: %s", exc)

    def _parse_fill_fields(self, exchange: str, symbol: str, order: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Best-effort extraction of actual fill metrics for storage/UI.

        Returns subset of:
        - filled_qty (base qty when available)
        - avg_price
        - cum_quote (USDT notional when available)
        - exchange_order_id
        - status (exchange-reported)
        """
        if not isinstance(order, dict):
            return {}

        ex = (exchange or "").lower().strip()

        def _float_or_none(val: Any) -> Optional[float]:
            if val is None:
                return None
            try:
                f = float(val)
            except Exception:
                return None
            if not math.isfinite(f):
                return None
            return f

        def _first_dict(val: Any) -> Dict[str, Any]:
            if isinstance(val, dict):
                return val
            if isinstance(val, list) and val and isinstance(val[0], dict):
                return val[0]
            return {}

        info: Dict[str, Any] = {}

        if ex == "binance":
            info["exchange_order_id"] = str(order.get("orderId") or "") or None
            info["filled_qty"] = _float_or_none(order.get("executedQty"))
            info["avg_price"] = _float_or_none(order.get("avgPrice"))
            info["cum_quote"] = _float_or_none(order.get("cumQuote"))
            status = order.get("status")
            info["status"] = str(status) if status is not None else None
            if info.get("exchange_order_id") and (
                info.get("filled_qty") is None
                or info.get("avg_price") is None
                or float(info.get("filled_qty") or 0.0) <= 0.0
            ):
                detail = None
                for _ in range(3):
                    try:
                        detail = get_binance_perp_order(symbol, order_id=info["exchange_order_id"])
                    except Exception:
                        detail = None
                    if isinstance(detail, dict):
                        filled = _float_or_none(detail.get("executedQty"))
                        avg_px = _float_or_none(detail.get("avgPrice"))
                        if filled and filled > 0 and avg_px and avg_px > 0:
                            break
                    time.sleep(0.25)
                if isinstance(detail, dict):
                    info["filled_qty"] = info.get("filled_qty") if info.get("filled_qty") is not None else _float_or_none(detail.get("executedQty"))
                    info["avg_price"] = info.get("avg_price") if info.get("avg_price") is not None else _float_or_none(detail.get("avgPrice"))
                    info["cum_quote"] = info.get("cum_quote") if info.get("cum_quote") is not None else _float_or_none(detail.get("cumQuote"))
                    st = detail.get("status")
                    if st is not None:
                        info["status"] = str(st)
            return {k: v for k, v in info.items() if v is not None}

        if ex == "okx":
            base = _first_dict(order.get("data"))
            ord_id = base.get("ordId")
            cl_id = base.get("clOrdId") or base.get("clientOid")
            info["exchange_order_id"] = str(ord_id or "") or None
            detail = None
            for _ in range(3):
                try:
                    detail = get_okx_swap_order(
                        symbol,
                        ord_id=str(ord_id) if ord_id else None,
                        client_order_id=str(cl_id) if cl_id else None,
                    )
                except Exception:
                    detail = None
                if isinstance(detail, dict):
                    avg_px = _float_or_none(detail.get("avgPx") or detail.get("fillPx"))
                    acc_fill = _float_or_none(detail.get("accFillSz") or detail.get("fillSz"))
                    if avg_px and acc_fill and acc_fill > 0:
                        break
                time.sleep(0.25)
            if isinstance(detail, dict):
                info["avg_price"] = _float_or_none(detail.get("avgPx") or detail.get("fillPx"))
                info["filled_qty"] = _float_or_none(detail.get("accFillSz") or detail.get("fillSz") or detail.get("sz"))
                info["cum_quote"] = _float_or_none(detail.get("accFillNotional") or detail.get("fillNotional"))
                status = detail.get("state") or detail.get("status")
                info["status"] = str(status) if status is not None else None
            return {k: v for k, v in info.items() if v is not None}

        if ex == "bitget":
            base = _first_dict(order.get("data"))
            order_id = base.get("orderId") or base.get("order_id")
            cl_id = base.get("clientOid") or base.get("client_oid")
            info["exchange_order_id"] = str(order_id or "") or None
            detail = None
            for _ in range(3):
                try:
                    detail = get_bitget_usdt_perp_order_detail(
                        symbol,
                        order_id=str(order_id) if order_id else None,
                        client_order_id=str(cl_id) if cl_id else None,
                    )
                except Exception:
                    detail = None
                if isinstance(detail, dict):
                    avg_px = _float_or_none(detail.get("priceAvg") or detail.get("avgPrice") or detail.get("avgPx"))
                    filled_sz = _float_or_none(detail.get("filledQty") or detail.get("filledSize") or detail.get("baseVolume"))
                    if avg_px and filled_sz and filled_sz > 0:
                        break
                time.sleep(0.25)
            if isinstance(detail, dict):
                info["avg_price"] = _float_or_none(detail.get("priceAvg") or detail.get("avgPrice") or detail.get("avgPx"))
                info["filled_qty"] = _float_or_none(
                    detail.get("filledQty")
                    or detail.get("filledSize")
                    or detail.get("baseVolume")
                    or detail.get("size")
                )
                info["cum_quote"] = _float_or_none(detail.get("quoteVolume") or detail.get("quoteVol"))
                status = detail.get("state") or detail.get("status")
                info["status"] = str(status) if status is not None else None
            return {k: v for k, v in info.items() if v is not None}

        if ex == "hyperliquid":
            info["exchange_order_id"] = str(order.get("oid") or order.get("orderId") or "") or None
            status = order.get("status")
            if status is not None:
                info["status"] = str(status)
            resp = order.get("response") if isinstance(order.get("response"), dict) else None
            data = resp.get("data") if resp and isinstance(resp.get("data"), dict) else None
            statuses = data.get("statuses") if data and isinstance(data.get("statuses"), list) else None
            first = statuses[0] if statuses and isinstance(statuses[0], dict) else None
            if first and first.get("error"):
                info["status"] = str(first.get("error"))
            filled = first.get("filled") if first and isinstance(first.get("filled"), dict) else None
            if filled:
                info["avg_price"] = _float_or_none(filled.get("avgPx") or filled.get("avgPrice"))
                info["filled_qty"] = _float_or_none(filled.get("totalSz") or filled.get("sz"))
                if info.get("exchange_order_id") is None:
                    info["exchange_order_id"] = str(filled.get("oid") or "") or None
            return {k: v for k, v in info.items() if v is not None}

        if ex == "bybit":
            base = order.get("result") if isinstance(order.get("result"), dict) else {}
            order_id = base.get("orderId") or order.get("orderId")
            cl_id = base.get("orderLinkId") or base.get("orderLinkID") or order.get("orderLinkId")
            info["exchange_order_id"] = str(order_id or "") or None
            detail = None
            for _ in range(3):
                try:
                    detail = get_bybit_linear_order(
                        symbol,
                        order_id=str(order_id) if order_id else None,
                        client_order_id=str(cl_id) if cl_id else None,
                        category="linear",
                    )
                except Exception:
                    detail = None
                if isinstance(detail, dict):
                    avg_px = _float_or_none(detail.get("avgPrice"))
                    filled_qty = _float_or_none(detail.get("cumExecQty"))
                    if avg_px and filled_qty and filled_qty > 0:
                        break
                time.sleep(0.25)
            if isinstance(detail, dict):
                info["avg_price"] = _float_or_none(detail.get("avgPrice"))
                info["filled_qty"] = _float_or_none(detail.get("cumExecQty"))
                info["cum_quote"] = _float_or_none(detail.get("cumExecValue"))
                status = detail.get("orderStatus") or detail.get("orderStatus")
                if status is not None:
                    info["status"] = str(status)
            return {k: v for k, v in info.items() if v is not None}

        return {}

    def _get_hyperliquid_position_szi(self, symbol: str) -> float:
        sym = str(symbol or "").upper()
        if not sym:
            return 0.0
        positions = get_hyperliquid_perp_positions()
        for item in positions or []:
            if not isinstance(item, dict):
                continue
            position = item.get("position") if isinstance(item.get("position"), dict) else None
            if not position:
                continue
            if str(position.get("coin") or "").upper() != sym:
                continue
            try:
                return float(position.get("szi") or 0.0)
            except Exception:
                return 0.0
        return 0.0

    def _verify_hyperliquid_position_after_open(self, symbol: str, side: str, requested_qty: float) -> None:
        """
        Hyperliquid SDK order response can be misleading; confirm the position exists before we consider the leg opened.
        """
        side_key = (side or "").strip().lower()
        expected_sign = 1.0 if side_key == "long" else -1.0
        min_abs = max(1e-12, abs(float(requested_qty or 0.0)) * 0.2)
        for _ in range(5):
            try:
                szi = float(self._get_hyperliquid_position_szi(symbol))
            except Exception:
                szi = 0.0
            if szi * expected_sign > 0 and abs(szi) >= min_abs:
                return
            time.sleep(0.4)
        raise TradeExecutionError(
            f"Hyperliquid position not detected after open: symbol={symbol} side={side_key} requested_qty={requested_qty}"
        )

    def _get_exchange_position_size(self, exchange: str, symbol: str) -> Optional[float]:
        """Best-effort signed position size for a single symbol on an exchange (perp only).

        Positive -> long exposure, negative -> short exposure.
        """
        ex = (exchange or "").lower()
        sym = (symbol or "").upper()
        if not ex or not sym:
            return None
        try:
            if ex == "hyperliquid":
                return float(self._get_hyperliquid_position_szi(sym))
            if ex == "binance":
                rows = get_binance_perp_positions(symbol=f"{sym}USDT")
                if not rows:
                    return 0.0
                if isinstance(rows[0], dict):
                    return float(rows[0].get("positionAmt") or 0)
                return 0.0
            if ex == "okx":
                rows = get_okx_swap_positions(symbol=sym)
                total = 0.0
                for r in rows or []:
                    if not isinstance(r, dict):
                        continue
                    pos_side = str(r.get("posSide") or r.get("side") or "").lower()
                    pos_raw = r.get("pos") or r.get("sz") or 0
                    try:
                        pos = float(pos_raw or 0)
                    except Exception:
                        pos = 0.0
                    if pos_side == "short":
                        total -= abs(pos)
                    elif pos_side == "long":
                        total += abs(pos)
                    elif pos_side == "net":
                        total += pos
                return total
            if ex == "bybit":
                rows = get_bybit_linear_positions(symbol=f"{sym}USDT", category="linear")
                total = 0.0
                for r in rows or []:
                    if not isinstance(r, dict):
                        continue
                    size_raw = r.get("size") or r.get("qty") or 0
                    try:
                        size = float(size_raw or 0)
                    except Exception:
                        size = 0.0
                    side = str(r.get("side") or "").lower()  # Buy/Sell
                    if side == "sell":
                        total -= abs(size)
                    elif side == "buy":
                        total += abs(size)
                return total
            if ex == "bitget":
                rows = get_bitget_usdt_perp_positions(symbol=f"{sym}USDT")
                total = 0.0
                for r in rows or []:
                    if not isinstance(r, dict):
                        continue
                    hold = str(r.get("holdSide") or "").lower()
                    qty_raw = r.get("available") or r.get("total") or r.get("openQty") or r.get("pos") or 0
                    try:
                        qty = float(qty_raw or 0)
                    except Exception:
                        qty = 0.0
                    if hold == "short":
                        total -= abs(qty)
                    elif hold == "long":
                        total += abs(qty)
                return total
        except Exception:
            return None
        return None

    def _post_fail_safety_flatten(self, conn, signal_id: int, symbol: str, exchange: str) -> None:
        """After an open failure, ensure the exchange-side symbol position is flat (best-effort)."""
        size = self._get_exchange_position_size(exchange, symbol)
        if size is None:
            self._record_error(
                conn,
                signal_id=signal_id,
                stage="post_fail_position_check",
                error_type="PositionQueryError",
                message="position query failed or returned None",
                context={"symbol": symbol, "exchange": exchange},
            )
            return
        if abs(float(size)) <= 1e-9:
            return

        pos_leg = "long" if float(size) > 0 else "short"
        qty = abs(float(size))
        client_id = f"wl{signal_id}F{int(time.time())}-{'L' if pos_leg=='long' else 'S'}"
        try:
            close_order = self._place_close_order(
                exchange=exchange,
                symbol=symbol,
                position_leg=pos_leg,
                quantity=qty,
                client_order_id=client_id,
            )
            close_order_param = _jsonb(close_order) if close_order is not None else None
            fill = self._parse_fill_fields(exchange, symbol, close_order)
            conn.execute(
                """
                INSERT INTO watchlist.live_trade_order(
                    signal_id, action, leg, exchange, side, market_type, notional_usdt, quantity,
                    filled_qty, avg_price, cum_quote, exchange_order_id,
                    client_order_id, submitted_at, order_resp, status
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """,
                (
                    int(signal_id),
                    "close",
                    "long" if pos_leg == "long" else "short",
                    str(exchange),
                    "short" if pos_leg == "long" else "long",
                    "perp",
                    None,
                    str(qty),
                    float(fill.get("filled_qty")) if fill.get("filled_qty") is not None else None,
                    float(fill.get("avg_price")) if fill.get("avg_price") is not None else None,
                    float(fill.get("cum_quote")) if fill.get("cum_quote") is not None else None,
                    str(fill.get("exchange_order_id")) if fill.get("exchange_order_id") is not None else None,
                    client_id,
                    _utcnow(),
                    close_order_param,
                    str(fill.get("status") or "submitted"),
                ),
            )
        except Exception as exc:
            self._record_error(
                conn,
                signal_id=signal_id,
                stage="post_fail_safety_close",
                error_type=type(exc).__name__,
                message=str(exc),
                context={"symbol": symbol, "exchange": exchange, "size": size},
            )

    def _apply_runtime_config_migrations(self, conn) -> None:
        """
        Apply safe, best-effort migrations driven by runtime config changes.

        Currently:
        - When max_hold_days changes, shorten force_close_at for already-open trades so the monitor
          enforces the new policy even for positions opened under older settings.
        """
        max_hold_days = int(self.config.max_hold_days)
        if max_hold_days <= 0:
            return

        conn.execute(
            """
            UPDATE watchlist.live_trade_signal
               SET force_close_at = (COALESCE(opened_at, created_at) + make_interval(days := %s))
             WHERE status IN ('opening','open')
               AND COALESCE(opened_at, created_at) IS NOT NULL
               AND (
                   force_close_at IS NULL
                   OR force_close_at > (COALESCE(opened_at, created_at) + make_interval(days := %s))
               );
            """,
            (max_hold_days, max_hold_days),
        )

    def _conn(self):
        if psycopg is None:
            raise RuntimeError("psycopg not available")
        return psycopg.connect(self.config.dsn, row_factory=dict_row, autocommit=True)

    def _active_trade_count(self, conn) -> int:
        row = conn.execute(
            "SELECT count(*) AS n FROM watchlist.live_trade_signal WHERE status IN ('opening','open','closing');"
        ).fetchone()
        return int(row["n"] or 0)

    def _insert_signal(
        self,
        conn,
        *,
        event: Dict[str, Any],
        leg_long_exchange: str,
        leg_short_exchange: str,
        pnl_hat: float,
        win_prob: float,
        pnl_hat_ob: float,
        win_prob_ob: float,
        payload: Dict[str, Any],
        status: str,
        reason: Optional[str],
        client_order_id_base: str,
    ) -> Optional[int]:
        try:
            payload_param = _jsonb(payload) if payload is not None else None
            row = conn.execute(
                """
                INSERT INTO watchlist.live_trade_signal(
                    event_id, symbol, signal_type, horizon_min,
                    pnl_hat, win_prob, pnl_hat_ob, win_prob_ob,
                    leg_long_exchange, leg_short_exchange,
                    status, reason, payload, client_order_id_base,
                    updated_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                RETURNING id;
                """,
                (
                    int(event["id"]),
                    str(event["symbol"]),
                    str(event["signal_type"]),
                    int(self.config.horizon_min),
                    float(pnl_hat),
                    float(win_prob),
                    float(pnl_hat_ob),
                    float(win_prob_ob),
                    leg_long_exchange,
                    leg_short_exchange,
                    status,
                    reason,
                    payload_param,
                    client_order_id_base,
                ),
            ).fetchone()
            return int(row["id"]) if row else None
        except Exception as exc:
            self.logger.warning("insert signal failed: %s", exc)
            return None

    def _update_signal_status(self, conn, signal_id: int, status: str, *, reason: Optional[str] = None) -> None:
        conn.execute(
            """
            UPDATE watchlist.live_trade_signal
               SET status=%s,
                   reason=COALESCE(%s, reason),
                   updated_at=now(),
                   opened_at=CASE WHEN %s='open' THEN COALESCE(opened_at, now()) ELSE opened_at END,
                   force_close_at=CASE
                       WHEN %s='open' THEN COALESCE(force_close_at, now() + make_interval(days := %s))
                       ELSE force_close_at
                   END,
                   closed_at=CASE WHEN %s='closed' THEN COALESCE(closed_at, now()) ELSE closed_at END
             WHERE id=%s;
            """,
            (status, reason, status, status, int(self.config.max_hold_days), status, int(signal_id)),
        )

    def _record_error(
        self,
        conn,
        *,
        signal_id: Optional[int],
        stage: str,
        error_type: str,
        message: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        context_param = _jsonb(context) if context is not None else None
        conn.execute(
            """
            INSERT INTO watchlist.live_trade_error(signal_id, stage, error_type, message, context)
            VALUES (%s,%s,%s,%s,%s);
            """,
            (int(signal_id) if signal_id else None, stage, error_type, message, context_param),
        )

    def _fetch_candidates(self, conn) -> List[Dict[str, Any]]:
        return conn.execute(
            """
            WITH cand AS (
              SELECT
                e.id,
                e.start_ts,
                e.exchange,
                e.symbol,
                e.signal_type,
                e.leg_a_exchange,
                e.leg_b_exchange,
                e.leg_a_price_last,
                e.leg_b_price_last,
                (e.features_agg #>> '{meta_last,pnl_regression,pred,240,pnl_hat}')::double precision AS pnl_hat_240,
                (e.features_agg #>> '{meta_last,pnl_regression,pred,240,win_prob}')::double precision AS win_prob_240,
                (e.features_agg #> '{meta_last,factors}') AS factors,
                (e.features_agg #> '{meta_last,trigger_details}') AS trigger_details
              FROM watchlist.watch_signal_event e
              LEFT JOIN watchlist.live_trade_signal s
                ON s.event_id = e.id
              WHERE s.event_id IS NULL
                AND e.signal_type = 'B'
                AND e.start_ts >= now() - make_interval(mins := %s)
                AND (e.features_agg #> '{meta_last,factors}') IS NOT NULL
              ORDER BY e.start_ts DESC
              LIMIT %s
            )
            SELECT * FROM cand;
            """,
            (
                int(self.config.event_lookback_minutes),
                int(self.config.candidate_limit),
            ),
        ).fetchall()

    def _supported_exchange(self, exchange: str) -> bool:
        allowed = {str(x).lower() for x in (self.config.allowed_exchanges or ()) if str(x).strip()}
        if not allowed:
            allowed = {"binance", "bybit", "okx", "bitget", "hyperliquid"}
        return (exchange or "").lower() in allowed

    def _pick_high_low(
        self,
        *,
        symbol: str,
        trigger_details: Optional[Dict[str, Any]],
        leg_a_exchange: Optional[str],
        leg_b_exchange: Optional[str],
        leg_a_price_last: Optional[float],
        leg_b_price_last: Optional[float],
    ) -> Optional[Tuple[str, str]]:
        if trigger_details and isinstance(trigger_details, dict):
            pair = trigger_details.get("pair") or []
            prices = trigger_details.get("prices") or {}
            if isinstance(pair, list) and len(pair) == 2 and isinstance(prices, dict):
                ex1, ex2 = str(pair[0]), str(pair[1])
                p1 = prices.get(ex1)
                p2 = prices.get(ex2)
                try:
                    p1f = float(p1)
                    p2f = float(p2)
                except Exception:
                    p1f = None
                    p2f = None
                if p1f and p2f:
                    high, low = (ex1, ex2) if p1f >= p2f else (ex2, ex1)
                    return high, low
        if leg_a_exchange and leg_b_exchange and leg_a_price_last and leg_b_price_last:
            try:
                a = float(leg_a_price_last)
                b = float(leg_b_price_last)
            except Exception:
                return None
            high, low = (leg_a_exchange, leg_b_exchange) if a >= b else (leg_b_exchange, leg_a_exchange)
            return str(high), str(low)
        return None

    def _revalidate_with_orderbook(
        self,
        *,
        symbol: str,
        high_exchange: str,
        low_exchange: str,
        base_factors: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        # 注意：watchlist 的触发信息里 “高/低交易所” 可能来自 last/mark 等价格口径；
        # 这里必须以可成交的 bid/ask（按同名义金额扫盘）来重新选择开仓方向，
        # 否则会出现“long 更贵、short 更便宜”的反向开仓，进而导致止盈判断与实际收益口径错位。
        ex_a = str(high_exchange)
        ex_b = str(low_exchange)

        ob_a = fetch_orderbook_prices(
            ex_a,
            symbol,
            self.config.orderbook_market_type,
            notional=float(self.config.per_leg_notional_usdt),
        )
        ob_b = fetch_orderbook_prices(
            ex_b,
            symbol,
            self.config.orderbook_market_type,
            notional=float(self.config.per_leg_notional_usdt),
        )
        if not ob_a or ob_a.get("error") or not ob_b or ob_b.get("error"):
            return {
                "ok": False,
                "reason": "orderbook_unavailable",
                "orderbook": {"a": ob_a, "b": ob_b},
            }

        candidates = [
            # short on A, long on B
            {"short_ex": ex_a, "long_ex": ex_b, "short_px": ob_a.get("sell"), "long_px": ob_b.get("buy")},
            # short on B, long on A (swap)
            {"short_ex": ex_b, "long_ex": ex_a, "short_px": ob_b.get("sell"), "long_px": ob_a.get("buy")},
        ]

        best: Optional[Dict[str, Any]] = None
        best_score: Tuple[float, float] = (-1e9, -1e9)

        for cand in candidates:
            short_px = cand.get("short_px")
            long_px = cand.get("long_px")
            if short_px is None or long_px is None:
                continue
            try:
                short_f = float(short_px)
                long_f = float(long_px)
            except Exception:
                continue
            if short_f <= 0 or long_f <= 0:
                continue
            tradable_spread = (short_f - long_f) / long_f
            # 必须可成交价差为正，否则这个方向“买贵卖便宜”，不符合本策略的开仓定义。
            if tradable_spread <= 0:
                continue

            factors = dict(base_factors or {})
            factors["spread_log_short_over_long"] = float(math.log(short_f / long_f))
            factors["raw_best_buy_high_sell_low"] = float(tradable_spread)

            pred = predict_bc(signal_type="B", factors=factors, horizons=(int(self.config.horizon_min),))
            pred_map = (pred or {}).get("pred") or {}
            hpred = pred_map.get(str(int(self.config.horizon_min))) or {}
            pnl_hat = hpred.get("pnl_hat")
            win_prob = hpred.get("win_prob")
            if pnl_hat is None or win_prob is None:
                continue
            score = (float(pnl_hat), float(win_prob))
            if score > best_score:
                best_score = score
                best = {
                    "short_exchange": str(cand["short_ex"]),
                    "long_exchange": str(cand["long_ex"]),
                    "pnl_hat": float(pnl_hat),
                    "win_prob": float(win_prob),
                    "tradable_spread": float(tradable_spread),
                    "orderbook": {"a": ob_a, "b": ob_b},
                    "factors": factors,
                }

        if not best:
            return {
                "ok": False,
                "reason": "no_tradable_direction",
                "orderbook": {"a": ob_a, "b": ob_b},
            }

        best["ok"] = bool(
            float(best["pnl_hat"]) > self.config.pnl_threshold and float(best["win_prob"]) > self.config.win_prob_threshold
        )
        return best

    def _confirm_open_signal_with_orderbook(
        self,
        *,
        symbol: str,
        ex_a: str,
        ex_b: str,
        base_factors: Dict[str, Any],
        initial_reval: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        samples = max(1, int(self.config.orderbook_confirm_samples or 1))
        sleep_s = float(self.config.orderbook_confirm_sleep_seconds or 0.0)

        series: List[Dict[str, Any]] = []
        chosen_long: Optional[str] = None
        chosen_short: Optional[str] = None
        ok_all = True
        fail_reason: Optional[str] = None

        for i in range(samples):
            if i == 0 and initial_reval is not None:
                reval = dict(initial_reval)
            else:
                reval = self._revalidate_with_orderbook(
                    symbol=symbol,
                    high_exchange=ex_a,
                    low_exchange=ex_b,
                    base_factors=base_factors,
                )
            if not reval:
                ok_all = False
                fail_reason = fail_reason or "revalidate_none"
                series.append({"i": i, "ts": _utcnow().isoformat(), "ok": False, "reason": "revalidate_none"})
            else:
                long_ex = str(reval.get("long_exchange") or "")
                short_ex = str(reval.get("short_exchange") or "")
                ok = bool(reval.get("ok"))
                reason = str(reval.get("reason") or "")
                pnl_hat = reval.get("pnl_hat")
                win_prob = reval.get("win_prob")
                tradable_spread = reval.get("tradable_spread")

                if not (long_ex and short_ex):
                    ok = False
                    reason = reason or "missing_direction"

                if chosen_long is None and chosen_short is None and long_ex and short_ex:
                    chosen_long, chosen_short = long_ex, short_ex
                elif chosen_long and chosen_short and (long_ex != chosen_long or short_ex != chosen_short):
                    ok = False
                    reason = reason or "direction_flap"

                if not ok:
                    ok_all = False
                    fail_reason = fail_reason or (reason or "not_ok")

                series.append(
                    {
                        "i": i,
                        "ts": _utcnow().isoformat(),
                        "ok": bool(ok),
                        "reason": reason or None,
                        "long_exchange": long_ex or None,
                        "short_exchange": short_ex or None,
                        "pnl_hat": float(pnl_hat) if pnl_hat is not None else None,
                        "win_prob": float(win_prob) if win_prob is not None else None,
                        "tradable_spread": float(tradable_spread) if tradable_spread is not None else None,
                    }
                )

            if i < samples - 1 and sleep_s > 0:
                time.sleep(sleep_s)

        return {
            "ok": bool(ok_all and chosen_long and chosen_short),
            "reason": None if ok_all else (fail_reason or "orderbook_unstable"),
            "long_exchange": chosen_long,
            "short_exchange": chosen_short,
            "series": series,
        }

    def process_once(self) -> None:
        if not self.config.enabled:
            return
        if psycopg is None:
            return

        with self._conn() as conn:
            active = self._active_trade_count(conn)
            if active >= int(self.config.max_concurrent_trades):
                return

            candidates = self._fetch_candidates(conn)
            if not candidates:
                return

            by_symbol: Dict[str, List[Dict[str, Any]]] = {}
            for row in candidates:
                sym = str(row.get("symbol") or "").upper()
                if not sym:
                    continue
                by_symbol.setdefault(sym, []).append(row)

            # For each symbol, only evaluate top-k (sorted by base pnl/win from SQL)
            for symbol, rows in by_symbol.items():
                if active >= int(self.config.max_concurrent_trades):
                    break
                rows = rows[: max(1, int(self.config.per_symbol_top_k))]
                lock = self._get_symbol_lock(symbol)
                if not lock.acquire(blocking=False):
                    continue
                try:
                    active = self._active_trade_count(conn)
                    if active >= int(self.config.max_concurrent_trades):
                        break
                    self._process_symbol_candidates(conn, symbol, rows)
                finally:
                    lock.release()

    def _process_symbol_candidates(self, conn, symbol: str, rows: List[Dict[str, Any]]) -> None:
        # Fast check: already active trade for symbol?
        exists = conn.execute(
            "SELECT 1 FROM watchlist.live_trade_signal WHERE symbol=%s AND status IN ('opening','open','closing') LIMIT 1;",
            (symbol,),
        ).fetchone()
        if exists:
            return

        best: Optional[Dict[str, Any]] = None
        best_score: Tuple[float, float] = (-1e9, -1e9)

        for event in rows:
            try:
                high_low = self._pick_high_low(
                    symbol=symbol,
                    trigger_details=event.get("trigger_details"),
                    leg_a_exchange=event.get("leg_a_exchange"),
                    leg_b_exchange=event.get("leg_b_exchange"),
                    leg_a_price_last=event.get("leg_a_price_last"),
                    leg_b_price_last=event.get("leg_b_price_last"),
                )
                if not high_low:
                    continue
                high_ex, low_ex = high_low
                if not (self._supported_exchange(high_ex) and self._supported_exchange(low_ex)):
                    continue

                base_factors = event.get("factors") or {}
                if not isinstance(base_factors, dict):
                    continue

                reval = self._revalidate_with_orderbook(
                    symbol=symbol,
                    high_exchange=high_ex,
                    low_exchange=low_ex,
                    base_factors=base_factors,
                )
                if not reval or not reval.get("ok"):
                    continue
                score = (float(reval.get("pnl_hat") or -1e9), float(reval.get("win_prob") or -1e9))
                if score > best_score:
                    best_score = score
                    best = {
                        "event": event,
                        "short_ex": str(reval.get("short_exchange") or high_ex),
                        "long_ex": str(reval.get("long_exchange") or low_ex),
                        "reval": reval,
                    }
            except Exception:
                continue

        if not best:
            return

        event = best["event"]
        high_ex = best["short_ex"]
        low_ex = best["long_ex"]
        reval = best["reval"]

        confirm = self._confirm_open_signal_with_orderbook(
            symbol=symbol,
            ex_a=str(high_ex),
            ex_b=str(low_ex),
            base_factors=event.get("factors") or {},
            initial_reval=reval if isinstance(reval, dict) else None,
        )
        if not confirm.get("ok"):
            client_base = f"wl{event['id']}-{symbol}-{int(time.time())}"
            payload = {
                "event": {
                    "id": int(event["id"]),
                    "start_ts": str(event.get("start_ts")),
                    "symbol": symbol,
                    "signal_type": event.get("signal_type"),
                    "trigger_details": event.get("trigger_details"),
                },
                "threshold": {
                    "horizon_min": int(self.config.horizon_min),
                    "pnl_threshold": float(self.config.pnl_threshold),
                    "win_prob_threshold": float(self.config.win_prob_threshold),
                    "per_leg_notional_usdt": float(self.config.per_leg_notional_usdt),
                },
                "orderbook_revalidation": {
                    "pnl_hat": reval.get("pnl_hat"),
                    "win_prob": reval.get("win_prob"),
                    "orderbook": reval.get("orderbook"),
                },
                "orderbook_confirm_open": confirm,
            }
            signal_id = self._insert_signal(
                conn,
                event=event,
                leg_long_exchange=str(confirm.get("long_exchange") or low_ex),
                leg_short_exchange=str(confirm.get("short_exchange") or high_ex),
                pnl_hat=float(event.get("pnl_hat_240") or 0.0),
                win_prob=float(event.get("win_prob_240") or 0.0),
                pnl_hat_ob=float(reval.get("pnl_hat") or 0.0),
                win_prob_ob=float(reval.get("win_prob") or 0.0),
                payload=payload,
                status="skipped",
                reason=str(confirm.get("reason") or "orderbook_unstable"),
                client_order_id_base=client_base,
            )
            if signal_id:
                self._record_error(
                    conn,
                    signal_id=signal_id,
                    stage="orderbook_confirm_open",
                    error_type="unstable",
                    message=str(confirm.get("reason") or "orderbook_unstable"),
                    context={"symbol": symbol, "event_id": int(event["id"]), "confirm": confirm},
                )
            return

        # Confirm passed; lock in direction to avoid "flapping".
        high_ex = str(confirm.get("short_exchange") or high_ex)
        low_ex = str(confirm.get("long_exchange") or low_ex)

        client_base = f"wl{event['id']}-{symbol}-{int(time.time())}"
        payload = {
            "event": {
                "id": int(event["id"]),
                "start_ts": str(event.get("start_ts")),
                "symbol": symbol,
                "signal_type": event.get("signal_type"),
                "trigger_details": event.get("trigger_details"),
            },
            "threshold": {
                "horizon_min": int(self.config.horizon_min),
                "pnl_threshold": float(self.config.pnl_threshold),
                "win_prob_threshold": float(self.config.win_prob_threshold),
                "per_leg_notional_usdt": float(self.config.per_leg_notional_usdt),
            },
            "orderbook_revalidation": {
                "pnl_hat": reval.get("pnl_hat"),
                "win_prob": reval.get("win_prob"),
                "orderbook": reval.get("orderbook"),
            },
            "orderbook_confirm_open": confirm,
        }

        signal_id = self._insert_signal(
            conn,
            event=event,
            leg_long_exchange=low_ex,
            leg_short_exchange=high_ex,
            pnl_hat=float(event.get("pnl_hat_240") or 0.0),
            win_prob=float(event.get("win_prob_240") or 0.0),
            pnl_hat_ob=float(reval.get("pnl_hat") or 0.0),
            win_prob_ob=float(reval.get("win_prob") or 0.0),
            payload=payload,
            status="opening",
            reason=None,
            client_order_id_base=client_base,
        )
        if not signal_id:
            return

        try:
            self._open_trade(
                conn,
                signal_id,
                symbol,
                low_ex,
                high_ex,
                client_base,
                pnl_hat_ob=float(reval.get("pnl_hat") or 0.0),
            )
            self._update_signal_status(conn, signal_id, "open")
        except TradeExecutionError as exc:
            self._record_error(
                conn,
                signal_id=signal_id,
                stage="open_trade",
                error_type="TradeExecutionError",
                message=str(exc),
                context={"event_id": int(event["id"]), "symbol": symbol, "long_ex": low_ex, "short_ex": high_ex},
            )
            self._update_signal_status(conn, signal_id, "failed", reason=str(exc))
        except Exception as exc:
            self._record_error(
                conn,
                signal_id=signal_id,
                stage="open_trade",
                error_type=type(exc).__name__,
                message=str(exc),
                context={"event_id": int(event["id"]), "symbol": symbol, "long_ex": low_ex, "short_ex": high_ex},
            )
            self._update_signal_status(conn, signal_id, "failed", reason=str(exc))

    def _maybe_set_leverage_1x(self, symbol: str, exchange: str) -> None:
        ex = (exchange or "").lower()
        if ex == "binance":
            set_binance_perp_leverage(symbol=f"{symbol}USDT", leverage=1)
        elif ex == "okx":
            set_okx_swap_leverage(symbol=f"{symbol}-USDT-SWAP", leverage=1, td_mode="cross")
        elif ex == "bybit":
            set_bybit_linear_leverage(symbol=f"{symbol}USDT", leverage=1, category="linear")
        elif ex == "bitget":
            set_bitget_usdt_perp_leverage(symbol=f"{symbol}USDT", leverage=1, margin_coin="USDT")

    def _open_trade(
        self,
        conn,
        signal_id: int,
        symbol: str,
        long_ex: str,
        short_ex: str,
        client_base: str,
        *,
        pnl_hat_ob: float,
    ) -> None:
        # Safety: avoid mixing with manual/external positions (especially Hyperliquid one-way positions).
        if str(long_ex).lower() == "hyperliquid":
            szi = float(self._get_hyperliquid_position_szi(symbol))
            if abs(szi) > 1e-9:
                raise TradeExecutionError(f"Hyperliquid {symbol} position not flat before open (szi={szi}); abort")
        if str(short_ex).lower() == "hyperliquid":
            szi = float(self._get_hyperliquid_position_szi(symbol))
            if abs(szi) > 1e-9:
                raise TradeExecutionError(f"Hyperliquid {symbol} position not flat before open (szi={szi}); abort")

        # Best-effort leverage=1
        for ex in {long_ex, short_ex}:
            try:
                self._maybe_set_leverage_1x(symbol, ex)
            except Exception as exc:
                self._record_error(
                    conn,
                    signal_id=signal_id,
                    stage="set_leverage",
                    error_type=type(exc).__name__,
                    message=str(exc),
                    context={"symbol": symbol, "exchange": ex, "leverage": 1},
                )

        # Derive quantities based on orderbook sweep prices for the same notional used in validation.
        ob_long = fetch_orderbook_prices(
            long_ex, symbol, self.config.orderbook_market_type, notional=float(self.config.per_leg_notional_usdt)
        ) or {}
        ob_short = fetch_orderbook_prices(
            short_ex, symbol, self.config.orderbook_market_type, notional=float(self.config.per_leg_notional_usdt)
        ) or {}
        if ob_long.get("error") or ob_short.get("error"):
            raise TradeExecutionError(f"Orderbook unavailable for execution: long={ob_long.get('error')} short={ob_short.get('error')}")
        long_px = ob_long.get("buy")
        short_px = ob_short.get("sell")
        if not long_px or not short_px:
            raise TradeExecutionError("Orderbook missing buy/sell prices for execution")
        if float(short_px) <= float(long_px):
            raise TradeExecutionError(
                f"Non-tradable entry spread: short_sell_px={short_px} <= long_buy_px={long_px} (symbol={symbol} "
                f"long_ex={long_ex} short_ex={short_ex})"
            )

        entry_spread_metric = float(math.log(float(short_px) / float(long_px)))
        long_qty = float(self.config.per_leg_notional_usdt) / float(long_px)
        short_qty = float(self.config.per_leg_notional_usdt) / float(short_px)

        take_profit_pnl = float(self.config.take_profit_ratio) * float(pnl_hat_ob)
        conn.execute(
            """
            UPDATE watchlist.live_trade_signal
               SET entry_spread_metric=%s,
                   take_profit_pnl=%s,
                   updated_at=now(),
                   payload = COALESCE(payload, '{}'::jsonb) || jsonb_build_object(
                       'orderbook_execution', jsonb_build_object(
                           'ts', now(),
                           'long_exchange', %s::text,
                           'short_exchange', %s::text,
                           'long_buy_px', %s::double precision,
                           'short_sell_px', %s::double precision,
                           'entry_spread_metric', %s::double precision,
                           'long_qty', %s::double precision,
                           'short_qty', %s::double precision,
                           'orderbook_long', %s::jsonb,
                           'orderbook_short', %s::jsonb
                       )
                   )
             WHERE id=%s::bigint;
            """,
            (
                float(entry_spread_metric),
                float(take_profit_pnl),
                str(long_ex),
                str(short_ex),
                float(long_px),
                float(short_px),
                float(entry_spread_metric),
                float(long_qty),
                float(short_qty),
                _jsonb(ob_long),
                _jsonb(ob_short),
                int(signal_id),
            ),
        )

        legs = [
            {
                "exchange": long_ex,
                "side": "long",
                "quantity": long_qty,
                "order_kwargs": {"client_order_id": f"{client_base}-L"},
            },
            {
                "exchange": short_ex,
                "side": "short",
                "quantity": short_qty,
                "order_kwargs": {"client_order_id": f"{client_base}-S"},
            },
        ]

        opened: List[Dict[str, Any]] = []
        try:
            for leg in legs:
                order = execute_perp_market_order(
                    str(leg["exchange"]),
                    symbol,
                    float(leg["quantity"]),
                    side=str(leg["side"]),
                    order_kwargs=dict(leg.get("order_kwargs") or {}),
                )
                result = {
                    "exchange": str(leg["exchange"]),
                    "side": str(leg["side"]),
                    "quantity": float(leg["quantity"]),
                    "order": order,
                }
                opened.append(result)

                order_resp_param = _jsonb(order) if order is not None else None
                fill = self._parse_fill_fields(str(leg["exchange"]), symbol, order)
                conn.execute(
                    """
                    INSERT INTO watchlist.live_trade_order(
                        signal_id, action, leg, exchange, side, market_type, notional_usdt, quantity,
                        filled_qty, avg_price, cum_quote, exchange_order_id,
                        client_order_id, submitted_at, order_resp, status
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                    """,
                    (
                        int(signal_id),
                        "open",
                        "long" if str(leg["side"]) == "long" else "short",
                        str(leg["exchange"]),
                        str(leg["side"]),
                        "perp",
                        float(self.config.per_leg_notional_usdt),
                        str(leg["quantity"]),
                        float(fill.get("filled_qty")) if fill.get("filled_qty") is not None else None,
                        float(fill.get("avg_price")) if fill.get("avg_price") is not None else None,
                        float(fill.get("cum_quote")) if fill.get("cum_quote") is not None else None,
                        str(fill.get("exchange_order_id")) if fill.get("exchange_order_id") is not None else None,
                        str((leg.get("order_kwargs") or {}).get("client_order_id") or ""),
                        _utcnow(),
                        order_resp_param,
                        str(fill.get("status") or "submitted"),
                    ),
                )
                if str(leg["exchange"]).lower() == "hyperliquid":
                    self._verify_hyperliquid_position_after_open(symbol, str(leg["side"]), float(leg["quantity"]))
        except Exception as exc:
            # Best-effort rollback: close any legs that were opened before a failure.
            rollback_base = f"wl{signal_id}R{int(time.time())}"
            for opened_leg in reversed(opened):
                try:
                    pos_leg = str(opened_leg.get("side") or "")
                    qty = float(opened_leg.get("quantity") or 0.0)
                    if qty <= 0:
                        continue
                    exchange = str(opened_leg.get("exchange") or "")
                    close_client_id = f"{rollback_base}-{'L' if pos_leg == 'long' else 'S'}"
                    close_order = self._place_close_order(
                        exchange=exchange,
                        symbol=symbol,
                        position_leg=pos_leg,
                        quantity=qty,
                        client_order_id=close_client_id,
                    )
                    close_order_param = _jsonb(close_order) if close_order is not None else None
                    fill = self._parse_fill_fields(exchange, symbol, close_order)
                    conn.execute(
                        """
                        INSERT INTO watchlist.live_trade_order(
                            signal_id, action, leg, exchange, side, market_type, notional_usdt, quantity,
                            filled_qty, avg_price, cum_quote, exchange_order_id,
                            client_order_id, submitted_at, order_resp, status
                        )
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                        """,
                        (
                            int(signal_id),
                            "close",
                            "long" if pos_leg == "long" else "short",
                            str(exchange),
                            "short" if pos_leg == "long" else "long",
                            "perp",
                            None,
                            str(qty),
                            float(fill.get("filled_qty")) if fill.get("filled_qty") is not None else None,
                            float(fill.get("avg_price")) if fill.get("avg_price") is not None else None,
                            float(fill.get("cum_quote")) if fill.get("cum_quote") is not None else None,
                            str(fill.get("exchange_order_id")) if fill.get("exchange_order_id") is not None else None,
                            close_client_id,
                            _utcnow(),
                            close_order_param,
                            str(fill.get("status") or "submitted"),
                        ),
                    )
                except Exception as rb_exc:
                    self._record_error(
                        conn,
                        signal_id=signal_id,
                        stage="rollback_close",
                        error_type=type(rb_exc).__name__,
                        message=str(rb_exc),
                        context={
                            "symbol": symbol,
                            "exchange": opened_leg.get("exchange"),
                            "quantity": opened_leg.get("quantity"),
                        },
                    )
            # Post-failure safety: verify and flatten any residual exposure on both exchanges.
            for ex in {str(long_ex), str(short_ex)}:
                if ex:
                    self._post_fail_safety_flatten(conn, int(signal_id), symbol, ex)
            raise TradeExecutionError(str(exc)) from exc

    def monitor_open_trades_once(self) -> None:
        """
        Phase 2:
        - Poll both exchanges' orderbooks (REST) every ~1min for each open trade.
        - Compute pnl_spread using the canonical metric:
            pnl_spread_now = log(short/long)_entry - log(short_buy/long_sell)_now
        - Take profit: pnl_spread_now >= take_profit_ratio * pnl_hat_ob (or take_profit_pnl).
        - Force close: holding time > max_hold_days.
        - Record samples and close results to PG.
        """
        if not self.config.enabled:
            return
        if psycopg is None:
            return

        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT *
                  FROM watchlist.live_trade_signal
                 WHERE status IN ('open','closing')
                 ORDER BY opened_at ASC NULLS LAST, created_at ASC
                 LIMIT %s;
                """,
                (int(self.config.max_concurrent_trades) * 2,),
            ).fetchall()

            for row in rows or []:
                if not isinstance(row, dict):
                    continue
                symbol = str(row.get("symbol") or "").upper()
                if not symbol:
                    continue
                lock = self._get_symbol_lock(symbol)
                if not lock.acquire(blocking=False):
                    continue
                try:
                    self._monitor_one(conn, row)
                finally:
                    lock.release()

    def _monitor_one(self, conn, signal_row: Dict[str, Any]) -> None:
        signal_id = int(signal_row.get("id") or 0)
        if not signal_id:
            return
        symbol = str(signal_row.get("symbol") or "").upper()
        long_ex = str(signal_row.get("leg_long_exchange") or "")
        short_ex = str(signal_row.get("leg_short_exchange") or "")
        if not (symbol and long_ex and short_ex):
            return

        status = str(signal_row.get("status") or "")
        if status not in {"open", "closing"}:
            return

        now = _utcnow()
        opened_at = signal_row.get("opened_at") or signal_row.get("created_at")
        if isinstance(opened_at, datetime):
            opened_at = opened_at.astimezone(timezone.utc)
        else:
            opened_at = now

        # Force close after max_hold_days.
        force_close_at = signal_row.get("force_close_at")
        if isinstance(force_close_at, datetime):
            force_close_at = force_close_at.astimezone(timezone.utc)
        else:
            force_close_at = opened_at + timedelta(days=int(self.config.max_hold_days))

        # Canonical entry spread metric for Type B.
        entry_spread_metric = signal_row.get("entry_spread_metric")
        if entry_spread_metric is None:
            payload = signal_row.get("payload") or {}
            if isinstance(payload, dict):
                entry_spread_metric = ((payload.get("orderbook_execution") or {}) or {}).get("entry_spread_metric")
        try:
            entry_spread_metric_f = float(entry_spread_metric)
        except Exception:
            entry_spread_metric_f = None  # type: ignore[assignment]

        if entry_spread_metric_f is None:
            self._record_error(
                conn,
                signal_id=signal_id,
                stage="monitor",
                error_type="missing_entry_spread",
                message="entry_spread_metric missing; cannot compute pnl_spread",
                context={"symbol": symbol},
            )
            return

        qty_long, qty_short = self._load_open_quantities(conn, signal_id)
        if qty_long is None or qty_short is None:
            self._record_error(
                conn,
                signal_id=signal_id,
                stage="monitor",
                error_type="missing_quantity",
                message="open quantities missing; cannot compute pnl_spread",
                context={"symbol": symbol},
            )
            return

        decision = "hold"
        close_reason = None
        if now >= force_close_at:
            decision = "force_close"
            close_reason = "max_hold_days"

        ob_long, ob_short = self._fetch_pair_orderbooks_for_close(symbol, long_ex, short_ex, qty_long, qty_short)
        if ob_long.get("error") or ob_short.get("error"):
            self._record_error(
                conn,
                signal_id=signal_id,
                stage="monitor_orderbook",
                error_type="orderbook_error",
                message=f"orderbook error long={ob_long.get('error')} short={ob_short.get('error')}",
                context={"symbol": symbol, "long_ex": long_ex, "short_ex": short_ex},
            )
            return

        long_sell = ob_long.get("sell")
        short_buy = ob_short.get("buy")
        if not long_sell or not short_buy:
            self._record_error(
                conn,
                signal_id=signal_id,
                stage="monitor_orderbook",
                error_type="missing_prices",
                message="missing long sell / short buy for close pricing",
                context={"symbol": symbol, "long_ex": long_ex, "short_ex": short_ex, "ob_long": ob_long, "ob_short": ob_short},
            )
            return

        spread_now = float(math.log(float(short_buy) / float(long_sell)))
        pnl_spread_now = float(entry_spread_metric_f) - float(spread_now)

        pnl_hat_ob = signal_row.get("pnl_hat_ob")
        try:
            pnl_hat_ob_f = float(pnl_hat_ob) if pnl_hat_ob is not None else None
        except Exception:
            pnl_hat_ob_f = None

        take_profit_pnl = signal_row.get("take_profit_pnl")
        try:
            take_profit_pnl_f = float(take_profit_pnl) if take_profit_pnl is not None else None
        except Exception:
            take_profit_pnl_f = None

        # Negative/zero tp is treated as invalid (historically used in tests to force close);
        # fall back to computed tp from pnl_hat_ob.
        if take_profit_pnl_f is not None and float(take_profit_pnl_f) <= 0:
            take_profit_pnl_f = None

        if take_profit_pnl_f is None and pnl_hat_ob_f is not None:
            take_profit_pnl_f = float(self.config.take_profit_ratio) * float(pnl_hat_ob_f)

        confirm_take_profit: Optional[Dict[str, Any]] = None
        if (
            close_reason is None
            and take_profit_pnl_f is not None
            and float(take_profit_pnl_f) > 0
            and pnl_spread_now >= float(take_profit_pnl_f)
        ):
            confirm_take_profit = self._confirm_take_profit_with_orderbook(
                symbol=symbol,
                long_ex=long_ex,
                short_ex=short_ex,
                qty_long=float(qty_long),
                qty_short=float(qty_short),
                entry_spread_metric=float(entry_spread_metric_f),
                take_profit_pnl=float(take_profit_pnl_f),
                initial_ob_long=ob_long if isinstance(ob_long, dict) else None,
                initial_ob_short=ob_short if isinstance(ob_short, dict) else None,
            )
            if confirm_take_profit.get("ok"):
                decision = "take_profit"
                close_reason = "take_profit"
            else:
                decision = "take_profit_rejected"
                try:
                    self._record_error(
                        conn,
                        signal_id=signal_id,
                        stage="orderbook_confirm_take_profit",
                        error_type="unstable",
                        message="take_profit_rejected_by_confirm",
                        context={
                            "symbol": symbol,
                            "long_ex": long_ex,
                            "short_ex": short_ex,
                            "take_profit_pnl": float(take_profit_pnl_f),
                            "pnl_spread_now": float(pnl_spread_now),
                            "confirm": confirm_take_profit,
                        },
                    )
                except Exception:
                    pass

        conn.execute(
            """
            INSERT INTO watchlist.live_trade_spread_sample(
                signal_id, symbol, long_exchange, short_exchange,
                long_sell_px, short_buy_px,
                spread_metric, pnl_spread, pnl_hat_ob, take_profit_pnl,
                decision, context
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """,
            (
                signal_id,
                symbol,
                long_ex,
                short_ex,
                float(long_sell),
                float(short_buy),
                float(spread_now),
                float(pnl_spread_now),
                float(pnl_hat_ob_f) if pnl_hat_ob_f is not None else None,
                float(take_profit_pnl_f) if take_profit_pnl_f is not None else None,
                decision,
                _jsonb(
                    {
                        "ob_long": ob_long,
                        "ob_short": ob_short,
                        "opened_at": opened_at.isoformat(),
                        "force_close_at": force_close_at.isoformat(),
                        "confirm_take_profit": confirm_take_profit,
                    }
                ),
            ),
        )

        conn.execute(
            """
            UPDATE watchlist.live_trade_signal
               SET last_check_at=now(),
                   last_spread_metric=%s,
                   last_pnl_spread=%s,
                   take_profit_pnl=CASE
                       WHEN take_profit_pnl IS NULL OR take_profit_pnl <= 0 THEN %s
                       ELSE take_profit_pnl
                   END,
                   force_close_at=COALESCE(force_close_at, %s),
                   updated_at=now()
             WHERE id=%s;
            """,
            (
                float(spread_now),
                float(pnl_spread_now),
                float(take_profit_pnl_f) if take_profit_pnl_f is not None else None,
                force_close_at,
                signal_id,
            ),
        )

        if close_reason:
            self._attempt_close(
                conn,
                signal_row=signal_row,
                close_reason=close_reason,
                close_pnl_spread=pnl_spread_now,
                qty_long=qty_long,
                qty_short=qty_short,
            )

    def _confirm_take_profit_with_orderbook(
        self,
        *,
        symbol: str,
        long_ex: str,
        short_ex: str,
        qty_long: float,
        qty_short: float,
        entry_spread_metric: float,
        take_profit_pnl: float,
        initial_ob_long: Optional[Dict[str, Any]] = None,
        initial_ob_short: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        samples = max(1, int(self.config.orderbook_confirm_samples or 1))
        sleep_s = float(self.config.orderbook_confirm_sleep_seconds or 0.0)

        series: List[Dict[str, Any]] = []
        ok_all = True

        for i in range(samples):
            if i == 0 and initial_ob_long is not None and initial_ob_short is not None:
                ob_l, ob_s = dict(initial_ob_long), dict(initial_ob_short)
            else:
                ob_l, ob_s = self._fetch_pair_orderbooks_for_close(symbol, long_ex, short_ex, qty_long, qty_short)
            long_sell = ob_l.get("sell")
            short_buy = ob_s.get("buy")

            ok = True
            reason = None
            pnl_spread = None
            spread_metric = None

            if ob_l.get("error") or ob_s.get("error") or not long_sell or not short_buy:
                ok = False
                reason = f"orderbook_error long={ob_l.get('error')} short={ob_s.get('error')}"
            else:
                try:
                    spread_metric = float(math.log(float(short_buy) / float(long_sell)))
                    pnl_spread = float(entry_spread_metric) - float(spread_metric)
                    if pnl_spread < float(take_profit_pnl):
                        ok = False
                        reason = "below_threshold"
                except Exception:
                    ok = False
                    reason = "calc_error"

            if not ok:
                ok_all = False

            series.append(
                {
                    "i": i,
                    "ts": _utcnow().isoformat(),
                    "ok": bool(ok),
                    "reason": reason,
                    "long_sell_px": float(long_sell) if long_sell is not None else None,
                    "short_buy_px": float(short_buy) if short_buy is not None else None,
                    "spread_metric": float(spread_metric) if spread_metric is not None else None,
                    "pnl_spread": float(pnl_spread) if pnl_spread is not None else None,
                    "take_profit_pnl": float(take_profit_pnl),
                    "ob_long": ob_l,
                    "ob_short": ob_s,
                }
            )

            if i < samples - 1 and sleep_s > 0:
                time.sleep(sleep_s)

        return {"ok": bool(ok_all), "series": series}

    def _fetch_pair_orderbooks_for_close(
        self, symbol: str, long_ex: str, short_ex: str, qty_long: float, qty_short: float
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_long = pool.submit(
                fetch_orderbook_prices_for_quantity,
                long_ex,
                symbol,
                self.config.orderbook_market_type,
                quantity=float(qty_long),
            )
            fut_short = pool.submit(
                fetch_orderbook_prices_for_quantity,
                short_ex,
                symbol,
                self.config.orderbook_market_type,
                quantity=float(qty_short),
            )
            ob_long = fut_long.result() or {"error": "no_data"}
            ob_short = fut_short.result() or {"error": "no_data"}
            return ob_long, ob_short

    def _load_open_quantities(self, conn, signal_id: int) -> Tuple[Optional[float], Optional[float]]:
        rows = conn.execute(
            """
            SELECT leg, quantity
              FROM watchlist.live_trade_order
             WHERE signal_id=%s
               AND action='open'
               AND leg IN ('long','short')
             ORDER BY created_at ASC;
            """,
            (int(signal_id),),
        ).fetchall()
        qty_long = qty_short = None
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            leg = str(r.get("leg") or "")
            qty_raw = r.get("quantity")
            try:
                qty_val = float(qty_raw)
            except Exception:
                continue
            if leg == "long" and qty_long is None:
                qty_long = qty_val
            elif leg == "short" and qty_short is None:
                qty_short = qty_val
        return qty_long, qty_short

    def _attempt_close(
        self,
        conn,
        *,
        signal_row: Dict[str, Any],
        close_reason: str,
        close_pnl_spread: float,
        qty_long: float,
        qty_short: float,
    ) -> None:
        signal_id = int(signal_row.get("id") or 0)
        if not signal_id:
            return

        status = str(signal_row.get("status") or "")
        if status not in {"open", "closing"}:
            return

        close_requested_at = signal_row.get("close_requested_at")
        if isinstance(close_requested_at, datetime):
            close_requested_at = close_requested_at.astimezone(timezone.utc)
            if (_utcnow() - close_requested_at).total_seconds() < float(self.config.close_retry_cooldown_seconds):
                return

        conn.execute(
            """
            UPDATE watchlist.live_trade_signal
               SET status='closing',
                   close_reason=%s,
                   close_pnl_spread=%s,
                   close_requested_at=now(),
                   updated_at=now()
             WHERE id=%s
               AND status IN ('open','closing');
            """,
            (str(close_reason), float(close_pnl_spread), int(signal_id)),
        )

        symbol = str(signal_row.get("symbol") or "").upper()
        long_ex = str(signal_row.get("leg_long_exchange") or "")
        short_ex = str(signal_row.get("leg_short_exchange") or "")
        # Keep client order IDs short enough for venues like Binance (<=36 chars).
        # Use signal_id instead of the (potentially longer) event/symbol base.
        close_base = f"wl{signal_id}C{int(time.time())}"

        try:
            long_order = self._place_close_order(
                exchange=long_ex,
                symbol=symbol,
                position_leg="long",
                quantity=float(qty_long),
                client_order_id=f"{close_base}-L",
            )
            long_order_param = _jsonb(long_order) if long_order is not None else None
            long_fill = self._parse_fill_fields(long_ex, symbol, long_order)
            conn.execute(
                """
                INSERT INTO watchlist.live_trade_order(
                    signal_id, action, leg, exchange, side, market_type, notional_usdt, quantity,
                    filled_qty, avg_price, cum_quote, exchange_order_id,
                    client_order_id, submitted_at, order_resp, status
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """,
                (
                    int(signal_id),
                    "close",
                    "long",
                    str(long_ex),
                    "short",
                    "perp",
                    None,
                    str(qty_long),
                    float(long_fill.get("filled_qty")) if long_fill.get("filled_qty") is not None else None,
                    float(long_fill.get("avg_price")) if long_fill.get("avg_price") is not None else None,
                    float(long_fill.get("cum_quote")) if long_fill.get("cum_quote") is not None else None,
                    str(long_fill.get("exchange_order_id")) if long_fill.get("exchange_order_id") is not None else None,
                    f"{close_base}-L",
                    _utcnow(),
                    long_order_param,
                    str(long_fill.get("status") or "submitted"),
                ),
            )
        except Exception as exc:
            self._record_error(
                conn,
                signal_id=signal_id,
                stage="close_long",
                error_type=type(exc).__name__,
                message=str(exc),
                context={"symbol": symbol, "exchange": long_ex, "quantity": qty_long},
            )
            return

        try:
            short_order = self._place_close_order(
                exchange=short_ex,
                symbol=symbol,
                position_leg="short",
                quantity=float(qty_short),
                client_order_id=f"{close_base}-S",
            )
            short_order_param = _jsonb(short_order) if short_order is not None else None
            short_fill = self._parse_fill_fields(short_ex, symbol, short_order)
            conn.execute(
                """
                INSERT INTO watchlist.live_trade_order(
                    signal_id, action, leg, exchange, side, market_type, notional_usdt, quantity,
                    filled_qty, avg_price, cum_quote, exchange_order_id,
                    client_order_id, submitted_at, order_resp, status
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """,
                (
                    int(signal_id),
                    "close",
                    "short",
                    str(short_ex),
                    "long",
                    "perp",
                    None,
                    str(qty_short),
                    float(short_fill.get("filled_qty")) if short_fill.get("filled_qty") is not None else None,
                    float(short_fill.get("avg_price")) if short_fill.get("avg_price") is not None else None,
                    float(short_fill.get("cum_quote")) if short_fill.get("cum_quote") is not None else None,
                    str(short_fill.get("exchange_order_id")) if short_fill.get("exchange_order_id") is not None else None,
                    f"{close_base}-S",
                    _utcnow(),
                    short_order_param,
                    str(short_fill.get("status") or "submitted"),
                ),
            )
        except Exception as exc:
            self._record_error(
                conn,
                signal_id=signal_id,
                stage="close_short",
                error_type=type(exc).__name__,
                message=str(exc),
                context={"symbol": symbol, "exchange": short_ex, "quantity": qty_short},
            )
            return

        self._update_signal_status(conn, signal_id, "closed")

    def _place_close_order(
        self,
        *,
        exchange: str,
        symbol: str,
        position_leg: str,
        quantity: float,
        client_order_id: str,
    ) -> Dict[str, Any]:
        ex = (exchange or "").lower()
        pos = (position_leg or "").lower()
        if ex == "binance":
            side = "SELL" if pos == "long" else "BUY"
            position_side = "LONG" if pos == "long" else "SHORT"
            try:
                # In hedge-mode, positionSide is sufficient to indicate a closing action.
                return place_binance_perp_market_order(
                    symbol,
                    side,
                    quantity,
                    reduce_only=None,
                    client_order_id=client_order_id,
                    position_side=position_side,
                )
            except TradeExecutionError as exc:
                # One-way mode: retry without positionSide, but keep reduceOnly.
                msg = str(exc).lower()
                if "positionside" in msg or "dual-side" in msg or "position side does not match" in msg:
                    return place_binance_perp_market_order(
                        symbol,
                        side,
                        quantity,
                        reduce_only=True,
                        client_order_id=client_order_id,
                        position_side=None,
                    )
                raise
        if ex == "okx":
            if pos == "long":
                return place_okx_swap_market_order(
                    symbol,
                    "sell",
                    quantity,
                    pos_side="long",
                    reduce_only=True,
                    client_order_id=client_order_id,
                )
            return place_okx_swap_market_order(
                symbol,
                "buy",
                quantity,
                pos_side="short",
                reduce_only=True,
                client_order_id=client_order_id,
            )
        if ex == "bybit":
            if pos == "long":
                return place_bybit_linear_market_order(
                    symbol,
                    "sell",
                    quantity,
                    reduce_only=True,
                    client_order_id=client_order_id,
                    position_idx=1,
                    category="linear",
                )
            return place_bybit_linear_market_order(
                symbol,
                "buy",
                quantity,
                reduce_only=True,
                client_order_id=client_order_id,
                position_idx=2,
                category="linear",
            )
        if ex == "bitget":
            # Bitget mix V2 close semantics are account-mode dependent; `tradeSide=close` + side is widely accepted.
            close_qty = float(quantity)
            try:
                positions = get_bitget_usdt_perp_positions(symbol=symbol)
                target_hold = "long" if pos == "long" else "short"
                for p in positions or []:
                    if not isinstance(p, dict):
                        continue
                    if str(p.get("holdSide") or "").lower() != target_hold:
                        continue
                    raw_qty = p.get("available") or p.get("total") or p.get("openQty") or p.get("pos") or 0
                    try:
                        discovered = float(raw_qty or 0)
                    except Exception:
                        discovered = 0.0
                    if discovered > 0:
                        close_qty = discovered
                        break
            except Exception:
                close_qty = float(quantity)
            if close_qty <= 0:
                raise TradeExecutionError("Bitget close quantity invalid/zero")
            primary_side = "sell" if pos == "long" else "buy"
            try:
                return place_bitget_usdt_perp_market_order(
                    symbol,
                    primary_side,
                    close_qty,
                    trade_side="close",
                    client_order_id=client_order_id,
                    margin_coin="USDT",
                )
            except TradeExecutionError as exc:
                # Some Bitget accounts return "No position to close" / "side mismatch" depending on mode;
                # retry once with the opposite side to avoid leaving residual positions.
                msg = str(exc).lower()
                if "22002" in msg or "side mismatch" in msg or "400172" in msg:
                    opposite_side = "buy" if primary_side == "sell" else "sell"
                    return place_bitget_usdt_perp_market_order(
                        symbol,
                        opposite_side,
                        close_qty,
                        trade_side="close",
                        client_order_id=client_order_id,
                        margin_coin="USDT",
                    )
                raise
        if ex == "hyperliquid":
            # Hyperliquid close should be reduce-only IOC with explicit side to avoid SDK market_close() returning None.
            close_side = "sell" if pos == "long" else "buy"
            return place_hyperliquid_perp_market_order(
                symbol,
                close_side,
                quantity,
                reduce_only=True,
                client_order_id=client_order_id,
            )
        raise TradeExecutionError(f"Unsupported exchange for close: {exchange}")
