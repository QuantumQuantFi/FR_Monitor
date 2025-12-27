from __future__ import annotations

import logging
import re
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

# psycopg is not in requirements yet; keep import lazy and fail soft.
try:
    import psycopg
except Exception:  # pragma: no cover - optional dependency
    psycopg = None  # type: ignore

import requests

import math

from orderbook_utils import fetch_orderbook_prices
from watchlist_pnl_regression_model import predict_bc
from watchlist_series_factors import SeriesCache, compute_event_series_factors
from watchlist_v2_infer import infer_v2


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class PgWriterConfig:
    dsn: str = "postgresql://wl_writer:wl_writer_A3f9xB2@127.0.0.1:5432/watchlist"
    enabled: bool = False
    batch_size: int = 500
    flush_seconds: float = 5.0
    consecutive_required: int = 2  # N 连续分钟归并事件
    cooldown_minutes: int = 3  # M 分钟冷静期
    enable_event_merge: bool = False
    orderbook_validation_on_write: bool = True


class PgWriter:
    """
    Minimal async-ish buffered writer to Postgres for watchlist tables.
    - enqueue_raw: push raw signal rows (dict) -> flush in batch.
    - optional event merge (consecutive N, cooldown M) to reduce重复事件。
    """

    def __init__(self, config: PgWriterConfig):
        self.config = config
        self.logger = logging.getLogger("pg_writer")
        self._queue: Deque[Dict[str, Any]] = deque()
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._last_priority_flush_ts: float = 0.0
        # Best-effort: notify external 8010 orderbook service to subscribe symbols on raw triggers.
        self._monitor_8010_queue: Deque[Dict[str, Any]] = deque()
        self._monitor_8010_lock = threading.Lock()
        self._monitor_8010_wakeup = threading.Event()
        self._monitor_8010_thread: Optional[threading.Thread] = None
        self._monitor_8010_last_sent: Dict[Tuple[str, Tuple[str, ...]], float] = {}
        # state for event merge: (exchange, symbol, signal_type) -> state dict
        self._event_state: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        # 缓存最近一次双腿信息，便于事件聚合时保留首/末腿快照
        self._last_legs: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        # funding history cache: key=(exchange,symbol,day_bucket)
        self._funding_cache: Dict[Tuple[str, str, int], List[Tuple[datetime, float]]] = {}
        self._funding_calls = 0
        self._series_cache: Optional[SeriesCache] = None
        # Optional hook: called after watch_signal_event rows are inserted.
        # Signature: callback(event_ids: List[int]) -> None
        self.on_event_written: Optional[Callable[[List[int]], None]] = None

    def start(self) -> None:
        if not self.config.enabled:
            self.logger.info("PG writer disabled; skip start")
            return
        if psycopg is None:
            self.logger.error("psycopg not installed; cannot start PG writer")
            return
        try:
            self._ensure_event_schedule_columns()
        except Exception as exc:  # pragma: no cover - best-effort DDL
            self.logger.warning("ensure PG schema failed: %s", exc)
        if self._thread:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_loop, name="pg-writer", daemon=True)
        self._thread.start()
        self._start_monitor_8010_thread()
        self.logger.info("PG writer started (batch=%s, flush=%ss)", self.config.batch_size, self.config.flush_seconds)

    def _start_monitor_8010_thread(self) -> None:
        if self._monitor_8010_thread:
            return
        try:
            import config as _cfg  # local import: avoid import side-effects at module load

            enabled = bool((_cfg.MONITOR_8010_CONFIG or {}).get("enabled", True))
            base_url = str((_cfg.MONITOR_8010_CONFIG or {}).get("base_url") or "").strip()
            if not enabled or not base_url:
                return
        except Exception:
            return
        self._monitor_8010_thread = threading.Thread(
            target=self._monitor_8010_loop, name="pg-writer-monitor8010", daemon=True
        )
        self._monitor_8010_thread.start()

    def _ensure_event_schedule_columns(self) -> None:
        """
        Ensure watch_signal_event has funding schedule columns.
        This prevents runtime failures after code adds new INSERT columns.
        """
        if psycopg is None:
            return
        ddl = """
        ALTER TABLE watchlist.watch_signal_event
          ADD COLUMN IF NOT EXISTS funding_interval_hours double precision,
          ADD COLUMN IF NOT EXISTS next_funding_time timestamptz,
          ADD COLUMN IF NOT EXISTS leg_a_funding_interval_hours double precision,
          ADD COLUMN IF NOT EXISTS leg_a_next_funding_time timestamptz,
          ADD COLUMN IF NOT EXISTS leg_b_funding_interval_hours double precision,
          ADD COLUMN IF NOT EXISTS leg_b_next_funding_time timestamptz;
        """
        with psycopg.connect(self.config.dsn, autocommit=True) as conn:
            conn.execute(ddl)

    def stop(self) -> None:
        if not self._thread:
            return
        self._stop.set()
        self._monitor_8010_wakeup.set()
        self._thread.join(timeout=5)
        self._thread = None
        # final flush
        try:
            self.flush()
        except Exception as exc:  # pragma: no cover - shutdown best-effort
            self.logger.warning("final flush failed: %s", exc)

    def enqueue_raw(self, row: Dict[str, Any]) -> None:
        if not self.config.enabled:
            return
        with self._lock:
            self._queue.append(row)
            try:
                self._maybe_notify_monitor_8010(row)
            except Exception:
                pass
            # Best-effort: for "high-value" triggered rows, flush immediately to reduce event latency.
            # This matters when consecutive_required=1 and live trading is kick-driven.
            try:
                is_triggered = bool(row.get("triggered"))
                sig_type = str(row.get("signal_type") or "").strip().upper()
                if is_triggered and sig_type in ("B", "C"):
                    meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
                    pnl_reg = meta.get("pnl_regression") if isinstance(meta, dict) else None
                    pred = (pnl_reg or {}).get("pred") if isinstance(pnl_reg, dict) else None
                    pred_240 = pred.get("240") if isinstance(pred, dict) else None
                    pnl_hat = (
                        float(pred_240.get("pnl_hat"))
                        if isinstance(pred_240, dict) and pred_240.get("pnl_hat") is not None
                        else None
                    )
                    win_prob = (
                        float(pred_240.get("win_prob"))
                        if isinstance(pred_240, dict) and pred_240.get("win_prob") is not None
                        else None
                    )
                    spread_est = row.get("spread_rel")
                    spread_est_f = float(spread_est) if spread_est is not None else None
                    # Mirror the cheap prefilter in _maybe_add_orderbook_validation (avoid flushing for weak signals).
                    try:
                        import config as _cfg  # local import: avoid import side-effects at module load

                        pnl_thr = float((_cfg.LIVE_TRADING_CONFIG or {}).get("pnl_threshold", 0.0085))
                        prob_thr = float((_cfg.LIVE_TRADING_CONFIG or {}).get("win_prob_threshold", 0.85))
                        if sig_type == "C":
                            pnl_thr = float((_cfg.LIVE_TRADING_CONFIG or {}).get("type_c_pnl_threshold", pnl_thr))
                            prob_thr = float((_cfg.LIVE_TRADING_CONFIG or {}).get("type_c_win_prob_threshold", prob_thr))
                    except Exception:
                        pnl_thr = 0.0085
                        prob_thr = 0.85
                    if (
                        pnl_hat is not None
                        and win_prob is not None
                        and spread_est_f is not None
                        and math.isfinite(pnl_hat)
                        and math.isfinite(win_prob)
                        and math.isfinite(spread_est_f)
                        and pnl_hat >= pnl_thr
                        and win_prob >= prob_thr
                        and spread_est_f >= 0.003
                    ):
                        now_ts = time.time()
                        if now_ts - float(self._last_priority_flush_ts or 0.0) >= 1.0:
                            self._last_priority_flush_ts = now_ts
                            self._flush_locked()
                            return
            except Exception:
                pass
            if len(self._queue) >= self.config.batch_size:
                self._flush_locked()

    @staticmethod
    def _to_monitor_8010_symbol(symbol: str, *, default_quote: str = "USDC") -> str:
        s = str(symbol or "").strip().upper()
        if not s:
            return ""
        # If already "BTC-USDC-PERP"/"BTC-USDT-PERP", pass through.
        if "-" in s:
            return s
        base = re.sub(r"[^A-Z0-9]", "", s)
        if not base:
            return ""
        return f"{base}-{default_quote.strip().upper()}-PERP"

    def _monitor_8010_enqueue_add(
        self,
        *,
        monitor_symbol: str,
        exchanges: List[str],
        ttl_seconds: int,
        source: str,
        reason: str,
        all_exchanges: bool = False,
    ) -> None:
        if not monitor_symbol:
            return
        if (not all_exchanges) and (not exchanges):
            return
        with self._monitor_8010_lock:
            self._monitor_8010_queue.append(
                {
                    "monitor_symbol": monitor_symbol,
                    "exchanges": exchanges,
                    "ttl_seconds": ttl_seconds,
                    "source": source,
                    "reason": reason,
                    "all_exchanges": bool(all_exchanges),
                }
            )
        self._monitor_8010_wakeup.set()

    def _maybe_notify_monitor_8010(self, row: Dict[str, Any]) -> None:
        # Only act on triggered raw rows: this is the "signal happens" moment.
        if not bool(row.get("triggered")):
            return
        sig_type = str(row.get("signal_type") or "").strip().upper()
        if sig_type not in ("A", "B", "C"):
            return
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            return

        try:
            import config as _cfg  # local import: avoid import side-effects at module load

            mcfg = _cfg.MONITOR_8010_CONFIG or {}
            if not bool(mcfg.get("enabled", True)):
                return
            default_quote = str(mcfg.get("default_quote") or "USDC").strip().upper()
            ttl_seconds = int(float(mcfg.get("watchlist_ttl_seconds") or 86400))
            source = str(mcfg.get("watchlist_source") or "FR_Monitor").strip() or "FR_Monitor"
            max_ex = int(float(mcfg.get("watchlist_max_exchanges_per_symbol") or 8))
            ex_override = str(mcfg.get("watchlist_exchanges") or "").strip()
        except Exception:
            default_quote = "USDC"
            ttl_seconds = 86400
            source = "FR_Monitor"
            max_ex = 8
            ex_override = ""

        # Exchange selection:
        # - If MONITOR_8010_WL_EXCHANGES is set to a CSV, use it.
        # - If empty / "all" / "*" → subscribe ALL exchanges configured on 8010 (pass exchanges=null).
        use_all = False
        exchanges: List[str] = []
        if ex_override:
            ex_override_l = ex_override.strip().lower()
            if ex_override_l in ("*", "all", "all_exchanges"):
                use_all = True
            else:
                for part in ex_override.split(","):
                    ex = part.strip().lower()
                    if ex and ex not in ("unsupported", "none") and ex not in exchanges:
                        exchanges.append(ex)
        else:
            use_all = True

        if (not use_all) and max_ex > 0 and len(exchanges) > max_ex:
            exchanges = exchanges[:max_ex]

        monitor_symbol = self._to_monitor_8010_symbol(symbol, default_quote=default_quote)
        if not monitor_symbol:
            return
        self._monitor_8010_enqueue_add(
            monitor_symbol=monitor_symbol,
            exchanges=exchanges,
            ttl_seconds=ttl_seconds,
            source=source,
            reason=f"raw_trigger:{sig_type}",
            all_exchanges=use_all,
        )

    def _monitor_8010_loop(self) -> None:
        while not self._stop.is_set():
            self._monitor_8010_wakeup.wait(timeout=1.0)
            self._monitor_8010_wakeup.clear()
            if self._stop.is_set():
                return
            batch: List[Dict[str, Any]] = []
            with self._monitor_8010_lock:
                while self._monitor_8010_queue and len(batch) < 200:
                    batch.append(self._monitor_8010_queue.popleft())
            if not batch:
                continue
            try:
                import config as _cfg  # local import: avoid import side-effects at module load

                mcfg = _cfg.MONITOR_8010_CONFIG or {}
                if not bool(mcfg.get("enabled", True)):
                    continue
                base_url = str(mcfg.get("base_url") or "").strip()
                if not base_url:
                    continue
                timeout = float(mcfg.get("timeout_seconds") or 3.0)
                min_interval_sec = float(mcfg.get("watchlist_signal_min_interval_sec") or 600.0)
                max_ex = int(float(mcfg.get("watchlist_max_exchanges_per_symbol") or 0))
            except Exception:
                continue

            for item in batch:
                mon_sym = str(item.get("monitor_symbol") or "").strip().upper()
                want_all = bool(item.get("all_exchanges"))
                exchanges = [str(x).strip().lower() for x in (item.get("exchanges") or []) if str(x).strip()]
                exchanges = sorted(set(exchanges))
                if not mon_sym or ((not want_all) and (not exchanges)):
                    continue

                # Debounce key: "all exchanges" stays stable even if /health changes ordering.
                key = (mon_sym, ("__all__",) if want_all else tuple(exchanges))
                now_s = time.time()
                last_s = float(self._monitor_8010_last_sent.get(key) or 0.0)
                if now_s - last_s < min_interval_sec:
                    continue

                if (not want_all) and max_ex > 0 and len(exchanges) > max_ex:
                    exchanges = exchanges[:max_ex]

                payload: Dict[str, Any] = {
                    "symbol": mon_sym,
                    "ttl_seconds": int(item.get("ttl_seconds") or 86400),
                    "source": str(item.get("source") or "FR_Monitor"),
                    "reason": str(item.get("reason") or "raw_trigger"),
                }
                if not want_all:
                    payload["exchanges"] = exchanges
                try:
                    resp = requests.post(f"{base_url}/watchlist/add", json=payload, timeout=timeout)
                    if resp.status_code == 200:
                        self._monitor_8010_last_sent[key] = now_s
                    else:
                        self.logger.debug(
                            "monitor8010 add failed status=%s body=%s",
                            resp.status_code,
                            (resp.text or "")[:200],
                        )
                except Exception as exc:  # pragma: no cover - network best-effort
                    self.logger.debug("monitor8010 add exception: %s", exc)

    def _run_loop(self) -> None:
        while not self._stop.wait(timeout=self.config.flush_seconds):
            try:
                self.flush()
            except Exception as exc:
                self.logger.warning("PG flush failed: %s", exc)

    def flush(self) -> None:
        if not self.config.enabled:
            return
        with self._lock:
            self._flush_locked()

    def _flush_locked(self) -> None:
        if not self._queue:
            return
        if psycopg is None:
            self.logger.error("psycopg not installed; drop %s rows", len(self._queue))
            self._queue.clear()
            return
        rows: List[Dict[str, Any]] = []
        while self._queue:
            rows.append(self._queue.popleft())
        if not rows:
            return
        event_ops: Dict[str, Any] = {}
        if self.config.enable_event_merge:
            event_ops = self._merge_events(rows)
        self._write_raw(rows)
        if event_ops:
            self._apply_event_ops(event_ops)

    def _merge_events(self, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Event merge tuned for回测：
        - 仅在满足 N 连续触发后开事件。
        - 若超出冷静期 M 分钟未再触发，则关闭事件，避免同一机会被记多次。
        - 聚合首/末/极值（最小/最大）便于回测标签。
        """
        if not rows:
            return {}
        rows_sorted = sorted(rows, key=lambda r: r.get("ts") or _utcnow())
        ops: Dict[str, Any] = {"insert": [], "update": []}
        now = _utcnow()
        n_required = max(1, int(self.config.consecutive_required or 1))
        cooldown = max(1, int(self.config.cooldown_minutes or 1))

        def _funding_stats(series: List[Tuple[datetime, float]]) -> Dict[str, Any]:
            if not series:
                return {}
            vals = [v for _, v in series]
            mean = sum(vals) / len(vals)
            var = sum((v - mean) ** 2 for v in vals) / len(vals) if vals else 0
            std = var ** 0.5
            last_ts, last_val = series[-1]
            momentum = vals[-1] - vals[0] if len(vals) >= 2 else None
            return {
                "n": len(vals),
                "mean": mean,
                "std": std,
                "min": min(vals),
                "max": max(vals),
                "last_ts": last_ts.isoformat(),
                "last": last_val,
                "momentum": momentum,
            }

        def _fetch_funding_hist(exchange: str, symbol: str, days: int = 7) -> List[Tuple[datetime, float]]:
            # 简单缓存 + 限频，避免阻塞
            if self._funding_calls >= 50:
                return []
            end_ts = _utcnow()
            start_ts = end_ts - timedelta(days=days)
            bucket = int(end_ts.timestamp()) // 3600  # 按小时缓存
            key = (exchange, symbol, bucket)
            if key in self._funding_cache:
                return self._funding_cache[key]
            url = None
            params: Dict[str, Any] = {}
            ex = exchange.lower()
            if ex == "binance":
                sym = symbol.upper()
                if not sym.endswith("USDT"):
                    sym = f"{sym}USDT"
                url = "https://fapi.binance.com/fapi/v1/fundingRate"
                params = {
                    "symbol": sym,
                    "startTime": int(start_ts.timestamp() * 1000),
                    "endTime": int(end_ts.timestamp() * 1000),
                    "limit": 1000,
                }
            elif ex == "okx":
                inst = f"{symbol.upper()}-USDT-SWAP"
                url = "https://www.okx.com/api/v5/public/funding-rate-history"
                params = {"instId": inst, "limit": 100}
            elif ex == "bybit":
                sym = f"{symbol.upper()}USDT"
                url = "https://api.bybit.com/v5/market/funding/history"
                params = {
                    "category": "linear",
                    "symbol": sym,
                    "start": int(start_ts.timestamp() * 1000),
                    "end": int(end_ts.timestamp() * 1000),
                    "limit": 200,
                }
            elif ex == "bitget":
                sym = f"{symbol.upper()}USDT"
                url = "https://api.bitget.com/api/v2/mix/market/history-fund-rate"
                params = {
                    "symbol": sym,
                    "productType": "umcbl",
                    "startTime": int(start_ts.timestamp() * 1000),
                    "endTime": int(end_ts.timestamp() * 1000),
                    "limit": 1000,
                }
            else:
                return []

            try:
                resp = requests.get(url, params=params, timeout=5)
                resp.raise_for_status()
                data = resp.json()
            except Exception:
                return []

            out: List[Tuple[datetime, float]] = []
            def _add(ts_ms: int, rate_raw: Any) -> None:
                try:
                    ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    if not (start_ts <= ts <= end_ts):
                        return
                    rate = float(rate_raw)
                    out.append((ts, rate))
                except Exception:
                    return

            if ex == "binance":
                for item in data or []:
                    _add(int(item.get("fundingTime")), item.get("fundingRate"))
            elif ex == "okx":
                for item in data.get("data") or []:
                    _add(int(item.get("fundingTime")), item.get("realizedRate") or item.get("fundingRate"))
            elif ex == "bybit":
                for item in (data.get("result") or {}).get("list") or []:
                    _add(int(item.get("fundingRateTimestamp")), item.get("fundingRate"))
            elif ex == "bitget":
                for item in data.get("data") or []:
                    ts_raw = item.get("fundingTime") or item.get("timestamp")
                    _add(int(ts_raw), item.get("fundingRate"))

            out.sort(key=lambda x: x[0])
            self._funding_cache[key] = out
            self._funding_calls += 1
            return out

        def leg_payload(legs: Optional[Dict[str, Any]], first: bool = False) -> Dict[str, Any]:
            if not legs:
                return {}
            a = legs.get("a") or {}
            b = legs.get("b") or {}
            funding_hist: Dict[str, Any] = {}
            for label, leg in (("a", a), ("b", b)):
                if not leg or leg.get("kind") != "perp":
                    continue
                hist = _fetch_funding_hist(leg.get("exchange", ""), leg.get("symbol", ""))
                if hist:
                    tail = hist[-50:]
                    funding_hist[label] = {
                        "stats": _funding_stats(tail),
                        "tail": [(ts.isoformat(), rate) for ts, rate in tail],
                    }
            payload = {
                "leg_a_exchange": a.get("exchange"),
                "leg_a_symbol": a.get("symbol"),
                "leg_a_kind": a.get("kind"),
                "leg_a_price_first": a.get("price") if first else None,
                "leg_a_price_last": a.get("price"),
                "leg_a_funding_rate_first": a.get("funding_rate") if first else None,
                "leg_a_funding_rate_last": a.get("funding_rate"),
                "leg_a_funding_interval_hours": a.get("funding_interval_hours"),
                "leg_a_next_funding_time": a.get("next_funding_time"),
                "leg_b_exchange": b.get("exchange"),
                "leg_b_symbol": b.get("symbol"),
                "leg_b_kind": b.get("kind"),
                "leg_b_price_first": b.get("price") if first else None,
                "leg_b_price_last": b.get("price"),
                "leg_b_funding_rate_first": b.get("funding_rate") if first else None,
                "leg_b_funding_rate_last": b.get("funding_rate"),
                "leg_b_funding_interval_hours": b.get("funding_interval_hours"),
                "leg_b_next_funding_time": b.get("next_funding_time"),
                "funding_hist": funding_hist if funding_hist else None,
            }
            return payload

        for row in rows_sorted:
            ts = row.get("ts") or now
            ex = str(row.get("exchange") or "")
            sym = str(row.get("symbol") or "")
            sig = str(row.get("signal_type") or "")
            triggered = bool(row.get("triggered"))
            key = (ex, sym, sig)
            state = self._event_state.get(key)
            if not state:
                state = {
                    "pending_count": 0,
                    "open": False,
                    "event_id": None,
                    "start_ts": None,
                    "last_ts": None,
                    "last_trigger_ts": None,
                    "agg": None,
                    "triggered_count": 0,
                }
                self._event_state[key] = state

            # If open and gap exceeds cooldown -> close
            if state["open"] and state["last_trigger_ts"] and (ts - state["last_trigger_ts"]).total_seconds() > cooldown * 60:
                ops["update"].append(
                    {
                        "event_id": state["event_id"],
                        "end_ts": state["last_trigger_ts"],
                        "duration_sec": int((state["last_trigger_ts"] - state["start_ts"]).total_seconds())
                        if state["start_ts"]
                        else None,
                        "features_agg": self._agg_finalize(state["agg"], legs=self._last_legs.get(key)),
                        "close": True,
                    }
                )
                state.update(
                    {
                        "open": False,
                        "event_id": None,
                        "pending_count": 0,
                        "agg": None,
                        "start_ts": None,
                        "last_ts": ts,
                        "last_trigger_ts": None,
                        "triggered_count": 0,
                    }
                )

            if not triggered:
                state["pending_count"] = 0
                state["last_ts"] = ts
                continue

            state["pending_count"] += 1
            state["last_ts"] = ts
            state["last_trigger_ts"] = ts
            state["triggered_count"] += 1
            state["agg"] = self._agg_update(state["agg"], row)
            # 缓存最新腿信息
            if row.get("legs"):
                self._last_legs[key] = row.get("legs")

            if not state["open"] and state["pending_count"] >= n_required:
                # open new event
                state["open"] = True
                state["start_ts"] = ts
                state["event_id"] = None
                legs = self._last_legs.get(key)
                leg_fields = leg_payload(legs, first=True)
                payload = {
                    "exchange": ex,
                    "symbol": sym,
                    "signal_type": sig,
                    "start_ts": ts,
                    "end_ts": ts,
                    "duration_sec": 0,
                    "triggered_count": state["triggered_count"],
                    "funding_interval_hours": row.get("funding_interval_hours"),
                    "next_funding_time": row.get("next_funding_time"),
                    "features_agg": self._agg_finalize(
                        state["agg"],
                        legs=legs,
                        funding_hist=leg_fields.get("funding_hist"),
                    ),
                }
                leg_fields.pop("funding_hist", None)
                payload.update(leg_fields)
                ops["insert"].append(payload)
            elif state["open"] and state["event_id"]:
                legs = self._last_legs.get(key)
                leg_fields = leg_payload(legs, first=False)
                ops["update"].append(
                    {
                        "event_id": state["event_id"],
                        "end_ts": ts,
                        "duration_sec": int((ts - (state["start_ts"] or ts)).total_seconds()),
                        "funding_interval_hours": row.get("funding_interval_hours"),
                        "next_funding_time": row.get("next_funding_time"),
                        "features_agg": self._agg_finalize(
                            state["agg"],
                            legs=self._last_legs.get(key),
                            funding_hist=leg_fields.get("funding_hist"),
                        ),
                        "triggered_count": state["triggered_count"],
                        "close": False,
                        **leg_fields,
                    }
                )

        return ops

    def _agg_update(self, agg: Optional[Dict[str, Any]], row: Dict[str, Any]) -> Dict[str, Any]:
        if agg is None:
            agg = {
                "count": 0,
                "fields": {},
                "meta_first": row.get("meta"),
            }
        metrics = (
            "spread_rel",
            "funding_rate",
            "funding_interval_hours",
            "best_buy_high_sell_low",
            "best_sell_high_buy_low",
            "funding_diff_max",
            "premium_index_diff",
        )
        agg["count"] = agg.get("count", 0) + 1
        for name in metrics:
            val = row.get(name)
            if val is None:
                continue
            field = agg["fields"].get(name) or {"first": val, "last": val, "min": val, "max": val}
            field["last"] = val
            field["min"] = val if field.get("min") is None else min(field["min"], val)
            field["max"] = val if field.get("max") is None else max(field["max"], val)
            agg["fields"][name] = field
        agg["meta_last"] = row.get("meta")
        return agg

    def _agg_finalize(self, agg: Optional[Dict[str, Any]], legs: Optional[Dict[str, Any]] = None, funding_hist: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        if not agg:
            return None
        out: Dict[str, Any] = {}
        for name, stats in (agg.get("fields") or {}).items():
            out[f"{name}_first"] = stats.get("first")
            out[f"{name}_last"] = stats.get("last")
            out[f"{name}_min"] = stats.get("min")
            out[f"{name}_max"] = stats.get("max")
        out["meta_first"] = agg.get("meta_first")
        meta_last = agg.get("meta_last")
        meta_enrich = agg.get("meta_enrich")
        if isinstance(meta_last, dict) and isinstance(meta_enrich, dict) and meta_enrich:
            merged = dict(meta_last)
            merged.update(meta_enrich)
            out["meta_last"] = merged
        else:
            out["meta_last"] = meta_last
        if legs:
            out["legs_last"] = legs
        if funding_hist:
            out["funding_hist"] = funding_hist
        out["count"] = agg.get("count")
        return out or None

    def _apply_event_ops(self, ops: Dict[str, Any]) -> None:
        if psycopg is None:
            self.logger.error("psycopg not installed; skip event ops")
            return
        inserts: List[Dict[str, Any]] = ops.get("insert") or []
        updates: List[Dict[str, Any]] = ops.get("update") or []
        if not inserts and not updates:
            return
        def _normalize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
            """确保 JSON 可序列化并与 jsonb 兼容。"""
            def _json_safe(val: Any) -> Any:
                if isinstance(val, dict):
                    return {k: _json_safe(v) for k, v in val.items()}
                if isinstance(val, list):
                    return [_json_safe(v) for v in val]
                if isinstance(val, tuple):
                    return [_json_safe(v) for v in val]
                if isinstance(val, (datetime,)):
                    return val.isoformat()
                return val

            if psycopg is not None:
                try:
                    from psycopg.types.json import Json  # type: ignore
                    if payload.get("features_agg") is not None:
                        payload["features_agg"] = Json(_json_safe(payload["features_agg"]))
                except Exception:
                    pass
            return payload
        def _ensure_event_defaults(payload: Dict[str, Any]) -> Dict[str, Any]:
            required_keys = [
                "funding_interval_hours",
                "next_funding_time",
                "leg_a_exchange", "leg_a_symbol", "leg_a_kind", "leg_a_price_first", "leg_a_price_last",
                "leg_a_funding_rate_first", "leg_a_funding_rate_last",
                "leg_a_funding_interval_hours", "leg_a_next_funding_time",
                "leg_b_exchange", "leg_b_symbol", "leg_b_kind", "leg_b_price_first", "leg_b_price_last",
                "leg_b_funding_rate_first", "leg_b_funding_rate_last",
                "leg_b_funding_interval_hours", "leg_b_next_funding_time",
                "triggered_count", "features_agg", "close", "end_ts", "duration_sec",
            ]
            for k in required_keys:
                payload.setdefault(k, None if k != "close" else False)
            return payload

        try:
            inserted_event_ids: List[int] = []
            with psycopg.connect(self.config.dsn, autocommit=True) as conn:
                with conn.cursor() as cur:
                    for ins in inserts:
                        # Best-effort: compute series-based factors for the new event.
                        try:
                            ins = self._maybe_add_series_factors(dict(ins))
                        except Exception as exc:  # pragma: no cover - best-effort enrichment
                            self.logger.debug("series factor enrichment skipped/failed: %s", exc)
                        # Best-effort: enrich TypeB events with orderbook-based validation/prediction.
                        # NOTE: This adds network I/O latency; default off.
                        if bool(getattr(self.config, "orderbook_validation_on_write", False)):
                            try:
                                ins = self._maybe_add_orderbook_validation(dict(ins))
                            except Exception as exc:  # pragma: no cover - best-effort enrichment
                                self.logger.debug("orderbook validation skipped/failed: %s", exc)

                        # Persist enrichment into in-memory event state so subsequent UPDATE payloads
                        # keep these fields (otherwise later updates overwrite features_agg.meta_last).
                        try:
                            key = (ins.get("exchange"), ins.get("symbol"), ins.get("signal_type"))
                            features_agg = ins.get("features_agg")
                            meta_last = features_agg.get("meta_last") if isinstance(features_agg, dict) else None
                            enrich: Dict[str, Any] = {}
                            if isinstance(meta_last, dict):
                                if meta_last.get("orderbook_validation") is not None:
                                    enrich["orderbook_validation"] = meta_last.get("orderbook_validation")
                                if meta_last.get("pnl_regression_ob") is not None:
                                    enrich["pnl_regression_ob"] = meta_last.get("pnl_regression_ob")
                                if meta_last.get("factors_v2") is not None:
                                    enrich["factors_v2"] = meta_last.get("factors_v2")
                                if meta_last.get("factors_v2_meta") is not None:
                                    enrich["factors_v2_meta"] = meta_last.get("factors_v2_meta")
                                if meta_last.get("pred_v2") is not None:
                                    enrich["pred_v2"] = meta_last.get("pred_v2")
                                if meta_last.get("pred_v2_meta") is not None:
                                    enrich["pred_v2_meta"] = meta_last.get("pred_v2_meta")
                                if meta_last.get("pred_v2_ob") is not None:
                                    enrich["pred_v2_ob"] = meta_last.get("pred_v2_ob")
                                if meta_last.get("pred_v2_ob_meta") is not None:
                                    enrich["pred_v2_ob_meta"] = meta_last.get("pred_v2_ob_meta")
                            if enrich and key in self._event_state:
                                st = self._event_state.get(key) or {}
                                agg = st.get("agg")
                                if isinstance(agg, dict):
                                    agg["meta_enrich"] = enrich
                        except Exception:
                            pass
                        cur.execute(
                            """
                            INSERT INTO watchlist.watch_signal_event
                              (exchange, symbol, signal_type, start_ts, end_ts, duration_sec, triggered_count, status,
                               funding_interval_hours, next_funding_time,
                               features_agg,
                               leg_a_exchange, leg_a_symbol, leg_a_kind, leg_a_price_first, leg_a_price_last,
                               leg_a_funding_rate_first, leg_a_funding_rate_last, leg_a_funding_interval_hours, leg_a_next_funding_time,
                               leg_b_exchange, leg_b_symbol, leg_b_kind, leg_b_price_first, leg_b_price_last,
                               leg_b_funding_rate_first, leg_b_funding_rate_last, leg_b_funding_interval_hours, leg_b_next_funding_time)
                            VALUES (%(exchange)s, %(symbol)s, %(signal_type)s, %(start_ts)s, %(end_ts)s, %(duration_sec)s, %(triggered_count)s, 'open',
                                    %(funding_interval_hours)s, %(next_funding_time)s,
                                    %(features_agg)s,
                                    %(leg_a_exchange)s, %(leg_a_symbol)s, %(leg_a_kind)s, %(leg_a_price_first)s, %(leg_a_price_last)s,
                                    %(leg_a_funding_rate_first)s, %(leg_a_funding_rate_last)s, %(leg_a_funding_interval_hours)s, %(leg_a_next_funding_time)s,
                                    %(leg_b_exchange)s, %(leg_b_symbol)s, %(leg_b_kind)s, %(leg_b_price_first)s, %(leg_b_price_last)s,
                                    %(leg_b_funding_rate_first)s, %(leg_b_funding_rate_last)s, %(leg_b_funding_interval_hours)s, %(leg_b_next_funding_time)s)
                            RETURNING id
                            """,
                            _normalize_payload(_ensure_event_defaults(ins)),
                        )
                        event_id = cur.fetchone()[0]
                        try:
                            inserted_event_ids.append(int(event_id))
                        except Exception:
                            pass
                        key = (ins["exchange"], ins["symbol"], ins["signal_type"])
                        if key in self._event_state:
                            self._event_state[key]["event_id"] = event_id
                    for upd in updates:
                        cur.execute(
                            """
                            UPDATE watchlist.watch_signal_event
                               SET end_ts = COALESCE(%(end_ts)s, end_ts),
                                   duration_sec = COALESCE(%(duration_sec)s, duration_sec),
                                   triggered_count = COALESCE(%(triggered_count)s, triggered_count),
                                   funding_interval_hours = COALESCE(funding_interval_hours, %(funding_interval_hours)s),
                                   next_funding_time = COALESCE(next_funding_time, %(next_funding_time)s),
                                   features_agg = COALESCE(%(features_agg)s::jsonb, features_agg),
                                   leg_a_exchange = COALESCE(%(leg_a_exchange)s, leg_a_exchange),
                                   leg_a_symbol = COALESCE(%(leg_a_symbol)s, leg_a_symbol),
                                   leg_a_kind = COALESCE(%(leg_a_kind)s, leg_a_kind),
                                   leg_a_price_first = COALESCE(%(leg_a_price_first)s, leg_a_price_first),
                                   leg_a_price_last = COALESCE(%(leg_a_price_last)s, leg_a_price_last),
                                   leg_a_funding_rate_first = COALESCE(%(leg_a_funding_rate_first)s, leg_a_funding_rate_first),
                                   leg_a_funding_rate_last = COALESCE(%(leg_a_funding_rate_last)s, leg_a_funding_rate_last),
                                   leg_a_funding_interval_hours = COALESCE(leg_a_funding_interval_hours, %(leg_a_funding_interval_hours)s),
                                   leg_a_next_funding_time = COALESCE(leg_a_next_funding_time, %(leg_a_next_funding_time)s),
                                   leg_b_exchange = COALESCE(%(leg_b_exchange)s, leg_b_exchange),
                                   leg_b_symbol = COALESCE(%(leg_b_symbol)s, leg_b_symbol),
                                   leg_b_kind = COALESCE(%(leg_b_kind)s, leg_b_kind),
                                   leg_b_price_first = COALESCE(%(leg_b_price_first)s, leg_b_price_first),
                                   leg_b_price_last = COALESCE(%(leg_b_price_last)s, leg_b_price_last),
                                   leg_b_funding_rate_first = COALESCE(%(leg_b_funding_rate_first)s, leg_b_funding_rate_first),
                                   leg_b_funding_rate_last = COALESCE(%(leg_b_funding_rate_last)s, leg_b_funding_rate_last),
                                   leg_b_funding_interval_hours = COALESCE(leg_b_funding_interval_hours, %(leg_b_funding_interval_hours)s),
                                   leg_b_next_funding_time = COALESCE(leg_b_next_funding_time, %(leg_b_next_funding_time)s),
                                   status = CASE WHEN %(close)s THEN 'closed' ELSE status END
                             WHERE id = %(event_id)s
                            """,
                            _normalize_payload(_ensure_event_defaults(upd)),
                        )
            self.logger.info("event ops applied: %s inserts, %s updates", len(inserts), len(updates))
            if inserted_event_ids and self.on_event_written:
                try:
                    self.on_event_written(list(inserted_event_ids))
                except Exception as exc:  # pragma: no cover - callback best-effort
                    self.logger.debug("on_event_written callback failed: %s", exc)
        except Exception as exc:
            self.logger.warning("apply event ops failed: %s", exc)

    def _maybe_add_orderbook_validation(self, ins: Dict[str, Any]) -> Dict[str, Any]:
        """
        在写入 watch_signal_event 的“首次 INSERT”时，按需补齐订单簿口径的验证结果与预测（Type B/C）。

        目标：
        - 使用与 live trading 一致的 “按名义金额扫订单簿 buy/sell 可成交价” 口径，重算
          spread_log_short_over_long/raw_best_buy_high_sell_low，并调用 predict_bc 得到 pnl_hat_ob/win_prob_ob。
        - 只对“高概率存在价差”的信号做订单簿请求，避免每个 raw 都打订单簿。
        """
        try:
            stype = str(ins.get("signal_type") or "").strip().upper()
            if stype not in ("B", "C"):
                return ins
            is_type_c = stype == "C"

            features_agg = ins.get("features_agg")
            if not isinstance(features_agg, dict):
                return ins
            meta_last = features_agg.get("meta_last")
            if not isinstance(meta_last, dict):
                return ins

            # Quick prefilter: require trigger_details.pair and base regression prediction.
            trigger = meta_last.get("trigger_details") or {}
            if not isinstance(trigger, dict):
                return ins
            pair = trigger.get("pair") or []
            if not (isinstance(pair, list) and len(pair) == 2):
                if not is_type_c:
                    return ins
            ex1, ex2 = (str(pair[0]), str(pair[1])) if isinstance(pair, list) and len(pair) == 2 else ("", "")
            if not is_type_c and (not ex1 or not ex2):
                return ins

            # Candidate exchanges/pairs: best-effort context for Top-K orderbook validation.
            candidate_pairs = trigger.get("candidate_pairs")
            if not isinstance(candidate_pairs, list):
                candidate_pairs = []
            candidate_pairs_norm: List[Dict[str, Any]] = []
            for cp in candidate_pairs:
                if not isinstance(cp, dict):
                    continue
                p = cp.get("pair") or []
                if not (isinstance(p, list) and len(p) == 2):
                    continue
                a, b = str(p[0]), str(p[1])
                if not (a and b):
                    continue
                try:
                    sp = float(cp.get("spread")) if cp.get("spread") is not None else None
                except Exception:
                    sp = None
                candidate_pairs_norm.append(
                    {
                        "pair": [a, b],
                        "spread": sp,
                        "prices": cp.get("prices") if isinstance(cp.get("prices"), dict) else None,
                        "funding": cp.get("funding") if isinstance(cp.get("funding"), dict) else None,
                        "market_types": cp.get("market_types") if isinstance(cp.get("market_types"), dict) else None,
                    }
                )
            if not candidate_pairs_norm:
                candidate_pairs_norm = [
                    {
                        "pair": [ex1, ex2],
                        "spread": float(trigger.get("spread")) if trigger.get("spread") is not None else None,
                        "prices": trigger.get("prices") if isinstance(trigger.get("prices"), dict) else None,
                        "funding": trigger.get("funding") if isinstance(trigger.get("funding"), dict) else None,
                        "market_types": trigger.get("market_types") if isinstance(trigger.get("market_types"), dict) else None,
                    }
                ]

            candidate_exchanges = trigger.get("candidate_exchanges")
            if not isinstance(candidate_exchanges, list):
                candidate_exchanges = []
            candidate_exchanges_norm: List[str] = []
            for ex in candidate_exchanges:
                exs = str(ex)
                if exs and exs not in candidate_exchanges_norm:
                    candidate_exchanges_norm.append(exs)
            if not candidate_exchanges_norm:
                for cp in candidate_pairs_norm:
                    p = cp.get("pair") or []
                    if isinstance(p, list) and len(p) == 2:
                        for ex in p:
                            exs = str(ex)
                            if exs and exs not in candidate_exchanges_norm:
                                candidate_exchanges_norm.append(exs)

            # Only spend orderbook calls for events that already pass live trading thresholds
            # (v1 or v2). This keeps write-time orderbook validation selective and bounded.
            try:
                import config as _cfg  # local import: avoid import side-effects at module load

                lt_cfg = getattr(_cfg, "LIVE_TRADING_CONFIG", {}) or {}
                v1_thr_pnl = float(lt_cfg.get("pnl_threshold", 0.0085))
                v1_thr_prob = float(lt_cfg.get("win_prob_threshold", 0.85))
                if is_type_c:
                    v1_thr_pnl = float(lt_cfg.get("type_c_pnl_threshold", v1_thr_pnl))
                    v1_thr_prob = float(lt_cfg.get("type_c_win_prob_threshold", v1_thr_prob))
                v2_enabled = bool(lt_cfg.get("v2_enabled", True)) and (not is_type_c)
                v2_thr_pnl_240 = float(lt_cfg.get("v2_pnl_threshold_240", v1_thr_pnl))
                v2_thr_prob_240 = float(lt_cfg.get("v2_win_prob_threshold_240", v1_thr_prob))
                v2_thr_pnl_1440 = float(lt_cfg.get("v2_pnl_threshold_1440", v1_thr_pnl))
                v2_thr_prob_1440 = float(lt_cfg.get("v2_win_prob_threshold_1440", v1_thr_prob))
            except Exception:
                v1_thr_pnl = 0.0085
                v1_thr_prob = 0.85
                v2_enabled = not is_type_c
                v2_thr_pnl_240 = v1_thr_pnl
                v2_thr_prob_240 = v1_thr_prob
                v2_thr_pnl_1440 = v1_thr_pnl
                v2_thr_prob_1440 = v1_thr_prob

            v1_pass = False
            pnl_reg = meta_last.get("pnl_regression") or {}
            pred_240 = ((pnl_reg or {}).get("pred") or {}).get("240") if isinstance(pnl_reg, dict) else None
            if isinstance(pred_240, dict):
                try:
                    pnl_hat_240_f = float(pred_240.get("pnl_hat"))
                    win_prob_240_f = float(pred_240.get("win_prob"))
                    v1_pass = (
                        math.isfinite(pnl_hat_240_f)
                        and math.isfinite(win_prob_240_f)
                        and pnl_hat_240_f >= v1_thr_pnl
                        and win_prob_240_f >= v1_thr_prob
                    )
                except Exception:
                    v1_pass = False

            v2_pass = False
            if v2_enabled:
                pv2 = meta_last.get("pred_v2") or {}
                if isinstance(pv2, dict):
                    try:
                        p240 = pv2.get("240") if isinstance(pv2.get("240"), dict) else None
                        if isinstance(p240, dict) and bool(p240.get("ok")):
                            pnl2 = float(p240.get("pnl_hat"))
                            prob2 = float(p240.get("win_prob"))
                            if (
                                math.isfinite(pnl2)
                                and math.isfinite(prob2)
                                and pnl2 >= v2_thr_pnl_240
                                and prob2 >= v2_thr_prob_240
                            ):
                                v2_pass = True
                    except Exception:
                        pass
                    if not v2_pass:
                        try:
                            p1440 = pv2.get("1440") if isinstance(pv2.get("1440"), dict) else None
                            if isinstance(p1440, dict) and bool(p1440.get("ok")):
                                pnl2 = float(p1440.get("pnl_hat"))
                                prob2 = float(p1440.get("win_prob"))
                                if (
                                    math.isfinite(pnl2)
                                    and math.isfinite(prob2)
                                    and pnl2 >= v2_thr_pnl_1440
                                    and prob2 >= v2_thr_prob_1440
                                ):
                                    v2_pass = True
                        except Exception:
                            pass

            if not (v1_pass or v2_pass):
                return ins

            # Another cheap filter: require spread estimate >= 0.3% (from trigger/factors).
            spread_watch = None
            if trigger.get("spread") is not None:
                try:
                    spread_watch = float(trigger.get("spread"))
                except Exception:
                    spread_watch = None
            if spread_watch is None:
                try:
                    spread_watch = max(
                        float(cp.get("spread") or 0.0)
                        for cp in candidate_pairs_norm
                        if cp.get("spread") is not None and math.isfinite(float(cp.get("spread")))
                    )
                except Exception:
                    spread_watch = None
            if spread_watch is None:
                factors0 = meta_last.get("factors") or {}
                if isinstance(factors0, dict) and factors0.get("raw_best_buy_high_sell_low") is not None:
                    try:
                        spread_watch = float(factors0.get("raw_best_buy_high_sell_low"))
                    except Exception:
                        spread_watch = None
            if spread_watch is None or not math.isfinite(float(spread_watch)) or float(spread_watch) < 0.003:
                return ins

            # Only validate when at least 2 exchanges are tradable in live trading.
            try:
                import config as _cfg  # local import: avoid import side-effects at module load

                allowed_raw = _cfg.LIVE_TRADING_CONFIG.get("allowed_exchanges") or []
                if isinstance(allowed_raw, str):
                    allowed_list = [x.strip() for x in allowed_raw.split(",") if x.strip()]
                elif isinstance(allowed_raw, (list, tuple, set)):
                    allowed_list = [str(x).strip() for x in allowed_raw if str(x).strip()]
                else:
                    allowed_list = []
                allowed_set = {str(x).lower() for x in allowed_list if str(x).strip()}
                notional = float(_cfg.LIVE_TRADING_CONFIG.get("per_leg_notional_usdt") or 50.0)
                market_type = str(_cfg.LIVE_TRADING_CONFIG.get("orderbook_market_type") or "perp")
                topk_exchanges = int(_cfg.LIVE_TRADING_CONFIG.get("watchlist_event_topk_exchanges") or 5)
                topk_exchanges = max(2, min(10, topk_exchanges))
                live_pnl_threshold = float(_cfg.LIVE_TRADING_CONFIG.get("pnl_threshold") or 0.0)
                live_prob_threshold = float(_cfg.LIVE_TRADING_CONFIG.get("win_prob_threshold") or 0.0)
                if is_type_c:
                    live_pnl_threshold = float(_cfg.LIVE_TRADING_CONFIG.get("type_c_pnl_threshold") or live_pnl_threshold)
                    live_prob_threshold = float(_cfg.LIVE_TRADING_CONFIG.get("type_c_win_prob_threshold") or live_prob_threshold)
            except Exception:
                _cfg = None
                # Safe defaults when config import fails.
                allowed_set = set()
                notional = 50.0
                market_type = "perp"
                topk_exchanges = 5
                live_pnl_threshold = 0.0
                live_prob_threshold = 0.0

            symbol = str(ins.get("symbol") or "")
            if not symbol:
                return ins

            if is_type_c:
                def _extract_c_pair(cp: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
                    spot_ex = None
                    perp_ex = None
                    if isinstance(cp, dict):
                        spot_ex = cp.get("spot_exchange")
                        perp_ex = cp.get("futures_exchange") or cp.get("perp_exchange")
                        pair = cp.get("pair") or []
                        mtypes = cp.get("market_types") or {}
                        if (
                            (not spot_ex or not perp_ex)
                            and isinstance(pair, list)
                            and len(pair) == 2
                            and isinstance(mtypes, dict)
                        ):
                            a, b = str(pair[0]), str(pair[1])
                            mt_a = str(mtypes.get(a) or "").lower()
                            mt_b = str(mtypes.get(b) or "").lower()
                            if mt_a == "spot" and mt_b == "perp":
                                spot_ex, perp_ex = a, b
                            elif mt_a == "perp" and mt_b == "spot":
                                spot_ex, perp_ex = b, a
                    return (str(spot_ex) if spot_ex else None, str(perp_ex) if perp_ex else None)

                spot_ranked: List[str] = []
                perp_ranked: List[str] = []
                cps_sorted = sorted(candidate_pairs_norm, key=lambda x: float(x.get("spread") or 0.0), reverse=True)
                for cp in cps_sorted:
                    spot_ex, perp_ex = _extract_c_pair(cp)
                    if spot_ex and spot_ex not in spot_ranked:
                        spot_ranked.append(spot_ex)
                    if perp_ex and perp_ex not in perp_ranked:
                        perp_ranked.append(perp_ex)
                    if len(spot_ranked) >= topk_exchanges and len(perp_ranked) >= topk_exchanges:
                        break

                if not spot_ranked or not perp_ranked:
                    spot_ex = str(trigger.get("spot_exchange") or "").strip()
                    perp_ex = str(trigger.get("futures_exchange") or "").strip()
                    if spot_ex and spot_ex not in spot_ranked:
                        spot_ranked.append(spot_ex)
                    if perp_ex and perp_ex not in perp_ranked:
                        perp_ranked.append(perp_ex)

                if allowed_set:
                    spot_ranked = [ex for ex in spot_ranked if ex.lower() in allowed_set]
                    perp_ranked = [ex for ex in perp_ranked if ex.lower() in allowed_set]

                if not spot_ranked or not perp_ranked:
                    return ins

                orderbooks: Dict[str, Any] = {}
                spot_ok: List[str] = []
                perp_ok: List[str] = []
                for ex in spot_ranked:
                    try:
                        ob = fetch_orderbook_prices(ex, symbol, "spot", notional=notional)
                    except Exception as exc:
                        ob = {"error": "exception", "exception": f"{type(exc).__name__}: {exc}"}
                    ex_row = orderbooks.get(ex)
                    if not isinstance(ex_row, dict):
                        ex_row = {}
                    ex_row["spot"] = ob
                    orderbooks[ex] = ex_row
                    try:
                        if ob and not ob.get("error") and ob.get("buy") is not None and ob.get("sell") is not None:
                            spot_ok.append(ex)
                    except Exception:
                        continue

                for ex in perp_ranked:
                    try:
                        ob = fetch_orderbook_prices(ex, symbol, "perp", notional=notional)
                    except Exception as exc:
                        ob = {"error": "exception", "exception": f"{type(exc).__name__}: {exc}"}
                    ex_row = orderbooks.get(ex)
                    if not isinstance(ex_row, dict):
                        ex_row = {}
                    ex_row["perp"] = ob
                    orderbooks[ex] = ex_row
                    try:
                        if ob and not ob.get("error") and ob.get("buy") is not None and ob.get("sell") is not None:
                            perp_ok.append(ex)
                    except Exception:
                        continue

                if not spot_ok or not perp_ok:
                    meta_last["orderbook_validation"] = {
                        "ok": False,
                        "reason": "orderbook_unavailable",
                        "ts": _utcnow().isoformat(),
                        "notional_usdt": notional,
                        "market_type": "spot-perp",
                        "spot_exchanges": spot_ranked,
                        "perp_exchanges": perp_ranked,
                        "orderbooks": orderbooks,
                    }
                    features_agg["meta_last"] = meta_last
                    ins["features_agg"] = features_agg
                    return ins

                base_factors = meta_last.get("factors") or {}
                if not isinstance(base_factors, dict):
                    base_factors = {}

                evaluated: List[Dict[str, Any]] = []
                best: Optional[Dict[str, Any]] = None
                best_score: Tuple[float, float] = (-1e9, -1e9)

                for spot_ex in spot_ok:
                    for perp_ex in perp_ok:
                        ob_spot = (orderbooks.get(spot_ex) or {}).get("spot") or {}
                        ob_perp = (orderbooks.get(perp_ex) or {}).get("perp") or {}
                        long_px = ob_spot.get("buy")
                        short_px = ob_perp.get("sell")
                        try:
                            short_f = float(short_px)
                            long_f = float(long_px)
                        except Exception:
                            evaluated.append(
                                {
                                    "spot_exchange": str(spot_ex),
                                    "perp_exchange": str(perp_ex),
                                    "short_px": short_px,
                                    "long_px": long_px,
                                    "tradable_spread": None,
                                    "entry_spread_metric": None,
                                    "note": "px_parse_error",
                                }
                            )
                            continue
                        if short_f <= 0 or long_f <= 0:
                            evaluated.append(
                                {
                                    "spot_exchange": str(spot_ex),
                                    "perp_exchange": str(perp_ex),
                                    "short_px": float(short_f),
                                    "long_px": float(long_f),
                                    "tradable_spread": None,
                                    "entry_spread_metric": None,
                                    "note": "non_positive_px",
                                }
                            )
                            continue

                        tradable_spread = (short_f - long_f) / long_f
                        entry_spread_metric = float(math.log(short_f / long_f))

                        factors = dict(base_factors)
                        factors["spread_log_short_over_long"] = float(entry_spread_metric)
                        factors["raw_best_buy_high_sell_low"] = float(tradable_spread)
                        pred = predict_bc(signal_type="C", factors=factors, horizons=(240,))
                        pred_map = (pred or {}).get("pred") or {}
                        hpred = pred_map.get("240") or {}
                        pnl_hat_ob = hpred.get("pnl_hat")
                        win_prob_ob = hpred.get("win_prob")

                        evaluated.append(
                            {
                                "spot_exchange": str(spot_ex),
                                "perp_exchange": str(perp_ex),
                                "short_px": float(short_f),
                                "long_px": float(long_f),
                                "tradable_spread": float(tradable_spread),
                                "entry_spread_metric": float(entry_spread_metric),
                                "pnl_hat": float(pnl_hat_ob) if pnl_hat_ob is not None else None,
                                "win_prob": float(win_prob_ob) if win_prob_ob is not None else None,
                                "ok": bool(
                                    pnl_hat_ob is not None
                                    and win_prob_ob is not None
                                    and float(pnl_hat_ob) >= float(live_pnl_threshold)
                                    and float(win_prob_ob) >= float(live_prob_threshold)
                                    and tradable_spread > 0
                                ),
                            }
                        )

                        if pnl_hat_ob is None or win_prob_ob is None:
                            continue
                        if tradable_spread <= 0:
                            continue
                        score = (float(pnl_hat_ob), float(win_prob_ob))
                        if score > best_score:
                            best_score = score
                            best = {
                                "long_exchange": str(spot_ex),
                                "short_exchange": str(perp_ex),
                                "long_px": float(long_f),
                                "short_px": float(short_f),
                                "pnl_hat_ob": float(pnl_hat_ob),
                                "win_prob_ob": float(win_prob_ob),
                                "tradable_spread": float(tradable_spread),
                                "entry_spread_metric": float(entry_spread_metric),
                                "factors": factors,
                            }

                if not best:
                    meta_last["orderbook_validation"] = {
                        "ok": False,
                        "reason": "no_tradable_direction",
                        "ts": _utcnow().isoformat(),
                        "notional_usdt": notional,
                        "market_type": "spot-perp",
                        "spot_exchanges": spot_ranked,
                        "perp_exchanges": perp_ranked,
                        "orderbooks": orderbooks,
                        "candidates": evaluated,
                    }
                    features_agg["meta_last"] = meta_last
                    ins["features_agg"] = features_agg
                    return ins

                meta_last["orderbook_validation"] = {
                    "ok": bool(best["pnl_hat_ob"] >= live_pnl_threshold and best["win_prob_ob"] >= live_prob_threshold),
                    "reason": None,
                    "ts": _utcnow().isoformat(),
                    "notional_usdt": notional,
                    "market_type": "spot-perp",
                    "threshold": {"pnl": live_pnl_threshold, "win_prob": live_prob_threshold},
                    "spot_exchanges": spot_ranked,
                    "perp_exchanges": perp_ranked,
                    "orderbooks": orderbooks,
                    "candidates": evaluated,
                    "chosen": {
                        "long_exchange": best["long_exchange"],
                        "short_exchange": best["short_exchange"],
                        "long_px": best.get("long_px"),
                        "short_px": best.get("short_px"),
                        "tradable_spread": best["tradable_spread"],
                        "entry_spread_metric": best["entry_spread_metric"],
                        "pnl_hat": best["pnl_hat_ob"],
                        "win_prob": best["win_prob_ob"],
                    },
                }
                meta_last["pnl_regression_ob"] = {
                    "model": ((pnl_reg or {}).get("model") if isinstance(pnl_reg, dict) else None),
                    "fee_threshold": ((pnl_reg or {}).get("fee_threshold") if isinstance(pnl_reg, dict) else None),
                    "pred": {"240": {"pnl_hat": best["pnl_hat_ob"], "win_prob": best["win_prob_ob"]}},
                }
                features_agg["meta_last"] = meta_last
                ins["features_agg"] = features_agg
                return ins

            # Build Top-K exchanges list, so we can evaluate many pairs in-memory while
            # keeping REST calls bounded (K orderbooks, not K^2).
            ranked_exchanges: List[str] = []
            for ex in (ex1, ex2):
                if ex and ex not in ranked_exchanges:
                    ranked_exchanges.append(ex)

            cps_sorted = sorted(candidate_pairs_norm, key=lambda x: float(x.get("spread") or 0.0), reverse=True)
            for cp in cps_sorted:
                p = cp.get("pair") or []
                if not (isinstance(p, list) and len(p) == 2):
                    continue
                for ex in (str(p[0]), str(p[1])):
                    if ex and ex not in ranked_exchanges:
                        ranked_exchanges.append(ex)
                if len(ranked_exchanges) >= topk_exchanges:
                    break
            if len(ranked_exchanges) < topk_exchanges:
                for ex in candidate_exchanges_norm:
                    if ex and ex not in ranked_exchanges:
                        ranked_exchanges.append(ex)
                    if len(ranked_exchanges) >= topk_exchanges:
                        break

            if allowed_set:
                ranked_exchanges = [ex for ex in ranked_exchanges if ex.lower() in allowed_set]
            if len(ranked_exchanges) < 2:
                return ins

            # Fetch orderbooks once per exchange (same notional/market_type as live trading).
            # Important: never let an exception here drop the whole enrichment silently — always
            # persist a failure snapshot for post-mortem.
            orderbooks: Dict[str, Any] = {}
            orderbook_ok: List[str] = []
            for ex in ranked_exchanges:
                try:
                    ob = fetch_orderbook_prices(ex, symbol, market_type, notional=notional)
                except Exception as exc:
                    ob = {"error": "exception", "exception": f"{type(exc).__name__}: {exc}"}
                orderbooks[ex] = ob
                try:
                    if ob and not ob.get("error") and ob.get("buy") is not None and ob.get("sell") is not None:
                        orderbook_ok.append(ex)
                except Exception:
                    continue

            if len(orderbook_ok) < 2:
                meta_last["orderbook_validation"] = {
                    "ok": False,
                    "reason": "orderbook_unavailable",
                    "ts": _utcnow().isoformat(),
                    "notional_usdt": notional,
                    "market_type": market_type,
                    "topk_exchanges": ranked_exchanges,
                    "orderbooks": orderbooks,
                }
                features_agg["meta_last"] = meta_last
                ins["features_agg"] = features_agg
                return ins

            base_factors = meta_last.get("factors") or {}
            if not isinstance(base_factors, dict):
                base_factors = {}

            evaluated: List[Dict[str, Any]] = []
            best: Optional[Dict[str, Any]] = None
            best_score: Tuple[float, float] = (-1e9, -1e9)

            # Evaluate all pairs in-memory using the fetched orderbooks.
            for i in range(len(orderbook_ok)):
                for j in range(i + 1, len(orderbook_ok)):
                    ex_a = orderbook_ok[i]
                    ex_b = orderbook_ok[j]
                    ob_a = orderbooks.get(ex_a) or {}
                    ob_b = orderbooks.get(ex_b) or {}
                    candidates = [
                        # long on A (buy ask), short on B (sell bid)
                        {"long_ex": ex_a, "short_ex": ex_b, "long_px": ob_a.get("buy"), "short_px": ob_b.get("sell")},
                        # swap direction
                        {"long_ex": ex_b, "short_ex": ex_a, "long_px": ob_b.get("buy"), "short_px": ob_a.get("sell")},
                    ]
                    for cand in candidates:
                        short_px = cand.get("short_px")
                        long_px = cand.get("long_px")
                        try:
                            short_f = float(short_px)
                            long_f = float(long_px)
                        except Exception:
                            evaluated.append(
                                {
                                    "short_exchange": str(cand.get("short_ex") or ""),
                                    "long_exchange": str(cand.get("long_ex") or ""),
                                    "short_px": short_px,
                                    "long_px": long_px,
                                    "tradable_spread": None,
                                    "entry_spread_metric": None,
                                    "pnl_hat": None,
                                    "win_prob": None,
                                    "note": "px_parse_error",
                                }
                            )
                            continue
                        if short_f <= 0 or long_f <= 0:
                            evaluated.append(
                                {
                                    "short_exchange": str(cand.get("short_ex") or ""),
                                    "long_exchange": str(cand.get("long_ex") or ""),
                                    "short_px": float(short_f),
                                    "long_px": float(long_f),
                                    "tradable_spread": None,
                                    "entry_spread_metric": None,
                                    "pnl_hat": None,
                                    "win_prob": None,
                                    "note": "non_positive_px",
                                }
                            )
                            continue

                        tradable_spread = (short_f - long_f) / long_f
                        entry_spread_metric = float(math.log(short_f / long_f))

                        factors = dict(base_factors)
                        factors["spread_log_short_over_long"] = float(entry_spread_metric)
                        factors["raw_best_buy_high_sell_low"] = float(tradable_spread)
                        pred = predict_bc(signal_type="B", factors=factors, horizons=(240,))
                        pred_map = (pred or {}).get("pred") or {}
                        hpred = pred_map.get("240") or {}
                        pnl_hat_ob = hpred.get("pnl_hat")
                        win_prob_ob = hpred.get("win_prob")

                        evaluated.append(
                            {
                                "short_exchange": str(cand.get("short_ex") or ""),
                                "long_exchange": str(cand.get("long_ex") or ""),
                                "short_px": float(short_f),
                                "long_px": float(long_f),
                                "tradable_spread": float(tradable_spread),
                                "entry_spread_metric": float(entry_spread_metric),
                                "pnl_hat": float(pnl_hat_ob) if pnl_hat_ob is not None else None,
                                "win_prob": float(win_prob_ob) if win_prob_ob is not None else None,
                                "ok": bool(
                                    pnl_hat_ob is not None
                                    and win_prob_ob is not None
                                    and float(pnl_hat_ob) >= float(live_pnl_threshold)
                                    and float(win_prob_ob) >= float(live_prob_threshold)
                                    and tradable_spread > 0
                                ),
                            }
                        )

                        if pnl_hat_ob is None or win_prob_ob is None:
                            continue
                        if tradable_spread <= 0:
                            continue
                        score = (float(pnl_hat_ob), float(win_prob_ob))
                        if score > best_score:
                            best_score = score
                            best = {
                                "long_exchange": str(cand.get("long_ex") or ""),
                                "short_exchange": str(cand.get("short_ex") or ""),
                                "long_px": float(long_f),
                                "short_px": float(short_f),
                                "pnl_hat_ob": float(pnl_hat_ob),
                                "win_prob_ob": float(win_prob_ob),
                                "tradable_spread": float(tradable_spread),
                                "entry_spread_metric": float(entry_spread_metric),
                                "factors": factors,
                            }

            if not best:
                meta_last["orderbook_validation"] = {
                    "ok": False,
                    "reason": "no_tradable_direction",
                    "ts": _utcnow().isoformat(),
                    "notional_usdt": notional,
                    "market_type": market_type,
                    "topk_exchanges": ranked_exchanges,
                    "orderbooks": orderbooks,
                    "candidates": evaluated,
                }
                features_agg["meta_last"] = meta_last
                ins["features_agg"] = features_agg
                return ins

            meta_last["orderbook_validation"] = {
                "ok": bool(best["pnl_hat_ob"] >= live_pnl_threshold and best["win_prob_ob"] >= live_prob_threshold),
                "reason": None,
                "ts": _utcnow().isoformat(),
                "notional_usdt": notional,
                "market_type": market_type,
                "threshold": {"pnl": live_pnl_threshold, "win_prob": live_prob_threshold},
                "topk_exchanges": ranked_exchanges,
                "orderbooks": orderbooks,
                "candidates": evaluated,
                "chosen": {
                    "long_exchange": best["long_exchange"],
                    "short_exchange": best["short_exchange"],
                    "long_px": best.get("long_px"),
                    "short_px": best.get("short_px"),
                    "tradable_spread": best["tradable_spread"],
                    "entry_spread_metric": best["entry_spread_metric"],
                    "pnl_hat": best["pnl_hat_ob"],
                    "win_prob": best["win_prob_ob"],
                },
            }
            meta_last["pnl_regression_ob"] = {
                "model": ((pnl_reg or {}).get("model") if isinstance(pnl_reg, dict) else None),
                "fee_threshold": ((pnl_reg or {}).get("fee_threshold") if isinstance(pnl_reg, dict) else None),
                "pred": {"240": {"pnl_hat": best["pnl_hat_ob"], "win_prob": best["win_prob_ob"]}},
            }
            try:
                if _cfg is None:
                    raise RuntimeError("config_not_available")
                v2_cfg = getattr(_cfg, "WATCHLIST_V2_PRED_CONFIG", {}) or {}
                if bool(v2_cfg.get("enabled")) and isinstance(meta_last.get("factors_v2"), dict):
                    max_missing = float(v2_cfg.get("max_missing_ratio") or 0.2)

                    def _dt(val: Any) -> Optional[datetime]:
                        if isinstance(val, datetime):
                            return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
                        if isinstance(val, str):
                            try:
                                ts = datetime.fromisoformat(val)
                                return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
                            except Exception:
                                return None
                        return None

                    event_ts = _dt(ins.get("start_ts")) or _dt(ins.get("end_ts")) or _utcnow()
                    leg_a = {
                        "exchange": ins.get("leg_a_exchange"),
                        "symbol": ins.get("leg_a_symbol") or ins.get("symbol"),
                        "kind": ins.get("leg_a_kind") or "perp",
                        "funding_interval_hours": ins.get("leg_a_funding_interval_hours"),
                        "next_funding_time": ins.get("leg_a_next_funding_time"),
                    }
                    leg_b = {
                        "exchange": ins.get("leg_b_exchange"),
                        "symbol": ins.get("leg_b_symbol") or ins.get("symbol"),
                        "kind": ins.get("leg_b_kind") or "perp",
                        "funding_interval_hours": ins.get("leg_b_funding_interval_hours"),
                        "next_funding_time": ins.get("leg_b_next_funding_time"),
                    }

                    def _leg_for_exchange(ex: str) -> Optional[Dict[str, Any]]:
                        if ex and str(leg_a.get("exchange") or "") == ex:
                            return leg_a
                        if ex and str(leg_b.get("exchange") or "") == ex:
                            return leg_b
                        sym = ins.get("symbol")
                        if ex and sym:
                            return {
                                "exchange": ex,
                                "symbol": sym,
                                "kind": "perp",
                                "funding_interval_hours": None,
                                "next_funding_time": None,
                            }
                        return None

                    short_leg = _leg_for_exchange(str(best.get("short_exchange") or ""))
                    long_leg = _leg_for_exchange(str(best.get("long_exchange") or ""))
                    short_px = best.get("short_px")
                    long_px = best.get("long_px")
                    if short_leg and long_leg and short_px is not None and long_px is not None:
                        metrics_cfg = _cfg.WATCHLIST_METRICS_CONFIG if hasattr(_cfg, "WATCHLIST_METRICS_CONFIG") else {}
                        sqlite_path = _cfg.WATCHLIST_CONFIG.get("db_path", "market_data.db")
                        lookback_min = int(metrics_cfg.get("series_lookback_min", 240))
                        tol_sec = int(metrics_cfg.get("series_asof_tol_sec", 90))
                        ffill_bars = int(metrics_cfg.get("series_forward_fill_bars", 2))
                        min_valid_ratio = float(metrics_cfg.get("series_min_valid_ratio", 0.8))
                        max_backfill_bars = int(metrics_cfg.get("series_max_backfill_bars", ffill_bars))
                        cache_entries = int(metrics_cfg.get("series_cache_entries", 64))
                        if cache_entries > 0:
                            if self._series_cache is None or self._series_cache.max_entries != cache_entries:
                                self._series_cache = SeriesCache(max_entries=cache_entries)
                        else:
                            self._series_cache = None

                        factors_ob, meta_ob = compute_event_series_factors(
                            sqlite_path,
                            event_ts,
                            short_leg,
                            long_leg,
                            lookback_min=lookback_min,
                            tol_sec=tol_sec,
                            ffill_bars=ffill_bars,
                            min_valid_ratio=min_valid_ratio,
                            max_backfill_bars=max_backfill_bars,
                            override_short_price=float(short_px),
                            override_long_price=float(long_px),
                            series_cache=self._series_cache,
                        )
                        pred_v2_ob: Dict[str, Any] = {}
                        pred_v2_ob_meta: Dict[str, Any] = {
                            "ts": _utcnow().isoformat(),
                            "max_missing_ratio": max_missing,
                            "source": "orderbook_override",
                            "meta": meta_ob,
                        }
                        for horizon_key in ("240", "1440"):
                            model_key = f"model_path_{horizon_key}"
                            model_path = str(v2_cfg.get(model_key) or "").strip()
                            if not model_path:
                                continue
                            res = infer_v2(model_path=model_path, factors=factors_ob, max_missing_ratio=max_missing)
                            pred_v2_ob[horizon_key] = res
                        if pred_v2_ob:
                            meta_last["pred_v2_ob"] = pred_v2_ob
                            meta_last["pred_v2_ob_meta"] = pred_v2_ob_meta
            except Exception as exc:
                meta_last["pred_v2_ob_meta"] = {
                    "ok": False,
                    "reason": "v2_ob_exception",
                    "ts": _utcnow().isoformat(),
                    "exception": f"{type(exc).__name__}: {exc}",
                }
            features_agg["meta_last"] = meta_last
            ins["features_agg"] = features_agg
        except Exception as exc:
            # Never fail the event insert; but keep a minimal error snapshot so we can confirm
            # whether enrichment is being executed and why it failed.
            try:
                features_agg2 = ins.get("features_agg")
                if isinstance(features_agg2, dict):
                    meta_last2 = features_agg2.get("meta_last")
                    if isinstance(meta_last2, dict) and meta_last2.get("orderbook_validation") is None:
                        meta_last2["orderbook_validation"] = {
                            "ok": False,
                            "reason": "exception",
                            "ts": _utcnow().isoformat(),
                            "exception": f"{type(exc).__name__}: {exc}",
                        }
                        features_agg2["meta_last"] = meta_last2
                        ins["features_agg"] = features_agg2
            except Exception:
                pass
            return ins
        return ins

    def _maybe_add_series_factors(self, ins: Dict[str, Any]) -> Dict[str, Any]:
        """
        在首次插入 event 时，根据两腿的 1min K 线构造 spread/funding 序列并计算 T1 因子。
        因子写入 features_agg.meta_last.factors_v2 / factors_v2_meta，供回测与后续模型使用。
        """
        try:
            stype = str(ins.get("signal_type") or "").strip().upper()
            if stype not in ("B", "C"):
                return ins
            features_agg = ins.get("features_agg")
            if not isinstance(features_agg, dict):
                return ins
            meta_last = features_agg.get("meta_last")
            if not isinstance(meta_last, dict):
                return ins
            if isinstance(meta_last.get("factors_v2"), dict):
                return ins

            def _dt(val: Any) -> Optional[datetime]:
                if isinstance(val, datetime):
                    return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
                if isinstance(val, str):
                    try:
                        ts = datetime.fromisoformat(val)
                        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
                    except Exception:
                        return None
                return None

            event_ts = _dt(ins.get("start_ts")) or _dt(ins.get("end_ts")) or _utcnow()

            leg_a = {
                "exchange": ins.get("leg_a_exchange"),
                "symbol": ins.get("leg_a_symbol"),
                "kind": ins.get("leg_a_kind") or "perp",
                "price": ins.get("leg_a_price_first") or ins.get("leg_a_price_last"),
                "funding_interval_hours": ins.get("leg_a_funding_interval_hours"),
                "next_funding_time": ins.get("leg_a_next_funding_time"),
            }
            leg_b = {
                "exchange": ins.get("leg_b_exchange"),
                "symbol": ins.get("leg_b_symbol"),
                "kind": ins.get("leg_b_kind") or "perp",
                "price": ins.get("leg_b_price_first") or ins.get("leg_b_price_last"),
                "funding_interval_hours": ins.get("leg_b_funding_interval_hours"),
                "next_funding_time": ins.get("leg_b_next_funding_time"),
            }
            if not (leg_a.get("exchange") and leg_a.get("symbol") and leg_b.get("exchange") and leg_b.get("symbol")):
                meta_last["factors_v2_meta"] = {
                    "ok": False,
                    "reason": "missing_leg_identity",
                    "ts": _utcnow().isoformat(),
                }
                features_agg["meta_last"] = meta_last
                ins["features_agg"] = features_agg
                return ins

            price_a = leg_a.get("price")
            price_b = leg_b.get("price")
            if price_a is None or price_b is None:
                trigger = meta_last.get("trigger_details") or {}
                if isinstance(trigger, dict):
                    prices = trigger.get("prices") or {}
                    if isinstance(prices, dict):
                        try:
                            price_a = price_a if price_a is not None else prices.get(leg_a.get("exchange"))
                            price_b = price_b if price_b is not None else prices.get(leg_b.get("exchange"))
                        except Exception:
                            pass

            if price_a is None or price_b is None:
                meta_last["factors_v2_meta"] = {
                    "ok": False,
                    "reason": "missing_leg_price",
                    "ts": _utcnow().isoformat(),
                }
                features_agg["meta_last"] = meta_last
                ins["features_agg"] = features_agg
                return ins

            try:
                price_a_f = float(price_a)
                price_b_f = float(price_b)
            except Exception:
                meta_last["factors_v2_meta"] = {
                    "ok": False,
                    "reason": "invalid_leg_price",
                    "ts": _utcnow().isoformat(),
                }
                features_agg["meta_last"] = meta_last
                ins["features_agg"] = features_agg
                return ins

            if price_a_f == price_b_f:
                meta_last["factors_v2_meta"] = {
                    "ok": False,
                    "reason": "equal_leg_price",
                    "ts": _utcnow().isoformat(),
                }
                features_agg["meta_last"] = meta_last
                ins["features_agg"] = features_agg
                return ins

            if price_a_f > price_b_f:
                short_leg, long_leg = leg_a, leg_b
            else:
                short_leg, long_leg = leg_b, leg_a

            try:
                import config as _cfg  # local import

                sqlite_path = _cfg.WATCHLIST_CONFIG.get("db_path", "market_data.db")
                metrics_cfg = _cfg.WATCHLIST_METRICS_CONFIG if hasattr(_cfg, "WATCHLIST_METRICS_CONFIG") else {}
                lookback_min = int(metrics_cfg.get("series_lookback_min", 240))
                tol_sec = int(metrics_cfg.get("series_asof_tol_sec", 90))
                ffill_bars = int(metrics_cfg.get("series_forward_fill_bars", 2))
                min_valid_ratio = float(metrics_cfg.get("series_min_valid_ratio", 0.8))
                max_backfill_bars = int(metrics_cfg.get("series_max_backfill_bars", ffill_bars))
                cache_entries = int(metrics_cfg.get("series_cache_entries", 64))
            except Exception:
                sqlite_path = "market_data.db"
                lookback_min = 240
                tol_sec = 90
                ffill_bars = 2
                min_valid_ratio = 0.8
                max_backfill_bars = ffill_bars
                cache_entries = 0

            if cache_entries > 0:
                if self._series_cache is None or self._series_cache.max_entries != cache_entries:
                    self._series_cache = SeriesCache(max_entries=cache_entries)
            else:
                self._series_cache = None

            factors, meta = compute_event_series_factors(
                sqlite_path,
                event_ts,
                short_leg,
                long_leg,
                lookback_min=lookback_min,
                tol_sec=tol_sec,
                ffill_bars=ffill_bars,
                min_valid_ratio=min_valid_ratio,
                max_backfill_bars=max_backfill_bars,
                series_cache=self._series_cache,
            )
            meta_last["factors_v2"] = factors
            meta_last["factors_v2_meta"] = {
                "ok": True,
                "ts": _utcnow().isoformat(),
                **(meta or {}),
            }
            try:
                v2_cfg = getattr(_cfg, "WATCHLIST_V2_PRED_CONFIG", {}) or {}
                if bool(v2_cfg.get("enabled")) and isinstance(factors, dict):
                    max_missing = float(v2_cfg.get("max_missing_ratio") or 0.2)
                    pred_v2: Dict[str, Any] = {}
                    pred_v2_meta: Dict[str, Any] = {"ts": _utcnow().isoformat(), "max_missing_ratio": max_missing}
                    for horizon_key in ("240", "1440"):
                        model_key = f"model_path_{horizon_key}"
                        model_path = str(v2_cfg.get(model_key) or "").strip()
                        if not model_path:
                            continue
                        res = infer_v2(model_path=model_path, factors=factors, max_missing_ratio=max_missing)
                        pred_v2[horizon_key] = res
                    if pred_v2:
                        meta_last["pred_v2"] = pred_v2
                        meta_last["pred_v2_meta"] = pred_v2_meta
            except Exception as exc:
                meta_last["pred_v2_meta"] = {
                    "ok": False,
                    "reason": "v2_exception",
                    "ts": _utcnow().isoformat(),
                    "exception": f"{type(exc).__name__}: {exc}",
                }
            features_agg["meta_last"] = meta_last
            ins["features_agg"] = features_agg
        except Exception as exc:
            try:
                features_agg2 = ins.get("features_agg")
                if isinstance(features_agg2, dict):
                    meta_last2 = features_agg2.get("meta_last")
                    if isinstance(meta_last2, dict):
                        meta_last2["factors_v2_meta"] = {
                            "ok": False,
                            "reason": "exception",
                            "ts": _utcnow().isoformat(),
                            "exception": f"{type(exc).__name__}: {exc}",
                        }
                        features_agg2["meta_last"] = meta_last2
                        ins["features_agg"] = features_agg2
            except Exception:
                pass
            return ins
        return ins

    def _write_raw(self, rows: List[Dict[str, Any]]) -> None:
        cols = [
            "ts",
            "exchange",
            "symbol",
            "signal_type",
            "type_class",
            "triggered",
            "status",
            "spread_rel",
            "funding_rate",
            "funding_interval_hours",
            "next_funding_time",
            "range_1h",
            "range_12h",
            "volatility",
            "slope_3m",
            "crossings_1h",
            "drift_ratio",
            "best_buy_high_sell_low",
            "best_sell_high_buy_low",
            "funding_diff_max",
            "spot_perp_volume_ratio",
            "oi_to_volume_ratio",
            "bid_ask_spread",
            "depth_imbalance",
            "volume_spike_zscore",
            "premium_index_diff",
            "leg_a_exchange",
            "leg_a_symbol",
            "leg_a_kind",
            "leg_a_price",
            "leg_a_funding_rate",
            "leg_a_next_funding_time",
            "leg_b_exchange",
            "leg_b_symbol",
            "leg_b_kind",
            "leg_b_price",
            "leg_b_funding_rate",
            "leg_b_next_funding_time",
            "meta",
        ]
        placeholders = ",".join([f"%({c})s" for c in cols])
        sql = f"INSERT INTO watchlist.watch_signal_raw ({','.join(cols)}) VALUES ({placeholders})"
        def _normalize(row: Dict[str, Any]) -> Dict[str, Any]:
            r: Dict[str, Any] = {}
            for c in cols:
                r[c] = row.get(c)
            if psycopg is not None and r.get("meta") is not None:
                try:
                    from psycopg.types.json import Json  # type: ignore
                    r["meta"] = Json(r["meta"])
                except Exception:
                    pass
            # normalize legs timestamps to datetime for json
            for k in ("leg_a_next_funding_time", "leg_b_next_funding_time"):
                if isinstance(r.get(k), str):
                    try:
                        r[k] = datetime.fromisoformat(r[k])
                    except Exception:
                        pass
            return r
        try:
            with psycopg.connect(self.config.dsn, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.executemany(sql, [_normalize(r) for r in rows])
            self.logger.info("wrote %s raw rows to PG", len(rows))
        except Exception as exc:
            # TODO: add file/SQLite fallback
            self.logger.error("write raw failed, dropping %s rows: %s", len(rows), exc)
