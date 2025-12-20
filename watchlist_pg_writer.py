from __future__ import annotations

import logging
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
        # state for event merge: (exchange, symbol, signal_type) -> state dict
        self._event_state: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        # 缓存最近一次双腿信息，便于事件聚合时保留首/末腿快照
        self._last_legs: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        # funding history cache: key=(exchange,symbol,day_bucket)
        self._funding_cache: Dict[Tuple[str, str, int], List[Tuple[datetime, float]]] = {}
        self._funding_calls = 0
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
        self.logger.info("PG writer started (batch=%s, flush=%ss)", self.config.batch_size, self.config.flush_seconds)

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
            if len(self._queue) >= self.config.batch_size:
                self._flush_locked()

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
        out["meta_last"] = agg.get("meta_last")
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
                        # Best-effort: enrich TypeB events with orderbook-based validation/prediction.
                        try:
                            ins = self._maybe_add_orderbook_validation(dict(ins))
                        except Exception as exc:  # pragma: no cover - best-effort enrichment
                            self.logger.debug("orderbook validation skipped/failed: %s", exc)
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
        在写入 watch_signal_event 的“首次 INSERT”时，按需补齐订单簿口径的验证结果与预测（TypeB）。

        目标：
        - 使用与 live trading 一致的 “按名义金额扫订单簿 buy/sell 可成交价” 口径，重算
          spread_log_short_over_long/raw_best_buy_high_sell_low，并调用 predict_bc 得到 pnl_hat_ob/win_prob_ob。
        - 只对“高概率存在价差”的信号做订单簿请求，避免每个 raw 都打订单簿。
        """
        try:
            if str(ins.get("signal_type") or "").strip().upper() != "B":
                return ins

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
                return ins
            ex1, ex2 = str(pair[0]), str(pair[1])
            if not ex1 or not ex2:
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
                    }
                )
            if not candidate_pairs_norm:
                candidate_pairs_norm = [
                    {
                        "pair": [ex1, ex2],
                        "spread": float(trigger.get("spread")) if trigger.get("spread") is not None else None,
                        "prices": trigger.get("prices") if isinstance(trigger.get("prices"), dict) else None,
                        "funding": trigger.get("funding") if isinstance(trigger.get("funding"), dict) else None,
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

            pnl_reg = meta_last.get("pnl_regression") or {}
            pred_240 = ((pnl_reg or {}).get("pred") or {}).get("240") if isinstance(pnl_reg, dict) else None
            if not isinstance(pred_240, dict):
                return ins
            pnl_hat_240 = pred_240.get("pnl_hat")
            win_prob_240 = pred_240.get("win_prob")
            try:
                pnl_hat_240_f = float(pnl_hat_240)
                win_prob_240_f = float(win_prob_240)
            except Exception:
                return ins

            # Wider-than-live threshold to decide whether to spend orderbook calls.
            if pnl_hat_240_f < 0.009 or win_prob_240_f < 0.85:
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

                allowed = _cfg.LIVE_TRADING_CONFIG.get("allowed_exchanges") or []
                allowed_set = {str(x).lower() for x in allowed if str(x).strip()}
                notional = float(_cfg.LIVE_TRADING_CONFIG.get("per_leg_notional_usdt") or 50.0)
                market_type = str(_cfg.LIVE_TRADING_CONFIG.get("orderbook_market_type") or "perp")
                topk_exchanges = int(_cfg.LIVE_TRADING_CONFIG.get("watchlist_event_topk_exchanges") or 5)
                topk_exchanges = max(2, min(10, topk_exchanges))
                live_pnl_threshold = float(_cfg.LIVE_TRADING_CONFIG.get("pnl_threshold") or 0.0)
                live_prob_threshold = float(_cfg.LIVE_TRADING_CONFIG.get("win_prob_threshold") or 0.0)
            except Exception:
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
            orderbooks: Dict[str, Any] = {}
            orderbook_ok: List[str] = []
            for ex in ranked_exchanges:
                ob = fetch_orderbook_prices(ex, symbol, market_type, notional=notional)
                orderbooks[ex] = ob
                if ob and not ob.get("error") and ob.get("buy") is not None and ob.get("sell") is not None:
                    orderbook_ok.append(ex)

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
        except Exception:
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
