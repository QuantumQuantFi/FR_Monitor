from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

# psycopg is not in requirements yet; keep import lazy and fail soft.
try:
    import psycopg
except Exception:  # pragma: no cover - optional dependency
    psycopg = None  # type: ignore


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

    def start(self) -> None:
        if not self.config.enabled:
            self.logger.info("PG writer disabled; skip start")
            return
        if psycopg is None:
            self.logger.error("psycopg not installed; cannot start PG writer")
            return
        if self._thread:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_loop, name="pg-writer", daemon=True)
        self._thread.start()
        self.logger.info("PG writer started (batch=%s, flush=%ss)", self.config.batch_size, self.config.flush_seconds)

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

        def leg_payload(legs: Optional[Dict[str, Any]], first: bool = False) -> Dict[str, Any]:
            if not legs:
                return {}
            a = legs.get("a") or {}
            b = legs.get("b") or {}
            return {
                "leg_a_exchange": a.get("exchange"),
                "leg_a_symbol": a.get("symbol"),
                "leg_a_kind": a.get("kind"),
                "leg_a_price_first": a.get("price") if first else None,
                "leg_a_price_last": a.get("price"),
                "leg_a_funding_rate_first": a.get("funding_rate") if first else None,
                "leg_a_funding_rate_last": a.get("funding_rate"),
                "leg_b_exchange": b.get("exchange"),
                "leg_b_symbol": b.get("symbol"),
                "leg_b_kind": b.get("kind"),
                "leg_b_price_first": b.get("price") if first else None,
                "leg_b_price_last": b.get("price"),
                "leg_b_funding_rate_first": b.get("funding_rate") if first else None,
                "leg_b_funding_rate_last": b.get("funding_rate"),
            }

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
                        "features_agg": self._agg_finalize(state["agg"]),
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
                payload = {
                    "exchange": ex,
                    "symbol": sym,
                    "signal_type": sig,
                    "start_ts": ts,
                    "end_ts": ts,
                    "duration_sec": 0,
                    "triggered_count": state["triggered_count"],
                    "features_agg": self._agg_finalize(state["agg"], legs=legs),
                }
                payload.update(leg_payload(legs, first=True))
                ops["insert"].append(payload)
            elif state["open"] and state["event_id"]:
                legs = self._last_legs.get(key)
                leg_fields = leg_payload(legs, first=False)
                ops["update"].append(
                    {
                        "event_id": state["event_id"],
                        "end_ts": ts,
                        "duration_sec": int((ts - (state["start_ts"] or ts)).total_seconds()),
                        "features_agg": self._agg_finalize(state["agg"], legs=self._last_legs.get(key)),
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

    def _agg_finalize(self, agg: Optional[Dict[str, Any]], legs: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
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
            if psycopg is not None:
                try:
                    from psycopg.types.json import Json  # type: ignore
                    if payload.get("features_agg") is not None:
                        payload["features_agg"] = Json(payload["features_agg"])
                except Exception:
                    pass
            return payload

        try:
            with psycopg.connect(self.config.dsn, autocommit=True) as conn:
                with conn.cursor() as cur:
                    for ins in inserts:
                        cur.execute(
                            """
                            INSERT INTO watchlist.watch_signal_event
                              (exchange, symbol, signal_type, start_ts, end_ts, duration_sec, triggered_count, status, features_agg,
                               leg_a_exchange, leg_a_symbol, leg_a_kind, leg_a_price_first, leg_a_price_last, leg_a_funding_rate_first, leg_a_funding_rate_last,
                               leg_b_exchange, leg_b_symbol, leg_b_kind, leg_b_price_first, leg_b_price_last, leg_b_funding_rate_first, leg_b_funding_rate_last)
                            VALUES (%(exchange)s, %(symbol)s, %(signal_type)s, %(start_ts)s, %(end_ts)s, %(duration_sec)s, %(triggered_count)s, 'open', %(features_agg)s,
                                    %(leg_a_exchange)s, %(leg_a_symbol)s, %(leg_a_kind)s, %(leg_a_price_first)s, %(leg_a_price_last)s, %(leg_a_funding_rate_first)s, %(leg_a_funding_rate_last)s,
                                    %(leg_b_exchange)s, %(leg_b_symbol)s, %(leg_b_kind)s, %(leg_b_price_first)s, %(leg_b_price_last)s, %(leg_b_funding_rate_first)s, %(leg_b_funding_rate_last)s)
                            RETURNING id
                            """,
                            _normalize_payload(ins),
                        )
                        event_id = cur.fetchone()[0]
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
                                   features_agg = COALESCE(%(features_agg)s, features_agg),
                                   leg_a_exchange = COALESCE(%(leg_a_exchange)s, leg_a_exchange),
                                   leg_a_symbol = COALESCE(%(leg_a_symbol)s, leg_a_symbol),
                                   leg_a_kind = COALESCE(%(leg_a_kind)s, leg_a_kind),
                                   leg_a_price_first = COALESCE(%(leg_a_price_first)s, leg_a_price_first),
                                   leg_a_price_last = COALESCE(%(leg_a_price_last)s, leg_a_price_last),
                                   leg_a_funding_rate_first = COALESCE(%(leg_a_funding_rate_first)s, leg_a_funding_rate_first),
                                   leg_a_funding_rate_last = COALESCE(%(leg_a_funding_rate_last)s, leg_a_funding_rate_last),
                                   leg_b_exchange = COALESCE(%(leg_b_exchange)s, leg_b_exchange),
                                   leg_b_symbol = COALESCE(%(leg_b_symbol)s, leg_b_symbol),
                                   leg_b_kind = COALESCE(%(leg_b_kind)s, leg_b_kind),
                                   leg_b_price_first = COALESCE(%(leg_b_price_first)s, leg_b_price_first),
                                   leg_b_price_last = COALESCE(%(leg_b_price_last)s, leg_b_price_last),
                                   leg_b_funding_rate_first = COALESCE(%(leg_b_funding_rate_first)s, leg_b_funding_rate_first),
                                   leg_b_funding_rate_last = COALESCE(%(leg_b_funding_rate_last)s, leg_b_funding_rate_last),
                                   status = CASE WHEN %(close)s THEN 'closed' ELSE status END
                             WHERE id = %(event_id)s
                            """,
                            _normalize_payload(upd),
                        )
            self.logger.info("event ops applied: %s inserts, %s updates", len(inserts), len(updates))
        except Exception as exc:
            self.logger.warning("apply event ops failed: %s", exc)

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
