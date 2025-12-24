from __future__ import annotations

import logging
import math
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple
import sqlite3

from config import WATCHLIST_CONFIG, WATCHLIST_PG_CONFIG
from precision_utils import funding_rate_to_float
from watchlist_pg_writer import PgWriter, PgWriterConfig
from watchlist_metrics import compute_metrics_for_symbols, compute_metrics_for_legs
from watchlist_pnl_regression_model import predict_bc


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OSError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        # normalize trailing Z to +00:00
        if text.endswith('Z'):
            text = text[:-1] + '+00:00'
        try:
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            return None
    return None


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat()


@dataclass
class WatchlistEntry:
    symbol: str
    has_spot: bool
    has_perp: bool
    last_funding_rate: Optional[float]
    last_funding_time: Optional[str]
    funding_interval_hours: Optional[float]
    next_funding_time: Optional[str]
    max_abs_funding: float
    last_above_threshold_at: Optional[str]
    added_at: Optional[str]
    status: str
    removal_reason: Optional[str] = None
    updated_at: str = field(default_factory=lambda: _iso(_utcnow()))
    entry_type: str = "A"  # A: Binance funding spike; B: cross-exchange futures diff; C: spot vs futures diff
    trigger_details: Optional[Dict[str, Any]] = None
    # Type B/C: pnl regression prediction payload (see watchlist_pnl_regression_model.predict_bc)
    pnl_regression: Optional[Dict[str, Any]] = None


class WatchlistManager:
    """Maintain a Binance-only watchlist based on funding spikes."""

    def __init__(self, config: Optional[Dict[str, Any]] = None, pg_config: Optional[Dict[str, Any]] = None):
        cfg = config or WATCHLIST_CONFIG
        self.funding_abs_threshold = float(cfg.get('funding_abs_threshold', 0.003))
        self.lookback = timedelta(hours=float(cfg.get('lookback_hours', 2)))
        self.refresh_seconds = float(cfg.get('refresh_seconds', 150))
        self.db_path = cfg.get('db_path', 'market_data.db')
        # Type B/C 配置
        self.type_b_spread_threshold = float(cfg.get('type_b_spread_threshold', 0.01))
        self.type_b_funding_min = float(cfg.get('type_b_funding_min', -0.001))
        self.type_b_funding_max = float(cfg.get('type_b_funding_max', 0.001))
        self.type_b_funding_filter_mode = str(cfg.get('type_b_funding_filter_mode', 'range')).strip().lower()
        self.type_b_funding_net_cost_max = float(cfg.get('type_b_funding_net_cost_max', 0.001))
        self.type_b_funding_net_cost_horizon_hours = float(cfg.get('type_b_funding_net_cost_horizon_hours', 8))
        self.type_c_spread_threshold = float(cfg.get('type_c_spread_threshold', 0.01))
        self.type_c_funding_min = float(cfg.get('type_c_funding_min', -0.001))

        self._funding_history: Dict[str, Deque[Tuple[datetime, float]]] = {}
        self._funding_interval: Dict[str, Optional[float]] = {}
        self._next_funding_time: Dict[str, Optional[datetime]] = {}
        self._entries: Dict[str, WatchlistEntry] = {}
        self._lock = threading.Lock()
        self.logger = logging.getLogger('watchlist')
        self.logger.setLevel(logging.INFO)
        self._preloaded = False
        # PG writer (optional)
        pg_cfg_dict = pg_config or WATCHLIST_PG_CONFIG
        self.pg_writer: Optional[PgWriter] = None
        try:
            self.pg_writer = PgWriter(
                PgWriterConfig(
                    dsn=str(pg_cfg_dict.get('dsn')),
                    enabled=bool(pg_cfg_dict.get('enabled')),
                    batch_size=int(pg_cfg_dict.get('batch_size', 500)),
                    flush_seconds=float(pg_cfg_dict.get('flush_seconds', 5.0)),
                    consecutive_required=int(pg_cfg_dict.get('consecutive_required', 2)),
                    cooldown_minutes=int(pg_cfg_dict.get('cooldown_minutes', 3)),
                    enable_event_merge=bool(pg_cfg_dict.get('enable_event_merge', False)),
                    orderbook_validation_on_write=bool(pg_cfg_dict.get('orderbook_validation_on_write', False)),
                )
            )
        except Exception as exc:  # pragma: no cover - optional path
            self.logger.warning("pg writer init failed: %s", exc)
            self.pg_writer = None
        if self.pg_writer:
            self.pg_writer.start()
        self._live_trading_kick: Optional[Callable[[str], None]] = None

    def set_live_trading_kick(self, kick: Callable[[str], None]) -> None:
        """Register a kick callback so watchlist PG event insert can wake live trading immediately."""
        self._live_trading_kick = kick
        if not self.pg_writer:
            return

        def _on_event_written(event_ids: List[int]) -> None:
            if not self._live_trading_kick:
                return
            # Collapse burst inserts into a single wake-up (thread-safe event on the other side).
            latest = event_ids[-1] if event_ids else None
            reason = f"watchlist_event_inserted count={len(event_ids)} latest={latest}"
            try:
                self._live_trading_kick(reason)
            except Exception:
                pass

        self.pg_writer.on_event_written = _on_event_written

    def _type_b_funding_ok(
        self,
        *,
        exch_data: Dict[str, Dict[str, Any]],
        a_ex: str,
        a_price: float,
        a_fr: float,
        b_ex: str,
        b_price: float,
        b_fr: float,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Type B 资金费过滤。

        - range：要求两腿 funding_rate 同时落在 [min, max]
        - net_cost：按「高价做空/低价做多」方向计算净资金费（按小时归一并映射到 horizon），仅限制“亏损”不超过阈值
        """
        mode = (self.type_b_funding_filter_mode or "range").lower()
        if mode == "range":
            ok = (self.type_b_funding_min <= a_fr <= self.type_b_funding_max) and (self.type_b_funding_min <= b_fr <= self.type_b_funding_max)
            return ok, {"mode": "range", "ok": ok}

        if mode != "net_cost":
            ok = (self.type_b_funding_min <= a_fr <= self.type_b_funding_max) and (self.type_b_funding_min <= b_fr <= self.type_b_funding_max)
            return ok, {"mode": mode, "ok": ok, "fallback": "range"}

        if a_price == b_price:
            return False, {"mode": "net_cost", "ok": False, "reason": "equal_price"}

        short_ex = a_ex if a_price > b_price else b_ex
        long_ex = b_ex if short_ex == a_ex else a_ex
        short_fr = a_fr if short_ex == a_ex else b_fr
        long_fr = b_fr if long_ex == b_ex else a_fr
        short_iv = (exch_data.get(short_ex) or {}).get("funding_interval_hours")
        long_iv = (exch_data.get(long_ex) or {}).get("funding_interval_hours")
        try:
            short_iv_h = float(short_iv) if short_iv else None
            long_iv_h = float(long_iv) if long_iv else None
        except Exception:
            short_iv_h = None
            long_iv_h = None
        if not short_iv_h or not long_iv_h or short_iv_h <= 0 or long_iv_h <= 0:
            return (
                False,
                {
                    "mode": "net_cost",
                    "ok": False,
                    "reason": "missing_funding_interval",
                    "short_ex": short_ex,
                    "long_ex": long_ex,
                },
            )

        horizon_h = float(self.type_b_funding_net_cost_horizon_hours or 8.0)
        # funding_rate 通常是“每 interval”的比例；先按小时归一。
        net_per_hour = (float(short_fr) / short_iv_h) - (float(long_fr) / long_iv_h)
        net_over_horizon = net_per_hour * horizon_h
        net_loss = max(0.0, -net_over_horizon)
        ok = net_loss <= float(self.type_b_funding_net_cost_max or 0.0)
        return (
            ok,
            {
                "mode": "net_cost",
                "ok": ok,
                "horizon_hours": horizon_h,
                "net_over_horizon": net_over_horizon,
                "net_loss": net_loss,
                "short_ex": short_ex,
                "long_ex": long_ex,
                "short_fr": short_fr,
                "long_fr": long_fr,
                "short_interval_hours": short_iv_h,
                "long_interval_hours": long_iv_h,
            },
        )

    def _prune_history(self, symbol: str, now: datetime) -> None:
        interval_hours = self._funding_interval.get(symbol)
        dynamic_window = self.lookback
        if interval_hours and interval_hours > 0:
            dynamic_window = max(dynamic_window, timedelta(hours=interval_hours * 1.25 + 0.5))
        window_start = now - dynamic_window
        history = self._funding_history.get(symbol)
        if not history:
            return
        while history and history[0][0] < window_start:
            history.popleft()
        if not history:
            self._funding_history.pop(symbol, None)
            self._funding_interval.pop(symbol, None)
            self._next_funding_time.pop(symbol, None)

    def _append_event(
        self,
        symbol: str,
        funding_rate: float,
        ts: datetime,
        *,
        interval_hours: Optional[float] = None,
        next_funding_time: Optional[datetime] = None,
    ) -> None:
        if symbol not in self._funding_history:
            self._funding_history[symbol] = deque()
        self._funding_history[symbol].append((ts, funding_rate))
        if interval_hours is not None:
            self._funding_interval[symbol] = interval_hours
        if next_funding_time is not None:
            self._next_funding_time[symbol] = next_funding_time

    def preload_from_database(self, db_path: str, *, max_rows: int = 500000) -> int:
        """
        Seed funding history from recent DB records to avoid empty window after restart.
        Only pulls Binance rows within the lookback window and with both spot & futures prices.
        """
        if self._preloaded:
            return 0
        try:
            conn = sqlite3.connect(db_path, timeout=15.0)
            cur = conn.cursor()
            query = """
                SELECT symbol, funding_rate, timestamp, funding_interval_hours, next_funding_time
                FROM price_data
                WHERE exchange='binance'
                  AND funding_rate IS NOT NULL
                  AND ABS(funding_rate) > 0
                  AND spot_price > 0
                  AND futures_price > 0
                  AND timestamp >= datetime('now', ?)
                ORDER BY timestamp DESC
                LIMIT ?
            """
            # sqlite datetime offsets need a string like '-2 hours'
            hours_offset = f"-{int(self.lookback.total_seconds() // 3600)} hours"
            rows = cur.execute(query, (hours_offset, max_rows)).fetchall()
        except Exception as exc:
            self.logger.warning("watchlist preload failed: %s", exc)
            return 0

        now = _utcnow()
        cutoff = now - self.lookback
        added = 0
        for symbol, fr, ts, interval_hours, next_ft in rows:
            if fr is None:
                continue
            ts_dt = _parse_timestamp(ts) or now
            if ts_dt < cutoff:
                continue
            try:
                fr_val = float(fr)
            except (TypeError, ValueError):
                continue
            self._append_event(
                symbol,
                fr_val,
                ts_dt,
                interval_hours=float(interval_hours) if interval_hours not in (None, '') else None,
                next_funding_time=_parse_timestamp(next_ft),
            )
            added += 1

        self._preloaded = True
        if added:
            self.logger.info("watchlist preloaded %s funding points from DB", added)
        else:
            self.logger.info("watchlist preload finished with no recent rows")
        return added

    def refresh(
        self,
        all_data: Dict[str, Any],
        exchange_symbols: Dict[str, Dict[str, List[str]]],
        orderbook_snapshot: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Ingest latest funding ticks and rebuild watchlist entries."""
        now = _utcnow()
        spot_set = set(exchange_symbols.get('binance', {}).get('spot') or [])
        perp_set = set(exchange_symbols.get('binance', {}).get('futures') or [])

        binance_payload = all_data.get('binance', {})

        # Step 1: ingest funding updates
        for symbol, payload in binance_payload.items():
            futures = payload.get('futures') or {}
            funding_raw = futures.get('funding_rate')
            if funding_raw is None:
                continue
            funding_value = funding_rate_to_float(funding_raw)
            ts = _parse_timestamp(futures.get('timestamp')) or now
            interval = futures.get('funding_interval_hours')
            try:
                interval_val = float(interval) if interval not in (None, '') else None
            except (TypeError, ValueError):
                interval_val = None
            next_ft = _parse_timestamp(futures.get('next_funding_time'))
            self._append_event(symbol, funding_value, ts, interval_hours=interval_val, next_funding_time=next_ft)

        # Step 2: prune history and rebuild entries
        candidates = set(self._funding_history.keys())

        with self._lock:
            for symbol in list(candidates):
                self._prune_history(symbol, now)

            for symbol in set(self._funding_history.keys()) | candidates:
                history = self._funding_history.get(symbol, deque())
                if not history:
                    self._entries.pop(symbol, None)
                    continue

                has_spot = symbol in spot_set
                # 发生过资金费率事件即可视为存在合约，防止因接口受限导致 futures 列表为空
                has_perp = (symbol in perp_set) or bool(history)
                last_ts, last_rate = history[-1]
                max_abs = max(abs(rate) for _, rate in history) if history else 0.0
                last_above_ts = None
                for ts, rate in reversed(history):
                    if abs(rate) >= self.funding_abs_threshold:
                        last_above_ts = ts
                        break

                is_active = has_spot and has_perp and last_above_ts is not None
                previous = self._entries.get(symbol)
                added_at = previous.added_at if previous and previous.status == 'active' else None
                if is_active and not added_at:
                    added_at = _iso(now)

                removal_reason = None
                if not is_active:
                    if not (has_spot and has_perp):
                        removal_reason = 'missing_market'
                    else:
                        removal_reason = 'stale_funding'

                entry = WatchlistEntry(
                    symbol=symbol,
                    has_spot=has_spot,
                    has_perp=has_perp,
                    last_funding_rate=last_rate,
                    last_funding_time=_iso(last_ts),
                    funding_interval_hours=self._funding_interval.get(symbol),
                    next_funding_time=_iso(self._next_funding_time.get(symbol)),
                    max_abs_funding=max_abs,
                    last_above_threshold_at=_iso(last_above_ts),
                    added_at=added_at if is_active else None,
                    status='active' if is_active else 'inactive',
                    removal_reason=removal_reason,
                    updated_at=_iso(now),
                    entry_type='A',
                )
                self._entries[symbol] = entry

        # Type B/C：跨交易所价差与资金费过滤
        market_snapshots: Dict[str, Dict[str, Dict[str, Any]]] = {}
        def _to_price(val: Any) -> Optional[float]:
            try:
                f = float(val)
                return f if f > 0 else None
            except (TypeError, ValueError):
                return None
        for exch, sym_map in (all_data or {}).items():
            if not isinstance(sym_map, dict):
                continue
            for symbol, payload in sym_map.items():
                futures = payload.get('futures') or {}
                spot = payload.get('spot') or {}
                fut_price = _to_price(futures.get('price') or futures.get('last_price'))
                spot_price = _to_price(spot.get('price'))
                funding = futures.get('funding_rate')
                interval_raw = futures.get('funding_interval_hours')
                try:
                    interval_val = float(interval_raw) if interval_raw not in (None, '') else None
                except (TypeError, ValueError):
                    interval_val = None
                next_ft_val = _parse_timestamp(futures.get('next_funding_time'))
                if (
                    fut_price is None
                    and spot_price is None
                    and funding is None
                    and interval_val is None
                    and next_ft_val is None
                ):
                    continue
                if symbol not in market_snapshots:
                    market_snapshots[symbol] = {}
                market_snapshots[symbol][exch] = {
                    'futures_price': fut_price,
                    'funding_rate': funding_rate_to_float(funding) if funding is not None else None,
                    'funding_interval_hours': interval_val,
                    'next_funding_time': next_ft_val,
                    'spot_price': spot_price,
                }

        new_entries: Dict[str, WatchlistEntry] = dict(self._entries)

        def _set_entry(symbol: str, entry: WatchlistEntry) -> None:
            # A 优先，其次 B，再到 C；仅当已有 entry 处于 active 且优先级更高时跳过
            existing = new_entries.get(symbol)
            priority = {'A': 2, 'B': 1, 'C': 0}
            if existing and existing.status == 'active' and priority.get(existing.entry_type, 0) >= priority.get(entry.entry_type, 0):
                return
            new_entries[symbol] = entry

        now_iso = _iso(now)
        for symbol, exch_data in market_snapshots.items():
            if symbol in new_entries and new_entries[symbol].entry_type == 'A' and new_entries[symbol].status == 'active':
                continue  # A 触发优先，不再尝试 B/C

            futures_list = []
            spot_list = []
            for exch, vals in exch_data.items():
                if vals.get('futures_price'):
                    futures_list.append((exch, vals['futures_price'], vals.get('funding_rate')))
                if vals.get('spot_price'):
                    spot_list.append((exch, vals['spot_price']))

            # Type B：两家永续价差 > 阈值，且资金费在区间内
            best_b = None
            candidate_pairs: List[Dict[str, Any]] = []
            for i in range(len(futures_list)):
                for j in range(i + 1, len(futures_list)):
                    a_ex, a_price, a_fr = futures_list[i]
                    b_ex, b_price, b_fr = futures_list[j]
                    if a_fr is None or b_fr is None:
                        continue
                    funding_ok, funding_info = self._type_b_funding_ok(
                        exch_data=exch_data,
                        a_ex=a_ex,
                        a_price=a_price,
                        a_fr=a_fr,
                        b_ex=b_ex,
                        b_price=b_price,
                        b_fr=b_fr,
                    )
                    if not funding_ok:
                        continue
                    base = min(a_price, b_price)
                    if not base:
                        continue
                    diff = abs(a_price - b_price) / base
                    candidate_pairs.append(
                        {
                            "pair": [a_ex, b_ex],
                            "spread": diff,
                            "prices": {a_ex: a_price, b_ex: b_price},
                            "funding": {a_ex: a_fr, b_ex: b_fr},
                            "funding_filter": funding_info,
                        }
                    )

                    if diff > self.type_b_spread_threshold and (best_b is None or diff > best_b["spread"]):
                        best_b = {
                            "pair": [a_ex, b_ex],
                            "spread": diff,
                            "prices": {a_ex: a_price, b_ex: b_price},
                            "funding": {a_ex: a_fr, b_ex: b_fr},
                            "funding_filter": funding_info,
                        }

            if best_b:
                candidate_pairs_sorted = sorted(candidate_pairs, key=lambda x: float(x.get("spread") or 0.0), reverse=True)
                # Provide extra context for downstream orderbook validation (best-effort; does not change trading logic).
                # Keep payload bounded: top-N candidates only.
                top_n = 10
                best_b["candidate_pairs"] = candidate_pairs_sorted[:top_n]
                candidate_exchanges: List[str] = []
                for c in best_b["candidate_pairs"]:
                    p = c.get("pair") or []
                    if isinstance(p, list) and len(p) == 2:
                        for ex in p:
                            exs = str(ex)
                            if exs and exs not in candidate_exchanges:
                                candidate_exchanges.append(exs)
                best_b["candidate_exchanges"] = candidate_exchanges

                intervals = []
                next_fts = []
                for ex_name in best_b.get('pair') or []:
                    snap = exch_data.get(ex_name) or {}
                    iv = snap.get('funding_interval_hours')
                    if iv:
                        intervals.append(float(iv))
                    nft = snap.get('next_funding_time')
                    if isinstance(nft, datetime):
                        next_fts.append(nft)
                entry = WatchlistEntry(
                    symbol=symbol,
                    has_spot=bool(spot_list),
                    has_perp=True,
                    last_funding_rate=max(abs(v) for v in best_b['funding'].values() if v is not None),
                    last_funding_time=None,
                    funding_interval_hours=int(round(min(intervals))) if intervals else None,
                    next_funding_time=_iso(min(next_fts)) if next_fts else None,
                    max_abs_funding=max(abs(v) for v in best_b['funding'].values() if v is not None),
                    last_above_threshold_at=now_iso,
                    added_at=now_iso,
                    status='active',
                    removal_reason=None,
                    updated_at=now_iso,
                    entry_type='B',
                    trigger_details=best_b,
                )
                _set_entry(symbol, entry)
                continue

            # Type C：现货低于任一期货，价差 > 阈值，且两家期货资金费均大于下限
            if len(futures_list) >= 2 and spot_list:
                # 选 funding 足够的期货
                futures_ok = [(ex, p, fr) for ex, p, fr in futures_list if fr is not None and fr >= self.type_c_funding_min]
                if len(futures_ok) >= 2:
                    # 取最高期货价与最低现货价
                    futures_ok.sort(key=lambda x: x[1], reverse=True)
                    spots_sorted = sorted(spot_list, key=lambda x: x[1])
                    fut_ex, fut_price, fut_fr = futures_ok[0]
                    spot_ex, spot_price = spots_sorted[0]
                    base = spot_price
                    if base:
                        diff = (fut_price - spot_price) / base
                        if diff > self.type_c_spread_threshold:
                            # 再确认另一家期货资金费也满足
                            second_fr = futures_ok[1][2]
                            fut_snap = exch_data.get(fut_ex) or {}
                            fut_interval = fut_snap.get('funding_interval_hours')
                            fut_next_ft = fut_snap.get('next_funding_time')
                            entry = WatchlistEntry(
                                symbol=symbol,
                                has_spot=True,
                                has_perp=True,
                                last_funding_rate=fut_fr,
                                last_funding_time=None,
                                funding_interval_hours=int(round(float(fut_interval))) if fut_interval else None,
                                next_funding_time=_iso(fut_next_ft) if isinstance(fut_next_ft, datetime) else None,
                                max_abs_funding=max(abs(fut_fr or 0), abs(second_fr or 0)),
                                last_above_threshold_at=now_iso,
                                added_at=now_iso,
                                status='active',
                                removal_reason=None,
                                updated_at=now_iso,
                                entry_type='C',
                                trigger_details={
                                    'spot_exchange': spot_ex,
                                    'futures_exchange': fut_ex,
                                    'spot_price': spot_price,
                                    'futures_price': fut_price,
                                    'spread': diff,
                                    'funding': {
                                        fut_ex: fut_fr,
                                        futures_ok[1][0]: second_fr,
                                    },
                                },
                            )
                            _set_entry(symbol, entry)

        self._entries = new_entries
        self._emit_pg_raw(new_entries, market_snapshots, now, orderbook_snapshot or {})

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            entries = list(self._entries.values())

        # Sort: active first, then by max_abs_funding desc, then symbol
        entries_sorted = sorted(
            entries,
            key=lambda e: (e.status != 'active', -(e.max_abs_funding or 0), e.symbol),
        )
        type_counts: Dict[str, int] = {}
        for e in entries_sorted:
            type_counts[e.entry_type] = type_counts.get(e.entry_type, 0) + 1
        payload = {
            'summary': {
                'active_count': sum(1 for e in entries_sorted if e.status == 'active'),
                'total_tracked': len(entries_sorted),
                'threshold': self.funding_abs_threshold,
                'lookback_hours': self.lookback.total_seconds() / 3600.0,
                'refresh_seconds': self.refresh_seconds,
                'type_counts': type_counts,
            },
            'entries': [e.__dict__ for e in entries_sorted],
            'timestamp': _iso(_utcnow()),
        }
        return payload

    def active_symbols(self) -> List[str]:
        with self._lock:
            return [e.symbol for e in self._entries.values() if e.status == 'active']

    def _emit_pg_raw(
        self,
        entries: Dict[str, WatchlistEntry],
        market_snapshots: Dict[str, Dict[str, Dict[str, Any]]],
        now: datetime,
        orderbook_snapshot: Dict[str, Any],
    ) -> None:
        if not self.pg_writer or not self.pg_writer.config.enabled:
            return
        # 仅对当前 watchlist 符号计算指标，避免全市场扫表
        active_symbols = [e.symbol for e in entries.values() if e.status == 'active']
        metrics_map: Dict[str, Dict[str, Any]] = {}
        if active_symbols:
            try:
                metrics_map = compute_metrics_for_symbols(self.db_path, active_symbols, snapshot=market_snapshots)
            except Exception as exc:  # pragma: no cover - 运行时保护
                self.logger.warning("compute_metrics_for_symbols failed: %s", exc, exc_info=True)
                metrics_map = {}

        ob_data = orderbook_snapshot or {}
        ob_orderbook = ob_data.get('orderbook') or {}
        ob_cross = ob_data.get('cross_spreads') or {}
        ob_ts = ob_data.get('timestamp')
        ob_stale = ob_data.get('stale_reason')

        def _compact_leg(leg: Dict[str, Any]) -> Dict[str, Any]:
            price = leg.get('price') or {}
            return {
                'exchange': leg.get('exchange'),
                'type': leg.get('market_type'),
                'buy': price.get('buy'),
                'sell': price.get('sell'),
                'mid': price.get('mid'),
                'error': leg.get('error') or price.get('error'),
            }

        def _compact_pair(pair: Dict[str, Any]) -> Dict[str, Any]:
            return {
                'type': pair.get('type'),
                'exch_a': pair.get('exchange_a'),
                'exch_b': pair.get('exchange_b'),
                'forward': pair.get('forward'),
                'reverse': pair.get('reverse'),
            }

        rows: List[Dict[str, Any]] = []
        for entry in entries.values():
            if entry.status != 'active':
                continue
            exch = 'binance' if entry.entry_type == 'A' else 'multi'
            meta: Dict[str, Any] = {}
            if entry.trigger_details:
                meta['trigger_details'] = entry.trigger_details
            if entry.symbol in market_snapshots:
                snaps_meta: Dict[str, Any] = {}
                for ex_name, snap in (market_snapshots.get(entry.symbol) or {}).items():
                    if not isinstance(snap, dict):
                        continue
                    snap_copy = dict(snap)
                    nft = snap_copy.get('next_funding_time')
                    if isinstance(nft, datetime):
                        snap_copy['next_funding_time'] = _iso(nft)
                    snaps_meta[ex_name] = snap_copy
                meta['snapshots'] = snaps_meta
            ob_entry = ob_orderbook.get(entry.symbol) if isinstance(ob_orderbook, dict) else None
            cs_entry = ob_cross.get(entry.symbol) if isinstance(ob_cross, dict) else None
            orderbook_meta: Dict[str, Any] = {}
            if ob_entry:
                orderbook_meta['legs'] = [_compact_leg(l) for l in ob_entry.get('legs', [])]
                orderbook_meta['forward_spread'] = (ob_entry.get('forward') or {}).get('spread')
                orderbook_meta['reverse_spread'] = (ob_entry.get('reverse') or {}).get('spread')
            if cs_entry:
                pairs = cs_entry.get('pairs') or []
                orderbook_meta['cross_pairs'] = [_compact_pair(p) for p in pairs[:5]]
            if orderbook_meta:
                orderbook_meta['ts'] = ob_ts
                orderbook_meta['stale_reason'] = ob_stale
                meta['orderbook'] = orderbook_meta
            snaps = market_snapshots.get(entry.symbol) or {}
            futures_prices = [v.get('futures_price') for v in snaps.values() if v.get('futures_price')]
            funding_vals = [v.get('funding_rate') for v in snaps.values() if v.get('funding_rate') is not None]
            best_buy_high_sell_low = None
            best_sell_high_buy_low = None
            funding_diff_max = None
            if len(futures_prices) >= 2:
                try:
                    max_p = max(fp for fp in futures_prices if fp is not None)
                    min_p = min(fp for fp in futures_prices if fp is not None)
                    if min_p:
                        best_buy_high_sell_low = (max_p - min_p) / min_p
                    if max_p:
                        best_sell_high_buy_low = (max_p - min_p) / max_p
                except ValueError:
                    pass
            if len(funding_vals) >= 2:
                funding_diff_max = max(funding_vals) - min(funding_vals)

            metrics = metrics_map.get(entry.symbol) or {}
            leg_a, leg_b = self._build_legs(entry, snaps, metrics)
            if entry.entry_type in ('B', 'C') and leg_a and leg_b:
                try:
                    metrics = compute_metrics_for_legs(self.db_path, leg_a, leg_b)
                except Exception as exc:  # pragma: no cover - runtime safeguard
                    self.logger.warning(
                        "compute_metrics_for_legs failed symbol=%s type=%s: %s",
                        entry.symbol,
                        entry.entry_type,
                        exc,
                    )
            row = {
                'ts': now,
                'exchange': exch,
                'symbol': entry.symbol,
                'signal_type': entry.entry_type,
                'type_class': entry.entry_type,
                'triggered': entry.status == 'active',
                'status': entry.status,
                'spread_rel': entry.trigger_details.get('spread') if entry.trigger_details else metrics.get('last_spread'),
                'funding_rate': entry.last_funding_rate,
                'funding_interval_hours': entry.funding_interval_hours,
                'next_funding_time': _parse_timestamp(entry.next_funding_time),
                'range_1h': metrics.get('range_short'),
                'range_12h': metrics.get('range_long'),
                'volatility': metrics.get('volatility'),
                'slope_3m': metrics.get('recent_slope'),
                'crossings_1h': metrics.get('crossings_1h'),
                'drift_ratio': metrics.get('drift_ratio'),
                'best_buy_high_sell_low': best_buy_high_sell_low,
                'best_sell_high_buy_low': best_sell_high_buy_low,
                'funding_diff_max': funding_diff_max,
                'spot_perp_volume_ratio': None,
                'oi_to_volume_ratio': None,
                'bid_ask_spread': None,
                'depth_imbalance': None,
                'volume_spike_zscore': None,
                'premium_index_diff': None,
                'leg_a_exchange': leg_a.get('exchange') if leg_a else None,
                'leg_a_symbol': leg_a.get('symbol') if leg_a else None,
                'leg_a_kind': leg_a.get('kind') if leg_a else None,
                'leg_a_price': leg_a.get('price') if leg_a else None,
                'leg_a_funding_rate': leg_a.get('funding_rate') if leg_a else None,
                'leg_a_next_funding_time': leg_a.get('next_funding_time') if leg_a else None,
                'leg_b_exchange': leg_b.get('exchange') if leg_b else None,
                'leg_b_symbol': leg_b.get('symbol') if leg_b else None,
                'leg_b_kind': leg_b.get('kind') if leg_b else None,
                'leg_b_price': leg_b.get('price') if leg_b else None,
                'leg_b_funding_rate': leg_b.get('funding_rate') if leg_b else None,
                'leg_b_next_funding_time': leg_b.get('next_funding_time') if leg_b else None,
                'legs': {'a': leg_a, 'b': leg_b} if leg_a or leg_b else None,
                'meta': (
                    {
                        **meta,
                        **({"factors": metrics.get("extra_factors")} if metrics.get("extra_factors") else {}),
                    }
                    if meta or metrics.get("extra_factors")
                    else None
                ),
            }
            # 为 Type B/C 事件补齐 pnl_regression.pred(240) 写入，供自动实盘/回溯统计读取。
            if entry.entry_type in ('B', 'C') and isinstance(row.get('meta'), dict):
                try:
                    meta_obj: Dict[str, Any] = row['meta']  # type: ignore[assignment]
                    factors = meta_obj.get('factors') or {}
                    if isinstance(factors, dict):
                        factors2 = dict(factors)
                        # pnl_regression 线性模型需要的 5 个关键 raw_* 因子：
                        # - raw_slope_3m / raw_drift_ratio / raw_crossings_1h 来自本地 metrics 计算
                        # - spread_log_short_over_long / raw_best_buy_high_sell_low 尽量用触发时刻的价格补齐
                        slope_3m = row.get('slope_3m')
                        drift_ratio = row.get('drift_ratio')
                        crossings_1h = row.get('crossings_1h')
                        if slope_3m is not None:
                            factors2.setdefault('raw_slope_3m', float(slope_3m))
                        if drift_ratio is not None:
                            factors2.setdefault('raw_drift_ratio', float(drift_ratio))
                        if crossings_1h is not None:
                            factors2.setdefault('raw_crossings_1h', float(crossings_1h))
                        td = entry.trigger_details or {}
                        pair = td.get('pair') or []
                        prices = td.get('prices') or {}
                        if isinstance(pair, list) and len(pair) == 2 and isinstance(prices, dict):
                            p1 = float(prices.get(pair[0]) or 0.0)
                            p2 = float(prices.get(pair[1]) or 0.0)
                            if p1 > 0 and p2 > 0:
                                high = max(p1, p2)
                                low = min(p1, p2)
                                factors2.setdefault('spread_log_short_over_long', float(math.log(high / low)))
                                if low:
                                    factors2.setdefault('raw_best_buy_high_sell_low', float((high - low) / low))
                        # 若 trigger_details 缺失价格（或结构变化），退化到 snapshot 的 best_buy_high_sell_low
                        # 以避免 factors 不齐导致 pnl_regression 计算失败。
                        if 'spread_log_short_over_long' not in factors2 or 'raw_best_buy_high_sell_low' not in factors2:
                            try:
                                fallback_spread = row.get('best_buy_high_sell_low')
                                if fallback_spread is not None and math.isfinite(float(fallback_spread)):
                                    fb = float(fallback_spread)
                                    factors2.setdefault('raw_best_buy_high_sell_low', fb)
                                    if fb > -0.9:
                                        factors2.setdefault('spread_log_short_over_long', float(math.log(1.0 + fb)))
                            except Exception:
                                pass
                        pred = predict_bc(signal_type=str(entry.entry_type), factors=factors2, horizons=(240,))
                        pred_240 = ((pred or {}).get('pred') or {}).get('240') if isinstance(pred, dict) else None
                        if (
                            isinstance(pred, dict)
                            and isinstance(pred_240, dict)
                            and pred_240.get('pnl_hat') is not None
                            and pred_240.get('win_prob') is not None
                        ):
                            meta_obj['pnl_regression'] = pred
                            meta_obj['factors'] = factors2
                            # Also keep a lightweight copy on the in-memory entry for watchlist UI display.
                            # Do NOT attach factors2 here to keep payload small.
                            entry.pnl_regression = pred
                except Exception:
                    pass
            rows.append(row)
        if rows:
            try:
                self.pg_writer.flush()  # flush pending before new burst
                for r in rows:
                    self.pg_writer.enqueue_raw(r)
            except Exception as exc:  # pragma: no cover - best-effort
                self.logger.warning("enqueue pg raw failed: %s", exc)

    def _build_legs(self, entry: WatchlistEntry, snaps: Dict[str, Dict[str, Any]], metrics: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """
        构造双腿信息：
        - Type A：同所 spot/perp
        - Type B：两条 perp（跨所高/低）
        - Type C：spot vs perp（跨所）
        """
        leg_a = leg_b = None
        td = entry.trigger_details or {}
        if entry.entry_type == 'A':
            # 同所（Binance）现货/永续
            spot_price = None
            perp_price = None
            funding = None
            snap = snaps.get('binance') or {}
            spot_price = snap.get('spot_price')
            perp_price = snap.get('futures_price')
            funding = snap.get('funding_rate')
            leg_a = {
                'exchange': 'binance',
                'symbol': entry.symbol,
                'kind': 'spot',
                'price': spot_price or metrics.get('last_spot_price'),
                'funding_rate': 0.0,
                'next_funding_time': None,
                'funding_interval_hours': None,
            }
            leg_b = {
                'exchange': 'binance',
                'symbol': entry.symbol,
                'kind': 'perp',
                'price': perp_price or metrics.get('last_futures_price'),
                'funding_rate': funding or entry.last_funding_rate,
                'next_funding_time': _parse_timestamp(entry.next_funding_time),
                'funding_interval_hours': entry.funding_interval_hours,
            }
        elif entry.entry_type == 'B':
            pair = td.get('pair') or []
            prices = td.get('prices') or {}
            funding = td.get('funding') or {}
            if len(pair) == 2:
                snap_a = snaps.get(pair[0]) or {}
                snap_b = snaps.get(pair[1]) or {}
                leg_a = {
                    'exchange': pair[0],
                    'symbol': entry.symbol,
                    'kind': 'perp',
                    'price': prices.get(pair[0]),
                    'funding_rate': funding.get(pair[0]),
                    'next_funding_time': snap_a.get('next_funding_time'),
                    'funding_interval_hours': snap_a.get('funding_interval_hours'),
                }
                leg_b = {
                    'exchange': pair[1],
                    'symbol': entry.symbol,
                    'kind': 'perp',
                    'price': prices.get(pair[1]),
                    'funding_rate': funding.get(pair[1]),
                    'next_funding_time': snap_b.get('next_funding_time'),
                    'funding_interval_hours': snap_b.get('funding_interval_hours'),
                }
        elif entry.entry_type == 'C':
            spot_ex = td.get('spot_exchange')
            fut_ex = td.get('futures_exchange')
            if spot_ex and fut_ex:
                fut_snap = snaps.get(fut_ex) or {}
                leg_a = {
                    'exchange': spot_ex,
                    'symbol': entry.symbol,
                    'kind': 'spot',
                    'price': td.get('spot_price'),
                    'funding_rate': 0.0,
                    'next_funding_time': None,
                    'funding_interval_hours': None,
                }
                leg_b = {
                    'exchange': fut_ex,
                    'symbol': entry.symbol,
                    'kind': 'perp',
                    'price': td.get('futures_price'),
                    'funding_rate': (td.get('funding') or {}).get(fut_ex),
                    'next_funding_time': fut_snap.get('next_funding_time'),
                    'funding_interval_hours': fut_snap.get('funding_interval_hours'),
                }
        return leg_a, leg_b
