from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple
import sqlite3

from config import WATCHLIST_CONFIG
from precision_utils import funding_rate_to_float


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


class WatchlistManager:
    """Maintain a Binance-only watchlist based on funding spikes."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        cfg = config or WATCHLIST_CONFIG
        self.funding_abs_threshold = float(cfg.get('funding_abs_threshold', 0.003))
        self.lookback = timedelta(hours=float(cfg.get('lookback_hours', 2)))
        self.refresh_seconds = float(cfg.get('refresh_seconds', 150))
        # Type B/C 配置
        self.type_b_spread_threshold = float(cfg.get('type_b_spread_threshold', 0.01))
        self.type_b_funding_min = float(cfg.get('type_b_funding_min', -0.001))
        self.type_b_funding_max = float(cfg.get('type_b_funding_max', 0.001))
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

    def refresh(self, all_data: Dict[str, Any], exchange_symbols: Dict[str, Dict[str, List[str]]]) -> None:
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
                if symbol not in market_snapshots:
                    market_snapshots[symbol] = {}
                market_snapshots[symbol][exch] = {
                    'futures_price': fut_price,
                    'funding_rate': funding_rate_to_float(funding) if funding is not None else None,
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
            for i in range(len(futures_list)):
                for j in range(i + 1, len(futures_list)):
                    a_ex, a_price, a_fr = futures_list[i]
                    b_ex, b_price, b_fr = futures_list[j]
                    if a_fr is None or b_fr is None:
                        continue
                    if not (self.type_b_funding_min <= a_fr <= self.type_b_funding_max and self.type_b_funding_min <= b_fr <= self.type_b_funding_max):
                        continue
                    base = min(a_price, b_price)
                    if not base:
                        continue
                    diff = abs(a_price - b_price) / base
                    if diff > self.type_b_spread_threshold:
                        if best_b is None or diff > best_b['spread']:
                            best_b = {
                                'pair': [a_ex, b_ex],
                                'spread': diff,
                                'prices': {a_ex: a_price, b_ex: b_price},
                                'funding': {a_ex: a_fr, b_ex: b_fr},
                            }

            if best_b:
                entry = WatchlistEntry(
                    symbol=symbol,
                    has_spot=bool(spot_list),
                    has_perp=True,
                    last_funding_rate=max(abs(v) for v in best_b['funding'].values() if v is not None),
                    last_funding_time=None,
                    funding_interval_hours=None,
                    next_funding_time=None,
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
                            entry = WatchlistEntry(
                                symbol=symbol,
                                has_spot=True,
                                has_perp=True,
                                last_funding_rate=fut_fr,
                                last_funding_time=None,
                                funding_interval_hours=None,
                                next_funding_time=None,
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
