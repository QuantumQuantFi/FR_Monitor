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


class WatchlistManager:
    """Maintain a Binance-only watchlist based on funding spikes."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        cfg = config or WATCHLIST_CONFIG
        self.funding_abs_threshold = float(cfg.get('funding_abs_threshold', 0.003))
        self.lookback = timedelta(hours=float(cfg.get('lookback_hours', 2)))
        self.refresh_seconds = float(cfg.get('refresh_seconds', 150))

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
                )
                self._entries[symbol] = entry

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            entries = list(self._entries.values())

        # Sort: active first, then by max_abs_funding desc, then symbol
        entries_sorted = sorted(
            entries,
            key=lambda e: (e.status != 'active', -(e.max_abs_funding or 0), e.symbol),
        )
        payload = {
            'summary': {
                'active_count': sum(1 for e in entries_sorted if e.status == 'active'),
                'total_tracked': len(entries_sorted),
                'threshold': self.funding_abs_threshold,
                'lookback_hours': self.lookback.total_seconds() / 3600.0,
                'refresh_seconds': self.refresh_seconds,
            },
            'entries': [e.__dict__ for e in entries_sorted],
            'timestamp': _iso(_utcnow()),
        }
        return payload

    def active_symbols(self) -> List[str]:
        with self._lock:
            return [e.symbol for e in self._entries.values() if e.status == 'active']
