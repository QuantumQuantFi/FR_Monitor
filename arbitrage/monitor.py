"""Lightweight in-memory arbitrage signal detector."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from threading import Lock
from typing import Deque, DefaultDict, Dict, List, Optional, Tuple


@dataclass
class ArbitrageSignal:
    """Represents a detected cross-exchange arbitrage opportunity."""

    symbol: str
    short_exchange: str
    long_exchange: str
    spread_percent: float
    price_short: float
    price_long: float
    observed_at: datetime

    def to_dict(self) -> Dict[str, object]:
        data = asdict(self)
        # Convert datetime to ISO string for JSON serialization
        data["observed_at"] = self.observed_at.isoformat()
        return data


class ArbitrageMonitor:
    """Tracks spreads across exchanges and emits signals on sustained gaps."""

    def __init__(
        self,
        *,
        spread_threshold: float = 0.2,
        sample_window: int = 3,
        sample_interval_seconds: float = 3.0,
        cooldown_seconds: float = 30.0,
        recent_limit: int = 50,
    ) -> None:
        self.spread_threshold = spread_threshold
        self.sample_window = sample_window
        self.sample_interval_seconds = sample_interval_seconds
        self.cooldown_seconds = cooldown_seconds
        self._recent_limit = recent_limit

        self._spread_windows: DefaultDict[Tuple[str, str, str], Deque[Tuple[datetime, float]]] = defaultdict(
            lambda: deque(maxlen=self.sample_window)
        )
        self._last_sample_time: Dict[Tuple[str, str, str], datetime] = {}
        self._last_signal_time: Dict[Tuple[str, str, str], datetime] = {}
        self._recent_signals: Deque[ArbitrageSignal] = deque(maxlen=self._recent_limit)
        self._lock = Lock()

    def update(
        self,
        symbol: str,
        futures_prices: Dict[str, float],
        *,
        observed_at: Optional[datetime] = None,
    ) -> List[ArbitrageSignal]:
        """Consume latest futures prices and return any new arbitrage signals."""
        if observed_at is None:
            observed_at = datetime.now(timezone.utc)

        exchanges = list(futures_prices.items())
        signals: List[ArbitrageSignal] = []

        for idx in range(len(exchanges)):
            ex_a, price_a = exchanges[idx]
            if price_a is None or price_a <= 0:
                continue
            for jdx in range(idx + 1, len(exchanges)):
                ex_b, price_b = exchanges[jdx]
                if price_b is None or price_b <= 0:
                    continue

                if price_a >= price_b:
                    short_exchange, short_price = ex_a, price_a
                    long_exchange, long_price = ex_b, price_b
                else:
                    short_exchange, short_price = ex_b, price_b
                    long_exchange, long_price = ex_a, price_a

                spread_percent = ((short_price - long_price) / long_price) * 100
                if spread_percent <= 0:
                    continue

                pair_key = (symbol, short_exchange, long_exchange)

                window = self._spread_windows[pair_key]
                last_sample = self._last_sample_time.get(pair_key)
                if last_sample:
                    delta = (observed_at - last_sample).total_seconds()
                    if delta <= 0:
                        # Ignore out-of-order or duplicate timestamps.
                        continue
                    if delta >= self.sample_interval_seconds:
                        # Gap too large; reset accumulated window so only closely spaced
                        # samples (< sample_interval_seconds) contribute to a signal.
                        window.clear()

                self._last_sample_time[pair_key] = observed_at

                window.append((observed_at, spread_percent))

                if len(window) < self.sample_window:
                    continue

                if not all(entry[1] >= self.spread_threshold for entry in window):
                    continue

                last_signal_time = self._last_signal_time.get(pair_key)
                if last_signal_time and (observed_at - last_signal_time).total_seconds() < self.cooldown_seconds:
                    continue

                signal = ArbitrageSignal(
                    symbol=symbol,
                    short_exchange=short_exchange,
                    long_exchange=long_exchange,
                    spread_percent=spread_percent,
                    price_short=short_price,
                    price_long=long_price,
                    observed_at=observed_at,
                )
                self._record_signal(signal)
                self._last_signal_time[pair_key] = observed_at
                signals.append(signal)

        return signals

    def _record_signal(self, signal: ArbitrageSignal) -> None:
        with self._lock:
            self._recent_signals.appendleft(signal)

    def get_recent_signals(self) -> List[Dict[str, object]]:
        with self._lock:
            return [signal.to_dict() for signal in self._recent_signals]
