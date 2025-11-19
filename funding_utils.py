from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

DEFAULT_FUNDING_INTERVALS = {
    # Major CEX perpetuals settle every 8 hours unless documented otherwise.
    'binance': 8.0,
    'okx': 8.0,
    'bybit': 8.0,
    'bitget': 8.0,
    'grvt': 8.0,          # SDK exposes funding_rate_8h*
    'lighter': 1.0,       # 官方面板标注每小时结算
    'hyperliquid': 1.0,   # 官方文档：Funding occurs hourly
}


def _to_float(value: Any) -> Optional[float]:
    if value in (None, '', False):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_datetime(value: Any) -> Optional[datetime]:
    """Best-effort conversion to timezone-aware UTC datetime."""
    if value in (None, '', False):
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    if isinstance(value, (int, float)):
        # Distinguish seconds vs milliseconds
        seconds = float(value)
        if seconds > 1e12:
            seconds = seconds / 1000.0
        elif seconds > 1e10:
            # already in milliseconds
            seconds = seconds / 1000.0
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None

        if text.isdigit():
            try:
                return _normalize_datetime(int(text))
            except ValueError:
                return None

        lowered = text.lower()
        # Accept RFC3339/ISO strings ending with Z
        if lowered.endswith('z'):
            lowered = lowered[:-1] + '+00:00'
        try:
            dt = datetime.fromisoformat(lowered)
        except ValueError:
            return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    return None


def normalize_next_funding_time(value: Any) -> Optional[str]:
    """Convert various timestamp formats into an ISO8601 UTC string."""
    dt = _normalize_datetime(value)
    if not dt:
        return None
    return dt.astimezone(timezone.utc).isoformat()


def _parse_interval_hint(hint: Any) -> Optional[float]:
    """Parse funding interval hints (hours, seconds, milliseconds)."""
    if hint in (None, '', False):
        return None

    if isinstance(hint, (int, float)):
        val = float(hint)
        # Milliseconds range (>= 10 minutes)
        if val >= 3_600_000:
            return val / 3_600_000.0
        # Seconds range (>= 10 minutes)
        if 600 <= val < 3_600_000:
            return val / 3600.0
        return val if val > 0 else None

    if isinstance(hint, str):
        text = hint.strip().lower()
        if not text:
            return None
        suffix_map = {
            'ms': 3_600_000.0,
            'millisecond': 3_600_000.0,
            'milliseconds': 3_600_000.0,
            's': 3600.0,
            'sec': 3600.0,
            'secs': 3600.0,
            'second': 3600.0,
            'seconds': 3600.0,
            'm': 60.0,
            'min': 60.0,
            'mins': 60.0,
            'minute': 60.0,
            'minutes': 60.0,
            'h': 1.0,
            'hr': 1.0,
            'hrs': 1.0,
            'hour': 1.0,
            'hours': 1.0,
        }
        for suffix, divisor in suffix_map.items():
            if text.endswith(suffix):
                try:
                    return float(text[:-len(suffix)].strip() or 0) / (divisor if divisor != 1.0 else 1.0)
                except ValueError:
                    return None

        if text.replace('.', '', 1).isdigit():
            try:
                return float(text)
            except ValueError:
                return None

    return None


def derive_funding_interval_hours(exchange: str, interval_hint: Any = None, *, fallback: bool = True) -> Optional[float]:
    """Return the funding interval (hours) using hint values or defaults."""
    parsed = _parse_interval_hint(interval_hint)
    if parsed is not None and parsed > 0:
        return parsed

    if not fallback:
        return None

    return DEFAULT_FUNDING_INTERVALS.get((exchange or '').lower())

