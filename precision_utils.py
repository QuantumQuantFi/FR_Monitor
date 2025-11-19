from decimal import Decimal, InvalidOperation, localcontext
from typing import Any, Optional


def _coerce_to_decimal(value: Any) -> Optional[Decimal]:
    """Attempt to parse value into a Decimal, handling percent strings."""
    if value in (None, '', False):
        return None

    percent = False
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()

    if not text:
        return None

    if text.endswith('%'):
        percent = True
        text = text[:-1].strip()

    if not text:
        return None

    # Remove common number formatting such as commas
    text = text.replace(',', '')

    try:
        with localcontext() as ctx:
            ctx.prec = max(28, ctx.prec)
            decimal_value = Decimal(text)
            if percent:
                decimal_value = decimal_value / Decimal('100')
            return decimal_value
    except (InvalidOperation, ValueError):
        return None


def normalize_funding_rate(raw_value: Any, assume_percent: bool = False) -> Optional[str]:
    """
    Convert a funding rate value into a high-precision decimal string without losing detail.
    Handles inputs that may already include percent signs (e.g., \"0.012%\") by converting
    them into decimal form (0.00012 in the example above).
    """
    decimal_value = _coerce_to_decimal(raw_value)
    if decimal_value is None:
        return None

    if assume_percent:
        decimal_value = decimal_value / Decimal('100')

    if decimal_value == 0:
        return '0'

    normalized = format(decimal_value.normalize(), 'f')
    if '.' in normalized:
        normalized = normalized.rstrip('0').rstrip('.')
    if normalized in ('', '-0'):
        normalized = '0'
    return normalized


def funding_rate_to_float(raw_value: Any) -> float:
    """Utility to convert normalized funding rate representations back to float."""
    normalized = normalize_funding_rate(raw_value)
    if normalized is None:
        return 0.0
    try:
        return float(normalized)
    except (TypeError, ValueError):
        return 0.0
