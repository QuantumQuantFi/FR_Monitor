"""Utility helpers for submitting USDT-margined perpetual market orders.

Each function wires authentication, signing, and request payloads for a
specific exchange so callers can execute account queries or trades with
minimal boilerplate.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import socket
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any, Dict, List, Optional, Type, Union
from urllib.parse import urlencode

import requests

try:  # requests bundles urllib3, but allow user-installed urllib3 fallback
    import urllib3.util.connection as _urllib3_connection  # type: ignore
except Exception:  # pragma: no cover - optional dependency path
    _urllib3_connection = None

import config

CONFIG_PRIVATE = getattr(config, "config_private", None)
REQUEST_TIMEOUT = config.REST_CONNECTION_CONFIG.get("timeout", 10)
USER_AGENT = config.REST_CONNECTION_CONFIG.get("user_agent", "WLFI-Monitor/1.0")
LOGGER = logging.getLogger(__name__)


def _configure_ipv4_only() -> None:
    """Force outbound HTTP connections to resolve IPv4 addresses only."""
    if _urllib3_connection is None:
        return

    # Avoid repeatedly patching when the module is reloaded.
    if getattr(_configure_ipv4_only, "_patched", False):
        return

    def allowed_gai_family() -> int:
        return socket.AF_INET

    _configure_ipv4_only._patched = True  # type: ignore[attr-defined]
    _urllib3_connection.allowed_gai_family = allowed_gai_family  # type: ignore[attr-defined]


_configure_ipv4_only()


class TradeExecutionError(Exception):
    """Raised when an exchange rejects a trading request."""


def _json_or_error(response: requests.Response) -> Dict[str, Any]:
    try:
        return response.json()
    except ValueError as exc:
        raise TradeExecutionError(f"Non-JSON response: {response.text}") from exc


def _send_request(
    method: str,
    url: str,
    *,
    params: Optional[List[tuple[str, str]]] = None,
    data: Optional[str] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = REQUEST_TIMEOUT,
) -> requests.Response:
    """Perform an HTTP request and normalise network errors."""
    try:
        return requests.request(
            method=method,
            url=url,
            params=params,
            data=data,
            headers=headers,
            timeout=timeout,
        )
    except requests.RequestException as exc:  # pragma: no cover - network errors
        raise TradeExecutionError(f"{method} {url} failed: {exc}") from exc


@dataclass
class BinanceCredentials:
    api_key: str
    secret_key: str


@dataclass
class OKXCredentials:
    api_key: str
    secret_key: str
    passphrase: str


@dataclass
class BybitCredentials:
    api_key: str
    secret_key: str


@dataclass
class BitgetCredentials:
    api_key: str
    secret_key: str
    passphrase: str


@dataclass
class _BinanceSymbolFilters:
    min_qty: Decimal
    step_size: Decimal
    min_notional: Optional[Decimal]
    quantity_precision: Optional[int]
    fetched_at: float


_BINANCE_FILTER_CACHE: Dict[tuple[str, str], _BinanceSymbolFilters] = {}
_BINANCE_FILTER_CACHE_TTL = 15 * 60  # seconds


@dataclass
class _OKXInstrumentFilters:
    lot_size: Decimal
    min_size: Decimal
    contract_value: Decimal
    tick_size: Decimal
    fetched_at: float


_OKX_INSTRUMENT_CACHE: Dict[tuple[str, str], _OKXInstrumentFilters] = {}
_OKX_INSTRUMENT_CACHE_TTL = 15 * 60  # seconds


@dataclass
class _BybitSymbolFilters:
    min_qty: Decimal
    qty_step: Decimal
    max_qty: Optional[Decimal]
    tick_size: Optional[Decimal]
    fetched_at: float


_BYBIT_SYMBOL_CACHE: Dict[tuple[str, str, str], _BybitSymbolFilters] = {}
_BYBIT_SYMBOL_CACHE_TTL = 15 * 60  # seconds


@dataclass
class _BitgetContractFilters:
    min_size: Decimal
    size_step: Decimal
    size_multiplier: Decimal
    price_step: Decimal
    fetched_at: float


_BITGET_CONTRACT_CACHE: Dict[tuple[str, str, str], _BitgetContractFilters] = {}
_BITGET_CONTRACT_CACHE_TTL = 15 * 60  # seconds


SUPPORTED_PERPETUAL_EXCHANGES = ("binance", "okx", "bybit", "bitget")
SUPPORTED_SPOT_EXCHANGES: tuple[str, ...] = ()  # Spot helpers not yet implemented


def _require_credentials(name: str) -> Any:
    if CONFIG_PRIVATE is None:
        raise TradeExecutionError("config_private.py not available; unable to load credentials")
    if not hasattr(CONFIG_PRIVATE, name):
        raise TradeExecutionError(f"Missing credential `{name}` in config_private.py")
    return getattr(CONFIG_PRIVATE, name)


def _resolve_with_mapping(
    credential_cls: Type[Any],
    mapping: Dict[str, str],
    overrides: Dict[str, Optional[str]],
) -> Any:
    resolved = {}
    for field, config_attr in mapping.items():
        value = overrides.get(field)
        if value is None:
            resolved[field] = _require_credentials(config_attr)
        else:
            resolved[field] = value
    return credential_cls(**resolved)


def _resolve_binance_credentials(api_key: Optional[str], secret_key: Optional[str]) -> BinanceCredentials:
    return _resolve_with_mapping(
        BinanceCredentials,
        {"api_key": "BN_API_KEY_ACCOUNT2", "secret_key": "BN_SECRET_KEY_ACCOUNT2"},
        {"api_key": api_key, "secret_key": secret_key},
    )


def _resolve_okx_credentials(
    api_key: Optional[str], secret_key: Optional[str], passphrase: Optional[str]
) -> OKXCredentials:
    return _resolve_with_mapping(
        OKXCredentials,
        {
            "api_key": "OKX_API_KEY_JERRYPSY",
            "secret_key": "OKX_SECRET_KEY_JERRYPSY",
            "passphrase": "OKX_PASSPHRASE_JERRYPSY",
        },
        {"api_key": api_key, "secret_key": secret_key, "passphrase": passphrase},
    )


def _resolve_bybit_credentials(api_key: Optional[str], secret_key: Optional[str]) -> BybitCredentials:
    return _resolve_with_mapping(
        BybitCredentials,
        {
            "api_key": "BYBIT_API_KEY_ACCOUNT18810813576",
            "secret_key": "BYBIT_SECRET_KEY_ACCOUNT18810813576",
        },
        {"api_key": api_key, "secret_key": secret_key},
    )


def _resolve_bitget_credentials(
    api_key: Optional[str], secret_key: Optional[str], passphrase: Optional[str]
) -> BitgetCredentials:
    return _resolve_with_mapping(
        BitgetCredentials,
        {
            "api_key": "BITGET_API_KEY_ACCOUNT18810813576",
            "secret_key": "BITGET_SECRET_KEY_ACCOUNT18810813576",
            "passphrase": "BITGET_PASSPHRASE_ACCOUNT18810813576",
        },
        {
            "api_key": api_key,
            "secret_key": secret_key,
            "passphrase": passphrase,
        },
    )


def _utc_millis() -> int:
    return int(time.time() * 1000)


def _get_binance_symbol_filters(symbol: str, base_url: str) -> _BinanceSymbolFilters:
    """Fetch and cache Binance quantity filters for ``symbol``."""
    key = (base_url, symbol)
    now = time.time()
    cached = _BINANCE_FILTER_CACHE.get(key)
    if cached and now - cached.fetched_at < _BINANCE_FILTER_CACHE_TTL:
        return cached

    url = f"{base_url}/fapi/v1/exchangeInfo"
    params: List[tuple[str, str]] = [("symbol", symbol)]
    response = _send_request("GET", url, params=params)
    data = _json_or_error(response)
    if response.status_code != 200:
        raise TradeExecutionError(f"Failed to fetch Binance exchange info ({response.status_code}): {data}")

    symbols = data.get("symbols") or []
    if not symbols:
        raise TradeExecutionError(f"Binance exchange info payload missing symbol data: {data}")
    target_symbol = symbol.upper()
    if len(symbols) > 1:
        symbol_info = next((item for item in symbols if item.get("symbol") == target_symbol), None)
        if symbol_info is None:
            raise TradeExecutionError(
                f"Binance exchange info did not include requested symbol {target_symbol}: {data}"
            )
    else:
        symbol_info = symbols[0]
    filters = symbol_info.get("filters", [])
    lot_filter = next((f for f in filters if f.get("filterType") == "LOT_SIZE"), None)
    if lot_filter is None:
        raise TradeExecutionError(f"Binance exchange info missing LOT_SIZE filter: {data}")

    market_lot_filter = next((f for f in filters if f.get("filterType") == "MARKET_LOT_SIZE"), None)
    step_source = lot_filter
    min_source = lot_filter
    if market_lot_filter is not None:
        if market_lot_filter.get("stepSize"):
            step_source = market_lot_filter
        if market_lot_filter.get("minQty"):
            min_source = market_lot_filter

    step_size = Decimal(step_source["stepSize"])
    min_qty = Decimal(min_source["minQty"])

    notional_filter = next((f for f in filters if f.get("filterType") == "MIN_NOTIONAL"), None)
    min_notional: Optional[Decimal] = None
    if notional_filter is not None:
        raw_min_notional = notional_filter.get("notional") or notional_filter.get("minNotional")
        if raw_min_notional is not None:
            min_notional = Decimal(str(raw_min_notional))

    LOGGER.info("binance_symbol_info symbol=%s payload=%s", symbol, symbol_info)

    quantity_precision_raw = symbol_info.get("quantityPrecision")
    quantity_precision: Optional[int]
    try:
        quantity_precision = int(quantity_precision_raw) if quantity_precision_raw is not None else None
    except (TypeError, ValueError):
        quantity_precision = None

    cached_filters = _BinanceSymbolFilters(
        min_qty=min_qty,
        step_size=step_size,
        min_notional=min_notional,
        quantity_precision=quantity_precision,
        fetched_at=now,
    )
    _BINANCE_FILTER_CACHE[key] = cached_filters
    LOGGER.info(
        "binance_filter_cache symbol=%s step=%s min_qty=%s min_notional=%s precision=%s",
        symbol,
        step_size,
        min_qty,
        min_notional,
        quantity_precision,
    )
    return cached_filters


def _format_decimal_for_step(value: Decimal, step: Decimal) -> str:
    """Format ``value`` using decimal places implied by ``step``."""

    step_normalized = step.normalize()
    exponent = step_normalized.as_tuple().exponent
    quant = Decimal(1).scaleb(exponent) if exponent < 0 else Decimal(1)
    formatted = value.quantize(quant, rounding=ROUND_DOWN)
    return format(formatted, "f")


def _normalise_binance_quantity(
    symbol: str,
    quantity: Union[str, float, Decimal, int],
    base_url: str,
) -> tuple[str, Decimal]:
    """Snap ``quantity`` to Binance filter increments and enforce minimums."""
    filters = _get_binance_symbol_filters(symbol, base_url)
    try:
        dec_qty = Decimal(str(quantity))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise TradeExecutionError(f"`quantity` {quantity!r} is not a valid decimal amount") from exc
    if dec_qty <= 0:
        raise TradeExecutionError("`quantity` must be a positive number of contracts")
    step = filters.step_size
    minimum = filters.min_qty
    precision = filters.quantity_precision

    if precision is not None:
        precision_step = Decimal(1).scaleb(-precision) if precision > 0 else Decimal(1)
        if precision_step > step:
            LOGGER.info(
                "binance_step_adjustment symbol=%s old_step=%s new_step=%s precision=%s",
                symbol,
                step,
                precision_step,
                precision,
            )
            step = precision_step
        if minimum < precision_step:
            LOGGER.info(
                "binance_min_adjustment symbol=%s old_min=%s new_min=%s",
                symbol,
                minimum,
                precision_step,
            )
            minimum = precision_step

    units = (dec_qty / step).to_integral_value(rounding=ROUND_DOWN)
    normalised = units * step

    if normalised < minimum:
        message = (
            f"`quantity` {dec_qty} is below Binance minimum {minimum} for {symbol}; increase the order size"
        )
        LOGGER.info(
            "binance_quantity_rejected symbol=%s input=%s step=%s min_qty=%s reason=%s",
            symbol,
            dec_qty,
            step,
            minimum,
            "below_minimum",
        )
        raise TradeExecutionError(message)

    quantity_str = _format_decimal_for_step(normalised, step)

    if precision is not None and "." in quantity_str:
        whole, fractional = quantity_str.split(".", 1)
        trimmed_fraction = fractional.rstrip("0")

        if len(trimmed_fraction) > precision:
            quant = Decimal(1).scaleb(-precision) if precision > 0 else Decimal(1)
            adjusted = normalised.quantize(quant, rounding=ROUND_DOWN)
            quantity_str = _format_decimal_for_step(adjusted, step)
            normalised = adjusted
            if "." in quantity_str:
                whole, fractional = quantity_str.split(".", 1)
                trimmed_fraction = fractional.rstrip("0")

        if trimmed_fraction:
            quantity_str = f"{whole}.{trimmed_fraction[:precision]}"
        else:
            quantity_str = whole

    if "." in quantity_str:
        quantity_str = quantity_str.rstrip("0").rstrip(".") or "0"

    try:
        normalised = Decimal(quantity_str)
    except InvalidOperation as exc:
        raise TradeExecutionError(f"Unable to normalise Binance quantity `{quantity_str}`") from exc

    if normalised < minimum:
        message = (
            f"`quantity` {dec_qty} is below Binance minimum {minimum} for {symbol}; increase the order size"
        )
        LOGGER.info(
            "binance_quantity_rejected symbol=%s input=%s normalized=%s step=%s min_qty=%s reason=%s",
            symbol,
            dec_qty,
            quantity_str,
            step,
            minimum,
            "post_adjust_below_minimum",
        )
        raise TradeExecutionError(message)

    LOGGER.info(
        "binance_quantity_normalised symbol=%s input=%s step=%s min_qty=%s precision=%s normalized=%s",
        symbol,
        dec_qty,
        step,
        minimum,
        precision,
        quantity_str,
    )

    return quantity_str, normalised


def _normalise_okx_size(symbol: str, size: Any, base_url: str, inst_type: str = "SWAP") -> tuple[str, Decimal]:
    """Snap ``size`` to OKX lot size increments and enforce minimums."""
    filters = _get_okx_instrument_filters(symbol, base_url, inst_type)
    try:
        raw_size = Decimal(str(size))
    except InvalidOperation as exc:
        raise TradeExecutionError(f"`size` {size!r} is not a valid decimal quantity") from exc
    lot = filters.lot_size
    units = (raw_size / lot).to_integral_value(rounding=ROUND_DOWN)
    normalised = units * lot

    if normalised < filters.min_size:
        raise TradeExecutionError(
            f"`size` {raw_size} is below OKX minimum {filters.min_size} for {symbol}; increase the order size"
        )

    size_str = format(normalised.normalize(), "f")
    return size_str, normalised


def _binance_get_order(
    creds: BinanceCredentials,
    symbol: str,
    order_id: int,
    *,
    recv_window: int,
    base_url: str,
) -> Dict[str, Any]:
    params: List[tuple[str, str]] = [
        ("symbol", symbol),
        ("orderId", str(order_id)),
        ("timestamp", _utc_millis_str()),
        ("recvWindow", str(recv_window)),
    ]

    query = urlencode(params)
    signature = _hmac_sha256_hexdigest(creds.secret_key, query)
    params.append(("signature", signature))

    headers = {
        "X-MBX-APIKEY": creds.api_key,
        "User-Agent": USER_AGENT,
    }

    url = f"{base_url}/fapi/v1/order"
    response = _send_request("GET", url, params=params, headers=headers)
    data = _json_or_error(response)
    if response.status_code != 200:
        raise TradeExecutionError(f"Binance order query failed {response.status_code}: {data}")
    return data


def _iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _utc_millis_str() -> str:
    return str(_utc_millis())


def _compact_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"))


def _hmac_sha256_hexdigest(secret: str, message: str) -> str:
    return hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()


def _hmac_sha256_base64(secret: str, message: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def place_binance_perp_market_order(
    symbol: str,
    side: str,
    quantity: Union[str, float, Decimal, int],
    *,
    reduce_only: Optional[bool] = None,
    client_order_id: Optional[str] = None,
    position_side: Optional[str] = None,
    recv_window: int = 5000,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    base_url: str = "https://fapi.binance.com",
) -> Dict[str, Any]:
    """Submit a Binance USDT-margined futures market order.

    Parameters
    ----------
    symbol:
        Trading pair, e.g. ``ETHUSDT``. ``USDT`` is appended automatically when
        omitted.
    side:
        ``BUY`` or ``SELL``.
    quantity:
        Base asset contract size (e.g. ETH). Binance futures *does not* accept
        USDT notionals for market orders, so callers must pre-compute the
        correct base quantity before invoking this helper.
    """
    creds = _resolve_binance_credentials(api_key, secret_key)

    pair = symbol.upper()
    if not pair.endswith("USDT"):
        pair = f"{pair}USDT"

    params = [
        ("symbol", pair),
        ("side", side.upper()),
        ("type", "MARKET"),
        ("timestamp", _utc_millis_str()),
        ("recvWindow", str(recv_window)),
        ("newOrderRespType", "RESULT"),
    ]

    if quantity is None:
        raise TradeExecutionError("`quantity` must be provided in base asset units")

    quantity_str, _ = _normalise_binance_quantity(pair, quantity, base_url)
    params.append(("quantity", quantity_str))
    if reduce_only is not None:
        params.append(("reduceOnly", "true" if reduce_only else "false"))
    if client_order_id:
        params.append(("newClientOrderId", client_order_id))
    if position_side:
        params.append(("positionSide", position_side.upper()))

    query = urlencode(params)
    signature = _hmac_sha256_hexdigest(creds.secret_key, query)
    params.append(("signature", signature))

    headers = {
        "X-MBX-APIKEY": creds.api_key,
        "User-Agent": USER_AGENT,
    }

    url = f"{base_url}/fapi/v1/order"
    LOGGER.info(
        "binance_order_submit symbol=%s side=%s quantity=%s params=%s",
        pair,
        side,
        quantity_str,
        params,
    )
    response = _send_request("POST", url, params=params, headers=headers)
    data = _json_or_error(response)
    if response.status_code != 200:
        LOGGER.warning(
            "binance_order_http_error status=%s payload=%s",
            response.status_code,
            data,
        )
        raise TradeExecutionError(f"Binance error {response.status_code}: {data}")
    if "code" in data and data["code"] != 0:
        LOGGER.warning(
            "binance_order_reject payload=%s",
            data,
        )
        raise TradeExecutionError(f"Binance reject: {data}")

    executed_qty_raw = data.get("executedQty")
    status = data.get("status")
    order_id = data.get("orderId")
    try:
        executed_qty_val = float(executed_qty_raw)
    except (TypeError, ValueError):
        executed_qty_val = 0.0

    if (
        order_id is not None
        and (executed_qty_raw is None or executed_qty_val == 0.0)
        and status in {"NEW", "PARTIALLY_FILLED", None}
    ):
        try:
            data = _binance_get_order(
                creds,
                pair,
                int(order_id),
                recv_window=recv_window,
                base_url=base_url,
            )
        except TradeExecutionError:
            # Fall back to the original payload if the follow-up query fails
            pass
    return data


def get_binance_perp_positions(
    *,
    symbol: Optional[str] = None,
    recv_window: int = 5000,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    base_url: str = "https://fapi.binance.com",
) -> List[Dict[str, Any]]:
    """Fetch Binance futures position risk snapshots.

    Callers typically request ``symbol="ETHUSDT"`` (upper case) to retrieve the
    current long/short exposure for a single market.
    """
    creds = _resolve_binance_credentials(api_key, secret_key)

    params: List[tuple[str, str]] = [
        ("timestamp", _utc_millis_str()),
        ("recvWindow", str(recv_window)),
    ]
    if symbol:
        params.append(("symbol", symbol.upper()))

    query = urlencode(params)
    signature = _hmac_sha256_hexdigest(creds.secret_key, query)
    params.append(("signature", signature))

    headers = {
        "X-MBX-APIKEY": creds.api_key,
        "User-Agent": USER_AGENT,
    }

    url = f"{base_url}/fapi/v2/positionRisk"
    response = _send_request("GET", url, params=params, headers=headers)
    data = _json_or_error(response)
    if response.status_code != 200:
        raise TradeExecutionError(f"Binance error {response.status_code}: {data}")
    return data if isinstance(data, list) else [data]


def get_binance_perp_price(
    symbol: str,
    *,
    base_url: str = "https://fapi.binance.com",
) -> float:
    """Return the latest Binance futures price for ``symbol`` (e.g. ``ETHUSDT``).

    This helper is intended for scripts that need to convert a USDT notional
    target into a base-asset ``quantity`` before calling
    :func:`place_binance_perp_market_order`.
    """
    pair = symbol.upper()
    if not pair.endswith("USDT"):
        pair = f"{pair}USDT"

    price_url = f"{base_url}/fapi/v1/ticker/price"
    response = _send_request("GET", price_url, params=[("symbol", pair)])
    data = _json_or_error(response)
    if response.status_code != 200 or "price" not in data:
        raise TradeExecutionError(f"Binance price query failed: {data}")
    return float(data["price"])


def get_okx_swap_positions(
    *,
    symbol: Optional[str] = None,
    inst_type: str = "SWAP",
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    base_url: str = "https://www.okx.com",
) -> List[Dict[str, Any]]:
    """Fetch OKX perpetual swap positions via the account endpoint."""
    creds = _resolve_okx_credentials(api_key, secret_key, passphrase)

    request_path = "/api/v5/account/positions"
    params: List[tuple[str, str]] = [("instType", inst_type.upper())]
    if symbol:
        inst_id = symbol.upper()
        if not inst_id.endswith("-USDT-SWAP"):
            inst_id = f"{inst_id}-USDT-SWAP"
        params.append(("instId", inst_id))

    query_str = urlencode(params)
    timestamp = _iso_timestamp()
    target = f"{request_path}?{query_str}" if query_str else request_path
    message = f"{timestamp}GET{target}"
    signature = _hmac_sha256_base64(creds.secret_key, message)

    headers = {
        "OK-ACCESS-KEY": creds.api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": creds.passphrase,
        "User-Agent": USER_AGENT,
    }

    url = f"{base_url}{request_path}"
    response = _send_request("GET", url, params=params, headers=headers)
    data = _json_or_error(response)
    if response.status_code != 200 or data.get("code") != "0":
        raise TradeExecutionError(f"OKX positions reject: {data}")
    return data.get("data", [])


def set_okx_swap_leverage(
    symbol: str,
    leverage: Union[int, str],
    *,
    td_mode: str = "cross",
    pos_side: Optional[str] = None,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    base_url: str = "https://www.okx.com",
) -> Dict[str, Any]:
    """Configure OKX leverage for an instrument before placing trades."""
    creds = _resolve_okx_credentials(api_key, secret_key, passphrase)

    inst_id = symbol.upper()
    if not inst_id.endswith("-USDT-SWAP"):
        inst_id = f"{inst_id}-USDT-SWAP"

    try:
        leverage_value = str(int(leverage))
    except (TypeError, ValueError) as exc:
        raise TradeExecutionError(f"Invalid leverage value: {leverage}") from exc

    payload: Dict[str, Any] = {
        "instId": inst_id,
        "lever": leverage_value,
        "mgnMode": td_mode.lower(),
    }
    if pos_side:
        payload["posSide"] = pos_side.lower()

    request_path = "/api/v5/account/set-leverage"
    timestamp = _iso_timestamp()
    body_str = _compact_json(payload)
    message = f"{timestamp}POST{request_path}{body_str}"
    signature = _hmac_sha256_base64(creds.secret_key, message)

    headers = {
        "OK-ACCESS-KEY": creds.api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": creds.passphrase,
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }

    url = f"{base_url}{request_path}"
    response = _send_request("POST", url, data=body_str, headers=headers)
    data = _json_or_error(response)
    if response.status_code != 200 or data.get("code") != "0":
        raise TradeExecutionError(f"OKX leverage set reject: {data}")
    return data


def _get_okx_instrument_filters(
    symbol: str,
    base_url: str,
    inst_type: str = "SWAP",
) -> _OKXInstrumentFilters:
    key_symbol = symbol.upper()
    if inst_type.upper() == "SWAP" and not key_symbol.endswith("-USDT-SWAP"):
        if key_symbol.endswith("USDT"):
            key_symbol = f"{key_symbol[:-4]}-USDT-SWAP"
        else:
            key_symbol = f"{key_symbol}-USDT-SWAP"

    cache_key = (base_url, key_symbol)
    now = time.time()
    cached = _OKX_INSTRUMENT_CACHE.get(cache_key)
    if cached and now - cached.fetched_at < _OKX_INSTRUMENT_CACHE_TTL:
        return cached

    request_path = "/api/v5/public/instruments"
    params: List[tuple[str, str]] = [("instType", inst_type.upper())]
    params.append(("instId", key_symbol))

    url = f"{base_url}{request_path}"
    response = _send_request("GET", url, params=params)
    data = _json_or_error(response)
    if response.status_code != 200 or data.get("code") != "0":
        raise TradeExecutionError(f"OKX instrument metadata reject: {data}")

    instruments = data.get("data") or []
    if not instruments:
        raise TradeExecutionError(f"OKX instruments payload empty for {key_symbol}: {data}")

    instrument = instruments[0]

    try:
        lot_size = Decimal(str(instrument["lotSz"]))
        min_size = Decimal(str(instrument["minSz"]))
        contract_value = Decimal(str(instrument.get("ctVal", "1")))
        tick_size = Decimal(str(instrument.get("tickSz", "0")))
    except (KeyError, InvalidOperation) as exc:
        raise TradeExecutionError(f"OKX instrument payload invalid: {instrument}") from exc

    filters = _OKXInstrumentFilters(
        lot_size=lot_size,
        min_size=min_size,
        contract_value=contract_value,
        tick_size=tick_size,
        fetched_at=now,
    )
    _OKX_INSTRUMENT_CACHE[cache_key] = filters
    return filters


def _get_bybit_symbol_filters(
    symbol: str,
    base_url: str,
    category: str = "linear",
) -> _BybitSymbolFilters:
    symbol_id = symbol.upper()
    if not symbol_id.endswith("USDT"):
        symbol_id = f"{symbol_id}USDT"

    category_key = category.lower()
    cache_key = (base_url, category_key, symbol_id)
    now = time.time()
    cached = _BYBIT_SYMBOL_CACHE.get(cache_key)
    if cached and now - cached.fetched_at < _BYBIT_SYMBOL_CACHE_TTL:
        return cached

    params: List[tuple[str, str]] = [("category", category_key), ("symbol", symbol_id)]
    url = f"{base_url}/v5/market/instruments-info"
    response = _send_request("GET", url, params=params)
    data = _json_or_error(response)
    if response.status_code != 200 or data.get("retCode") != 0:
        raise TradeExecutionError(f"Bybit instrument metadata reject: {data}")

    result = data.get("result") or {}
    instruments = result.get("list") or []
    if not instruments:
        raise TradeExecutionError(f"Bybit instruments payload empty for {symbol_id}: {data}")

    instrument = instruments[0]
    lot_filter = instrument.get("lotSizeFilter") or {}

    try:
        qty_step = Decimal(str(lot_filter["qtyStep"]))
        min_qty = Decimal(str(lot_filter["minOrderQty"]))
    except (KeyError, InvalidOperation) as exc:
        raise TradeExecutionError(f"Bybit lot size metadata invalid: {lot_filter}") from exc

    max_qty_raw = lot_filter.get("maxOrderQty")
    max_qty: Optional[Decimal] = None
    if max_qty_raw is not None:
        try:
            max_qty = Decimal(str(max_qty_raw))
        except InvalidOperation:
            max_qty = None

    price_filter = instrument.get("priceFilter") or {}
    tick_size_raw = price_filter.get("tickSize")
    tick_size: Optional[Decimal] = None
    if tick_size_raw is not None:
        try:
            tick_size = Decimal(str(tick_size_raw))
        except InvalidOperation:
            tick_size = None

    filters = _BybitSymbolFilters(
        min_qty=min_qty,
        qty_step=qty_step,
        max_qty=max_qty,
        tick_size=tick_size,
        fetched_at=now,
    )
    _BYBIT_SYMBOL_CACHE[cache_key] = filters
    return filters


def _normalise_bybit_quantity(
    symbol: str,
    quantity: Any,
    base_url: str,
    category: str = "linear",
) -> tuple[str, Decimal]:
    filters = _get_bybit_symbol_filters(symbol, base_url, category)

    try:
        raw_qty = Decimal(str(quantity))
    except InvalidOperation as exc:
        raise TradeExecutionError(f"`qty` {quantity!r} is not a valid decimal quantity") from exc

    if raw_qty <= 0:
        raise TradeExecutionError("`qty` must be a positive number of contracts")

    step = filters.qty_step
    if step <= 0:
        raise TradeExecutionError("Bybit quantity step metadata invalid")

    units = (raw_qty / step).to_integral_value(rounding=ROUND_DOWN)
    normalised = units * step

    if normalised < filters.min_qty:
        raise TradeExecutionError(
            f"`qty` {raw_qty} is below Bybit minimum {filters.min_qty} for {symbol}; increase the order size"
        )

    if filters.max_qty is not None and normalised > filters.max_qty:
        raise TradeExecutionError(
            f"`qty` {raw_qty} exceeds Bybit maximum {filters.max_qty} for {symbol}; reduce the order size"
        )

    qty_str = format(normalised.normalize(), "f")
    return qty_str, normalised


def get_bybit_linear_positions(
    *,
    symbol: Optional[str] = None,
    category: str = "linear",
    settle_coin: Optional[str] = None,
    recv_window: int = 5000,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    base_url: str = "https://api.bybit.com",
) -> List[Dict[str, Any]]:
    """Fetch Bybit linear perpetual positions."""
    creds = _resolve_bybit_credentials(api_key, secret_key)

    request_path = "/v5/position/list"
    params: List[tuple[str, str]] = [("category", category.lower())]
    if symbol:
        params.append((
            "symbol",
            symbol.upper() if symbol.upper().endswith("USDT") else f"{symbol.upper()}USDT",
        ))
    if settle_coin:
        params.append(("settleCoin", settle_coin.upper()))

    params_sorted = sorted(params, key=lambda item: item[0])
    query_str = urlencode(params_sorted)

    timestamp = _utc_millis_str()
    recv_str = str(recv_window)
    sign_payload = f"{timestamp}{creds.api_key}{recv_str}{query_str}"
    signature = _hmac_sha256_hexdigest(creds.secret_key, sign_payload)

    headers = {
        "X-BAPI-API-KEY": creds.api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_str,
        "User-Agent": USER_AGENT,
    }

    url = f"{base_url}{request_path}"
    response = _send_request("GET", url, params=params_sorted, headers=headers)
    data = _json_or_error(response)
    if response.status_code != 200 or data.get("retCode") != 0:
        raise TradeExecutionError(f"Bybit positions reject: {data}")
    result = data.get("result", {})
    return result.get("list", []) if isinstance(result, dict) else []


def get_bitget_usdt_perp_positions(
    *,
    symbol: Optional[str] = None,
    product_type: str = "umcbl",
    margin_coin: Optional[str] = "USDT",
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    base_url: str = "https://api.bitget.com",
) -> List[Dict[str, Any]]:
    """Fetch Bitget USDT-margined perpetual positions.

    Bitget retired ``/position/list`` and now exposes consolidated snapshots via
    ``/position/allPosition-v2``. The response contains every position for the
    requested ``productType``, so callers can optionally filter the payload by
    ``symbol``.
    """
    creds = _resolve_bitget_credentials(api_key, secret_key, passphrase)

    inst_filter: Optional[str] = None
    if symbol:
        inst_filter = _normalise_bitget_symbol(symbol)

    request_path = "/api/mix/v1/position/allPosition-v2"
    params: List[tuple[str, str]] = [("productType", product_type)]
    if margin_coin:
        params.append(("marginCoin", margin_coin.upper()))

    query_str = urlencode(params)
    timestamp = _iso_timestamp()
    target = f"{request_path}?{query_str}" if query_str else request_path
    message = f"{timestamp}GET{target}"
    signature = _hmac_sha256_base64(creds.secret_key, message)

    headers = {
        "ACCESS-KEY": creds.api_key,
        "ACCESS-SIGN": signature,
        "ACCESS-TIMESTAMP": timestamp,
        "ACCESS-PASSPHRASE": creds.passphrase,
        "User-Agent": USER_AGENT,
    }

    url = f"{base_url}{request_path}"
    response = _send_request("GET", url, params=params, headers=headers)
    data = _json_or_error(response)
    if response.status_code != 200 or data.get("code") != "00000":
        raise TradeExecutionError(f"Bitget positions reject: {data}")

    payload = data.get("data") or []
    if not isinstance(payload, list):
        raise TradeExecutionError(f"Bitget positions payload malformed: {data}")

    if inst_filter:
        payload = [item for item in payload if (item.get("symbol") or "").upper() == inst_filter]
    return payload


def _normalise_bitget_symbol(symbol: str) -> str:
    inst = symbol.upper()
    if inst.endswith("_UMCBL"):
        return inst
    if inst.endswith("USDT"):
        return f"{inst}_UMCBL"
    return f"{inst}USDT_UMCBL"


def _get_bitget_contract_filters(
    symbol: str,
    *,
    base_url: str,
    product_type: str = "umcbl",
) -> _BitgetContractFilters:
    key_symbol = _normalise_bitget_symbol(symbol)
    cache_key = (base_url, product_type.lower(), key_symbol)
    now = time.time()
    cached = _BITGET_CONTRACT_CACHE.get(cache_key)
    if cached and now - cached.fetched_at < _BITGET_CONTRACT_CACHE_TTL:
        return cached

    request_path = "/api/mix/v1/market/contracts"
    params: List[tuple[str, str]] = [("productType", product_type)]
    url = f"{base_url}{request_path}"
    response = _send_request("GET", url, params=params)
    data = _json_or_error(response)
    if response.status_code != 200 or data.get("code") != "00000":
        raise TradeExecutionError(f"Bitget contract metadata reject: {data}")

    contracts = data.get("data") or []
    if not isinstance(contracts, list):
        raise TradeExecutionError(f"Bitget contract metadata invalid: {data}")

    contract = next((item for item in contracts if (item.get("symbol") or "").upper() == key_symbol), None)
    if not contract:
        raise TradeExecutionError(f"Bitget contract metadata missing {key_symbol}: {data}")

    try:
        min_trade = Decimal(str(contract["minTradeNum"]))
        size_multiplier = Decimal(str(contract["sizeMultiplier"]))
        volume_place = int(contract.get("volumePlace", 0))
        price_place = int(contract.get("pricePlace", 0))
    except (KeyError, InvalidOperation, ValueError) as exc:
        raise TradeExecutionError(f"Bitget contract payload malformed: {contract}") from exc

    size_step = Decimal("1").scaleb(-volume_place) if volume_place >= 0 else Decimal("1")
    if size_multiplier > 0:
        # Ensure the step honours the contract multiplier granularity.
        size_step = max(size_step, size_multiplier)

    price_step = Decimal("1").scaleb(-price_place) if price_place >= 0 else Decimal("1")

    filters = _BitgetContractFilters(
        min_size=min_trade,
        size_step=size_step,
        size_multiplier=size_multiplier,
        price_step=price_step,
        fetched_at=now,
    )
    _BITGET_CONTRACT_CACHE[cache_key] = filters
    return filters


def _normalise_bitget_size(
    symbol: str,
    size: Any,
    *,
    base_url: str,
    product_type: str = "umcbl",
) -> tuple[str, Decimal]:
    filters = _get_bitget_contract_filters(symbol, base_url=base_url, product_type=product_type)

    try:
        raw_size = Decimal(str(size))
    except InvalidOperation as exc:
        raise TradeExecutionError(f"`size` {size!r} is not a valid decimal quantity") from exc

    if raw_size <= 0:
        raise TradeExecutionError("`size` must be a positive number of contracts")

    step = filters.size_step
    if step <= 0:
        raise TradeExecutionError("Bitget size step metadata invalid")

    units = (raw_size / step).to_integral_value(rounding=ROUND_DOWN)
    normalised = units * step

    if normalised < filters.min_size:
        raise TradeExecutionError(
            f"`size` {raw_size} is below Bitget minimum {filters.min_size} for {symbol}; increase the order size"
        )

    size_str = format(normalised.normalize(), "f")
    return size_str, normalised


def get_bitget_usdt_perp_price(
    symbol: str,
    *,
    base_url: str = "https://api.bitget.com",
) -> float:
    inst = _normalise_bitget_symbol(symbol)
    url = f"{base_url}/api/mix/v1/market/ticker"
    response = _send_request("GET", url, params=[("symbol", inst)])
    data = _json_or_error(response)
    if response.status_code != 200 or data.get("code") != "00000":
        raise TradeExecutionError(f"Bitget price query failed: {data}")

    payload = data.get("data") or {}
    price_raw = payload.get("last")
    if price_raw is None:
        raise TradeExecutionError(f"Bitget ticker missing last price: {payload}")

    try:
        return float(price_raw)
    except (TypeError, ValueError) as exc:
        raise TradeExecutionError(f"Bitget ticker price invalid: {payload}") from exc


def derive_bitget_usdt_perp_size_from_usdt(
    symbol: str,
    notional_usdt: float,
    *,
    price: Optional[float] = None,
    base_url: str = "https://api.bitget.com",
    product_type: str = "umcbl",
) -> str:
    if notional_usdt is None or notional_usdt <= 0:
        raise TradeExecutionError("`notional_usdt` must be a positive value")

    if price is None:
        price = get_bitget_usdt_perp_price(symbol, base_url=base_url)

    if price <= 0:
        raise TradeExecutionError("Bitget price must be positive")

    base_amount = Decimal(str(notional_usdt)) / Decimal(str(price))
    size_str, _ = _normalise_bitget_size(
        symbol,
        base_amount,
        base_url=base_url,
        product_type=product_type,
    )
    return size_str


def set_bitget_usdt_perp_leverage(
    symbol: str,
    leverage: Union[int, float, str],
    *,
    margin_coin: str = "USDT",
    hold_side: Optional[str] = None,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    base_url: str = "https://api.bitget.com",
) -> Dict[str, Any]:
    creds = _resolve_bitget_credentials(api_key, secret_key, passphrase)

    inst = _normalise_bitget_symbol(symbol)

    try:
        leverage_value = Decimal(str(leverage))
    except InvalidOperation as exc:
        raise TradeExecutionError(f"Invalid leverage value: {leverage}") from exc
    if leverage_value <= 0:
        raise TradeExecutionError("Leverage must be greater than zero")

    payload: Dict[str, Any] = {
        "symbol": inst,
        "marginCoin": margin_coin.upper(),
        "leverage": format(leverage_value.normalize(), "f"),
    }
    if hold_side:
        payload["holdSide"] = hold_side

    request_path = "/api/mix/v1/account/setLeverage"
    timestamp = _iso_timestamp()
    body_str = _compact_json(payload)
    message = f"{timestamp}POST{request_path}{body_str}"
    signature = _hmac_sha256_base64(creds.secret_key, message)

    headers = {
        "ACCESS-KEY": creds.api_key,
        "ACCESS-SIGN": signature,
        "ACCESS-TIMESTAMP": timestamp,
        "ACCESS-PASSPHRASE": creds.passphrase,
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }

    url = f"{base_url}{request_path}"
    response = _send_request("POST", url, data=body_str, headers=headers)
    data = _json_or_error(response)
    if response.status_code != 200 or data.get("code") != "00000":
        raise TradeExecutionError(f"Bitget leverage reject: {data}")
    return data


def place_okx_swap_market_order(
    symbol: str,
    side: str,
    size: Any,
    *,
    td_mode: str = "cross",
    pos_side: Optional[str] = None,
    reduce_only: Optional[bool] = None,
    client_order_id: Optional[str] = None,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    base_url: str = "https://www.okx.com",
) -> Dict[str, Any]:
    """Submit a market order on OKX USDT perpetual swap."""
    creds = _resolve_okx_credentials(api_key, secret_key, passphrase)

    inst_id = symbol.upper()
    if not inst_id.endswith("-USDT-SWAP"):
        inst_id = f"{inst_id}-USDT-SWAP"

    if size is None:
        raise TradeExecutionError("`size` must be provided for OKX orders")

    size_str, _ = _normalise_okx_size(inst_id, size, base_url)

    payload: Dict[str, Any] = {
        "instId": inst_id,
        "tdMode": td_mode,
        "side": side.lower(),
        "ordType": "market",
        "sz": size_str,
    }
    if pos_side:
        payload["posSide"] = pos_side.lower()
    if reduce_only is not None:
        payload["reduceOnly"] = "true" if reduce_only else "false"
    if client_order_id:
        payload["clOrdId"] = client_order_id

    request_path = "/api/v5/trade/order"
    timestamp = _iso_timestamp()
    body_str = _compact_json(payload)
    message = f"{timestamp}POST{request_path}{body_str}"
    signature = _hmac_sha256_base64(creds.secret_key, message)

    headers = {
        "OK-ACCESS-KEY": creds.api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": creds.passphrase,
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }

    url = f"{base_url}{request_path}"
    response = _send_request("POST", url, data=body_str, headers=headers)
    data = _json_or_error(response)
    if response.status_code != 200 or data.get("code") != "0":
        raise TradeExecutionError(f"OKX reject: {data}")
    return data


def get_okx_swap_price(
    symbol: str,
    *,
    base_url: str = "https://www.okx.com",
) -> float:
    inst_id = symbol.upper()
    if not inst_id.endswith("-USDT-SWAP"):
        if inst_id.endswith("USDT"):
            inst_id = f"{inst_id[:-4]}-USDT-SWAP"
        else:
            inst_id = f"{inst_id}-USDT-SWAP"

    url = f"{base_url}/api/v5/market/ticker"
    response = _send_request("GET", url, params=[("instId", inst_id)])
    data = _json_or_error(response)
    if response.status_code != 200 or data.get("code") != "0":
        raise TradeExecutionError(f"OKX ticker reject: {data}")

    payload = data.get("data") or []
    if not payload:
        raise TradeExecutionError(f"OKX ticker payload empty for {inst_id}: {data}")

    ticker = payload[0]
    price_raw = ticker.get("last") or ticker.get("lastPx")
    if price_raw is None:
        raise TradeExecutionError(f"OKX ticker missing last price: {ticker}")
    try:
        return float(price_raw)
    except (TypeError, ValueError) as exc:
        raise TradeExecutionError(f"OKX ticker price invalid: {ticker}") from exc


def derive_okx_swap_size_from_usdt(
    symbol: str,
    notional_usdt: float,
    *,
    price: Optional[float] = None,
    base_url: str = "https://www.okx.com",
) -> str:
    if notional_usdt is None or notional_usdt <= 0:
        raise TradeExecutionError("`notional_usdt` must be a positive value")

    if price is None:
        price = get_okx_swap_price(symbol, base_url=base_url)

    filters = _get_okx_instrument_filters(symbol, base_url)
    if filters.contract_value <= 0:
        raise TradeExecutionError("OKX contract value metadata invalid")

    base_amount = Decimal(str(notional_usdt)) / Decimal(str(price))
    approximate_size = base_amount / filters.contract_value

    size_str, _ = _normalise_okx_size(symbol, approximate_size, base_url)
    return size_str


def get_bybit_linear_price(
    symbol: str,
    *,
    category: str = "linear",
    base_url: str = "https://api.bybit.com",
) -> float:
    symbol_id = symbol.upper()
    if not symbol_id.endswith("USDT"):
        symbol_id = f"{symbol_id}USDT"

    params = [("category", category.lower()), ("symbol", symbol_id)]
    url = f"{base_url}/v5/market/tickers"
    response = _send_request("GET", url, params=params)
    data = _json_or_error(response)
    if response.status_code != 200 or data.get("retCode") != 0:
        raise TradeExecutionError(f"Bybit price query failed: {data}")

    result = data.get("result") or {}
    tickers = result.get("list") or []
    if not tickers:
        raise TradeExecutionError(f"Bybit price payload missing ticker data: {data}")

    ticker = tickers[0]
    price_raw = ticker.get("lastPrice")
    if price_raw is None:
        raise TradeExecutionError(f"Bybit ticker missing last price: {ticker}")

    try:
        return float(price_raw)
    except (TypeError, ValueError) as exc:
        raise TradeExecutionError(f"Bybit ticker price invalid: {ticker}") from exc


def derive_bybit_linear_qty_from_usdt(
    symbol: str,
    notional_usdt: float,
    *,
    price: Optional[float] = None,
    category: str = "linear",
    base_url: str = "https://api.bybit.com",
) -> str:
    if notional_usdt is None or notional_usdt <= 0:
        raise TradeExecutionError("`notional_usdt` must be a positive value")

    if price is None:
        price = get_bybit_linear_price(symbol, category=category, base_url=base_url)

    if price <= 0:
        raise TradeExecutionError("Bybit price must be positive")

    base_amount = Decimal(str(notional_usdt)) / Decimal(str(price))
    quantity_str, _ = _normalise_bybit_quantity(symbol, base_amount, base_url, category)
    return quantity_str


def set_bybit_linear_leverage(
    symbol: str,
    leverage: Union[int, float, str],
    *,
    buy_leverage: Optional[Union[int, float, str]] = None,
    sell_leverage: Optional[Union[int, float, str]] = None,
    category: str = "linear",
    position_idx: Optional[int] = None,
    recv_window: int = 5000,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    base_url: str = "https://api.bybit.com",
) -> Dict[str, Any]:
    """Configure leverage for Bybit linear perpetuals."""
    creds = _resolve_bybit_credentials(api_key, secret_key)

    symbol_id = symbol.upper()
    if not symbol_id.endswith("USDT"):
        symbol_id = f"{symbol_id}USDT"

    def _format_leverage(value: Union[int, float, str]) -> str:
        try:
            dec_value = Decimal(str(value))
        except InvalidOperation as exc:
            raise TradeExecutionError(f"Invalid leverage value: {value}") from exc
        if dec_value <= 0:
            raise TradeExecutionError("Leverage must be greater than zero")
        return format(dec_value.normalize(), "f")

    buy_value = _format_leverage(buy_leverage if buy_leverage is not None else leverage)
    sell_value = _format_leverage(sell_leverage if sell_leverage is not None else leverage)

    payload: Dict[str, Any] = {
        "category": category.lower(),
        "symbol": symbol_id,
        "buyLeverage": buy_value,
        "sellLeverage": sell_value,
    }
    if position_idx is not None:
        payload["positionIdx"] = str(int(position_idx))

    body_str = _compact_json(payload)
    timestamp = _utc_millis_str()
    recv_str = str(recv_window)
    sign_payload = f"{timestamp}{creds.api_key}{recv_str}{body_str}"
    signature = _hmac_sha256_hexdigest(creds.secret_key, sign_payload)

    headers = {
        "X-BAPI-API-KEY": creds.api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_str,
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }

    url = f"{base_url}/v5/position/set-leverage"
    response = _send_request("POST", url, data=body_str, headers=headers)
    data = _json_or_error(response)
    if response.status_code != 200 or data.get("retCode") != 0:
        raise TradeExecutionError(f"Bybit leverage reject: {data}")
    return data


def _ensure_bybit_leverage_target(
    symbol: str,
    target_leverage: Union[int, float, str],
    *,
    category: str,
    position_idx: int,
    recv_window: int,
    api_key: Optional[str],
    secret_key: Optional[str],
    base_url: str,
) -> None:
    """Ensure Bybit symbol leverage equals the requested value before trading."""
    try:
        set_bybit_linear_leverage(
            symbol,
            leverage=target_leverage,
            category=category,
            position_idx=position_idx,
            recv_window=recv_window,
            api_key=api_key,
            secret_key=secret_key,
            base_url=base_url,
        )
        return
    except TradeExecutionError as exc:
        message = str(exc)
        if "110043" not in message and "leverage not modified" not in message:
            raise

        target_decimal = Decimal(str(target_leverage))
        try:
            positions = get_bybit_linear_positions(
                symbol=symbol,
                category=category,
                recv_window=recv_window,
                api_key=api_key,
                secret_key=secret_key,
                base_url=base_url,
            )
        except TradeExecutionError:
            raise TradeExecutionError(
                "Bybit返回 retCode=110043（leverage not modified），且无法确认当前杠杆设置。"
                "请在 Bybit 后台手动检查并调整杠杆。"
            ) from exc

        leverage_matches = False
        for position in positions:
            leverage_raw = position.get("leverage")
            if leverage_raw is None:
                continue
            try:
                leverage_dec = Decimal(str(leverage_raw))
            except (InvalidOperation, TypeError):
                continue
            if leverage_dec == target_decimal:
                leverage_matches = True
                continue

            size_raw = position.get("size")
            has_position = False
            try:
                has_position = Decimal(str(size_raw)) != 0
            except (InvalidOperation, TypeError):
                has_position = bool(size_raw)

            if has_position:
                raise TradeExecutionError(
                    "Bybit返回 retCode=110043（leverage not modified），且当前持仓杠杆"
                    f" 为 {leverage_raw}，无法自动调整至 {target_decimal}。"
                    "请检查保证金模式或手动调整杠杆。"
                ) from exc

        if leverage_matches:
            LOGGER.info(
                "Bybit leverage already configured at target=%s for %s; skipping update.",
                target_decimal,
                symbol,
            )
            return

        LOGGER.info(
            "Bybit leverage request returned 110043 without conflicting positions; assuming leverage=%s.",
            target_decimal,
        )


def place_bybit_linear_market_order(
    symbol: str,
    side: str,
    qty: Union[str, float, Decimal],
    *,
    category: str = "linear",
    time_in_force: str = "ImmediateOrCancel",
    reduce_only: Optional[bool] = None,
    position_idx: Optional[int] = None,
    client_order_id: Optional[str] = None,
    recv_window: int = 5000,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    base_url: str = "https://api.bybit.com",
) -> Dict[str, Any]:
    """Submit a market order on Bybit linear USDT perpetuals.

    ``qty`` is automatically normalised to the exchange's lot-size filter so
    callers can pass either a float or string amount.
    """
    creds = _resolve_bybit_credentials(api_key, secret_key)

    symbol_id = symbol.upper()
    if not symbol_id.endswith("USDT"):
        symbol_id = f"{symbol_id}USDT"

    category_key = category.lower()

    qty_str, _ = _normalise_bybit_quantity(symbol_id, qty, base_url, category_key)

    payload: Dict[str, Any] = {
        "category": category_key,
        "symbol": symbol_id,
        "side": side.capitalize(),
        "orderType": "Market",
        "qty": qty_str,
        "timeInForce": time_in_force,
    }
    if reduce_only is not None:
        payload["reduceOnly"] = reduce_only
    if position_idx is not None:
        payload["positionIdx"] = str(int(position_idx))
    if client_order_id:
        payload["orderLinkId"] = client_order_id

    body_str = _compact_json(payload)
    timestamp = _utc_millis_str()
    recv_str = str(recv_window)
    sign_payload = f"{timestamp}{creds.api_key}{recv_str}{body_str}"
    signature = _hmac_sha256_hexdigest(creds.secret_key, sign_payload)

    headers = {
        "X-BAPI-API-KEY": creds.api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_str,
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }

    url = f"{base_url}/v5/order/create"
    response = _send_request("POST", url, data=body_str, headers=headers)
    data = _json_or_error(response)
    if response.status_code != 200 or data.get("retCode") != 0:
        raise TradeExecutionError(f"Bybit reject: {data}")
    return data


def place_bitget_usdt_perp_market_order(
    symbol: str,
    side: str,
    size: Union[str, float, Decimal],
    *,
    margin_coin: str = "USDT",
    reduce_only: Optional[bool] = None,
    client_order_id: Optional[str] = None,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    base_url: str = "https://api.bitget.com",
) -> Dict[str, Any]:
    """Submit a market order on Bitget USDT-margined perpetuals.

    ``size`` is treated as the base asset amount (e.g. ETH) and normalised to the
    contract's minimum trade size and step. This keeps the interface consistent
    with Binance/OKX/Bybit helpers which accept base quantities.
    """
    creds = _resolve_bitget_credentials(api_key, secret_key, passphrase)

    inst = _normalise_bitget_symbol(symbol)

    size_str, _ = _normalise_bitget_size(inst, size, base_url=base_url)

    normalized_side = side.lower()
    if normalized_side in {"buy", "sell"}:
        if reduce_only:
            normalized_side = "close_long" if normalized_side == "sell" else "close_short"
        else:
            normalized_side = "open_long" if normalized_side == "buy" else "open_short"
    elif normalized_side not in {"open_long", "open_short", "close_long", "close_short"}:
        raise TradeExecutionError("Bitget side must be buy/sell or explicit open_/close_ value")

    payload: Dict[str, Any] = {
        "symbol": inst,
        "marginCoin": margin_coin.upper(),
        "size": size_str,
        "orderType": "market",
        "side": normalized_side,
    }
    if reduce_only is not None:
        payload["reduceOnly"] = str(bool(reduce_only)).lower()
    if client_order_id:
        payload["clientOid"] = client_order_id

    request_path = "/api/mix/v1/order/placeOrder"
    timestamp = _iso_timestamp()
    body_str = _compact_json(payload)
    message = f"{timestamp}POST{request_path}{body_str}"
    signature = _hmac_sha256_base64(creds.secret_key, message)

    headers = {
        "ACCESS-KEY": creds.api_key,
        "ACCESS-SIGN": signature,
        "ACCESS-TIMESTAMP": timestamp,
        "ACCESS-PASSPHRASE": creds.passphrase,
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }

    url = f"{base_url}{request_path}"
    response = _send_request("POST", url, data=body_str, headers=headers)
    data = _json_or_error(response)
    if response.status_code != 200 or data.get("code") != "00000":
        raise TradeExecutionError(f"Bitget reject: {data}")
    result_payload = data.get("data")
    if isinstance(result_payload, dict):
        result_payload.setdefault("symbol", inst)
        result_payload.setdefault("marginCoin", margin_coin.upper())
        result_payload.setdefault("size", size_str)
        # Preserve the original buy/sell intent for downstream logging.
        result_payload.setdefault("side", normalized_side)
    return data


def get_supported_trading_backends() -> Dict[str, List[str]]:
    """Return supported market types and exchanges for higher-level tooling."""

    return {
        "perpetual": list(SUPPORTED_PERPETUAL_EXCHANGES),
        "spot": list(SUPPORTED_SPOT_EXCHANGES),
    }


def _coerce_positive_quantity(value: Union[str, float, Decimal, int]) -> Decimal:
    try:
        quantity = Decimal(str(value))
    except (InvalidOperation, TypeError) as exc:  # pragma: no cover - input validation
        raise TradeExecutionError("`quantity` must be a positive decimal number") from exc
    if quantity <= 0:
        raise TradeExecutionError("`quantity` must be a positive decimal number")
    return quantity


def _normalise_perp_side(side: Union[str, None]) -> str:
    side_key = (side or "").strip().lower()
    if side_key in {"long", "buy", "b"}:
        return "long"
    if side_key in {"short", "sell", "s"}:
        return "short"
    raise TradeExecutionError("`side` must be LONG/BUY or SHORT/SELL")


def execute_perp_market_order(
    exchange: str,
    symbol: str,
    quantity: Union[str, float, Decimal, int],
    *,
    side: Union[str, None],
    order_kwargs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Submit a single directional perpetual market order.

    ``quantity`` is expressed in the base asset (e.g. ETH). Higher-level callers
    are expected to convert from USDT notionals when required.
    """

    exchange_key = (exchange or "").lower()
    if exchange_key not in SUPPORTED_PERPETUAL_EXCHANGES:
        raise TradeExecutionError(f"Exchange `{exchange}` is not supported for perpetual trading")

    direction = _normalise_perp_side(side)
    quantity_dec = _coerce_positive_quantity(quantity)
    kwargs = dict(order_kwargs or {})
    for blocked in ("side", "pos_side", "position_side"):
        kwargs.pop(blocked, None)
    bybit_position_idx = kwargs.pop("position_idx", None)

    LOGGER.info(
        "execute_perp_market_order exchange=%s symbol=%s direction=%s quantity=%s",
        exchange_key,
        symbol,
        direction,
        quantity_dec,
    )

    if exchange_key == "binance":
        order = place_binance_perp_market_order(
            symbol,
            "BUY" if direction == "long" else "SELL",
            quantity_dec,
            position_side="LONG" if direction == "long" else "SHORT",
            **kwargs,
        )
    elif exchange_key == "okx":
        order = place_okx_swap_market_order(
            symbol,
            "buy" if direction == "long" else "sell",
            str(quantity_dec),
            pos_side="long" if direction == "long" else "short",
            **kwargs,
        )
    elif exchange_key == "bybit":
        bybit_position_idx_resolved = bybit_position_idx if bybit_position_idx is not None else 0
        bybit_category = kwargs.get("category", "linear")
        receiver_window = kwargs.get("recv_window", 5000)
        # 为了兼容默认的单向持仓策略，下单前强制校准杠杆至1倍；
        # 若杠杆已满足或受账户设置限制，会在辅助函数中给出明确反馈。
        _ensure_bybit_leverage_target(
            symbol,
            target_leverage=1,
            category=bybit_category,
            position_idx=bybit_position_idx_resolved,
            recv_window=receiver_window,
            api_key=kwargs.get("api_key"),
            secret_key=kwargs.get("secret_key"),
            base_url=kwargs.get("base_url", "https://api.bybit.com"),
        )
        order = place_bybit_linear_market_order(
            symbol,
            "buy" if direction == "long" else "sell",
            str(quantity_dec),
            position_idx=bybit_position_idx_resolved,
            **kwargs,
        )
    elif exchange_key == "bitget":
        order = place_bitget_usdt_perp_market_order(
            symbol,
            "buy" if direction == "long" else "sell",
            str(quantity_dec),
            **kwargs,
        )
    else:  # pragma: no cover - guarded above but keeps mypy happy
        raise TradeExecutionError(f"Unhandled exchange `{exchange}`")

    return order


def execute_perp_market_batch(
    symbol: str,
    legs: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Execute multiple perpetual market orders sequentially.

    Each leg must provide ``exchange``, ``side`` and ``quantity`` (base asset).
    Optional ``order_kwargs`` are forwarded to the exchange helper.
    """

    if not isinstance(legs, list) or not legs:
        raise TradeExecutionError("At least one trading leg is required")

    results: List[Dict[str, Any]] = []
    for index, leg in enumerate(legs, start=1):
        if not isinstance(leg, dict):
            raise TradeExecutionError(f"Leg #{index} must be an object")
        exchange = leg.get("exchange")
        side = leg.get("side")
        quantity = leg.get("quantity")
        if not exchange:
            raise TradeExecutionError(f"Leg #{index} missing `exchange`")
        if side is None:
            raise TradeExecutionError(f"Leg #{index} missing `side`")
        if quantity is None:
            raise TradeExecutionError(f"Leg #{index} missing `quantity`")

        order = execute_perp_market_order(
            exchange,
            symbol,
            quantity,
            side=side,
            order_kwargs=leg.get("order_kwargs"),
        )

        results.append(
            {
                "exchange": exchange,
                "side": _normalise_perp_side(side),
                "quantity": str(_coerce_positive_quantity(quantity)),
                "order": order,
            }
        )

    return results


def execute_dual_perp_market_order(
    exchange: str,
    symbol: str,
    quantity: Union[str, float, Decimal, int],
    *,
    order_kwargs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Open long and short positions simultaneously on a derivatives venue.

    Parameters
    ----------
    exchange:
        Target exchange key (e.g. ``"binance"``, ``"okx"``).
    symbol:
        Trading pair without leverage suffixes (``ETHUSDT`` or ``ETH``).
    quantity:
        Base asset quantity per side. Each side submits its own market order.
    order_kwargs:
        Optional dictionary forwarded to the low-level submission helpers.
    """

    exchange_key = (exchange or "").lower()
    if exchange_key not in SUPPORTED_PERPETUAL_EXCHANGES:
        raise TradeExecutionError(f"Exchange `{exchange}` is not supported for perpetual trading")

    quantity_dec = _coerce_positive_quantity(quantity)
    base_kwargs = dict(order_kwargs or {})
    client_order_id = base_kwargs.pop("client_order_id", None)

    long_kwargs = dict(base_kwargs)
    short_kwargs = dict(base_kwargs)

    if client_order_id:
        base_id = str(client_order_id)
        trimmed = base_id[-24:] if len(base_id) > 24 else base_id
        long_kwargs["client_order_id"] = f"{trimmed}-LONG"
        short_kwargs["client_order_id"] = f"{trimmed}-SHORT"

    long_order = execute_perp_market_order(
        exchange_key,
        symbol,
        quantity_dec,
        side="long",
        order_kwargs=long_kwargs,
    )
    short_order = execute_perp_market_order(
        exchange_key,
        symbol,
        quantity_dec,
        side="short",
        order_kwargs=short_kwargs,
    )

    return {"long": long_order, "short": short_order}
