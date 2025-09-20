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
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any, Dict, List, Optional, Type, Union
from urllib.parse import urlencode

import requests

import config

CONFIG_PRIVATE = getattr(config, "config_private", None)
REQUEST_TIMEOUT = config.REST_CONNECTION_CONFIG.get("timeout", 10)
USER_AGENT = config.REST_CONNECTION_CONFIG.get("user_agent", "WLFI-Monitor/1.0")


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

    filters = symbols[0].get("filters", [])
    lot_filter = next((f for f in filters if f.get("filterType") == "LOT_SIZE"), None)
    if lot_filter is None:
        raise TradeExecutionError(f"Binance exchange info missing LOT_SIZE filter: {data}")

    step_size = Decimal(lot_filter["stepSize"])
    min_qty = Decimal(lot_filter["minQty"])

    notional_filter = next((f for f in filters if f.get("filterType") == "MIN_NOTIONAL"), None)
    min_notional: Optional[Decimal] = None
    if notional_filter is not None:
        raw_min_notional = notional_filter.get("notional") or notional_filter.get("minNotional")
        if raw_min_notional is not None:
            min_notional = Decimal(str(raw_min_notional))

    cached_filters = _BinanceSymbolFilters(
        min_qty=min_qty,
        step_size=step_size,
        min_notional=min_notional,
        fetched_at=now,
    )
    _BINANCE_FILTER_CACHE[key] = cached_filters
    return cached_filters


def _normalise_binance_quantity(symbol: str, quantity: float, base_url: str) -> tuple[str, Decimal]:
    """Snap ``quantity`` to Binance filter increments and enforce minimums."""
    filters = _get_binance_symbol_filters(symbol, base_url)
    dec_qty = Decimal(str(quantity))
    step = filters.step_size
    minimum = filters.min_qty

    units = (dec_qty / step).to_integral_value(rounding=ROUND_DOWN)
    normalised = units * step

    if normalised < minimum:
        raise TradeExecutionError(
            f"`quantity` {dec_qty} is below Binance minimum {minimum} for {symbol}; "
            "increase the order size"
        )

    quantity_str = format(normalised.normalize(), "f")
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
    quantity: float,
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
    if quantity <= 0:
        raise TradeExecutionError("`quantity` must be a positive number of contracts")

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
    response = _send_request("POST", url, params=params, headers=headers)
    data = _json_or_error(response)
    if response.status_code != 200:
        raise TradeExecutionError(f"Binance error {response.status_code}: {data}")
    if "code" in data and data["code"] != 0:
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
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    base_url: str = "https://api.bitget.com",
) -> List[Dict[str, Any]]:
    """Fetch Bitget USDT-margined perpetual positions."""
    creds = _resolve_bitget_credentials(api_key, secret_key, passphrase)

    request_path = "/api/mix/v1/position/list"
    params: List[tuple[str, str]] = [("productType", product_type)]
    if symbol:
        inst = symbol.upper()
        if not inst.endswith("_UMCBL"):
            inst = f"{inst}USDT_UMCBL" if not inst.endswith("USDT") else f"{inst}_UMCBL"
        params.append(("symbol", inst))

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
    payload = data.get("data", [])
    if isinstance(payload, dict):
        return payload.get("positions", [])
    return payload


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


def place_bybit_linear_market_order(
    symbol: str,
    side: str,
    qty: str,
    *,
    time_in_force: str = "ImmediateOrCancel",
    reduce_only: Optional[bool] = None,
    client_order_id: Optional[str] = None,
    recv_window: int = 5000,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    base_url: str = "https://api.bybit.com",
) -> Dict[str, Any]:
    """Submit a market order on Bybit linear USDT perpetuals."""
    creds = _resolve_bybit_credentials(api_key, secret_key)

    symbol_id = symbol.upper()
    if not symbol_id.endswith("USDT"):
        symbol_id = f"{symbol_id}USDT"

    payload: Dict[str, Any] = {
        "category": "linear",
        "symbol": symbol_id,
        "side": side.capitalize(),
        "orderType": "Market",
        "qty": str(qty),
        "timeInForce": time_in_force,
    }
    if reduce_only is not None:
        payload["reduceOnly"] = reduce_only
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
    size: str,
    *,
    margin_coin: str = "USDT",
    reduce_only: Optional[bool] = None,
    client_order_id: Optional[str] = None,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    base_url: str = "https://api.bitget.com",
) -> Dict[str, Any]:
    """Submit a market order on Bitget USDT-margined perpetuals."""
    creds = _resolve_bitget_credentials(api_key, secret_key, passphrase)

    inst = symbol.upper()
    if not inst.endswith("_UMCBL"):
        inst = f"{inst}USDT_UMCBL" if not inst.endswith("USDT") else f"{inst}_UMCBL"

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
        "marginCoin": margin_coin,
        "size": str(size),
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
    return data
