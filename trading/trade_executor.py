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
import os
import re
import socket
import threading
import time
import logging
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any, Dict, List, Optional, Type, Union
from urllib.parse import urlencode

import requests

try:
    from eth_account import Account  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    Account = None  # type: ignore[assignment]

try:
    from hyperliquid.exchange import Exchange as HyperliquidExchange  # type: ignore[import-not-found]
    from hyperliquid.info import Info as HyperliquidInfo  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    HyperliquidExchange = None  # type: ignore[assignment]
    HyperliquidInfo = None  # type: ignore[assignment]

try:
    from lighter.signer_client import SignerClient as LighterSignerClient  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    LighterSignerClient = None  # type: ignore[assignment]

try:
    from pysdk.grvt_ccxt import GrvtCcxt  # type: ignore[import-not-found]
    from pysdk.grvt_ccxt_env import GrvtEnv  # type: ignore[import-not-found]
    from pysdk.grvt_raw_base import GrvtApiConfig  # type: ignore[import-not-found]
    from pysdk.grvt_raw_env import GrvtEnv as GrvtRawEnv  # type: ignore[import-not-found]
    from pysdk.grvt_raw_sync import GrvtRawSync  # type: ignore[import-not-found]
    from pysdk import grvt_raw_types as grvt_raw_types  # type: ignore[import-not-found]
except Exception:  # pragma: no cover
    GrvtCcxt = None  # type: ignore[assignment]
    GrvtEnv = None  # type: ignore[assignment]
    GrvtApiConfig = None  # type: ignore[assignment]
    GrvtRawEnv = None  # type: ignore[assignment]
    GrvtRawSync = None  # type: ignore[assignment]
    grvt_raw_types = None  # type: ignore[assignment]

try:  # requests bundles urllib3, but allow user-installed urllib3 fallback
    import urllib3.util.connection as _urllib3_connection  # type: ignore
except Exception:  # pragma: no cover - optional dependency path
    _urllib3_connection = None

import config

CONFIG_PRIVATE = getattr(config, "config_private", None)
REQUEST_TIMEOUT = config.REST_CONNECTION_CONFIG.get("timeout", 10)
USER_AGENT = config.REST_CONNECTION_CONFIG.get("user_agent", "CrossExchange-Arb/1.0")
LOGGER = logging.getLogger(__name__)

_OKX_CLIENT_ID_RE = re.compile(r"[^0-9A-Za-z]")
_BINANCE_BAN_UNTIL_RE = re.compile(r"banned until (\\d+)", re.IGNORECASE)


def _sanitize_okx_client_order_id(value: str) -> str:
    """
    OKX clOrdId constraints are stricter than other venues; keep only [0-9A-Za-z]
    and cap to 32 chars.
    """
    cleaned = _OKX_CLIENT_ID_RE.sub("", str(value or ""))
    if not cleaned:
        return ""
    return cleaned[-32:]


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


@dataclass(frozen=True)
class HyperliquidCredentials:
    private_key: str
    address: str
    base_url: str


@dataclass(frozen=True)
class LighterCredentials:
    private_key: str
    account_index: int
    api_key_index: int
    base_url: str


@dataclass(frozen=True)
class GrvtCredentials:
    api_key: str
    private_key: str
    trading_account_id: str
    environment: str


@dataclass
class _BinanceSymbolFilters:
    min_qty: Decimal
    step_size: Decimal
    min_notional: Optional[Decimal]
    quantity_precision: Optional[int]
    fetched_at: float


_BINANCE_FILTER_CACHE: Dict[tuple[str, str], _BinanceSymbolFilters] = {}
_BINANCE_FILTER_CACHE_TTL = 15 * 60  # seconds

# Binance price cache to reduce per-symbol polling (helps avoid IP bans).
_BINANCE_PRICE_CACHE: Dict[str, tuple[float, float]] = {}
_BINANCE_PRICE_ALL_CACHE: tuple[Dict[str, float], float] = ({}, 0.0)
_BINANCE_PRICE_CACHE_TTL_SECONDS = 2.0
_BINANCE_BAN_UNTIL_MS: int = 0


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


@dataclass
class _GrvtInstrumentFilters:
    instrument: str
    min_size: Decimal
    tick_size: Decimal
    base_decimals: Optional[int]
    quote_decimals: Optional[int]
    funding_interval_hours: Optional[float]
    adjusted_funding_rate_cap_pp: Optional[Decimal]
    adjusted_funding_rate_floor_pp: Optional[Decimal]
    fetched_at: float


_GRVT_INSTRUMENT_CACHE: Dict[tuple[str, str], _GrvtInstrumentFilters] = {}
_GRVT_INSTRUMENT_CACHE_TTL = 15 * 60  # seconds
_GRVT_CLIENT_LOCK = threading.Lock()
_GRVT_CLIENT_CACHE: Dict[tuple[str, str], tuple[Any, float]] = {}


SUPPORTED_PERPETUAL_EXCHANGES = ("binance", "okx", "bybit", "bitget", "hyperliquid", "lighter", "grvt")
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


def _resolve_hyperliquid_credentials(
    private_key: Optional[str],
    address: Optional[str],
    *,
    base_url: Optional[str] = None,
) -> HyperliquidCredentials:
    pk = (
        private_key
        or (getattr(CONFIG_PRIVATE, "HYPERLIQUID_PRIVATE_KEY", None) if CONFIG_PRIVATE else None)
        or os.getenv("HYPERLIQUID_PRIVATE_KEY")
        or ""
    ).strip()
    if not pk:
        raise TradeExecutionError("Hyperliquid requires HYPERLIQUID_PRIVATE_KEY (env or config_private.py)")

    # Hyperliquid commonly uses an "API wallet" (agent key) that is authorized to trade on behalf of a different
    # main trading address. In that setup:
    # - private_key: API wallet key (agent)
    # - address: main trading address (user) used for user_state / positions / orderStatus queries
    addr = (
        address
        or (getattr(CONFIG_PRIVATE, "HYPERLIQUID_ADDRESS", None) if CONFIG_PRIVATE else None)
        or (getattr(CONFIG_PRIVATE, "HYPERLIQUID_USER_ADDRESS", None) if CONFIG_PRIVATE else None)
        or (getattr(CONFIG_PRIVATE, "HYPERLIQUID_WALLET_ADDRESS", None) if CONFIG_PRIVATE else None)
        or os.getenv("HYPERLIQUID_ADDRESS")
        or os.getenv("HYPERLIQUID_USER_ADDRESS")
        or os.getenv("HYPERLIQUID_WALLET_ADDRESS")
        or ""
    ).strip()
    if not addr:
        if Account is None:
            raise TradeExecutionError("Hyperliquid requires eth-account to derive address from private key")
        try:
            acct = Account.from_key(pk)
        except Exception as exc:
            raise TradeExecutionError("Invalid Hyperliquid private key") from exc
        addr = str(acct.address)

    base = (
        base_url
        or (getattr(CONFIG_PRIVATE, "HYPERLIQUID_API_BASE_URL", None) if CONFIG_PRIVATE else None)
        or os.getenv("HYPERLIQUID_API_BASE_URL")
        or getattr(config, "HYPERLIQUID_API_BASE_URL", None)
        or ""
    )
    base = (base or "https://api.hyperliquid.xyz").rstrip("/")
    return HyperliquidCredentials(private_key=pk, address=addr, base_url=base)


def _resolve_lighter_credentials(
    private_key: Optional[str],
    account_index: Optional[Union[int, str]],
    api_key_index: Optional[Union[int, str]],
    *,
    base_url: Optional[str] = None,
) -> LighterCredentials:
    if CONFIG_PRIVATE is None:
        raise TradeExecutionError("config_private.py not available; unable to load credentials")

    pk = (private_key or "").strip() or str(getattr(CONFIG_PRIVATE, "LIGHTER_PRIVATE_KEY", "") or "").strip()
    if not pk:
        raise TradeExecutionError("Missing LIGHTER_PRIVATE_KEY in config_private.py (or private_key=...)")

    raw_account = account_index if account_index not in (None, "") else getattr(CONFIG_PRIVATE, "LIGHTER_ACCOUNT_INDEX", None)
    raw_key = api_key_index if api_key_index not in (None, "") else getattr(CONFIG_PRIVATE, "LIGHTER_KEY_INDEX", None)
    if raw_account in (None, ""):
        raise TradeExecutionError("Missing LIGHTER_ACCOUNT_INDEX in config_private.py (or account_index=...)")
    if raw_key in (None, ""):
        raise TradeExecutionError("Missing LIGHTER_KEY_INDEX in config_private.py (or api_key_index=...)")
    try:
        acct = int(raw_account)
    except Exception as exc:
        raise TradeExecutionError(f"Invalid LIGHTER_ACCOUNT_INDEX: {raw_account!r}") from exc
    try:
        key_idx = int(raw_key)
    except Exception as exc:
        raise TradeExecutionError(f"Invalid LIGHTER_KEY_INDEX: {raw_key!r}") from exc

    host = (base_url or getattr(config, "LIGHTER_REST_BASE_URL", None) or "https://mainnet.zklighter.elliot.ai").rstrip("/")
    # config.LIGHTER_REST_BASE_URL typically ends with /api/v1; strip it for SignerClient host.
    if host.endswith("/api/v1"):
        host = host[: -len("/api/v1")]
    return LighterCredentials(private_key=pk, account_index=acct, api_key_index=key_idx, base_url=host)


def _resolve_grvt_credentials(
    api_key: Optional[str] = None,
    private_key: Optional[str] = None,
    trading_account_id: Optional[str] = None,
    environment: Optional[str] = None,
) -> GrvtCredentials:
    env = (environment or getattr(config, "GRVT_ENVIRONMENT", "") or "prod").lower().strip()
    ak = str(api_key or getattr(config, "GRVT_API_KEY", "") or "").strip()
    pk = str(private_key or getattr(config, "GRVT_API_SECRET", "") or "").strip()
    ta = str(trading_account_id or getattr(config, "GRVT_TRADING_ACCOUNT_ID", "") or "").strip()
    if not ak:
        raise TradeExecutionError("GRVT_API_KEY not configured")
    if not pk:
        raise TradeExecutionError("GRVT_SECRET_KEY/GRVT_PRIVATE_KEY not configured")
    if not ta:
        raise TradeExecutionError("GRVT_TRADING_ACCOUNT_ID not configured")
    return GrvtCredentials(api_key=ak, private_key=pk, trading_account_id=ta, environment=env)


def _grvt_env_enum(environment: str) -> Any:
    if GrvtEnv is None:
        raise TradeExecutionError("grvt-pysdk is required for GRVT trading APIs")
    env = (environment or "prod").lower().strip()
    mapping = {
        "prod": GrvtEnv.PROD,
        "testnet": GrvtEnv.TESTNET,
        "staging": GrvtEnv.STAGING,
        "dev": GrvtEnv.DEV,
    }
    return mapping.get(env, GrvtEnv.PROD)


def _grvt_raw_env_enum(environment: str) -> Any:
    if GrvtRawEnv is None:
        raise TradeExecutionError("grvt-pysdk is required for GRVT raw APIs")
    env = (environment or "prod").lower().strip()
    mapping = {
        "prod": GrvtRawEnv.PROD,
        "testnet": GrvtRawEnv.TESTNET,
        "staging": GrvtRawEnv.STAGING,
        "dev": GrvtRawEnv.DEV,
    }
    return mapping.get(env, GrvtRawEnv.PROD)


def _get_grvt_client(*, public: bool = False, creds: Optional[GrvtCredentials] = None) -> Any:
    if GrvtCcxt is None:
        raise TradeExecutionError("grvt-pysdk is required for GRVT trading APIs")

    if public:
        env = (getattr(config, "GRVT_ENVIRONMENT", "") or "prod").lower().strip()
        key = ("public", env)
        with _GRVT_CLIENT_LOCK:
            cached = _GRVT_CLIENT_CACHE.get(key)
            if cached and (time.time() - float(cached[1] or 0)) < 60.0:
                return cached[0]
            client = GrvtCcxt(env=_grvt_env_enum(env), parameters={})
            _GRVT_CLIENT_CACHE[key] = (client, time.time())
            return client

    resolved = creds or _resolve_grvt_credentials()
    cache_key = (resolved.environment, resolved.trading_account_id)
    with _GRVT_CLIENT_LOCK:
        cached = _GRVT_CLIENT_CACHE.get(cache_key)
        if cached and (time.time() - float(cached[1] or 0)) < 30.0:
            return cached[0]
        client = GrvtCcxt(
            env=_grvt_env_enum(resolved.environment),
            parameters={
                "api_key": resolved.api_key,
                "private_key": resolved.private_key,
                "trading_account_id": resolved.trading_account_id,
            },
        )
        _GRVT_CLIENT_CACHE[cache_key] = (client, time.time())
        return client


def _grvt_client_order_id(value: Optional[str]) -> int:
    """GRVT SDK expects client_order_id to be an integer (uint32-ish)."""
    if value is None:
        return int(time.time() * 1000) & 0xFFFFFFFF
    text = str(value).strip()
    if not text:
        return int(time.time() * 1000) & 0xFFFFFFFF
    if text.isdigit():
        try:
            return int(text) & 0xFFFFFFFF
        except Exception:
            return zlib.crc32(text.encode("utf-8")) & 0xFFFFFFFF
    return zlib.crc32(text.encode("utf-8")) & 0xFFFFFFFF


def _get_grvt_instrument_filters(symbol: str, *, environment: Optional[str] = None) -> _GrvtInstrumentFilters:
    base = (symbol or "").upper().strip()
    env = (environment or getattr(config, "GRVT_ENVIRONMENT", "") or "prod").lower().strip()
    if not base:
        raise TradeExecutionError("GRVT symbol missing")
    cache_key = (env, base)
    cached = _GRVT_INSTRUMENT_CACHE.get(cache_key)
    now = time.time()
    if cached and (now - float(cached.fetched_at or 0.0)) < _GRVT_INSTRUMENT_CACHE_TTL:
        return cached

    client = _get_grvt_client(public=True)
    markets: Any = []
    try:
        markets = client.fetch_markets(params={"kind": "PERPETUAL", "base": base, "quote": "USDT", "limit": 20})  # type: ignore[call-arg]
    except Exception:
        markets = []

    inst: Optional[Dict[str, Any]] = None
    if isinstance(markets, list) and markets:
        expected = f"{base}_USDT_Perp"
        for m in markets:
            if isinstance(m, dict) and str(m.get("instrument") or "") == expected:
                inst = m
                break
        if inst is None and isinstance(markets[0], dict):
            inst = markets[0]

    if not isinstance(inst, dict):
        inst = {"instrument": f"{base}_USDT_Perp"}

    def _int_or_none(v: Any) -> Optional[int]:
        try:
            return int(v)
        except Exception:
            return None

    def _dec_or_none(v: Any) -> Optional[Decimal]:
        if v in (None, ""):
            return None
        try:
            return Decimal(str(v))
        except Exception:
            return None

    try:
        min_size = Decimal(str(inst.get("min_size") or "0.01"))
    except Exception:
        min_size = Decimal("0.01")
    try:
        tick_size = Decimal(str(inst.get("tick_size") or "0.01"))
    except Exception:
        tick_size = Decimal("0.01")

    out = _GrvtInstrumentFilters(
        instrument=str(inst.get("instrument") or f"{base}_USDT_Perp"),
        min_size=min_size if min_size > 0 else Decimal("0.01"),
        tick_size=tick_size if tick_size > 0 else Decimal("0.01"),
        base_decimals=_int_or_none(inst.get("base_decimals")),
        quote_decimals=_int_or_none(inst.get("quote_decimals")),
        funding_interval_hours=float(inst.get("funding_interval_hours")) if inst.get("funding_interval_hours") is not None else None,
        adjusted_funding_rate_cap_pp=_dec_or_none(inst.get("adjusted_funding_rate_cap")),
        adjusted_funding_rate_floor_pp=_dec_or_none(inst.get("adjusted_funding_rate_floor")),
        fetched_at=now,
    )
    _GRVT_INSTRUMENT_CACHE[cache_key] = out
    return out


def _grvt_round_qty(symbol: str, quantity: Decimal) -> Decimal:
    """Round qty down to GRVT min_size increments (best-effort)."""
    filters = _get_grvt_instrument_filters(symbol)
    step = filters.min_size if filters.min_size and filters.min_size > 0 else Decimal("0.01")
    steps = (quantity / step).to_integral_value(rounding=ROUND_DOWN)
    out = steps * step
    if out < step:
        out = step
    return out


def _utc_millis() -> int:
    return int(time.time() * 1000)


def _run_async(coro):
    """Run an async coroutine from sync code (best-effort)."""
    import asyncio
    import concurrent.futures

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    def _worker():
        return asyncio.run(coro)

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        return executor.submit(_worker).result(timeout=60)


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
    """
    Convert base-asset quantity into OKX contract ``sz`` and enforce minimums.

    Note: OKX swap orders use ``sz`` in *contracts*, not base coin units. For
    consistency with other helpers (Binance/Bybit/Bitget), callers pass base
    coin size here (e.g. ETH). We convert it using the instrument ``ctVal``
    (contract value) and then snap to ``lotSz`` / ``minSz`` in contract units.
    """
    filters = _get_okx_instrument_filters(symbol, base_url, inst_type)
    try:
        raw_base = Decimal(str(size))
    except InvalidOperation as exc:
        raise TradeExecutionError(f"`size` {size!r} is not a valid decimal quantity") from exc
    if raw_base <= 0:
        raise TradeExecutionError("`size` must be a positive number of contracts")

    ct_val = filters.contract_value
    if ct_val <= 0:
        raise TradeExecutionError("OKX instrument metadata invalid: ctVal must be positive")

    raw_contracts = raw_base / ct_val
    lot = filters.lot_size
    if lot <= 0:
        raise TradeExecutionError("OKX instrument metadata invalid: lotSz must be positive")

    units = (raw_contracts / lot).to_integral_value(rounding=ROUND_DOWN)
    normalised_contracts = units * lot

    if normalised_contracts < filters.min_size:
        min_base = filters.min_size * ct_val
        raise TradeExecutionError(
            f"`size` {raw_base} is below OKX minimum {min_base} base units "
            f"(minSz={filters.min_size} contracts, ctVal={ct_val}) for {symbol}; increase the order size"
        )

    size_str = format(normalised_contracts.normalize(), "f")
    return size_str, normalised_contracts


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

    quantity_str, normalised_qty = _normalise_binance_quantity(pair, quantity, base_url)

    def _effective_step() -> Decimal:
        filters = _get_binance_symbol_filters(pair, base_url)
        step = filters.step_size
        precision = filters.quantity_precision
        if precision is not None:
            precision_step = Decimal(1).scaleb(-precision) if precision > 0 else Decimal(1)
            if precision_step > step:
                step = precision_step
        return step

    def _submit(qty_str: str) -> tuple[requests.Response, Dict[str, Any], List[tuple[str, str]]]:
        submit_params = [
            ("symbol", pair),
            ("side", side.upper()),
            ("type", "MARKET"),
            ("timestamp", _utc_millis_str()),
            ("recvWindow", str(recv_window)),
            ("newOrderRespType", "RESULT"),
            ("quantity", qty_str),
        ]
        if reduce_only is not None:
            submit_params.append(("reduceOnly", "true" if reduce_only else "false"))
        if client_order_id:
            submit_params.append(("newClientOrderId", client_order_id))
        if position_side:
            submit_params.append(("positionSide", position_side.upper()))

        query = urlencode(submit_params)
        signature = _hmac_sha256_hexdigest(creds.secret_key, query)
        submit_params.append(("signature", signature))

        headers = {
            "X-MBX-APIKEY": creds.api_key,
            "User-Agent": USER_AGENT,
        }

        url = f"{base_url}/fapi/v1/order"
        LOGGER.info(
            "binance_order_submit symbol=%s side=%s quantity=%s params=%s",
            pair,
            side,
            qty_str,
            submit_params,
        )
        resp = _send_request("POST", url, params=submit_params, headers=headers)
        payload = _json_or_error(resp)
        return resp, payload, submit_params

    response, data, signed_params = _submit(quantity_str)
    if response.status_code != 200:
        # Common case: quantity snapped down to step size makes notional slightly below exchange minimum.
        if (
            isinstance(data, dict)
            and data.get("code") == -4164
            and reduce_only is not True
            and normalised_qty is not None
        ):
            step = _effective_step()
            bumped_qty = Decimal(str(normalised_qty)) + step
            bumped_str = _format_decimal_for_step(bumped_qty, step)
            response2, data2, signed_params2 = _submit(bumped_str)
            if response2.status_code == 200:
                response, data, signed_params = response2, data2, signed_params2
            else:
                data = data2
                response = response2

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


def get_binance_perp_account(
    *,
    recv_window: int = 5000,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    base_url: str = "https://fapi.binance.com",
) -> Dict[str, Any]:
    """Fetch Binance USDT-margined futures account snapshot via GET /fapi/v2/account."""
    creds = _resolve_binance_credentials(api_key, secret_key)

    params: List[tuple[str, str]] = [
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

    url = f"{base_url}/fapi/v2/account"
    response = _send_request("GET", url, params=params, headers=headers)
    data = _json_or_error(response)
    if response.status_code != 200:
        raise TradeExecutionError(f"Binance account error {response.status_code}: {data}")
    if isinstance(data, dict) and "code" in data and data.get("code") not in (0, "0", None):
        raise TradeExecutionError(f"Binance account reject: {data}")
    if not isinstance(data, dict):
        raise TradeExecutionError(f"Binance account payload malformed: {data!r}")
    return data


def get_binance_funding_fee_income(
    *,
    symbol: Optional[str] = None,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    limit: int = 1000,
    recv_window: int = 5000,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    base_url: str = "https://fapi.binance.com",
) -> List[Dict[str, Any]]:
    """Return Binance futures funding fee income records via GET /fapi/v1/income.

    Notes
    - Uses `incomeType=FUNDING_FEE`.
    - Amount is returned in `income` (typically USDT) with its own sign.
    """
    creds = _resolve_binance_credentials(api_key, secret_key)

    params: List[tuple[str, str]] = [
        ("incomeType", "FUNDING_FEE"),
        ("timestamp", _utc_millis_str()),
        ("recvWindow", str(recv_window)),
        ("limit", str(min(max(int(limit or 1000), 1), 1000))),
    ]
    if symbol:
        pair = symbol.upper()
        if not pair.endswith("USDT"):
            pair = f"{pair}USDT"
        params.append(("symbol", pair))
    if start_time_ms is not None:
        params.append(("startTime", str(int(start_time_ms))))
    if end_time_ms is not None:
        params.append(("endTime", str(int(end_time_ms))))

    query = urlencode(params)
    signature = _hmac_sha256_hexdigest(creds.secret_key, query)
    params.append(("signature", signature))

    headers = {"X-MBX-APIKEY": creds.api_key, "User-Agent": USER_AGENT}
    url = f"{base_url}/fapi/v1/income"
    response = _send_request("GET", url, params=params, headers=headers)
    data = _json_or_error(response)
    if response.status_code != 200:
        raise TradeExecutionError(f"Binance income query failed {response.status_code}: {data}")
    if isinstance(data, dict) and "code" in data and data.get("code") not in (0, "0", None):
        raise TradeExecutionError(f"Binance income reject: {data}")
    if not isinstance(data, list):
        raise TradeExecutionError(f"Binance income payload malformed: {data!r}")
    return data


def get_binance_perp_usdt_balance(
    *,
    recv_window: int = 5000,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    base_url: str = "https://fapi.binance.com",
) -> Dict[str, Any]:
    """Return a normalized USDT balance snapshot for Binance futures."""
    account = get_binance_perp_account(
        recv_window=recv_window,
        api_key=api_key,
        secret_key=secret_key,
        base_url=base_url,
    )
    assets = account.get("assets") or []
    usdt = None
    if isinstance(assets, list):
        for item in assets:
            if not isinstance(item, dict):
                continue
            if str(item.get("asset") or "").upper() == "USDT":
                usdt = item
                break
    out: Dict[str, Any] = {"currency": "USDT"}
    if isinstance(usdt, dict):
        out.update(
            {
                "wallet_balance": usdt.get("walletBalance"),
                "available_balance": usdt.get("availableBalance"),
                "margin_balance": usdt.get("marginBalance"),
                "unrealized_pnl": usdt.get("unrealizedProfit"),
            }
        )
    return out


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
    global _BINANCE_BAN_UNTIL_MS, _BINANCE_PRICE_ALL_CACHE

    pair = symbol.upper()
    if not pair.endswith("USDT"):
        pair = f"{pair}USDT"

    now = time.time()
    cached = _BINANCE_PRICE_CACHE.get(pair)
    if cached and (now - cached[1]) < _BINANCE_PRICE_CACHE_TTL_SECONDS:
        return float(cached[0])

    # If the IP is currently banned, only allow returning from cache (if available).
    if _BINANCE_BAN_UNTIL_MS and int(now * 1000) < _BINANCE_BAN_UNTIL_MS:
        all_prices, fetched_at = _BINANCE_PRICE_ALL_CACHE
        if all_prices and (now - fetched_at) < max(5.0, _BINANCE_PRICE_CACHE_TTL_SECONDS):
            if pair in all_prices:
                price = float(all_prices[pair])
                _BINANCE_PRICE_CACHE[pair] = (price, now)
                return price
        raise TradeExecutionError(
            f"Binance IP banned until {_BINANCE_BAN_UNTIL_MS}; use websocket/cached prices to avoid bans."
        )

    price_url = f"{base_url}/fapi/v1/ticker/price"

    def _record_price(val: float) -> float:
        _BINANCE_PRICE_CACHE[pair] = (float(val), now)
        return float(val)

    # Attempt per-symbol query first.
    response = _send_request("GET", price_url, params=[("symbol", pair)])
    data = _json_or_error(response)
    if response.status_code == 200 and isinstance(data, dict) and "price" in data:
        return _record_price(float(data["price"]))

    # Detect ban window and cache it, then fall back to bulk prices if possible.
    if isinstance(data, dict) and data.get("code") == -1003:
        msg = str(data.get("msg") or "")
        match = _BINANCE_BAN_UNTIL_RE.search(msg)
        if match:
            try:
                _BINANCE_BAN_UNTIL_MS = int(match.group(1))
            except Exception:
                _BINANCE_BAN_UNTIL_MS = max(_BINANCE_BAN_UNTIL_MS, int(now * 1000) + 60_000)

    # Fallback: bulk ticker/price, then read the requested symbol from the bulk map.
    response2 = _send_request("GET", price_url)
    data2 = _json_or_error(response2)
    if response2.status_code == 200 and isinstance(data2, list):
        prices: Dict[str, float] = {}
        for item in data2:
            if not isinstance(item, dict):
                continue
            sym = str(item.get("symbol") or "").upper()
            if not sym:
                continue
            try:
                px = float(item.get("price"))
            except Exception:
                continue
            if px > 0:
                prices[sym] = px
        if prices:
            _BINANCE_PRICE_ALL_CACHE = (prices, now)
            if pair in prices:
                return _record_price(prices[pair])

    raise TradeExecutionError(f"Binance price query failed: {data}")


def get_binance_perp_order(
    symbol: str,
    *,
    order_id: Optional[Union[int, str]] = None,
    client_order_id: Optional[str] = None,
    recv_window: int = 5000,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    base_url: str = "https://fapi.binance.com",
) -> Dict[str, Any]:
    """Fetch a Binance USDT perpetual order snapshot via GET /fapi/v1/order."""
    creds = _resolve_binance_credentials(api_key, secret_key)

    pair = symbol.upper()
    if not pair.endswith("USDT"):
        pair = f"{pair}USDT"

    if order_id is None and not client_order_id:
        raise TradeExecutionError("Either `order_id` or `client_order_id` is required")

    params: List[tuple[str, str]] = [
        ("symbol", pair),
        ("timestamp", _utc_millis_str()),
        ("recvWindow", str(recv_window)),
    ]
    if order_id is not None:
        params.append(("orderId", str(order_id)))
    if client_order_id:
        params.append(("origClientOrderId", str(client_order_id)))

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
    if isinstance(data, dict) and "code" in data and data.get("code") not in (0, "0", None):
        raise TradeExecutionError(f"Binance order query reject: {data}")
    return data if isinstance(data, dict) else {"data": data}


def set_binance_perp_leverage(
    symbol: str,
    leverage: Union[int, float, str],
    *,
    recv_window: int = 5000,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    base_url: str = "https://fapi.binance.com",
) -> Dict[str, Any]:
    """Set leverage for a Binance USDT-margined perpetual symbol."""
    creds = _resolve_binance_credentials(api_key, secret_key)

    pair = symbol.upper()
    if not pair.endswith("USDT"):
        pair = f"{pair}USDT"

    try:
        leverage_val = int(float(leverage))
    except Exception as exc:
        raise TradeExecutionError(f"Invalid leverage value: {leverage}") from exc
    if leverage_val <= 0:
        raise TradeExecutionError("Leverage must be greater than zero")

    params: List[tuple[str, str]] = [
        ("symbol", pair),
        ("leverage", str(leverage_val)),
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

    url = f"{base_url}/fapi/v1/leverage"
    response = _send_request("POST", url, params=params, headers=headers)
    data = _json_or_error(response)
    if response.status_code != 200:
        raise TradeExecutionError(f"Binance leverage error {response.status_code}: {data}")
    if "code" in data and data.get("code") not in (0, "0", None):
        raise TradeExecutionError(f"Binance leverage reject: {data}")
    return data


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


def get_okx_account_balance(
    *,
    ccy: Optional[str] = "USDT",
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    base_url: str = "https://www.okx.com",
) -> Dict[str, Any]:
    """Fetch OKX account balance snapshot via GET /api/v5/account/balance."""
    creds = _resolve_okx_credentials(api_key, secret_key, passphrase)

    request_path = "/api/v5/account/balance"
    params: List[tuple[str, str]] = []
    if ccy:
        params.append(("ccy", str(ccy).upper()))

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
        raise TradeExecutionError(f"OKX balance reject: {data}")
    payload = data.get("data") or []
    first = payload[0] if isinstance(payload, list) and payload else None
    if not isinstance(first, dict):
        return {"currency": (ccy or "").upper() or None, "raw": payload}
    details = first.get("details") or []
    cur = (ccy or "").upper() if ccy else None
    item = None
    if isinstance(details, list) and cur:
        for d in details:
            if not isinstance(d, dict):
                continue
            if str(d.get("ccy") or "").upper() == cur:
                item = d
                break
    if not isinstance(item, dict):
        item = first
    return {
        "currency": cur,
        "cash_bal": item.get("cashBal"),
        "avail_bal": item.get("availBal"),
        "eq": item.get("eq"),
        "u_time": first.get("uTime"),
        "raw": item,
    }


def get_okx_funding_fee_bills(
    *,
    symbol: str,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    inst_type: str = "SWAP",
    limit: int = 100,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    base_url: str = "https://www.okx.com",
) -> List[Dict[str, Any]]:
    """Return OKX funding fee bills (best-effort) via GET /api/v5/account/bills.

    We try server-side filtering with `type=8` first (funding fees on most OKX
    accounts). When that returns empty, we fall back to fetching without the
    `type` filter and then filter client-side.
    """
    creds = _resolve_okx_credentials(api_key, secret_key, passphrase)

    inst_id = symbol.upper()
    if not inst_id.endswith("-USDT-SWAP"):
        inst_id = f"{inst_id}-USDT-SWAP"

    request_path = "/api/v5/account/bills"

    def _do_query(type_filter: Optional[str]) -> List[Dict[str, Any]]:
        params: List[tuple[str, str]] = [("instType", inst_type.upper()), ("instId", inst_id)]
        if type_filter:
            params.append(("type", str(type_filter)))
        if start_time_ms is not None:
            params.append(("begin", str(int(start_time_ms))))
        if end_time_ms is not None:
            params.append(("end", str(int(end_time_ms))))
        params.append(("limit", str(min(max(int(limit or 100), 1), 100))))

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
            raise TradeExecutionError(f"OKX bills reject: {data}")
        payload = data.get("data") or []
        if not isinstance(payload, list):
            raise TradeExecutionError(f"OKX bills payload malformed: {data!r}")
        return payload

    rows = _do_query("8")
    if rows:
        return rows
    rows = _do_query(None)
    return [r for r in rows if str(r.get("type") or "").strip() == "8"]


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


def get_okx_swap_contract_value(
    symbol: str,
    *,
    base_url: str = "https://www.okx.com",
    inst_type: str = "SWAP",
) -> float:
    """Return OKX SWAP contract value (ctVal) for `symbol` (base units per contract)."""
    filters = _get_okx_instrument_filters(symbol, base_url, inst_type=inst_type)
    try:
        return float(filters.contract_value)
    except Exception as exc:
        raise TradeExecutionError(f"OKX ctVal invalid for {symbol}: {filters.contract_value}") from exc


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


def get_bybit_wallet_balance(
    *,
    account_type: str = "UNIFIED",
    coin: Optional[str] = "USDT",
    recv_window: int = 5000,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    base_url: str = "https://api.bybit.com",
) -> Dict[str, Any]:
    """Fetch Bybit wallet balance via GET /v5/account/wallet-balance."""
    creds = _resolve_bybit_credentials(api_key, secret_key)
    request_path = "/v5/account/wallet-balance"
    params: List[tuple[str, str]] = [("accountType", str(account_type).upper())]
    if coin:
        params.append(("coin", str(coin).upper()))

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
        raise TradeExecutionError(f"Bybit wallet balance reject: {data}")

    result = data.get("result") or {}
    rows = result.get("list") if isinstance(result, dict) else None
    first = rows[0] if isinstance(rows, list) and rows else None
    if not isinstance(first, dict):
        return {"currency": (coin or "").upper() or None, "raw": data}
    coins = first.get("coin") or []
    cur = (coin or "").upper() if coin else None
    item = None
    if isinstance(coins, list) and cur:
        for c in coins:
            if not isinstance(c, dict):
                continue
            if str(c.get("coin") or "").upper() == cur:
                item = c
                break
    if not isinstance(item, dict):
        item = first
    available = item.get("availableToWithdraw") or item.get("availableBalance")
    if available in ("", None):
        try:
            equity = float(item.get("equity") or 0.0)
            pos_im = float(item.get("totalPositionIM") or 0.0)
            order_im = float(item.get("totalOrderIM") or 0.0)
            est = equity - pos_im - order_im
            if est < 0:
                est = 0.0
            available = f"{est:.8f}"
        except Exception:
            available = None
    return {
        "currency": cur,
        "wallet_balance": item.get("walletBalance") or item.get("equity"),
        "available_balance": available,
        "equity": item.get("equity"),
        "raw": item,
    }


def get_bybit_funding_fee_transactions(
    *,
    symbol: str,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    category: str = "linear",
    limit: int = 50,
    recv_window: int = 5000,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    base_url: str = "https://api.bybit.com",
) -> List[Dict[str, Any]]:
    """Return Bybit funding fee transaction logs via GET /v5/account/transaction-log."""
    creds = _resolve_bybit_credentials(api_key, secret_key)

    symbol_id = symbol.upper()
    if not symbol_id.endswith("USDT"):
        symbol_id = f"{symbol_id}USDT"

    request_path = "/v5/account/transaction-log"
    params: List[tuple[str, str]] = [
        ("category", str(category).lower()),
        ("symbol", symbol_id),
        ("type", "FUNDING"),
        ("limit", str(min(max(int(limit or 50), 1), 50))),
    ]
    if start_time_ms is not None:
        params.append(("startTime", str(int(start_time_ms))))
    if end_time_ms is not None:
        params.append(("endTime", str(int(end_time_ms))))

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
        raise TradeExecutionError(f"Bybit transaction-log reject: {data}")
    result = data.get("result") or {}
    rows = result.get("list") if isinstance(result, dict) else None
    return rows if isinstance(rows, list) else []


def get_bybit_linear_order(
    symbol: str,
    *,
    order_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
    category: str = "linear",
    recv_window: int = 5000,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    base_url: str = "https://api.bybit.com",
) -> Dict[str, Any]:
    """Fetch a Bybit order status/fill snapshot via `/v5/order/realtime`."""
    creds = _resolve_bybit_credentials(api_key, secret_key)

    symbol_id = symbol.upper()
    if not symbol_id.endswith("USDT"):
        symbol_id = f"{symbol_id}USDT"

    if not order_id and not client_order_id:
        raise TradeExecutionError("Either `order_id` or `client_order_id` is required")

    request_path = "/v5/order/realtime"
    params: List[tuple[str, str]] = [
        ("category", str(category).lower()),
        ("symbol", symbol_id),
    ]
    if order_id:
        params.append(("orderId", str(order_id)))
    if client_order_id:
        params.append(("orderLinkId", str(client_order_id)))

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
        raise TradeExecutionError(f"Bybit order query reject: {data}")
    result = data.get("result") or {}
    order_list = result.get("list") if isinstance(result, dict) else None
    if isinstance(order_list, list) and order_list:
        first = order_list[0]
        return first if isinstance(first, dict) else {"data": first}
    return data


def get_bitget_usdt_perp_positions(
    *,
    symbol: Optional[str] = None,
    product_type: str = "USDT-FUTURES",
    margin_coin: Optional[str] = "USDT",
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    base_url: str = "https://api.bitget.com",
) -> List[Dict[str, Any]]:
    """Fetch Bitget USDT-margined perpetual positions.

    Bitget V1 endpoints were decommissioned; this uses the V2 private endpoint
    ``/api/v2/mix/position/all-position``. The response contains every position
    for the requested ``productType``, so callers can optionally filter the
    payload by ``symbol``.
    """
    creds = _resolve_bitget_credentials(api_key, secret_key, passphrase)

    inst_filter: Optional[str] = None
    if symbol:
        inst_filter = _normalise_bitget_symbol(symbol)

    request_path = "/api/v2/mix/position/all-position"
    params: List[tuple[str, str]] = [("productType", _normalise_bitget_product_type(product_type))]
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


def get_bitget_usdt_perp_account(
    *,
    product_type: str = "USDT-FUTURES",
    margin_coin: str = "USDT",
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    base_url: str = "https://api.bitget.com",
) -> Dict[str, Any]:
    """Fetch Bitget mix account snapshot (best-effort V2 endpoint)."""
    creds = _resolve_bitget_credentials(api_key, secret_key, passphrase)
    request_path = "/api/v2/mix/account/accounts"
    params: List[tuple[str, str]] = [
        ("productType", _normalise_bitget_product_type(product_type)),
        ("marginCoin", str(margin_coin).upper()),
    ]

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
        raise TradeExecutionError(f"Bitget account reject: {data}")
    payload = data.get("data")
    if isinstance(payload, list) and payload:
        first = payload[0] if isinstance(payload[0], dict) else {"data": payload[0]}
        return first
    if isinstance(payload, dict):
        return payload
    return data


def get_bitget_usdt_balance(
    *,
    product_type: str = "USDT-FUTURES",
    margin_coin: str = "USDT",
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    base_url: str = "https://api.bitget.com",
) -> Dict[str, Any]:
    """Return a normalized USDT balance snapshot for Bitget (best-effort)."""
    data = get_bitget_usdt_perp_account(
        product_type=product_type,
        margin_coin=margin_coin,
        api_key=api_key,
        secret_key=secret_key,
        passphrase=passphrase,
        base_url=base_url,
    )
    return {
        "currency": str(margin_coin).upper(),
        "available_balance": data.get("available"),
        "equity": data.get("equity") or data.get("accountEquity") or data.get("usdtEquity"),
        "raw": data,
    }


def get_bitget_funding_fee_bills(
    *,
    symbol: str,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    product_type: str = "USDT-FUTURES",
    margin_coin: str = "USDT",
    limit: int = 100,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    base_url: str = "https://api.bitget.com",
) -> List[Dict[str, Any]]:
    """Return Bitget funding fee bill rows (best-effort).

    Bitget OpenAPI V2 has seen multiple bill endpoints across versions.
    We try a short list of known paths and return the first successful response.
    """
    creds = _resolve_bitget_credentials(api_key, secret_key, passphrase)

    params: List[tuple[str, str]] = [
        ("productType", _normalise_bitget_product_type(product_type)),
        ("marginCoin", str(margin_coin).upper()),
        ("symbol", _normalise_bitget_symbol(symbol)),
        ("limit", str(min(max(int(limit or 100), 1), 100))),
    ]
    if start_time_ms is not None:
        params.append(("startTime", str(int(start_time_ms))))
    if end_time_ms is not None:
        params.append(("endTime", str(int(end_time_ms))))

    candidate_paths = (
        "/api/v2/mix/account/account-bill",
        "/api/v2/mix/account/accountBill",
        "/api/v2/mix/account/bill",
    )

    last_error: Optional[str] = None
    for request_path in candidate_paths:
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

        # 404/40404: path likely not present in this deployment.
        if response.status_code == 404 or str(data.get("code") or "") == "40404":
            last_error = f"{request_path} not found: {data}"
            continue
        if response.status_code != 200 or data.get("code") != "00000":
            last_error = f"{request_path} reject: {data}"
            continue

        payload = data.get("data")
        if isinstance(payload, dict):
            rows = payload.get("list") or payload.get("dataList") or payload.get("rows")
            if isinstance(rows, list):
                return _filter_bitget_funding_rows(rows)
        if isinstance(payload, list):
            return _filter_bitget_funding_rows(payload)
        return []

    raise TradeExecutionError(f"Bitget account bill reject: {last_error or 'no valid endpoint'}")


def _filter_bitget_funding_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Best-effort client-side filter for Bitget funding fee bill rows."""
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        raw_type = str(r.get("businessType") or r.get("bizType") or r.get("type") or "").lower()
        raw_sub = str(r.get("subType") or r.get("subBizType") or "").lower()
        if "fund" in raw_type or "fund" in raw_sub:
            out.append(r)
            continue
        # Some payloads use numeric enums; keep those too when we can detect.
        if raw_type in {"8", "fundingfee", "funding_fee", "funding"}:
            out.append(r)
            continue
    return out


def _normalise_bitget_symbol(symbol: str) -> str:
    inst = symbol.upper()
    if inst.endswith("_UMCBL"):
        inst = inst[: -len("_UMCBL")]
    if inst.endswith("USDT"):
        return inst
    return f"{inst}USDT"


def _normalise_bitget_product_type(product_type: str) -> str:
    raw = (product_type or "").strip()
    if not raw:
        return "USDT-FUTURES"
    raw_upper = raw.upper()
    # Legacy v1 aliases.
    if raw_upper in {"UMCBL", "USDT_PERP", "USDT-PERP"}:
        return "USDT-FUTURES"
    return raw_upper


def _get_bitget_contract_filters(
    symbol: str,
    *,
    base_url: str,
    product_type: str = "USDT-FUTURES",
) -> _BitgetContractFilters:
    key_symbol = _normalise_bitget_symbol(symbol)
    normalized_product = _normalise_bitget_product_type(product_type)
    cache_key = (base_url, normalized_product.lower(), key_symbol)
    now = time.time()
    cached = _BITGET_CONTRACT_CACHE.get(cache_key)
    if cached and now - cached.fetched_at < _BITGET_CONTRACT_CACHE_TTL:
        return cached

    request_path = "/api/v2/mix/market/contracts"
    params: List[tuple[str, str]] = [("productType", normalized_product)]
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
    product_type: str = "USDT-FUTURES",
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
    url = f"{base_url}/api/v2/mix/market/ticker"
    response = _send_request("GET", url, params=[("symbol", inst), ("productType", "USDT-FUTURES")])
    data = _json_or_error(response)
    if response.status_code != 200 or data.get("code") != "00000":
        raise TradeExecutionError(f"Bitget price query failed: {data}")

    payload_list = data.get("data") or []
    payload = payload_list[0] if isinstance(payload_list, list) and payload_list else {}
    price_raw = payload.get("lastPr")
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
    product_type: str = "USDT-FUTURES",
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
    product_type: str = "USDT-FUTURES",
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
        "productType": _normalise_bitget_product_type(product_type),
        "marginCoin": margin_coin.upper(),
        "leverage": format(leverage_value.normalize(), "f"),
    }
    if hold_side:
        payload["holdSide"] = hold_side

    request_path = "/api/v2/mix/account/set-leverage"
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
        sanitized = _sanitize_okx_client_order_id(client_order_id)
        if sanitized:
            payload["clOrdId"] = sanitized

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


def get_okx_swap_order(
    symbol: str,
    *,
    ord_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    base_url: str = "https://www.okx.com",
) -> Dict[str, Any]:
    """Fetch an OKX swap order by `ordId` or `clOrdId`."""
    creds = _resolve_okx_credentials(api_key, secret_key, passphrase)

    inst_id = symbol.upper()
    if not inst_id.endswith("-USDT-SWAP"):
        inst_id = f"{inst_id}-USDT-SWAP"

    if not ord_id and not client_order_id:
        raise TradeExecutionError("Either `ord_id` or `client_order_id` is required")

    request_path = "/api/v5/trade/order"
    params: List[tuple[str, str]] = [("instId", inst_id)]
    if ord_id:
        params.append(("ordId", str(ord_id)))
    if client_order_id:
        sanitized = _sanitize_okx_client_order_id(client_order_id)
        if sanitized:
            params.append(("clOrdId", sanitized))

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
        raise TradeExecutionError(f"OKX order query reject: {data}")
    payload = data.get("data") or []
    if isinstance(payload, list) and payload:
        return payload[0] if isinstance(payload[0], dict) else {"data": payload[0]}
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

    base_amount = Decimal(str(notional_usdt)) / Decimal(str(price))
    size_str, _ = _normalise_okx_size(symbol, base_amount, base_url)
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
    product_type: str = "USDT-FUTURES",
    margin_coin: str = "USDT",
    margin_mode: str = "crossed",
    reduce_only: Optional[bool] = None,
    trade_side: Optional[str] = None,
    pos_side: Optional[str] = None,
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

    size_str, _ = _normalise_bitget_size(inst, size, base_url=base_url, product_type=product_type)

    normalized_side = side.lower().strip()
    if normalized_side in {"long", "buy", "b"}:
        normalized_side = "buy"
    elif normalized_side in {"short", "sell", "s"}:
        normalized_side = "sell"
    elif normalized_side not in {"buy", "sell"}:
        raise TradeExecutionError("Bitget side must be BUY/LONG or SELL/SHORT")

    def _unilateral_trade_side(reduce_only_flag: Optional[bool]) -> str:
        if reduce_only_flag is None:
            return "open"
        return "close" if reduce_only_flag else "open"

    def _hedge_trade_side(side_value: str, reduce_only_flag: Optional[bool]) -> str:
        closing = bool(reduce_only_flag)
        if side_value == "buy":
            return "close_short" if closing else "open_long"
        return "close_long" if closing else "open_short"

    chosen_trade_side = (trade_side or "").strip().lower() if trade_side else ""
    if not chosen_trade_side:
        chosen_trade_side = _unilateral_trade_side(reduce_only)

    payload: Dict[str, Any] = {
        "symbol": inst,
        "productType": _normalise_bitget_product_type(product_type),
        "marginCoin": margin_coin.upper(),
        "marginMode": str(margin_mode).lower(),
        "size": size_str,
        "orderType": "market",
        "side": normalized_side,
        "tradeSide": chosen_trade_side,
    }
    if pos_side:
        payload["posSide"] = str(pos_side).lower()
    # Bitget V2 mix order uses `tradeSide` (open/close or open_long/close_short, etc).
    # Some accounts reject `reduceOnly` for this endpoint; keep the payload minimal.
    if client_order_id:
        payload["clientOid"] = client_order_id

    request_path = "/api/v2/mix/order/place-order"
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
        retryable_codes = {"40774", "22002"}
        if isinstance(data, dict) and data.get("code") in retryable_codes and not trade_side:
            # Position mode mismatch (40774) or close semantics mismatch (22002); retry once with hedge tradeSide.
            fallback_trade_side = _hedge_trade_side(normalized_side, reduce_only)
            if fallback_trade_side != chosen_trade_side:
                payload["tradeSide"] = fallback_trade_side
                body_str = _compact_json(payload)
                message = f"{timestamp}POST{request_path}{body_str}"
                signature = _hmac_sha256_base64(creds.secret_key, message)
                headers["ACCESS-SIGN"] = signature
                response = _send_request("POST", url, data=body_str, headers=headers)
                data = _json_or_error(response)
                if response.status_code == 200 and isinstance(data, dict) and data.get("code") == "00000":
                    pass
                else:
                    raise TradeExecutionError(f"Bitget reject: {data}")
            else:
                raise TradeExecutionError(f"Bitget reject: {data}")
        else:
            raise TradeExecutionError(f"Bitget reject: {data}")
    result_payload = data.get("data")
    if isinstance(result_payload, dict):
        result_payload.setdefault("symbol", inst)
        result_payload.setdefault("marginCoin", margin_coin.upper())
        result_payload.setdefault("size", size_str)
        # Preserve the original buy/sell intent for downstream logging.
        result_payload.setdefault("side", normalized_side)
    return data


def get_bitget_usdt_perp_order_detail(
    symbol: str,
    *,
    order_id: Optional[str] = None,
    client_order_id: Optional[str] = None,
    product_type: str = "USDT-FUTURES",
    api_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    passphrase: Optional[str] = None,
    base_url: str = "https://api.bitget.com",
) -> Dict[str, Any]:
    """Fetch Bitget USDT perpetual order detail (best-effort, V2 endpoint)."""
    creds = _resolve_bitget_credentials(api_key, secret_key, passphrase)

    inst = _normalise_bitget_symbol(symbol)
    if not order_id and not client_order_id:
        raise TradeExecutionError("Either `order_id` or `client_order_id` is required")

    request_path = "/api/v2/mix/order/detail"
    params: List[tuple[str, str]] = [
        ("symbol", inst),
        ("productType", _normalise_bitget_product_type(product_type)),
    ]
    if order_id:
        params.append(("orderId", str(order_id)))
    if client_order_id:
        params.append(("clientOid", str(client_order_id)))

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
        raise TradeExecutionError(f"Bitget order detail reject: {data}")
    payload = data.get("data")
    if isinstance(payload, list) and payload:
        return payload[0] if isinstance(payload[0], dict) else {"data": payload[0]}
    if isinstance(payload, dict):
        return payload
    return data


def get_hyperliquid_user_state(
    *,
    address: Optional[str] = None,
    private_key: Optional[str] = None,
    base_url: Optional[str] = None,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    """Fetch Hyperliquid user state (public endpoint, but requires an address)."""
    if HyperliquidInfo is None:
        raise TradeExecutionError("hyperliquid-python-sdk is required for Hyperliquid trading/position APIs")
    creds = _resolve_hyperliquid_credentials(private_key, address, base_url=base_url)
    info = HyperliquidInfo(base_url=creds.base_url, skip_ws=True, timeout=timeout)
    state = info.user_state(creds.address)
    if not isinstance(state, dict):
        raise TradeExecutionError(f"Hyperliquid user_state payload malformed: {state!r}")
    return state


def get_hyperliquid_perp_positions(
    *,
    address: Optional[str] = None,
    private_key: Optional[str] = None,
    base_url: Optional[str] = None,
    timeout: float = 10.0,
) -> List[Dict[str, Any]]:
    """Return Hyperliquid perp positions as a normalized list."""
    state = get_hyperliquid_user_state(address=address, private_key=private_key, base_url=base_url, timeout=timeout)
    positions = state.get("assetPositions") or []
    return positions if isinstance(positions, list) else []


def get_hyperliquid_balance_summary(
    *,
    address: Optional[str] = None,
    private_key: Optional[str] = None,
    base_url: Optional[str] = None,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    """Return a normalized account balance summary for Hyperliquid (USDC-margined)."""
    state = get_hyperliquid_user_state(address=address, private_key=private_key, base_url=base_url, timeout=timeout)
    summary = state.get("marginSummary") or {}
    cross = state.get("crossMarginSummary") or {}
    withdrawable = summary.get("withdrawable")
    if withdrawable in (None, "", "null"):
        withdrawable = state.get("withdrawable")
    return {
        "currency": "USDC",
        "account_value": summary.get("accountValue"),
        "total_margin_used": summary.get("totalMarginUsed"),
        "total_ntl_pos": summary.get("totalNtlPos"),
        "withdrawable": withdrawable,
        "available_balance": withdrawable,
        "raw_margin_summary": summary,
        "raw_cross_margin_summary": cross,
    }


def get_grvt_balance_summary(
    *,
    api_key: Optional[str] = None,
    private_key: Optional[str] = None,
    trading_account_id: Optional[str] = None,
    environment: Optional[str] = None,
) -> Dict[str, Any]:
    """Return a normalized account balance summary for GRVT (USDT-margined)."""
    creds = _resolve_grvt_credentials(
        api_key=api_key,
        private_key=private_key,
        trading_account_id=trading_account_id,
        environment=environment,
    )
    client = _get_grvt_client(creds=creds)
    summary = client.get_account_summary()  # ccxt-style wrapper around /account_summary
    if not isinstance(summary, dict):
        raise TradeExecutionError(f"GRVT get_account_summary payload malformed: {summary!r}")
    settle = str(summary.get("settle_currency") or "USDT").upper()
    return {
        "currency": settle,
        "available_balance": summary.get("available_balance"),
        "wallet_balance": summary.get("total_equity"),
        "equity": summary.get("total_equity"),
        "unrealized_pnl": summary.get("unrealized_pnl"),
        "raw_summary": summary,
    }


def get_grvt_perp_positions(
    *,
    symbol: Optional[str] = None,
    api_key: Optional[str] = None,
    private_key: Optional[str] = None,
    trading_account_id: Optional[str] = None,
    environment: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return GRVT perp positions (private API)."""
    creds = _resolve_grvt_credentials(
        api_key=api_key,
        private_key=private_key,
        trading_account_id=trading_account_id,
        environment=environment,
    )
    client = _get_grvt_client(creds=creds)
    instruments: List[str] = []
    if symbol:
        filters = _get_grvt_instrument_filters(symbol, environment=creds.environment)
        instruments = [filters.instrument]
    rows = client.fetch_positions(symbols=instruments)
    if not isinstance(rows, list):
        raise TradeExecutionError(f"GRVT fetch_positions payload malformed: {rows!r}")
    return rows


def get_grvt_order(
    order_id: str,
    *,
    api_key: Optional[str] = None,
    private_key: Optional[str] = None,
    trading_account_id: Optional[str] = None,
    environment: Optional[str] = None,
) -> Dict[str, Any]:
    """Fetch an order by exchange order_id (private API)."""
    if not order_id:
        raise TradeExecutionError("GRVT order_id is required")
    creds = _resolve_grvt_credentials(
        api_key=api_key,
        private_key=private_key,
        trading_account_id=trading_account_id,
        environment=environment,
    )
    client = _get_grvt_client(creds=creds)
    data = client.fetch_order(id=str(order_id))
    if not isinstance(data, dict):
        raise TradeExecutionError(f"GRVT fetch_order payload malformed: {data!r}")
    return data


def place_grvt_perp_market_order(
    symbol: str,
    side: str,
    quantity: Union[str, float, Decimal, int],
    *,
    reduce_only: bool = False,
    client_order_id: Optional[str] = None,
    api_key: Optional[str] = None,
    private_key: Optional[str] = None,
    trading_account_id: Optional[str] = None,
    environment: Optional[str] = None,
) -> Dict[str, Any]:
    """Place a GRVT perp market order (best-effort).

    Notes
    - GRVT expects amount to respect min_size increments; we round down to min_size.
    - reduce_only is forwarded via params["reduce_only"].
    """
    sym = (symbol or "").upper().strip()
    if not sym:
        raise TradeExecutionError("GRVT symbol is required")
    side_key = (side or "").lower().strip()
    if side_key not in {"buy", "sell"}:
        raise TradeExecutionError("GRVT side must be buy/sell")

    creds = _resolve_grvt_credentials(
        api_key=api_key,
        private_key=private_key,
        trading_account_id=trading_account_id,
        environment=environment,
    )
    filters = _get_grvt_instrument_filters(sym, environment=creds.environment)
    qty_dec = _coerce_positive_quantity(quantity)
    qty_dec = _grvt_round_qty(sym, qty_dec)

    client = _get_grvt_client(creds=creds)
    params = {"reduce_only": bool(reduce_only), "client_order_id": _grvt_client_order_id(client_order_id)}
    resp = client.create_order(filters.instrument, "market", side_key, str(qty_dec), None, params=params)
    if not isinstance(resp, dict) or not resp:
        raise TradeExecutionError(f"GRVT create_order returned empty payload: {resp!r}")
    return resp


def get_grvt_funding_payment_history(
    *,
    symbol: Optional[str] = None,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    limit: int = 1000,
    api_key: Optional[str] = None,
    private_key: Optional[str] = None,
    trading_account_id: Optional[str] = None,
    environment: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Query GRVT funding payment ledger (private API) and normalize sign.

    GRVT docs: amount is positive if paid, negative if received.
    We normalize to `income` semantics: positive -> received, negative -> paid.
    """
    if GrvtRawSync is None or GrvtApiConfig is None or grvt_raw_types is None:
        raise TradeExecutionError("grvt-pysdk is required for GRVT funding payment history")

    creds = _resolve_grvt_credentials(
        api_key=api_key,
        private_key=private_key,
        trading_account_id=trading_account_id,
        environment=environment,
    )

    start_ns = str(int(start_time_ms) * 1_000_000) if start_time_ms else None
    end_ns = str(int(end_time_ms) * 1_000_000) if end_time_ms else None
    inst = None
    if symbol:
        filters = _get_grvt_instrument_filters(symbol, environment=creds.environment)
        inst = filters.instrument

    cfg = GrvtApiConfig(
        env=_grvt_raw_env_enum(creds.environment),
        trading_account_id=creds.trading_account_id,
        private_key=creds.private_key,
        api_key=creds.api_key,
        logger=LOGGER,
    )
    client = GrvtRawSync(cfg)
    req = grvt_raw_types.ApiFundingPaymentHistoryRequest(
        sub_account_id=str(creds.trading_account_id),
        instrument=str(inst) if inst else None,
        start_time=start_ns,
        end_time=end_ns,
        limit=int(limit) if int(limit) > 0 else 500,
        cursor=None,
    )
    resp = client.funding_payment_history_v1(req)
    # resp can be GrvtError or ApiFundingPaymentHistoryResponse
    if hasattr(resp, "code") and hasattr(resp, "message"):
        raise TradeExecutionError(f"GRVT funding_payment_history reject: {resp}")
    rows = getattr(resp, "result", None)
    if not isinstance(rows, list):
        return []

    out: List[Dict[str, Any]] = []
    for item in rows:
        if not hasattr(item, "event_time"):
            continue
        try:
            ts_ms = int(int(str(getattr(item, "event_time", "0") or "0")) / 1_000_000)
        except Exception:
            ts_ms = None
        try:
            amount = float(str(getattr(item, "amount", "0") or "0"))
        except Exception:
            amount = None
        currency = str(getattr(item, "currency", "") or "").upper() or None
        instrument = str(getattr(item, "instrument", "") or "") or None
        # Normalize: positive income means received (opposite of GRVT amount).
        income = (-amount) if amount is not None else None
        out.append(
            {
                "time": ts_ms,
                "instrument": instrument,
                "currency": currency,
                "income": income,
                "raw_amount": amount,
                "raw": {
                    "tx_id": str(getattr(item, "tx_id", "") or ""),
                    "sub_account_id": str(getattr(item, "sub_account_id", "") or ""),
                },
            }
        )
    return out


def get_grvt_fill_history(
    *,
    symbol: Optional[str] = None,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    limit: int = 200,
    api_key: Optional[str] = None,
    private_key: Optional[str] = None,
    trading_account_id: Optional[str] = None,
    environment: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetch GRVT private fill history (executions) for an account.

    Returns a normalized list of fills with:
      - time (ms)
      - instrument
      - price
      - size
      - fee
      - client_order_id
      - order_id
      - is_buyer
    """
    if GrvtRawSync is None or GrvtApiConfig is None or grvt_raw_types is None:
        raise TradeExecutionError("grvt-pysdk is required for GRVT fill history")

    creds = _resolve_grvt_credentials(
        api_key=api_key,
        private_key=private_key,
        trading_account_id=trading_account_id,
        environment=environment,
    )

    start_ns = str(int(start_time_ms) * 1_000_000) if start_time_ms else None
    end_ns = str(int(end_time_ms) * 1_000_000) if end_time_ms else None
    base = (symbol or "").upper().strip() if symbol else None

    cfg = GrvtApiConfig(
        env=_grvt_raw_env_enum(creds.environment),
        trading_account_id=creds.trading_account_id,
        private_key=creds.private_key,
        api_key=creds.api_key,
        logger=LOGGER,
    )
    client = GrvtRawSync(cfg)
    req = grvt_raw_types.ApiFillHistoryRequest(
        sub_account_id=str(creds.trading_account_id),
        kind=None,
        base=[base] if base else None,
        quote=["USDT"] if base else None,
        start_time=start_ns,
        end_time=end_ns,
        limit=int(limit) if int(limit) > 0 else 200,
        cursor=None,
    )
    resp = client.fill_history_v1(req)
    if hasattr(resp, "code") and hasattr(resp, "message"):
        raise TradeExecutionError(f"GRVT fill_history reject: {resp}")
    rows = getattr(resp, "result", None)
    if not isinstance(rows, list):
        return []

    out: List[Dict[str, Any]] = []
    for item in rows:
        if not hasattr(item, "event_time"):
            continue
        try:
            ts_ms = int(int(str(getattr(item, "event_time", "0") or "0")) / 1_000_000)
        except Exception:
            ts_ms = None
        try:
            price = float(str(getattr(item, "price", "0") or "0"))
        except Exception:
            price = None
        try:
            size = float(str(getattr(item, "size", "0") or "0"))
        except Exception:
            size = None
        try:
            fee = float(str(getattr(item, "fee", "0") or "0"))
        except Exception:
            fee = None
        out.append(
            {
                "time": ts_ms,
                "instrument": str(getattr(item, "instrument", "") or ""),
                "price": price,
                "size": size,
                "fee": fee,
                "client_order_id": str(getattr(item, "client_order_id", "") or ""),
                "order_id": str(getattr(item, "order_id", "") or ""),
                "is_buyer": bool(getattr(item, "is_buyer", False)),
                "raw": {
                    "trade_id": str(getattr(item, "trade_id", "") or ""),
                    "venue": str(getattr(item, "venue", "") or ""),
                },
            }
        )
    return out


def get_lighter_balance_summary(
    *,
    account_index: Optional[Union[int, str]] = None,
    base_url: Optional[str] = None,
    timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """Return a normalized balance summary for Lighter.

    Notes
    - This uses the public `GET /account?by=index&value=...` endpoint (no auth).
    - Collateral on Lighter is USDC; we normalize currency to USDC.
    """

    resolved_account_index = account_index
    if resolved_account_index in (None, ""):
        resolved_account_index = (
            getattr(CONFIG_PRIVATE, "LIGHTER_ACCOUNT_INDEX", None) if CONFIG_PRIVATE else None
        ) or os.getenv("LIGHTER_ACCOUNT_INDEX")
    if resolved_account_index in (None, ""):
        raise TradeExecutionError("Lighter balance requires LIGHTER_ACCOUNT_INDEX (or account_index=...)")

    try:
        resolved_account_index_int = int(str(resolved_account_index))
    except Exception as exc:
        raise TradeExecutionError(f"Invalid LIGHTER_ACCOUNT_INDEX: {resolved_account_index!r}") from exc

    base = (base_url or getattr(config, "LIGHTER_REST_BASE_URL", "") or "").rstrip("/")
    if not base:
        base = "https://mainnet.zklighter.elliot.ai/api/v1"

    url = f"{base}/account"
    response = _send_request(
        "GET",
        url,
        params=[("by", "index"), ("value", str(resolved_account_index_int))],
        timeout=float(timeout) if timeout is not None else REQUEST_TIMEOUT,
    )
    data = _json_or_error(response)
    if response.status_code != 200 or not isinstance(data, dict) or data.get("code") != 200:
        raise TradeExecutionError(f"Lighter account query failed {response.status_code}: {data}")

    accounts = data.get("accounts") or []
    account = accounts[0] if isinstance(accounts, list) and accounts and isinstance(accounts[0], dict) else {}

    return {
        "currency": "USDC",
        "available_balance": account.get("available_balance"),
        "wallet_balance": account.get("collateral"),
        "account_value": account.get("total_asset_value"),
        "account_index": account.get("account_index"),
        "l1_address": account.get("l1_address"),
        "status": account.get("status"),
        "raw_account": account,
    }


def get_lighter_market_meta(
    symbol: str,
    *,
    base_url: str = "https://mainnet.zklighter.elliot.ai",
    timeout: float = 10.0,
) -> Dict[str, Any]:
    """Return Lighter market metadata for a symbol via public REST GET /api/v1/orderBooks."""
    sym = (symbol or "").upper().strip()
    if not sym:
        raise TradeExecutionError("Lighter symbol is required")
    url = f"{base_url.rstrip('/')}/api/v1/orderBooks"
    resp = _send_request("GET", url, timeout=int(timeout))
    data = _json_or_error(resp)
    if resp.status_code != 200 or not isinstance(data, dict) or data.get("code") != 200:
        raise TradeExecutionError(f"Lighter orderBooks query failed {resp.status_code}: {data}")
    for entry in data.get("order_books") or []:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("symbol") or "").upper() != sym:
            continue
        if str(entry.get("status") or "").lower() not in {"active", ""}:
            continue
        return entry
    raise TradeExecutionError(f"Lighter market not found/active: {sym}")


def get_lighter_orderbook_orders(
    market_id: int,
    *,
    limit: int = 50,
    base_url: str = "https://mainnet.zklighter.elliot.ai",
    timeout: float = 10.0,
) -> Dict[str, Any]:
    """Return Lighter L2 order book via public REST GET /api/v1/orderBookOrders."""
    url = f"{base_url.rstrip('/')}/api/v1/orderBookOrders"
    params = [("market_id", str(int(market_id))), ("limit", str(int(limit)))]
    resp = _send_request("GET", url, params=params, timeout=int(timeout))
    data = _json_or_error(resp)
    if resp.status_code != 200 or not isinstance(data, dict) or data.get("code") != 200:
        raise TradeExecutionError(f"Lighter orderBookOrders query failed {resp.status_code}: {data}")
    return data


def get_lighter_recent_trades(
    market_id: int,
    *,
    limit: int = 50,
    base_url: str = "https://mainnet.zklighter.elliot.ai",
    timeout: float = 10.0,
) -> Dict[str, Any]:
    """Return recent trades via public REST GET /api/v1/recentTrades."""
    url = f"{base_url.rstrip('/')}/api/v1/recentTrades"
    params = [("market_id", str(int(market_id))), ("limit", str(int(limit)))]
    resp = _send_request("GET", url, params=params, timeout=int(timeout))
    data = _json_or_error(resp)
    if resp.status_code != 200 or not isinstance(data, dict) or data.get("code") != 200:
        raise TradeExecutionError(f"Lighter recentTrades failed {resp.status_code}: {data}")
    return data


def get_lighter_funding_rates_map(
    *,
    base_url: str = "https://mainnet.zklighter.elliot.ai",
    timeout: float = 10.0,
) -> Dict[str, Any]:
    """Return funding rates via public REST GET /api/v1/funding-rates."""
    url = f"{base_url.rstrip('/')}/api/v1/funding-rates"
    resp = _send_request("GET", url, timeout=int(timeout))
    data = _json_or_error(resp)
    if resp.status_code != 200 or not isinstance(data, dict) or data.get("code") != 200:
        raise TradeExecutionError(f"Lighter funding-rates query failed {resp.status_code}: {data}")
    out: Dict[str, Any] = {"timestamp": datetime.now(timezone.utc).isoformat(), "rates": {}}
    for entry in data.get("funding_rates") or []:
        if not isinstance(entry, dict):
            continue
        sym = str(entry.get("symbol") or "").upper()
        if not sym:
            continue
        try:
            out["rates"][sym] = float(entry.get("rate"))
        except Exception:
            continue
    return out


def place_lighter_perp_market_order(
    symbol: str,
    side: str,
    size: Union[str, float, Decimal, int],
    *,
    reduce_only: bool = False,
    client_order_id: Optional[Union[int, str]] = None,
    max_slippage_bps: float = 30.0,
    private_key: Optional[str] = None,
    account_index: Optional[Union[int, str]] = None,
    api_key_index: Optional[Union[int, str]] = None,
    base_url: str = "https://mainnet.zklighter.elliot.ai",
    timeout: float = 15.0,
) -> Dict[str, Any]:
    """Submit a market order on Lighter perp via SignerClient (async under the hood)."""
    if LighterSignerClient is None:
        raise TradeExecutionError("lighter SDK is required for Lighter trading (pip install lighter)")

    creds = _resolve_lighter_credentials(private_key, account_index, api_key_index, base_url=base_url)
    market = get_lighter_market_meta(symbol, base_url=creds.base_url, timeout=timeout)
    try:
        market_id = int(market.get("market_id"))
    except Exception as exc:
        raise TradeExecutionError(f"Lighter market_id invalid for {symbol}: {market.get('market_id')!r}") from exc

    try:
        size_dec = Decimal(str(size))
    except Exception as exc:
        raise TradeExecutionError(f"Invalid Lighter size: {size!r}") from exc
    if size_dec <= 0:
        raise TradeExecutionError("Lighter size must be positive")

    try:
        size_decimals = int(market.get("supported_size_decimals") or 0)
        price_decimals = int(market.get("supported_price_decimals") or 0)
    except Exception:
        size_decimals = 0
        price_decimals = 0
    base_multiplier = Decimal("10") ** Decimal(str(max(0, size_decimals)))
    price_multiplier = Decimal("10") ** Decimal(str(max(0, price_decimals)))

    book = get_lighter_orderbook_orders(market_id, limit=50, base_url=creds.base_url, timeout=timeout)
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    try:
        best_bid = float((bids[0] or {}).get("price")) if isinstance(bids, list) and bids else None
        best_ask = float((asks[0] or {}).get("price")) if isinstance(asks, list) and asks else None
    except Exception:
        best_bid, best_ask = None, None
    if not best_bid or not best_ask:
        raise TradeExecutionError(f"Lighter orderbook missing best levels for market_id={market_id}")
    mid = (float(best_bid) + float(best_ask)) / 2.0
    slippage = float(max_slippage_bps) / 10000.0

    side_key = (side or "").strip().lower()
    if side_key in {"long", "buy", "b"}:
        is_ask = False
        px = mid * (1.0 + slippage)
    elif side_key in {"short", "sell", "s"}:
        is_ask = True
        px = mid * (1.0 - slippage)
    else:
        raise TradeExecutionError("Lighter side must be BUY/LONG or SELL/SHORT")

    step = Decimal("1").scaleb(-max(0, size_decimals))
    size_dec = size_dec.quantize(step, rounding=ROUND_DOWN)
    if size_dec <= 0:
        raise TradeExecutionError(f"Lighter size too small after rounding ({size_decimals} dp): {size!r}")

    base_amount_int = int((size_dec * base_multiplier).to_integral_value(rounding=ROUND_DOWN))
    if base_amount_int <= 0:
        raise TradeExecutionError("Lighter base_amount becomes 0 after scaling")

    px_int = int((Decimal(str(px)) * price_multiplier).to_integral_value(rounding=ROUND_DOWN))
    if px_int <= 0:
        raise TradeExecutionError("Lighter price becomes 0 after scaling")

    try:
        min_base = Decimal(str(market.get("min_base_amount"))) if market.get("min_base_amount") is not None else None
        if min_base is not None and size_dec < min_base:
            raise TradeExecutionError(f"Lighter size below min_base_amount={min_base} for {symbol}")
    except Exception:
        pass
    try:
        min_quote = Decimal(str(market.get("min_quote_amount"))) if market.get("min_quote_amount") is not None else None
        if min_quote is not None and (size_dec * Decimal(str(mid))) < min_quote:
            raise TradeExecutionError(f"Lighter notional below min_quote_amount={min_quote} for {symbol}")
    except Exception:
        pass

    if client_order_id is None:
        client_order_index = int(time.time() * 1000) % 2_000_000_000
    else:
        try:
            client_order_index = int(str(client_order_id))
        except Exception:
            digest = hashlib.sha256(str(client_order_id).encode("utf-8")).hexdigest()
            client_order_index = int(digest[:12], 16) % 2_000_000_000

    async def _submit():
        signer = LighterSignerClient(
            creds.base_url,
            creds.private_key,
            int(creds.api_key_index),
            int(creds.account_index),
        )
        try:
            tx_info, tx_hash, error = await signer.create_market_order(
            market_id,
            int(client_order_index),
            int(base_amount_int),
            int(px_int),
            bool(is_ask),
            reduce_only=bool(reduce_only),
            )
            if error is not None:
                raise TradeExecutionError(f"Lighter create_market_order error: {error}")
            tx_hash_value = None
            try:
                tx_hash_value = getattr(tx_hash, "tx_hash", None)
            except Exception:
                tx_hash_value = None
            if not tx_hash_value:
                tx_hash_value = str(tx_hash)

            if hasattr(tx_info, "to_dict"):
                try:
                    tx_info_value = tx_info.to_dict()  # type: ignore[attr-defined]
                except Exception:
                    tx_info_value = getattr(tx_info, "__dict__", str(tx_info))
            else:
                tx_info_value = getattr(tx_info, "__dict__", str(tx_info))

            return {"tx_info": tx_info_value, "tx_hash": tx_hash_value}
        finally:
            try:
                close = getattr(signer, "close", None)
                if callable(close):
                    await close()
            except Exception:
                pass

    try:
        resp = _run_async(_submit())
    finally:
        pass

    return {
        "symbol": symbol.upper(),
        "market_id": market_id,
        "side": side_key,
        "reduce_only": bool(reduce_only),
        "size": str(size_dec),
        "client_order_index": int(client_order_index),
        "price_limit": float(px),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "response": resp,
    }


def get_hyperliquid_user_funding_history(
    *,
    start_time_ms: int,
    end_time_ms: Optional[int] = None,
    address: Optional[str] = None,
    private_key: Optional[str] = None,
    base_url: Optional[str] = None,
    timeout: float = 10.0,
) -> List[Dict[str, Any]]:
    """Return Hyperliquid user funding history via Info.user_funding_history."""
    if HyperliquidInfo is None:
        raise TradeExecutionError("hyperliquid-python-sdk is required for Hyperliquid funding history")
    creds = _resolve_hyperliquid_credentials(private_key, address, base_url=base_url)
    info = HyperliquidInfo(base_url=creds.base_url, skip_ws=True, timeout=timeout)
    payload = info.user_funding_history(
        creds.address,
        int(start_time_ms),
        int(end_time_ms) if end_time_ms is not None else None,
    )
    if not isinstance(payload, list):
        raise TradeExecutionError(f"Hyperliquid user_funding_history payload malformed: {payload!r}")
    return payload


_HYPERLIQUID_META_AT = 0.0
_HYPERLIQUID_SZ_DECIMALS: Dict[str, int] = {}
_HYPERLIQUID_META_LOCK = threading.Lock()
_HYPERLIQUID_META_TTL_SECONDS = 300.0


def _get_hyperliquid_sz_decimals(
    symbol: str,
    *,
    base_url: str,
    timeout: float,
) -> int:
    global _HYPERLIQUID_META_AT
    if HyperliquidInfo is None:
        raise TradeExecutionError("hyperliquid-python-sdk is required for Hyperliquid trading/position APIs")

    sym = (symbol or "").upper().strip()
    if not sym:
        raise TradeExecutionError("Hyperliquid symbol is required")

    now = time.time()
    with _HYPERLIQUID_META_LOCK:
        if _HYPERLIQUID_SZ_DECIMALS and (now - _HYPERLIQUID_META_AT) < _HYPERLIQUID_META_TTL_SECONDS:
            if sym in _HYPERLIQUID_SZ_DECIMALS:
                return _HYPERLIQUID_SZ_DECIMALS[sym]

    info = HyperliquidInfo(base_url=base_url, skip_ws=True, timeout=timeout)
    meta = info.meta()
    if not isinstance(meta, dict):
        raise TradeExecutionError(f"Hyperliquid meta payload malformed: {meta!r}")
    universe = meta.get("universe") or []
    if not isinstance(universe, list):
        raise TradeExecutionError(f"Hyperliquid meta universe malformed: {meta!r}")

    mapping: Dict[str, int] = {}
    for entry in universe:
        if not isinstance(entry, dict):
            continue
        name = (entry.get("name") or "").upper()
        if not name:
            continue
        raw_decimals = entry.get("szDecimals")
        try:
            decimals = int(raw_decimals)
        except Exception:
            continue
        mapping[name] = decimals

    with _HYPERLIQUID_META_LOCK:
        if mapping:
            _HYPERLIQUID_SZ_DECIMALS.clear()
            _HYPERLIQUID_SZ_DECIMALS.update(mapping)
            _HYPERLIQUID_META_AT = now

    if sym not in mapping:
        raise TradeExecutionError(f"Hyperliquid meta missing symbol {sym}")
    return mapping[sym]


def place_hyperliquid_perp_market_order(
    symbol: str,
    side: str,
    size: Union[str, float, Decimal],
    *,
    reduce_only: Optional[bool] = None,
    client_order_id: Optional[str] = None,
    slippage: float = 0.01,
    address: Optional[str] = None,
    private_key: Optional[str] = None,
    base_url: Optional[str] = None,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    """Submit a market order on Hyperliquid perpetuals via the official SDK."""
    if HyperliquidExchange is None or Account is None:
        raise TradeExecutionError("hyperliquid-python-sdk and eth-account are required for Hyperliquid trading")

    creds = _resolve_hyperliquid_credentials(private_key, address, base_url=base_url)
    try:
        wallet = Account.from_key(creds.private_key)
    except Exception as exc:
        raise TradeExecutionError("Invalid Hyperliquid private key") from exc

    exchange = HyperliquidExchange(wallet, base_url=creds.base_url, account_address=creds.address, timeout=timeout)

    try:
        size_dec = Decimal(str(size))
    except Exception as exc:
        raise TradeExecutionError(f"Invalid Hyperliquid size: {size!r}") from exc
    if size_dec <= 0:
        raise TradeExecutionError("Hyperliquid size must be positive")

    side_key = (side or "").strip().lower()
    if side_key in {"long", "buy", "b"}:
        is_buy = True
    elif side_key in {"short", "sell", "s"}:
        is_buy = False
    else:
        raise TradeExecutionError("Hyperliquid side must be BUY/LONG or SELL/SHORT")

    cloid = None
    if client_order_id:
        try:
            from hyperliquid.utils.types import Cloid  # type: ignore[import-not-found]

            # Hyperliquid Cloid must be a hex string; derive a stable hex id from the arbitrary client_order_id.
            digest = hashlib.sha256(str(client_order_id).encode("utf-8")).hexdigest()[:32]
            cloid = Cloid.from_str(digest)
        except Exception:
            cloid = None

    sz_decimals = _get_hyperliquid_sz_decimals(symbol, base_url=creds.base_url, timeout=timeout)
    step = Decimal("1").scaleb(-sz_decimals)
    size_dec = size_dec.quantize(step, rounding=ROUND_DOWN)
    if size_dec <= 0:
        raise TradeExecutionError(f"Hyperliquid size too small after rounding ({sz_decimals} dp): {size!r}")
    raw_size = float(size_dec)

    if reduce_only:
        # The SDK's market_close() returns None when it cannot find an existing position via user_state().
        # For our live trading we already know the close direction; submit an aggressive reduce-only IOC limit instead
        # so we always get a response payload for logging/backfill.
        close_side = (side or "").strip().lower()
        if close_side in {"long", "buy", "b"}:
            is_buy_close = True
        elif close_side in {"short", "sell", "s"}:
            is_buy_close = False
        else:
            raise TradeExecutionError("Hyperliquid close side must be BUY/LONG or SELL/SHORT")
        px = exchange._slippage_price(symbol.upper(), is_buy_close, float(slippage), None)  # type: ignore[attr-defined]
        resp = exchange.order(
            symbol.upper(),
            is_buy_close,
            raw_size,
            float(px),
            order_type={"limit": {"tif": "Ioc"}},
            reduce_only=True,
            cloid=cloid,
        )
    else:
        resp = exchange.market_open(symbol.upper(), is_buy=is_buy, sz=raw_size, slippage=float(slippage), cloid=cloid)

    if not isinstance(resp, dict):
        return {"result": resp}
    return resp


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
    for blocked in ("side", "pos_side"):
        kwargs.pop(blocked, None)
    binance_position_side_override = kwargs.pop("position_side", None)
    bybit_position_idx = kwargs.pop("position_idx", None)

    LOGGER.info(
        "execute_perp_market_order exchange=%s symbol=%s direction=%s quantity=%s",
        exchange_key,
        symbol,
        direction,
        quantity_dec,
    )

    if exchange_key == "binance":
        binance_kwargs = dict(kwargs)
        reduce_only = binance_kwargs.pop("reduce_only", None)
        client_order_id = binance_kwargs.pop("client_order_id", None)

        if binance_position_side_override is not None:
            position_side = str(binance_position_side_override).upper()
        else:
            position_side = "LONG" if direction == "long" else "SHORT"

        def _submit(*, position_side_val: Optional[str], reduce_only_val: Optional[bool]) -> Dict[str, Any]:
            return place_binance_perp_market_order(
                symbol,
                "BUY" if direction == "long" else "SELL",
                quantity_dec,
                reduce_only=reduce_only_val,
                client_order_id=client_order_id,
                position_side=position_side_val,
                **binance_kwargs,
            )

        try:
            order = _submit(position_side_val=position_side, reduce_only_val=reduce_only)
        except TradeExecutionError as exc:
            # Some Binance accounts run in one-way mode and reject positionSide.
            msg = str(exc).lower()
            if reduce_only is True and "reduceonly" in msg and "not required" in msg:
                order = _submit(position_side_val=position_side, reduce_only_val=None)
            elif "positionside" in msg or "dual-side position" in msg:
                order = _submit(position_side_val=None, reduce_only_val=reduce_only)
            else:
                raise
    elif exchange_key == "okx":
        order = place_okx_swap_market_order(
            symbol,
            "buy" if direction == "long" else "sell",
            str(quantity_dec),
            pos_side="long" if direction == "long" else "short",
            **kwargs,
        )
    elif exchange_key == "bybit":
        bybit_category = kwargs.get("category", "linear")
        receiver_window = kwargs.get("recv_window", 5000)
        base_url = kwargs.get("base_url", "https://api.bybit.com")

        def _submit(position_idx: int) -> Dict[str, Any]:
            _ensure_bybit_leverage_target(
                symbol,
                target_leverage=1,
                category=bybit_category,
                position_idx=position_idx,
                recv_window=receiver_window,
                api_key=kwargs.get("api_key"),
                secret_key=kwargs.get("secret_key"),
                base_url=base_url,
            )
            return place_bybit_linear_market_order(
                symbol,
                "buy" if direction == "long" else "sell",
                str(quantity_dec),
                position_idx=position_idx,
                **kwargs,
            )

        bybit_position_idx_resolved = bybit_position_idx if bybit_position_idx is not None else 0
        try:
            order = _submit(int(bybit_position_idx_resolved))
        except TradeExecutionError as exc:
            # Some accounts use hedge-mode (positionIdx=1 long / 2 short).
            msg = str(exc).lower()
            if bybit_position_idx is None and "position idx not match position mode" in msg:
                hedge_idx = 1 if direction == "long" else 2
                order = _submit(int(hedge_idx))
            else:
                raise
    elif exchange_key == "bitget":
        order = place_bitget_usdt_perp_market_order(
            symbol,
            "buy" if direction == "long" else "sell",
            str(quantity_dec),
            **kwargs,
        )
    elif exchange_key == "hyperliquid":
        hl_kwargs = dict(kwargs)
        reduce_only = hl_kwargs.pop("reduce_only", None)
        client_order_id = hl_kwargs.pop("client_order_id", None)
        slippage = hl_kwargs.pop("slippage", 0.01)
        order = place_hyperliquid_perp_market_order(
            symbol,
            "buy" if direction == "long" else "sell",
            str(quantity_dec),
            reduce_only=reduce_only,
            client_order_id=client_order_id,
            slippage=float(slippage),
            address=hl_kwargs.pop("address", None),
            private_key=hl_kwargs.pop("private_key", None),
            base_url=hl_kwargs.pop("base_url", None),
            timeout=float(hl_kwargs.pop("timeout", 10.0)),
        )
    elif exchange_key == "lighter":
        lt_kwargs = dict(kwargs)
        reduce_only = bool(lt_kwargs.pop("reduce_only", False))
        client_order_id = lt_kwargs.pop("client_order_id", None)
        max_slippage_bps = float(lt_kwargs.pop("max_slippage_bps", 30.0))
        order = place_lighter_perp_market_order(
            symbol,
            "buy" if direction == "long" else "sell",
            str(quantity_dec),
            reduce_only=reduce_only,
            client_order_id=client_order_id,
            max_slippage_bps=max_slippage_bps,
            private_key=lt_kwargs.pop("private_key", None),
            account_index=lt_kwargs.pop("account_index", None),
            api_key_index=lt_kwargs.pop("api_key_index", None),
            base_url=lt_kwargs.pop("base_url", "https://mainnet.zklighter.elliot.ai"),
            timeout=float(lt_kwargs.pop("timeout", 15.0)),
        )
    elif exchange_key == "grvt":
        gv_kwargs = dict(kwargs)
        reduce_only = bool(gv_kwargs.pop("reduce_only", False))
        client_order_id = gv_kwargs.pop("client_order_id", None)
        order = place_grvt_perp_market_order(
            symbol,
            "buy" if direction == "long" else "sell",
            str(quantity_dec),
            reduce_only=reduce_only,
            client_order_id=client_order_id,
            api_key=gv_kwargs.pop("api_key", None),
            private_key=gv_kwargs.pop("private_key", None),
            trading_account_id=gv_kwargs.pop("trading_account_id", None),
            environment=gv_kwargs.pop("environment", None),
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
