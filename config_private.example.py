"""
Local secrets template (DO NOT COMMIT REAL KEYS).

- Copy to `config_private.py` (gitignored) and fill in your own credentials.
- Alternatively, many credentials can be provided via environment variables.
"""

# ---- Hyperliquid (perp trading) ----
# Required for trading:
HYPERLIQUID_PRIVATE_KEY = ""  # hex private key, e.g. "0x..." (keep empty in repo)
# Optional (derived from private key if empty):
HYPERLIQUID_ADDRESS = ""  # "0x..."
# Optional endpoints:
HYPERLIQUID_API_BASE_URL = ""  # default: https://api.hyperliquid.xyz

# ---- Lighter (DEX) ----
# This repo currently uses Lighter primarily for market data. If you add trading later,
# keep all secrets here (or env vars) and never commit them.
LIGHTER_KEY_INDEX = ""  # e.g. "3"
LIGHTER_PUBLIC_KEY = ""
LIGHTER_PRIVATE_KEY = ""
LIGHTER_REST_BASE_URL = ""  # optional
LIGHTER_WS_PUBLIC_URL = ""  # optional

# ---- Bitget / OKX / Binance / Bybit etc. ----
# See `trading/README.md` and existing code in `trading/trade_executor.py` for variable names.
