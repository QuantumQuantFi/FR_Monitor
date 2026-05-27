# GRVT Query and Position Monitor (Python)

This guide provides a minimal, self-contained GRVT query module that can be copied
into another project. It reuses the same secret variable names:
`GRVT_API_KEY`, `GRVT_API_SECRET`, `GRVT_TRADING_ACCOUNT_ID`, `GRVT_ENVIRONMENT`.

Dependencies:
- `grvt-pysdk` (provides `pysdk.grvt_ccxt`)

## Minimal module (balance + positions + monitor)

```python
from __future__ import annotations

import os
import time
from typing import Any, Dict, List

from pysdk.grvt_ccxt import GrvtCcxt
from pysdk.grvt_ccxt_env import GrvtEnv

# Keep the same variable names.
GRVT_API_KEY = os.getenv("GRVT_API_KEY", "").strip()
GRVT_API_SECRET = os.getenv("GRVT_API_SECRET", "").strip()
GRVT_TRADING_ACCOUNT_ID = os.getenv("GRVT_TRADING_ACCOUNT_ID", "").strip()
GRVT_ENVIRONMENT = os.getenv("GRVT_ENVIRONMENT", "prod").strip()


def _env_enum(environment: str) -> GrvtEnv:
    env = (environment or "prod").lower().strip()
    mapping = {
        "prod": GrvtEnv.PROD,
        "testnet": GrvtEnv.TESTNET,
        "staging": GrvtEnv.STAGING,
        "dev": GrvtEnv.DEV,
    }
    return mapping.get(env, GrvtEnv.PROD)


def _require_credentials() -> None:
    if not GRVT_API_KEY:
        raise RuntimeError("Missing GRVT_API_KEY")
    if not GRVT_API_SECRET:
        raise RuntimeError("Missing GRVT_API_SECRET")
    if not GRVT_TRADING_ACCOUNT_ID:
        raise RuntimeError("Missing GRVT_TRADING_ACCOUNT_ID")


def _get_client() -> GrvtCcxt:
    _require_credentials()
    return GrvtCcxt(
        env=_env_enum(GRVT_ENVIRONMENT),
        parameters={
            "api_key": GRVT_API_KEY,
            "private_key": GRVT_API_SECRET,
            "trading_account_id": GRVT_TRADING_ACCOUNT_ID,
        },
    )


def get_grvt_balance_summary() -> Dict[str, Any]:
    client = _get_client()
    summary = client.get_account_summary()
    if not isinstance(summary, dict):
        raise RuntimeError(f"GRVT get_account_summary malformed: {summary!r}")
    return {
        "currency": str(summary.get("settle_currency") or "USDT").upper(),
        "available_balance": summary.get("available_balance"),
        "wallet_balance": summary.get("total_equity"),
        "equity": summary.get("total_equity"),
        "unrealized_pnl": summary.get("unrealized_pnl"),
        "raw_summary": summary,
    }


def get_grvt_perp_positions(symbol: str | None = None) -> List[Dict[str, Any]]:
    client = _get_client()
    instruments = [symbol] if symbol else []
    rows = client.fetch_positions(symbols=instruments)
    if not isinstance(rows, list):
        raise RuntimeError(f"GRVT fetch_positions malformed: {rows!r}")
    return rows


def monitor_positions(interval_sec: float = 5.0) -> None:
    while True:
        positions = get_grvt_perp_positions()
        print(f"positions={positions}")
        time.sleep(interval_sec)


if __name__ == "__main__":
    print(get_grvt_balance_summary())
    print(get_grvt_perp_positions())
```

## Usage notes

- Provide credentials via environment variables (same names):
  - `GRVT_API_KEY`
  - `GRVT_API_SECRET`
  - `GRVT_TRADING_ACCOUNT_ID`
  - `GRVT_ENVIRONMENT` (default: `prod`)
- For a simple position monitor, call `monitor_positions(interval_sec=5)`.
