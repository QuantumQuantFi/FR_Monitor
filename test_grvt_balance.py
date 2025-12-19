import json
from typing import Any, Dict

from config import GRVT_API_KEY, GRVT_API_SECRET, GRVT_ENVIRONMENT, GRVT_TRADING_ACCOUNT_ID

try:
    from pysdk.grvt_ccxt import GrvtCcxt
    from pysdk.grvt_ccxt_env import GrvtEnv
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"Missing grvt-pysdk dependency: {exc}")


def _env_enum() -> "GrvtEnv":
    env = (GRVT_ENVIRONMENT or "prod").lower()
    mapping = {
        "prod": GrvtEnv.PROD,
        "testnet": GrvtEnv.TESTNET,
        "staging": GrvtEnv.STAGING,
        "dev": GrvtEnv.DEV,
    }
    return mapping.get(env, GrvtEnv.PROD)


def _mask(s: str, keep: int = 4) -> str:
    if not s:
        return ""
    s = str(s)
    if len(s) <= keep * 2:
        return "*" * len(s)
    return f"{s[:keep]}...{s[-keep:]}"


def _summarize_balance(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {"raw_type": str(type(payload))}

    if "total" in payload or "free" in payload:
        free = payload.get("free") or {}
        total = payload.get("total") or {}
        keys = sorted(set(list(free.keys()) + list(total.keys())))
        assets = []
        for k in keys:
            assets.append(
                {
                    "asset": k,
                    "free": free.get(k),
                    "total": total.get(k),
                }
            )
        return {"assets": assets, "raw_keys": sorted(payload.keys())}

    return {"raw_keys": sorted(payload.keys())}


def main() -> int:
    if not GRVT_API_KEY:
        print("❌ GRVT_API_KEY not configured in config_private.py")
        return 2
    if not GRVT_API_SECRET:
        print("❌ GRVT_SECRET_KEY/GRVT_PRIVATE_KEY not configured in config_private.py")
        return 2

    params: Dict[str, Any] = {"api_key": GRVT_API_KEY}
    if GRVT_API_SECRET:
        params["private_key"] = GRVT_API_SECRET
    if GRVT_TRADING_ACCOUNT_ID:
        params["trading_account_id"] = GRVT_TRADING_ACCOUNT_ID

    print(
        json.dumps(
            {
                "env": GRVT_ENVIRONMENT,
                "api_key": _mask(GRVT_API_KEY),
                "trading_account_id": _mask(GRVT_TRADING_ACCOUNT_ID),
            },
            ensure_ascii=False,
        )
    )

    client = GrvtCcxt(env=_env_enum(), parameters=params)

    summary = client.get_account_summary()
    print("✅ GRVT get_account_summary() ok")
    print(
        json.dumps(
            {
                "settle_currency": summary.get("settle_currency"),
                "available_balance": summary.get("available_balance"),
                "total_equity": summary.get("total_equity"),
                "unrealized_pnl": summary.get("unrealized_pnl"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )

    balance = client.fetch_balance()
    print("✅ GRVT fetch_balance() ok (ccxt-style)")
    print(json.dumps(_summarize_balance(balance), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
