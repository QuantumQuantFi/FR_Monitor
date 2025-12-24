from __future__ import annotations

import json
import math
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


def _sigmoid(x: float) -> float:
    if x > 50:
        return 1.0
    if x < -50:
        return 0.0
    return 1.0 / (1.0 + math.exp(-x))


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        out = float(val)
    except Exception:
        return None
    if math.isfinite(out):
        return out
    return None


@dataclass
class V2Model:
    model_path: str
    features: Tuple[str, ...]
    median: Tuple[float, ...]
    mean: Tuple[float, ...]
    std: Tuple[float, ...]
    ridge_coef: Tuple[float, ...]
    ridge_intercept: float
    log_coef: Tuple[float, ...]
    log_intercept: float

    def infer(self, factors: Dict[str, Any]) -> Dict[str, Any]:
        if not self.features:
            return {"error": "no_features"}
        missing = 0
        x = []
        for idx, name in enumerate(self.features):
            raw = _safe_float(factors.get(name)) if isinstance(factors, dict) else None
            if raw is None:
                raw = self.median[idx]
                missing += 1
            val = (raw - self.mean[idx]) / self.std[idx] if self.std[idx] != 0 else (raw - self.mean[idx])
            x.append(val)

        n = len(self.features)
        missing_rate = float(missing) / float(n) if n else 1.0
        pnl_hat = sum(c * v for c, v in zip(self.ridge_coef, x)) + float(self.ridge_intercept)
        logit = sum(c * v for c, v in zip(self.log_coef, x)) + float(self.log_intercept)
        win_prob = _sigmoid(logit)

        return {
            "pnl_hat": float(pnl_hat),
            "win_prob": float(win_prob),
            "missing": int(missing),
            "missing_rate": float(missing_rate),
            "features": int(n),
        }


def _resolve_model_path(path: str) -> str:
    raw = str(path or "").strip()
    if not raw:
        return ""
    p = Path(raw)
    if p.is_absolute():
        return str(p)
    # Resolve relative to repo/module directory so services launched from other CWDs still work.
    base = Path(__file__).resolve().parent
    return str((base / p).resolve())


@lru_cache(maxsize=8)
def _load_model_cached(path: str) -> Optional[V2Model]:
    resolved = _resolve_model_path(path)
    if not resolved:
        return None
    try:
        with open(resolved, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return None
    try:
        features = tuple(payload.get("features") or [])
        scaler = payload.get("scaler") or {}
        median = tuple(float(x) for x in scaler.get("median") or [])
        mean = tuple(float(x) for x in scaler.get("mean") or [])
        std = tuple(float(x) for x in scaler.get("std") or [])
        ridge = payload.get("ridge") or {}
        logreg = payload.get("logistic") or {}
        ridge_coef = tuple(float(x) for x in ridge.get("coef") or [])
        log_coef = tuple(float(x) for x in logreg.get("coef") or [])
        if not (features and median and mean and std and ridge_coef and log_coef):
            return None
        return V2Model(
            model_path=resolved,
            features=features,
            median=median,
            mean=mean,
            std=std,
            ridge_coef=ridge_coef,
            ridge_intercept=float(ridge.get("intercept") or 0.0),
            log_coef=log_coef,
            log_intercept=float(logreg.get("intercept") or 0.0),
        )
    except Exception:
        return None


def infer_v2(
    *,
    model_path: str,
    factors: Dict[str, Any],
    max_missing_ratio: float = 0.2,
) -> Dict[str, Any]:
    resolved = _resolve_model_path(model_path)
    model = _load_model_cached(resolved)
    if model is None:
        return {"error": "model_not_loaded", "model_path": resolved or str(model_path or "").strip()}
    result = model.infer(factors)
    if result.get("error"):
        return result
    missing_rate_raw = result.get("missing_rate")
    missing_rate = float(missing_rate_raw) if missing_rate_raw is not None else 1.0
    ok = missing_rate <= float(max_missing_ratio)
    result["ok"] = bool(ok)
    result["model_path"] = model.model_path
    return result
