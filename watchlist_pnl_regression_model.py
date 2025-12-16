from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class LinearModel:
    model_name: str
    fee_threshold: float
    intercept: float
    beta: Dict[str, float]
    resid_std: float

    def predict_pnl(self, factors: Dict[str, float]) -> Optional[float]:
        try:
            y = float(self.intercept)
            for k, b in self.beta.items():
                if k not in factors:
                    return None
                v = float(factors[k])
                if not math.isfinite(v):
                    return None
                y += float(b) * v
            if not math.isfinite(y):
                return None
            return y
        except Exception:
            return None

    def predict_win_prob(self, factors: Dict[str, float], *, fee_threshold: Optional[float] = None) -> Optional[float]:
        """
        Approximate P(pnl > fee_threshold) under Normal(resid_std) assumption:
          pnl ~ Normal(mean=pnl_hat, std=resid_std)
        """
        pnl_hat = self.predict_pnl(factors)
        if pnl_hat is None:
            return None
        thr = self.fee_threshold if fee_threshold is None else float(fee_threshold)
        if not self.resid_std or self.resid_std <= 0:
            return None
        z = (thr - float(pnl_hat)) / float(self.resid_std)
        # 1 - Phi(z) = Phi(-z)
        win = 0.5 * (1.0 + math.erf((-z) / math.sqrt(2.0)))
        if win < 0:
            return 0.0
        if win > 1:
            return 1.0
        return float(win)


MODEL_VERSION = "ols_5factors_fee10bps_2025-12-13"
DEFAULT_FEE_THRESHOLD = 0.001  # 10bps in pnl units (bps/10000)

# Coefficients copied from reports/pnl_linear_regression_5factors_fee10bps.md (beta_raw)
# Intercepts and resid_std are computed from the same dataset (canonical outcomes only).
MODELS: Dict[str, Dict[int, LinearModel]] = {
    "B": {
        60: LinearModel(
            model_name=MODEL_VERSION,
            fee_threshold=DEFAULT_FEE_THRESHOLD,
            intercept=-0.0003623053171953745,
            beta={
                "raw_slope_3m": 0.26581410,
                "raw_drift_ratio": 0.00892611,
                "spread_log_short_over_long": 0.19618032,
                "raw_crossings_1h": 0.00001460,
                "raw_best_buy_high_sell_low": -0.00213036,
            },
            resid_std=0.006949101800227788,
        ),
        240: LinearModel(
            model_name=MODEL_VERSION,
            fee_threshold=DEFAULT_FEE_THRESHOLD,
            intercept=0.0012004576999063957,
            beta={
                "raw_slope_3m": 0.52041864,
                "raw_drift_ratio": 0.00868822,
                "spread_log_short_over_long": 0.12390842,
                "raw_crossings_1h": 0.00001012,
                "raw_best_buy_high_sell_low": -0.00263329,
            },
            resid_std=0.006915784259576688,
        ),
        480: LinearModel(
            model_name=MODEL_VERSION,
            fee_threshold=DEFAULT_FEE_THRESHOLD,
            intercept=0.0012120172914026398,
            beta={
                "raw_slope_3m": 0.43691677,
                "raw_drift_ratio": 0.00945341,
                "spread_log_short_over_long": 0.06982480,
                "raw_crossings_1h": 0.00002229,
                "raw_best_buy_high_sell_low": -0.00308295,
            },
            resid_std=0.007178890674728138,
        ),
    },
    "C": {
        60: LinearModel(
            model_name=MODEL_VERSION,
            fee_threshold=DEFAULT_FEE_THRESHOLD,
            intercept=-0.007059170232420167,
            beta={
                "raw_slope_3m": 0.50976837,
                "raw_drift_ratio": 0.00443312,
                "spread_log_short_over_long": 0.95055801,
                "raw_crossings_1h": 0.00004299,
                "raw_best_buy_high_sell_low": -0.11814283,
            },
            resid_std=0.0059748478596738375,
        ),
        240: LinearModel(
            model_name=MODEL_VERSION,
            fee_threshold=DEFAULT_FEE_THRESHOLD,
            intercept=-0.004965363254004658,
            beta={
                "raw_slope_3m": 0.40667273,
                "raw_drift_ratio": 0.00490197,
                "spread_log_short_over_long": 0.78382843,
                "raw_crossings_1h": 0.00003507,
                "raw_best_buy_high_sell_low": -0.00861370,
            },
            resid_std=0.006515089805963541,
        ),
        480: LinearModel(
            model_name=MODEL_VERSION,
            fee_threshold=DEFAULT_FEE_THRESHOLD,
            intercept=-0.005975565302655149,
            beta={
                "raw_slope_3m": 0.71548415,
                "raw_drift_ratio": 0.00297289,
                "spread_log_short_over_long": 0.92552473,
                "raw_crossings_1h": 0.00003822,
                "raw_best_buy_high_sell_low": 0.00024534,
            },
            resid_std=0.00689004742733532,
        ),
    },
}


def predict_bc(
    *,
    signal_type: str,
    factors: Dict[str, float],
    horizons: tuple = (60, 240, 480),
) -> Optional[Dict[str, object]]:
    sig = (signal_type or "").strip().upper()
    if sig not in ("B", "C"):
        return None
    model_map = MODELS.get(sig) or {}
    preds = {}
    for h in horizons:
        m = model_map.get(int(h))
        if not m:
            continue
        pnl_hat = m.predict_pnl(factors)
        win_prob = m.predict_win_prob(factors)
        preds[str(int(h))] = {
            "pnl_hat": pnl_hat,
            "win_prob": win_prob,
            "fee_threshold": m.fee_threshold,
            "model": m.model_name,
        }
    return {"model": MODEL_VERSION, "fee_threshold": DEFAULT_FEE_THRESHOLD, "pred": preds}

