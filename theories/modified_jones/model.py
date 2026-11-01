"""Estimation of the Jones (1991) and Modified Jones (Dechow, Sloan & Sweeney 1995) models.

Cross-sectional estimation by industry-year (DeFond & Jiambalvo 1994), the
standard modern implementation:

  Jones / estimation equation, per (2-digit SIC, fiscal year) group:
      TA/A[t-1] = a1 * 1/A[t-1] + a2 * dREV/A[t-1] + a3 * PPE/A[t-1] + e

  Modified Jones: the same estimated coefficients, but non-discretionary
  accruals use revenue growth net of receivables growth, on the assumption
  that credit sales are where managers have discretion:
      NDA = a1 * 1/A[t-1] + a2 * (dREV - dREC)/A[t-1] + a3 * PPE/A[t-1]
      DA  = TA/A[t-1] - NDA

  Kothari, Leone & Wasley (2005) performance-matched variant (``variant="kothari"``):
      adds an intercept and ROA to both equations.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

VARIANTS = {
    #           estimation regressors                      NDA regressors (same coefficients)
    "modified_jones": (["x_inv_a", "x_drev", "x_ppe"], ["x_inv_a", "x_drev_drec", "x_ppe"], False),
    "kothari": (["x_inv_a", "x_drev", "x_ppe", "x_roa"], ["x_inv_a", "x_drev_drec", "x_ppe", "x_roa"], True),
}
COEF_NAMES = {"x_inv_a": "a1_inv_assets", "x_drev": "a2_d_rev", "x_ppe": "a3_ppe",
              "x_roa": "a4_roa", "const": "a0_const"}


def winsorize(df: pd.DataFrame, cols, p: float = 0.01, by: str | None = "fyear") -> pd.DataFrame:
    """Clip each column to its [p, 1-p] quantiles (within ``by`` groups if given)."""
    out = df.copy()
    if not p:
        return out
    for c in cols:
        if by:
            g = out.groupby(by)[c]
            lo, hi = g.transform(lambda s: s.quantile(p)), g.transform(lambda s: s.quantile(1 - p))
        else:
            lo, hi = out[c].quantile(p), out[c].quantile(1 - p)
        out[c] = out[c].clip(lo, hi)
    return out


def ols(X: np.ndarray, y: np.ndarray) -> dict:
    """OLS with classical standard errors. R^2 is centred (comparable across
    specifications with and without an intercept; may be negative)."""
    n, k = X.shape
    coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ coef
    ssr = float(resid @ resid)
    sst = float(((y - y.mean()) ** 2).sum())
    dof = n - k
    sigma2 = ssr / dof if dof > 0 else np.nan
    try:
        cov = sigma2 * np.linalg.inv(X.T @ X)
        se = np.sqrt(np.diag(cov))
    except np.linalg.LinAlgError:
        se = np.full(k, np.nan)
    r2 = 1 - ssr / sst if sst > 0 else np.nan
    adj_r2 = 1 - (1 - r2) * (n - 1) / dof if dof > 0 and not np.isnan(r2) else np.nan
    return {"coef": coef, "se": se, "t": coef / se, "r2": r2, "adj_r2": adj_r2, "n": n}


def _design(df: pd.DataFrame, cols, const: bool) -> np.ndarray:
    X = df[cols].to_numpy(dtype=float)
    return np.column_stack([np.ones(len(df)), X]) if const else X


def estimate(sample: pd.DataFrame, variant: str = "modified_jones", min_obs: int = 10,
             group_cols=("sic2", "fyear"), winsor: float = 0.01) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Estimate per industry-year. Returns (firm_years with DA, coefficients per group)."""
    est_cols, nda_cols, const = VARIANTS[variant]
    model_cols = sorted({"y_ta", *est_cols, *nda_cols})
    data = winsorize(sample, model_cols, p=winsor)

    names = (["const"] if const else []) + est_cols
    firm_parts, coef_rows = [], []
    for key, g in data.groupby(list(group_cols), sort=True):
        if len(g) < max(min_obs, len(names) + 1):
            continue
        y = g["y_ta"].to_numpy(dtype=float)
        res = ols(_design(g, est_cols, const), y)
        nda = _design(g, nda_cols, const) @ res["coef"]
        part = g.copy()
        part["nda"] = nda
        part["da"] = part["y_ta"] - nda                                    # Modified Jones DA
        part["da_jones"] = y - _design(g, est_cols, const) @ res["coef"]   # Jones residual
        firm_parts.append(part)

        row = dict(zip(group_cols, key if isinstance(key, tuple) else (key,)))
        row.update(n=res["n"], r2=res["r2"], adj_r2=res["adj_r2"])
        for name, c, t in zip(names, res["coef"], res["t"]):
            row[COEF_NAMES[name]] = c
            row[COEF_NAMES[name] + "_t"] = t
        coef_rows.append(row)

    firm_years = pd.concat(firm_parts, ignore_index=True) if firm_parts else data.iloc[0:0].assign(
        nda=[], da=[], da_jones=[])
    return firm_years, pd.DataFrame(coef_rows)


def summarize_coefficients(coefs: pd.DataFrame) -> pd.DataFrame:
    """Distribution of coefficients across industry-years, with a
    Fama-MacBeth style t-statistic (mean / (sd / sqrt(groups)))."""
    rows = []
    for col in [c for c in COEF_NAMES.values() if c in coefs.columns]:
        s = coefs[col].dropna()
        g = len(s)
        fm_t = s.mean() / (s.std(ddof=1) / np.sqrt(g)) if g > 1 and s.std(ddof=1) > 0 else np.nan
        rows.append({"coefficient": col, "groups": g, "mean": s.mean(), "median": s.median(),
                     "pct_positive": (s > 0).mean() * 100, "fama_macbeth_t": fm_t,
                     "pct_groups_t_gt_1.96": (coefs[col + "_t"].abs() > 1.96).mean() * 100})
    rows.append({"coefficient": "adj_r2", "groups": len(coefs), "mean": coefs["adj_r2"].mean(),
                 "median": coefs["adj_r2"].median()})
    return pd.DataFrame(rows)
