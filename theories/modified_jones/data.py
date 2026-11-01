"""Build the firm-year panel for the (Modified) Jones model from XBRL facts.

Variables (all scaled by lagged total assets A[t-1]):

    TA      = earnings[t] - CFO[t]                  (cash-flow approach, Hribar & Collins 2002)
    dREV    = revenue[t] - revenue[t-1]
    dREC    = receivables[t] - receivables[t-1]
    PPE     = property, plant & equipment at t
    ROA     = earnings[t] / A[t-1]                   (Kothari et al. 2005 variant only)

Prior-year values are looked up at ``start - 1 day`` of the current fiscal
year, i.e. the previous fiscal year-end. That date is reported as a
comparative in the current 10-K, so a lag exists even for a company's first
XBRL filing.
"""
from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from fsds.facts import facts_to_frame
from fsds.standardize import all_chain_concepts, fiscal_year, standardized_items

ITEMS = ("earnings", "cfo", "revenue", "receivables", "ppe", "assets")
NEEDED_CONCEPTS = all_chain_concepts(ITEMS)

# Excluded by convention in accruals research: financial institutions'
# accruals and regulated utilities' accruals are not comparable.
EXCLUDED_SIC_RANGES = ((4900, 4999), (6000, 6999))


def company_items(companyfacts: dict, policy: str = "first") -> dict[str, pd.DataFrame]:
    facts = facts_to_frame(companyfacts, concepts=NEEDED_CONCEPTS)
    return standardized_items(facts, ITEMS, policy=policy)


def concat_items(per_company: Iterable[dict[str, pd.DataFrame]]) -> dict[str, pd.DataFrame]:
    buckets: dict[str, list[pd.DataFrame]] = {k: [] for k in ITEMS}
    for items in per_company:
        for k in ITEMS:
            if not items[k].empty:
                buckets[k].append(items[k])
    return {k: (pd.concat(v, ignore_index=True) if v else
                pd.DataFrame(columns=["cik", "start", "end", k, f"{k}_tag"]))
            for k, v in buckets.items()}


def _at(items: pd.DataFrame, name: str, date_col: str, suffix: str = "") -> pd.DataFrame:
    """Re-key an item table on (cik, <date_col>) for merging."""
    out = items[["cik", "end", name, f"{name}_tag"]].rename(
        columns={"end": date_col, name: name + suffix, f"{name}_tag": f"{name}_tag{suffix}"})
    return out.drop_duplicates(["cik", date_col])


def build_panel(items: dict[str, pd.DataFrame], sic: pd.Series | None = None) -> pd.DataFrame:
    """Firm-year panel with raw and scaled Jones-model variables.

    ``sic``: Series indexed by 10-digit CIK string with integer SIC codes.
    """
    spine = items["earnings"].drop_duplicates(["cik", "end"]).copy()
    spine["prior_end"] = spine["start"] - pd.Timedelta(days=1)
    spine["fyear"] = fiscal_year(spine["end"])

    p = spine.merge(_at(items["cfo"], "cfo", "end"), on=["cik", "end"], how="left")
    p = p.merge(_at(items["revenue"], "revenue", "end"), on=["cik", "end"], how="left")
    p = p.merge(_at(items["revenue"], "revenue", "prior_end", "_lag"), on=["cik", "prior_end"], how="left")
    p = p.merge(_at(items["receivables"], "receivables", "end"), on=["cik", "end"], how="left")
    p = p.merge(_at(items["receivables"], "receivables", "prior_end", "_lag"),
                on=["cik", "prior_end"], how="left")
    p = p.merge(_at(items["ppe"], "ppe", "end"), on=["cik", "end"], how="left")
    p = p.merge(_at(items["assets"], "assets", "prior_end", "_lag"), on=["cik", "prior_end"], how="left")

    if sic is not None:
        p["sic"] = p["cik"].map(sic)
    else:
        p["sic"] = np.nan
    p["sic"] = pd.to_numeric(p["sic"], errors="coerce")
    p["sic2"] = (p["sic"] // 100).astype("Int64")

    a = p["assets_lag"].where(p["assets_lag"] > 0)
    p["ta"] = p["earnings"] - p["cfo"]
    p["d_rev"] = p["revenue"] - p["revenue_lag"]
    p["d_rec"] = p["receivables"] - p["receivables_lag"]

    p["y_ta"] = p["ta"] / a
    p["x_inv_a"] = 1.0 / a
    p["x_drev"] = p["d_rev"] / a
    p["x_drev_drec"] = (p["d_rev"] - p["d_rec"]) / a
    p["x_ppe"] = p["ppe"] / a
    p["x_roa"] = p["earnings"] / a
    return p


def apply_sample_filters(panel: pd.DataFrame, require_sic: bool = True) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Standard sample screens. Returns (sample, attrition table)."""
    steps = [("firm-years with annual earnings", panel)]
    s = panel
    if require_sic:
        s = s[s["sic"].notna()]
        steps.append(("with SIC code", s))
        excl = np.zeros(len(s), dtype=bool)
        for lo, hi in EXCLUDED_SIC_RANGES:
            excl |= s["sic"].between(lo, hi).to_numpy()
        s = s[~excl]
        steps.append(("excluding financials (60-69) and utilities (49)", s))
    s = s[s["assets_lag"] > 0]
    steps.append(("with positive lagged total assets", s))
    s = s.dropna(subset=["y_ta", "x_drev", "x_drev_drec", "x_ppe"])
    steps.append(("with all model variables (CFO, revenue, receivables, PPE, lags)", s))
    attrition = pd.DataFrame({"step": [n for n, _ in steps], "firm_years": [len(d) for _, d in steps]})
    return s.reset_index(drop=True), attrition
