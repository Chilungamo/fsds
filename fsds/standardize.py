"""Standardisation of raw XBRL facts into one value per company-period.

Three problems with raw ``companyfacts`` data that this module solves:

1. **Repeated periods.** A period is reported again as a comparative in later
   filings (and sometimes restated). :func:`point_in_time` keeps exactly one
   value per period: the first-reported value (``policy="first"``, what an
   investor saw at the time) or the latest (``policy="latest"``, incorporating
   restatements).
2. **Tag changes.** Filers switch tags over time, e.g. ``Revenues`` →
   ``RevenueFromContractWithCustomerExcludingAssessedTax`` after ASC 606
   (2018). :func:`coalesce_concepts` walks a fallback chain per period and
   records which tag supplied the value.
3. **Mixed period lengths.** Annual reports also contain quarterly
   durations. :func:`annual_durations` keeps ~12-month periods only
   (52/53-week fiscal years included).
"""
from __future__ import annotations

from typing import Sequence

import pandas as pd

ANNUAL_FORMS = ("10-K", "10-K/A", "10-KT", "10-KT/A", "20-F", "20-F/A", "40-F", "40-F/A")

# Fallback chains: first tag present for a period wins. Order matters.
CONCEPT_CHAINS: dict[str, list[str]] = {
    "revenue": [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueNet",
        "SalesRevenueGoodsNet",
        "SalesRevenueServicesNet",
    ],
    # Income before extraordinary items / discontinued operations, as in
    # Hribar & Collins (2002); fall back to bottom-line net income.
    "earnings": [
        "IncomeLossFromContinuingOperations",
        "NetIncomeLoss",
        "ProfitLoss",
    ],
    "cfo": [
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
        "NetCashProvidedByUsedInOperatingActivities",
    ],
    "receivables": [
        "AccountsReceivableNetCurrent",
        "ReceivablesNetCurrent",
        "AccountsNotesAndLoansReceivableNetCurrent",
    ],
    # Jones (1991) uses gross PPE; many filers only tag net PPE on the face.
    "ppe": [
        "PropertyPlantAndEquipmentGross",
        "PropertyPlantAndEquipmentNet",
        "PropertyPlantAndEquipmentAndFinanceLeaseRightOfUseAssetAfterAccumulatedDepreciationAndAmortization",
    ],
    "assets": ["Assets"],
}

FLOW_ITEMS = ("revenue", "earnings", "cfo")
STOCK_ITEMS = ("receivables", "ppe", "assets")


def all_chain_concepts(items: Sequence[str]) -> list[str]:
    return [c for item in items for c in CONCEPT_CHAINS[item]]


def annual_filings(df: pd.DataFrame, forms: Sequence[str] = ANNUAL_FORMS) -> pd.DataFrame:
    return df[df["form"].isin(forms)]


def annual_durations(df: pd.DataFrame, min_days: int = 350, max_days: int = 380) -> pd.DataFrame:
    days = (df["end"] - df["start"]).dt.days
    return df[df["start"].notna() & days.between(min_days, max_days)]


def instants(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["start"].isna()]


def point_in_time(df: pd.DataFrame, policy: str = "first") -> pd.DataFrame:
    """One row per (cik, concept, start, end): first- or latest-filed value."""
    if policy not in ("first", "latest"):
        raise ValueError("policy must be 'first' or 'latest'")
    keys = ["cik", "concept", "start", "end"]
    ordered = df.sort_values(["filed", "accn"], ascending=True, kind="mergesort")
    pick = ordered.groupby(keys, dropna=False, sort=False)
    out = pick.head(1) if policy == "first" else pick.tail(1)
    return out.reset_index(drop=True)


def coalesce_concepts(df: pd.DataFrame, chain: Sequence[str], name: str) -> pd.DataFrame:
    """For each (cik, start, end) take the first concept in ``chain`` that has a value.

    Returns columns: cik, start, end, <name>, <name>_tag.
    """
    rank = {c: i for i, c in enumerate(chain)}
    sub = df[df["concept"].isin(rank) & df["val"].notna()].copy()
    sub["_rank"] = sub["concept"].map(rank)
    sub = sub.sort_values("_rank", kind="mergesort")
    best = sub.groupby(["cik", "start", "end"], dropna=False, sort=False).head(1)
    return (best[["cik", "start", "end", "val", "concept"]]
            .rename(columns={"val": name, "concept": f"{name}_tag"})
            .reset_index(drop=True))


def fiscal_year(end: pd.Series) -> pd.Series:
    """Compustat convention: FY = calendar year of period end if it ends in
    June or later, otherwise the previous year."""
    return end.dt.year.where(end.dt.month >= 6, end.dt.year - 1)


def standardized_items(facts: pd.DataFrame, items: Sequence[str], policy: str = "first"
                       ) -> dict[str, pd.DataFrame]:
    """Clean annual flows/instants for each requested item, keyed by item name."""
    base = point_in_time(annual_filings(facts), policy=policy)
    flows, stocks = annual_durations(base), instants(base)
    out = {}
    for item in items:
        src = flows if item in FLOW_ITEMS else stocks
        out[item] = coalesce_concepts(src, CONCEPT_CHAINS[item], item)
    return out
