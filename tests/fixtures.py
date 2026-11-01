"""Builders for synthetic SEC companyfacts JSON (same shape as the real API)."""
from __future__ import annotations

from collections import defaultdict


class FactsBuilder:
    def __init__(self, cik: int, name: str = "TEST CO"):
        self.cik, self.name = cik, name
        self.facts = defaultdict(list)

    def add(self, concept, val, end, start=None, accn="0000000000-24-000001", form="10-K",
            filed="2024-02-15", fy=2023, fp="FY", frame=None):
        e = {"end": end, "val": val, "accn": accn, "fy": fy, "fp": fp, "form": form, "filed": filed}
        if start:
            e["start"] = start
        if frame:
            e["frame"] = frame
        self.facts[concept].append(e)
        return self

    def build(self) -> dict:
        return {"cik": self.cik, "entityName": self.name,
                "facts": {"us-gaap": {c: {"label": c, "units": {"USD": v}} for c, v in self.facts.items()}}}


def annual_report(b: FactsBuilder, year: int, filed: str, accn: str, cur: dict, prior: dict,
                  revenue_tag: str = "Revenues", fye: str = "12-31", fys: str = "01-01"):
    """Add one 10-K with current-year and prior-year (comparative) values.

    ``cur``/``prior``: dicts with keys ni, cfo, rev, ar, ppe, assets.
    """
    for y, vals in ((year, cur), (year - 1, prior)):
        start, end = f"{y}-{fys}", f"{y}-{fye}"
        kw = dict(accn=accn, filed=filed, fy=year)
        b.add("NetIncomeLoss", vals["ni"], end, start, **kw)
        b.add("NetCashProvidedByUsedInOperatingActivities", vals["cfo"], end, start, **kw)
        b.add(revenue_tag, vals["rev"], end, start, **kw)
        b.add("AccountsReceivableNetCurrent", vals["ar"], end, **kw)
        b.add("PropertyPlantAndEquipmentNet", vals["ppe"], end, **kw)
        b.add("Assets", vals["assets"], end, **kw)
    return b
