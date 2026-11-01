"""Turn SEC ``companyfacts`` JSON into a tidy, long DataFrame.

One row per reported fact: (cik, concept, unit, start, end, val) plus the
filing it came from (accn, form, filed, fy, fp, frame). ``start`` is NaT for
instant (balance-sheet) facts.

Note: ``fy``/``fp`` describe the *filing* the fact appeared in, not the period
the fact measures. A FY2023 10-K also reports FY2022 comparatives with
fy=2023. Always identify periods by ``start``/``end``.
"""
from __future__ import annotations

from typing import Iterable

import pandas as pd

COLUMNS = ["cik", "entity", "taxonomy", "concept", "unit", "start", "end", "val",
           "accn", "form", "filed", "fy", "fp", "frame"]


def facts_to_frame(companyfacts: dict, concepts: Iterable[str] | None = None,
                   taxonomy: str = "us-gaap", units: Iterable[str] = ("USD",)) -> pd.DataFrame:
    wanted = set(concepts) if concepts is not None else None
    units = set(units)
    cik = str(companyfacts.get("cik", "")).zfill(10)
    entity = companyfacts.get("entityName")
    rows = []
    for concept, body in companyfacts.get("facts", {}).get(taxonomy, {}).items():
        if wanted is not None and concept not in wanted:
            continue
        for unit, entries in body.get("units", {}).items():
            if unit not in units:
                continue
            for e in entries:
                rows.append((cik, entity, taxonomy, concept, unit, e.get("start"), e.get("end"),
                             e.get("val"), e.get("accn"), e.get("form"), e.get("filed"),
                             e.get("fy"), e.get("fp"), e.get("frame")))
    df = pd.DataFrame(rows, columns=COLUMNS)
    for col in ("start", "end", "filed"):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df["val"] = pd.to_numeric(df["val"], errors="coerce")
    return df


def many_facts_to_frame(docs: Iterable[dict], **kwargs) -> pd.DataFrame:
    frames = [facts_to_frame(d, **kwargs) for d in docs if d]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=COLUMNS)
    return pd.concat(frames, ignore_index=True)
