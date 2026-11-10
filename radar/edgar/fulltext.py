"""EDGAR full-text search (efts.sec.gov). Not an official API: used for backfill
candidates and recall measurement only, never in the live path."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from urllib.parse import urlencode

from radar.edgar.client import EdgarClient
from radar.models import parse_items

EFTS = "https://efts.sec.gov/LATEST/search-index"
PAGE = 100
MAX_RESULTS = 10_000  # efts refuses to page beyond this


@dataclass(frozen=True)
class SearchHit:
    accession: str
    cik: int
    form: str
    company: str
    filed_date: date
    items: list[str]
    document: str  # file inside the filing that matched
    is_primary: bool  # matched file is the main document, not an exhibit


def search(
    edgar: EdgarClient, query: str, forms: list[str], start: date, end: date
) -> Iterator[SearchHit]:
    """Yield every hit (all pages), once per (accession, document).

    Each form is queried on its own: efts mishandles some multi-form filters
    (``forms=8-K,8-K/A`` returns only the 8-K/As, while ``forms=8-K`` returns both).
    """
    seen: set[tuple[str, str]] = set()
    for form in forms:
        for hit in _search_one(edgar, query, form, start, end):
            if (hit.accession, hit.document) not in seen:
                seen.add((hit.accession, hit.document))
                yield hit


def _search_one(
    edgar: EdgarClient, query: str, form: str, start: date, end: date
) -> Iterator[SearchHit]:
    offset = 0
    while True:
        params = {
            "q": query,
            "forms": form,
            "dateRange": "custom",
            "startdt": start.isoformat(),
            "enddt": end.isoformat(),
            "from": offset,
        }
        data = edgar.get_json(f"{EFTS}?{urlencode(params)}")
        hits = data.get("hits", {}).get("hits", [])
        for h in hits:
            src = h["_source"]
            acc, _, doc = h["_id"].partition(":")
            yield SearchHit(
                accession=acc,
                cik=int(src["ciks"][0]),
                form=src["form"],
                company=(src.get("display_names") or [""])[0].split("  (")[0].strip(),
                filed_date=date.fromisoformat(src["file_date"]),
                items=parse_items(src.get("items") or []) or [],
                document=doc,
                is_primary=src.get("file_type") == src["form"],
            )
        total = data.get("hits", {}).get("total", {}).get("value", 0)
        offset += PAGE
        if not hits or offset >= min(total, MAX_RESULTS):
            return
