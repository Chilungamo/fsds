"""Historical backfill via EDGAR full-text search.

Checking the items of every 8-K since 2019 would take ~700k requests, so backfill uses
full-text search to find *candidates* and keeps only hits whose SEC metadata lists Item
4.02. Live detection never depends on full-text search.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta

import psycopg

from radar.db import repo
from radar.edgar import fulltext
from radar.edgar.client import EdgarClient
from radar.models import AMENDMENT_FORMS, NON_RELIANCE_ITEM, WARNING_FORMS, FilingRef

log = logging.getLogger(__name__)

NON_RELIANCE_QUERIES = ('"Item 4.02"', '"Non-Reliance on Previously Issued"')
AMENDMENT_QUERIES = ('"as previously reported"',)


@dataclass
class BackfillResult:
    window: tuple[date, date]
    hits: int
    inserted: int


def quarters(start: date, end: date):
    """Calendar-quarter windows covering [start, end]; efts caps a query at 10k hits."""
    d = start
    while d <= end:
        first_month_next = (d.month - 1) // 3 * 3 + 4
        nxt = date(d.year + 1, 1, 1) if first_month_next > 12 else date(d.year, first_month_next, 1)
        yield d, min(nxt - timedelta(days=1), end)
        d = nxt


def _insert_hits(conn, hits, keep) -> int:
    inserted = 0
    seen: dict[str, fulltext.SearchHit] = {}
    for h in hits:
        if keep(h) and (h.accession not in seen or h.is_primary):
            seen[h.accession] = h
    for h in seen.values():
        ref = FilingRef(
            accession=h.accession,
            cik=h.cik,
            form=h.form,
            company=h.company,
            filed_date=h.filed_date,
            items=h.items if h.form in WARNING_FORMS else None,
            primary_doc=h.document if h.is_primary else None,
            source="backfill",
        )
        inserted += repo.insert_filing(conn, ref)
    return inserted


def backfill(
    conn: psycopg.Connection,
    edgar: EdgarClient,
    start: date,
    end: date,
    amendments: bool = False,
) -> Iterator[BackfillResult]:
    """Backfill Item 4.02 8-Ks (and optionally candidate restatement amendments),
    one calendar quarter at a time, yielding as each quarter completes."""
    for q_start, q_end in quarters(start, end):
        hits = []
        for q in NON_RELIANCE_QUERIES:
            hits += list(fulltext.search(edgar, q, sorted(WARNING_FORMS), q_start, q_end))
        n = _insert_hits(
            conn, hits, lambda h: h.form in WARNING_FORMS and NON_RELIANCE_ITEM in h.items
        )
        if amendments:
            for q in AMENDMENT_QUERIES:
                am = list(fulltext.search(edgar, q, sorted(AMENDMENT_FORMS), q_start, q_end))
                hits += am
                n += _insert_hits(conn, am, lambda h: h.form in AMENDMENT_FORMS)
        repo.set_cursor(conn, "backfill:last_day", q_end.isoformat())
        log.info("backfill %s..%s: %d hits, %d new filings", q_start, q_end, len(hits), n)
        yield BackfillResult((q_start, q_end), len(hits), n)
