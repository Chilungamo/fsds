"""Nightly reconciliation: compare each day's EDGAR form index with what the live
watcher saw, and enqueue anything it missed (items looked up via submissions)."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

import psycopg

from radar.db import repo
from radar.edgar.client import EdgarClient
from radar.edgar.feeds import fetch_daily_index, iter_days
from radar.edgar.submissions import SubmissionsCache
from radar.models import WARNING_FORMS

log = logging.getLogger(__name__)


@dataclass
class DayResult:
    day: date
    available: bool
    listed: int = 0
    missing: int = 0
    non_reliance: list[str] = field(default_factory=list)


def reconcile_day(
    conn: psycopg.Connection, edgar: EdgarClient, subs: SubmissionsCache, day: date
) -> DayResult:
    refs = fetch_daily_index(edgar, day)
    if refs is None:
        return DayResult(day, available=False)
    known = repo.known_accessions(conn, (r.accession for r in refs))
    result = DayResult(day, True, listed=len(refs))
    for ref in refs:
        if ref.accession in known:
            continue
        if ref.form in WARNING_FORMS:
            try:
                info = subs.lookup(ref.cik, ref.accession)
            except Exception as exc:  # leave items unknown; the pipeline retries the lookup
                log.warning("submissions lookup failed for %s: %s", ref.accession, exc)
                info = None
            if info is not None:
                ref.items, ref.primary_doc, ref.accepted_at = (
                    info.items,
                    info.primary_doc,
                    info.accepted_at,
                )
        if repo.insert_filing(conn, ref):
            result.missing += 1
            if ref.is_non_reliance:
                result.non_reliance.append(ref.accession)
    repo.set_cursor(conn, "reconcile:last_day", day.isoformat())
    return result


def reconcile(
    conn: psycopg.Connection, edgar: EdgarClient, start: date, end: date
) -> Iterator[DayResult]:
    """Reconcile each business day in [start, end], yielding as each day completes."""
    subs = SubmissionsCache(edgar, ttl=86_400)
    for d in iter_days(start, end):
        yield reconcile_day(conn, edgar, subs, d)
