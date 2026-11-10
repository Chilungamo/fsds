"""Measure detection recall against EDGAR full-text search (the M1 exit criterion)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import psycopg

from radar.edgar import fulltext
from radar.edgar.client import EdgarClient
from radar.models import NON_RELIANCE_ITEM, WARNING_FORMS
from radar.workers.backfill import NON_RELIANCE_QUERIES


@dataclass
class RecallReport:
    start: date
    end: date
    reference: set[str]  # Item 4.02 filings according to full-text search
    detected: set[str]  # Item 4.02 filings in our database

    @property
    def found(self) -> set[str]:
        return self.reference & self.detected

    @property
    def missed(self) -> set[str]:
        return self.reference - self.detected

    @property
    def extra(self) -> set[str]:
        """Detected by us but absent from full-text search."""
        return self.detected - self.reference

    @property
    def recall(self) -> float | None:
        return len(self.found) / len(self.reference) if self.reference else None


def measure(conn: psycopg.Connection, edgar: EdgarClient, start: date, end: date) -> RecallReport:
    reference = set()
    for q in NON_RELIANCE_QUERIES:
        for h in fulltext.search(edgar, q, sorted(WARNING_FORMS), start, end):
            if NON_RELIANCE_ITEM in h.items:
                reference.add(h.accession)
    rows = conn.execute(
        # backfill rows came *from* full-text search; counting them would be circular
        """SELECT accession FROM filing WHERE %s = ANY(items) AND form = ANY(%s)
           AND filed_date BETWEEN %s AND %s AND source <> 'backfill'""",
        (NON_RELIANCE_ITEM, sorted(WARNING_FORMS), start, end),
    )
    return RecallReport(start, end, reference, {r["accession"] for r in rows})
