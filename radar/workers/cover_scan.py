"""Nightly cover-flag scan (M5 step 1): read the error-correction check boxes of every
10-K filed on a day and record them in ``cover_scan``. Ticked boxes are the starting point
for finding corrections made without an amendment (M5 steps 2-3)."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

import psycopg

from radar.db import repo
from radar.detect.cover_flags import SCANNED_FORMS, fetch_cover_flags
from radar.edgar.client import EdgarClient
from radar.edgar.feeds import fetch_daily_index

log = logging.getLogger(__name__)


@dataclass
class ScanResult:
    day: date
    available: bool
    listed: int = 0
    scanned: int = 0
    missing: int = 0  # no cover report found
    flagged: list[str] = field(default_factory=list)  # accessions with the box ticked


def scan_day(conn: psycopg.Connection, edgar: EdgarClient, day: date) -> ScanResult:
    refs = fetch_daily_index(edgar, day, forms=SCANNED_FORMS)
    if refs is None:
        return ScanResult(day, available=False)
    done = {
        r["accession"]
        for r in conn.execute(
            "SELECT accession FROM cover_scan WHERE accession = ANY(%s)",
            ([r.accession for r in refs],),
        )
    }
    result = ScanResult(day, True, listed=len(refs))
    for ref in refs:
        if ref.accession in done:
            continue
        try:
            flags = fetch_cover_flags(edgar, ref.cik, ref.accession)
        except Exception as exc:  # one bad filing must not stop the night's scan
            log.warning("cover scan of %s failed: %s", ref.accession, exc)
            continue
        repo.upsert_company(conn, ref.cik, ref.company)
        conn.execute(
            """INSERT INTO cover_scan (accession, cik, form, filed_date, error_correction,
                 recovery_analysis, source) VALUES (%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (accession) DO NOTHING""",
            (
                ref.accession,
                ref.cik,
                ref.form,
                ref.filed_date,
                flags.error_correction,
                flags.recovery_analysis,
                flags.source,
            ),
        )
        result.scanned += 1
        result.missing += flags.source == "missing"
        if flags.flagged:
            result.flagged.append(ref.accession)
    return result
