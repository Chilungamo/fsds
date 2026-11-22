"""Cover-page error-correction check boxes on annual reports (M5 step 1, design doc §5.3).

Since 2023 the 10-K cover asks two questions, tagged in XBRL as DEI booleans:

* ``DocumentFinStmtErrorCorrectionFlag``: do the financial statements reflect the
  correction of an error in previously issued financial statements?
* ``DocumentFinStmtRestatementRecoveryAnalysisFlag``: were any of those corrections
  restatements that required a recovery (clawback) analysis?

A ticked first box on a regular 10-K is the strongest cheap signal of a correction that
never got an amendment: a "little r" revision, or a "Big R" restatement fixed in the next
annual report. No EDGAR API or full-text search exposes the boxes, so they are read from the
small "Cover" report EDGAR renders for every XBRL filing (R1.htm, ~65 KB) instead of the
primary document (2-8 MB).
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass

from radar.edgar.client import EdgarClient
from radar.edgar.urls import filing_document_url

SCANNED_FORMS = ("10-K", "10-KT")  # amendments (10-K/A) are handled by amendment triage

_TAGS = re.compile(r"<[^>]+>")
# A report row: the element it shows (from its "defref_" link) and the cells after the label.
# Labels vary by filer ("... Error Correction [Flag]", "... Error Correction Flag"), the
# element name does not.
_ROW = re.compile(r"<tr\b.*?</tr>", re.S | re.I)
_DEFREF = re.compile(r"defref_dei_(\w+)")
_CELLS = re.compile(r"<td\b[^>]*>(.*?)</td>", re.S | re.I)
ELEMENTS = {
    "DocumentFinStmtErrorCorrectionFlag": "error_correction",
    "DocumentFinStmtRestatementRecoveryAnalysisFlag": "recovery_analysis",
}
_IS_COVER = re.compile(r"dei_Document(?:Type|PeriodEndDate)|Document Type\b", re.I)


@dataclass(frozen=True)
class CoverFlags:
    error_correction: bool | None  # None: not reported (older filings, 10-Qs, non-XBRL)
    recovery_analysis: bool | None
    source: str  # report the flags were read from (e.g. "R1.htm"), or "missing"

    @property
    def flagged(self) -> bool:
        return self.error_correction is True


def _text(page: str) -> str:
    return " ".join(html.unescape(_TAGS.sub(" ", page)).split())


def parse_cover_report(page: str, source: str = "R1.htm") -> CoverFlags | None:
    """Flags from a rendered XBRL report, or None if the page is not the cover."""
    if not _IS_COVER.search(page):
        return None
    values: dict[str, bool | None] = dict.fromkeys(ELEMENTS.values())
    for row in _ROW.findall(page):
        m = _DEFREF.search(row)
        if not m or m.group(1) not in ELEMENTS:
            continue
        for cell in _CELLS.findall(row)[1:]:  # first cell is the label
            text = _text(cell).lower()
            if text in ("true", "false"):
                values[ELEMENTS[m.group(1)]] = text == "true"
                break
    return CoverFlags(values["error_correction"], values["recovery_analysis"], source)


def cover_report_name(filing_summary: str) -> str | None:
    """The cover report's file name from FilingSummary.xml (MenuCategory "Cover")."""
    for report in re.findall(r"<Report\b.*?</Report>", filing_summary, re.S):
        if re.search(r"<MenuCategory>\s*Cover\s*</MenuCategory>", report, re.I):
            m = re.search(r"<HtmlFileName>\s*([^<\s]+)\s*</HtmlFileName>", report)
            if m:
                return m.group(1)
    return None


def fetch_cover_flags(edgar: EdgarClient, cik: int, accession: str) -> CoverFlags:
    """Read the flags from R1.htm; if R1 is not the cover, find it via FilingSummary.xml."""
    page = edgar.get_optional(filing_document_url(cik, accession, "R1.htm"))
    if page is not None and (flags := parse_cover_report(page.text)):
        return flags
    summary = edgar.get_optional(filing_document_url(cik, accession, "FilingSummary.xml"))
    name = cover_report_name(summary.text) if summary is not None else None
    if name and name != "R1.htm":
        page = edgar.get_optional(filing_document_url(cik, accession, name))
        if page is not None and (flags := parse_cover_report(page.text, name)):
            return flags
    return CoverFlags(None, None, "missing")
