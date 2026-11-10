"""Helpers for EDGAR identifiers and URLs."""

from __future__ import annotations

import re

_ACC_RE = re.compile(r"^(\d{10})-?(\d{2})-?(\d{6})$")

WWW = "https://www.sec.gov"
DATA = "https://data.sec.gov"


def normalize_accession(accession: str) -> str:
    """Return the dashed form ``0000000000-YY-NNNNNN``."""
    m = _ACC_RE.match(accession.strip())
    if not m:
        raise ValueError(f"Not an EDGAR accession number: {accession!r}")
    return "-".join(m.groups())


def accession_nodash(accession: str) -> str:
    return normalize_accession(accession).replace("-", "")


def cik10(cik: int | str) -> str:
    return f"{int(cik):010d}"


def submissions_url(cik: int | str) -> str:
    return f"{DATA}/submissions/CIK{cik10(cik)}.json"


def companyfacts_url(cik: int | str) -> str:
    return f"{DATA}/api/xbrl/companyfacts/CIK{cik10(cik)}.json"


def filing_folder_url(cik: int | str, accession: str) -> str:
    return f"{WWW}/Archives/edgar/data/{int(cik)}/{accession_nodash(accession)}"


def filing_index_url(cik: int | str, accession: str) -> str:
    return f"{filing_folder_url(cik, accession)}/index.json"


def filing_document_url(cik: int | str, accession: str, filename: str) -> str:
    return f"{filing_folder_url(cik, accession)}/{filename}"
