"""Plain data objects shared between EDGAR readers, the pipeline and the database."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

WARNING_FORMS = {"8-K", "8-K/A"}
AMENDMENT_FORMS = {"10-K/A", "10-Q/A"}
WATCHED_FORMS = WARNING_FORMS | AMENDMENT_FORMS
NON_RELIANCE_ITEM = "4.02"


@dataclass
class FilingRef:
    """A filing discovered on EDGAR, before any document has been downloaded."""

    accession: str
    cik: int
    form: str
    company: str
    filed_date: date
    source: str
    accepted_at: datetime | None = None
    items: list[str] | None = None  # None = not known yet
    primary_doc: str | None = None
    meta: dict = field(default_factory=dict)

    @property
    def is_non_reliance(self) -> bool:
        return self.form in WARNING_FORMS and NON_RELIANCE_ITEM in (self.items or [])


def parse_items(raw: str | list[str] | None) -> list[str] | None:
    """'4.02,9.01' -> ['4.02', '9.01']; '' -> []; None -> None."""
    if raw is None:
        return None
    parts = raw if isinstance(raw, list) else raw.split(",")
    return sorted({p.strip() for p in parts if p and p.strip()})
