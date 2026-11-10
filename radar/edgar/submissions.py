"""Per-company filing metadata from data.sec.gov/submissions (8-K items, primary docs)."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime

from radar.edgar.client import EdgarClient
from radar.models import parse_items


@dataclass(frozen=True)
class SubmissionInfo:
    form: str
    filed: date
    items: list[str]
    primary_doc: str
    accepted_at: datetime | None


class SubmissionsCache:
    """Caches only the fields we need, per CIK, for ``ttl`` seconds.

    One request answers every filing of a company, which makes item lookup for a day's
    worth of 8-Ks far cheaper than fetching each filing's header.
    """

    def __init__(self, edgar: EdgarClient, ttl: float = 600) -> None:
        self.edgar = edgar
        self.ttl = ttl
        self._data: dict[int, tuple[float, dict[str, SubmissionInfo]]] = {}

    def company(self, cik: int, refresh: bool = False) -> dict[str, SubmissionInfo]:
        hit = self._data.get(cik)
        if hit and not refresh and time.monotonic() - hit[0] < self.ttl:
            return hit[1]
        recent = self.edgar.submissions(cik)["filings"]["recent"]
        info: dict[str, SubmissionInfo] = {}
        for i, acc in enumerate(recent["accessionNumber"]):
            accepted = recent.get("acceptanceDateTime", [None] * (i + 1))[i]
            info[acc] = SubmissionInfo(
                form=recent["form"][i],
                filed=date.fromisoformat(recent["filingDate"][i]),
                items=parse_items(recent.get("items", [""] * (i + 1))[i]) or [],
                primary_doc=recent["primaryDocument"][i],
                accepted_at=datetime.fromisoformat(accepted.replace("Z", "+00:00"))
                if accepted
                else None,
            )
        self._data[cik] = (time.monotonic(), info)
        return info

    def lookup(self, cik: int, accession: str) -> SubmissionInfo | None:
        info = self.company(cik).get(accession)
        if info is None:  # filed after our cached copy was taken
            info = self.company(cik, refresh=True).get(accession)
        return info
