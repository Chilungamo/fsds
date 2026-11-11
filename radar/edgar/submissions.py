"""Per-company filing metadata from data.sec.gov/submissions (8-K items, primary docs)."""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime

from radar.edgar.client import EdgarClient
from radar.models import parse_items

DEFAULT_MAX_COMPANIES = 128


@dataclass(frozen=True, slots=True)
class SubmissionInfo:
    form: str
    filed: date
    items: list[str]
    primary_doc: str
    accepted_at: datetime | None


def parse_recent(recent: dict) -> dict[str, SubmissionInfo]:
    """The "recent" block (up to ~1,000 filings) as accession -> SubmissionInfo."""
    accs = recent["accessionNumber"]
    n = len(accs)
    accepted = recent.get("acceptanceDateTime") or [None] * n
    items = recent.get("items") or [""] * n
    out: dict[str, SubmissionInfo] = {}
    for i, acc in enumerate(accs):
        ts = accepted[i]
        out[acc] = SubmissionInfo(
            form=recent["form"][i],
            filed=date.fromisoformat(recent["filingDate"][i]),
            items=parse_items(items[i]) or [],
            primary_doc=recent["primaryDocument"][i],
            accepted_at=datetime.fromisoformat(ts.replace("Z", "+00:00")) if ts else None,
        )
    return out


class SubmissionsCache:
    """Caches only the fields we need, per CIK, for ``ttl`` seconds.

    One request answers every filing of a company, which makes item lookup for a day's
    worth of 8-Ks far cheaper than fetching each filing's header. The cache keeps at most
    ``max_companies`` companies (least recently used are dropped): a busy filer brings up
    to ~1,000 filings, and an unbounded cache ran a 30-day reconcile out of memory on a
    512 MB instance.
    """

    def __init__(
        self, edgar: EdgarClient, ttl: float = 600, max_companies: int = DEFAULT_MAX_COMPANIES
    ) -> None:
        self.edgar = edgar
        self.ttl = ttl
        self.max_companies = max_companies
        self._data: OrderedDict[int, tuple[float, dict[str, SubmissionInfo]]] = OrderedDict()

    def __len__(self) -> int:
        return len(self._data)

    def company(self, cik: int, refresh: bool = False) -> dict[str, SubmissionInfo]:
        hit = self._data.get(cik)
        if hit and not refresh and time.monotonic() - hit[0] < self.ttl:
            self._data.move_to_end(cik)
            return hit[1]
        info = parse_recent(self.edgar.submissions(cik)["filings"]["recent"])
        self._data[cik] = (time.monotonic(), info)
        self._data.move_to_end(cik)
        while len(self._data) > self.max_companies:
            self._data.popitem(last=False)
        return info

    def lookup(self, cik: int, accession: str) -> SubmissionInfo | None:
        info = self.company(cik).get(accession)
        if info is None:  # filed after our cached copy was taken
            info = self.company(cik, refresh=True).get(accession)
        return info
