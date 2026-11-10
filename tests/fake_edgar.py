"""An in-memory EDGAR for pipeline tests, serving the real fixture documents."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import httpx

from radar.edgar.client import EdgarClient
from radar.extract.ixbrl import load_document

from .conftest import FIXTURES

EMPTY_FEED = b'<feed xmlns="http://www.w3.org/2005/Atom"></feed>'


@dataclass
class FakeFiling:
    cik: int
    accession: str
    form: str
    filed: str
    doc: str
    items: str = ""
    accepted: str = ""


# Real filings from the fixtures (metadata as listed in each company's submissions JSON).
FILINGS = [
    FakeFiling(
        1308208,
        "0001193125-26-098600",
        "8-K",
        "2026-03-09",
        "ulh-20260303.htm",
        "4.02,9.01",
        "2026-03-09T20:25:49.000Z",
    ),
    FakeFiling(
        1308208,
        "0001193125-26-098732",
        "10-Q/A",
        "2026-03-09",
        "ulh-20250927.htm",
        "",
        "2026-03-09T20:46:33.000Z",
    ),
    FakeFiling(
        1839341,
        "0001628280-26-013214",
        "8-K",
        "2026-03-02",
        "core-20260302.htm",
        "2.02,4.02,7.01,9.01",
        "2026-03-02T21:11:38.000Z",
    ),
    FakeFiling(
        1839341,
        "0001628280-26-013286",
        "10-Q/A",
        "2026-03-02",
        "core-20250331.htm",
        "",
        "2026-03-02T21:44:11.000Z",
    ),
    FakeFiling(
        1162461,
        "0001628280-23-042507",
        "8-K",
        "2023-12-21",
        "cutr-20231221.htm",
        "4.02",
        "2023-12-21T21:32:37.000Z",
    ),
    FakeFiling(
        1162461,
        "0001628280-24-008911",
        "10-Q/A",
        "2024-03-05",
        "cutr-20230331.htm",
        "",
        "2024-03-05T21:21:56.000Z",
    ),
    FakeFiling(
        1162461,
        "0001193125-23-130742",
        "10-K/A",
        "2023-05-01",
        "d492883d10ka.htm",
        "",
        "2023-05-01T21:25:08.000Z",
    ),
    FakeFiling(
        1992243,
        "0001213900-25-077935",
        "8-K",
        "2025-08-18",
        "ea0253661-8k_t1energy.htm",
        "4.02,9.01",
        "2025-08-18T20:15:46.000Z",
    ),
    FakeFiling(
        1992243,
        "0001992243-25-000029",
        "10-Q/A",
        "2025-08-18",
        "t1-20250331.htm",
        "",
        "2025-08-18T20:45:14.000Z",
    ),
]
NAMES = {
    1308208: "UNIVERSAL LOGISTICS HOLDINGS, INC.",
    1839341: "Core Scientific, Inc./tx",
    1162461: "CUTERA INC",
    1992243: "T1 Energy Inc.",
}


def by_accession(acc: str) -> FakeFiling:
    return next(f for f in FILINGS if f.accession == acc)


@dataclass
class FakeEdgar:
    feeds: dict[str, bytes] = field(default_factory=dict)  # feed type -> Atom XML
    daily_index: dict[str, str] = field(default_factory=dict)  # YYYYMMDD -> idx text
    broken_docs: set[str] = field(default_factory=set)  # accessions that answer 500
    extra: list[FakeFiling] = field(default_factory=list)
    not_yet_filed: set[str] = field(default_factory=set)  # hidden from submissions
    requests: list[str] = field(default_factory=list)

    def handler(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.requests.append(url)
        if m := re.search(r"/submissions/CIK(\d{10})\.json$", url):
            cik = int(m[1])
            fs = [
                f
                for f in FILINGS + self.extra
                if f.cik == cik and f.accession not in self.not_yet_filed
            ]
            recent = {
                "accessionNumber": [f.accession for f in fs],
                "form": [f.form for f in fs],
                "filingDate": [f.filed for f in fs],
                "items": [f.items for f in fs],
                "primaryDocument": [f.doc for f in fs],
                "acceptanceDateTime": [f.accepted for f in fs],
            }
            return httpx.Response(
                200, json={"name": NAMES.get(cik, ""), "filings": {"recent": recent}}
            )
        if m := re.search(r"/Archives/edgar/data/(\d+)/(\d{18})/(.+)$", url):
            acc = f"{m[2][:10]}-{m[2][10:12]}-{m[2][12:]}"
            if acc in self.broken_docs:
                return httpx.Response(500)
            path = FIXTURES / "filings" / f"{acc}_{m[3]}.gz"
            if path.exists():
                return httpx.Response(200, content=load_document(path))
            return httpx.Response(404)
        if m := re.search(r"action=getcurrent&type=([^&]+)", url):
            form = m[1].replace("%2F", "/")
            start = int(re.search(r"start=(\d+)", url)[1])
            return httpx.Response(
                200, content=self.feeds.get(form, EMPTY_FEED) if start == 0 else EMPTY_FEED
            )
        if m := re.search(r"/daily-index/\d{4}/QTR\d/form\.(\d{8})\.idx$", url):
            text = self.daily_index.get(m[1])
            return httpx.Response(200, text=text) if text else httpx.Response(403, text="denied")
        return httpx.Response(404)

    def client(self) -> EdgarClient:
        return EdgarClient(
            user_agent="RestatementRadar/test test@example.com",
            rate=1000,
            transport=httpx.MockTransport(self.handler),
            sleep=lambda s: None,
            max_retries=2,
        )


def feed_entry(f: FakeFiling, company: str | None = None) -> str:
    items = "".join(f"&lt;br&gt;Item {i}: x\n" for i in f.items.split(",") if i)
    return (
        f"<entry><title>{f.form} - {company or NAMES.get(f.cik, 'Noise Corp')} ({f.cik:010d}) (Filer)</title>"
        f'<summary type="html"> &lt;b&gt;Filed:&lt;/b&gt; {f.filed} &lt;b&gt;AccNo:&lt;/b&gt; '
        f"{f.accession} {items}</summary><updated>{f.filed}T16:00:00-05:00</updated>"
        f"<id>urn:tag:sec.gov,2008:accession-number={f.accession}</id></entry>"
    )


def feed(*filings: FakeFiling) -> bytes:
    return (
        '<feed xmlns="http://www.w3.org/2005/Atom">'
        + "".join(feed_entry(f) for f in filings)
        + "</feed>"
    ).encode()
