"""M5 step 1: 10-K cover error-correction check boxes, read from EDGAR's rendered cover report."""

import httpx
import pytest

from radar.detect.cover_flags import (
    CoverFlags,
    cover_report_name,
    fetch_cover_flags,
    parse_cover_report,
)
from radar.edgar.client import EdgarClient
from radar.extract.ixbrl import load_document

from .conftest import FIXTURES
from .test_pipeline_db import conn  # noqa: F401  (fixture)

F = FIXTURES / "filings"
ADTRAN = "0001193125-26-073878"  # both boxes ticked: a restatement needing recovery analysis
CABLE_ONE = "0001632127-26-000005"  # error corrected, no recovery analysis
ACI = "0000935036-26-000011"  # nothing corrected; filer's own label without "[Flag]"


def r1(acc: str) -> str:
    return load_document(F / f"{acc}_R1.htm.gz").decode()


@pytest.mark.parametrize(
    "acc,expected",
    [
        (ADTRAN, CoverFlags(True, True, "R1.htm")),
        (CABLE_ONE, CoverFlags(True, False, "R1.htm")),
        (ACI, CoverFlags(False, None, "R1.htm")),
    ],
)
def test_real_cover_reports(acc, expected):
    assert parse_cover_report(r1(acc)) == expected


def test_page_that_is_not_the_cover():
    assert parse_cover_report("<html><td>Consolidated Balance Sheets</td></html>") is None


SUMMARY = """<FilingSummary><MyReports>
<Report instance="x.htm"><HtmlFileName>R1.htm</HtmlFileName><MenuCategory>Statements</MenuCategory></Report>
<Report instance="x.htm"><HtmlFileName>R7.htm</HtmlFileName><MenuCategory>Cover</MenuCategory></Report>
</MyReports></FilingSummary>"""


def test_cover_report_found_via_filing_summary():
    assert cover_report_name(SUMMARY) == "R7.htm"
    pages = {
        "R1.htm": "<html><td>Consolidated Balance Sheets</td></html>",
        "FilingSummary.xml": SUMMARY,
        "R7.htm": r1(CABLE_ONE),
    }

    def handler(request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(200, text=pages[name]) if name in pages else httpx.Response(404)

    with EdgarClient("t t@example.com", rate=1000, transport=httpx.MockTransport(handler)) as ed:
        assert fetch_cover_flags(ed, 1, CABLE_ONE) == CoverFlags(True, False, "R7.htm")


def test_filing_without_xbrl_reports():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    with EdgarClient("t t@example.com", rate=1000, transport=httpx.MockTransport(handler)) as ed:
        flags = fetch_cover_flags(ed, 1, ACI)
    assert flags == CoverFlags(None, None, "missing") and not flags.flagged


# --- daily scan against the database ------------------------------------------------------

IDX = """Form Type   Company Name                                                  CIK         Date Filed  File Name
---------------------------------------------------------------------------------------------------------------------------------------------
10-K             ADTRAN Holdings, Inc.                                         926282      20260226    edgar/data/926282/0001193125-26-073878.txt
10-K             Cable One, Inc.                                               1632127     20260226    edgar/data/1632127/0001632127-26-000005.txt
10-K             ACI WORLDWIDE, INC.                                           935036      20260226    edgar/data/935036/0000935036-26-000011.txt
10-Q             Not An Annual Report Inc                                      1           20260226    edgar/data/1/0000000001-26-000001.txt
"""


def test_scan_day_records_every_annual_report_once(conn):  # noqa: F811
    from datetime import date

    from radar.workers.cover_scan import scan_day

    from .fake_edgar import FakeEdgar

    fake = FakeEdgar()
    fake.daily_index["20260226"] = IDX
    edgar = fake.client()
    r = scan_day(conn, edgar, date(2026, 2, 26))
    assert (r.available, r.listed, r.scanned, r.missing) == (True, 3, 3, 0)
    assert sorted(r.flagged) == sorted([ADTRAN, CABLE_ONE])
    rows = {
        x["accession"]: (x["error_correction"], x["recovery_analysis"])
        for x in conn.execute("SELECT * FROM cover_scan")
    }
    assert rows == {ADTRAN: (True, True), CABLE_ONE: (True, False), ACI: (False, None)}
    again = scan_day(conn, edgar, date(2026, 2, 26))  # nothing fetched twice
    assert (again.scanned, again.flagged) == (0, [])
    assert scan_day(conn, edgar, date(2026, 2, 28)).available is False  # no index (weekend)
