from datetime import date

import httpx

from radar.edgar.client import EdgarClient
from radar.edgar.feeds import (
    current_feed_url,
    daily_index_url,
    fetch_daily_index,
    iter_days,
    parse_current_feed,
    parse_daily_index,
)

from .conftest import FIXTURES

FEEDS = FIXTURES / "feeds"


def test_parse_real_current_feed():
    refs = parse_current_feed((FEEDS / "current_8k.xml").read_bytes())
    assert len(refs) == 5
    first = refs[0]
    assert first.accession == "0001213900-26-117631"
    assert first.cik == 1881551
    assert first.form == "8-K"
    assert first.company == "Solidion Technology Inc."
    assert first.items == ["2.02", "9.01"]
    assert first.filed_date == date(2026, 11, 9)
    assert first.accepted_at.isoformat() == "2026-11-09T17:30:36-05:00"
    assert refs[2].form == "8-K/A"
    assert not any(r.is_non_reliance for r in refs)


def test_feed_detects_item_402_and_prefers_filer_entry():
    entry = """<entry><title>8-K - UNIVERSAL LOGISTICS HOLDINGS, INC. (0001308208) ({role})</title>
      <summary type="html"> &lt;b&gt;Filed:&lt;/b&gt; 2026-03-09 &lt;b&gt;AccNo:&lt;/b&gt;
      0001193125-26-098600 &lt;br&gt;Item 4.02: Non-Reliance on Previously Issued Financial
      Statements &lt;br&gt;Item 9.01: Financial Statements and Exhibits</summary>
      <updated>2026-03-09T15:25:49-05:00</updated>
      <id>urn:tag:sec.gov,2008:accession-number=0001193125-26-098600</id></entry>"""
    xml = (
        '<feed xmlns="http://www.w3.org/2005/Atom">'
        + entry.format(role="Subject").replace("0001308208", "0000000001")
        + entry.format(role="Filer")
        + "</feed>"
    ).encode()
    (ref,) = parse_current_feed(xml)
    assert ref.cik == 1308208  # the Filer entry wins over the Subject entry
    assert ref.items == ["4.02", "9.01"]
    assert ref.is_non_reliance


def test_feed_ignores_unwatched_forms():
    xml = b"""<feed xmlns="http://www.w3.org/2005/Atom"><entry>
      <title>4 - Someone (0000000002) (Reporting)</title><summary>Filed: 2026-11-09</summary>
      <id>urn:tag:sec.gov,2008:accession-number=0000000002-26-000001</id></entry></feed>"""
    assert parse_current_feed(xml) == []


def test_current_feed_url_escapes_form():
    assert "type=10-K%2FA" in current_feed_url("10-K/A")
    assert "start=200" in current_feed_url("8-K", start=200)


def test_parse_real_daily_index():
    refs = parse_daily_index((FEEDS / "form.20261106.idx").read_text())
    forms = [r.form for r in refs]
    assert forms.count("10-K/A") == 2 and forms.count("10-Q/A") == 1
    assert forms.count("8-K") == 4 and forms.count("8-K/A") == 1
    assert "4" not in forms  # unwatched forms dropped
    acacia = refs[0]
    assert (acacia.accession, acacia.cik, acacia.company) == (
        "0000934549-26-000047",
        934549,
        "ACACIA RESEARCH CORP",
    )
    assert acacia.filed_date == date(2026, 11, 6)
    assert acacia.items is None  # the index has no item numbers


def test_daily_index_url_and_weekdays():
    assert daily_index_url(date(2026, 11, 6)).endswith("/2026/QTR4/form.20261106.idx")
    assert daily_index_url(date(2026, 3, 31)).endswith("/2026/QTR1/form.20260331.idx")
    assert [d.day for d in iter_days(date(2026, 11, 6), date(2026, 11, 10))] == [6, 9, 10]


def test_missing_daily_index_is_none_without_retrying():
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(403, text="<html>Access Denied</html>")

    edgar = EdgarClient(
        user_agent="t t@example.com", transport=httpx.MockTransport(handler), sleep=lambda s: None
    )
    assert fetch_daily_index(edgar, date(2026, 11, 8)) is None
    assert len(calls) == 1


def test_throttled_403_is_retried():
    calls = []

    def handler(request):
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(403, text="Request Rate Threshold Exceeded")
        return httpx.Response(200, text="ok")

    edgar = EdgarClient(
        user_agent="t t@example.com", transport=httpx.MockTransport(handler), sleep=lambda s: None
    )
    assert edgar.get("https://www.sec.gov/x").text == "ok" and len(calls) == 2
