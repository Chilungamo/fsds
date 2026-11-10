"""Readers for EDGAR filing lists: the live Atom feed and the daily form index."""

from __future__ import annotations

import html
import re
from collections.abc import Iterable, Iterator
from datetime import date, datetime

from lxml import etree

from radar.edgar import urls
from radar.edgar.client import EdgarClient
from radar.models import WATCHED_FORMS, FilingRef, parse_items

ATOM = {"a": "http://www.w3.org/2005/Atom"}
_TITLE_RE = re.compile(r"^(?P<form>.+?) - (?P<company>.*) \((?P<cik>\d{10})\) \((?P<role>[^)]+)\)$")
_ACC_RE = re.compile(r"accession-number=(\d{10}-\d{2}-\d{6})")
_ITEM_RE = re.compile(r"Item (\d{1,2}\.\d{2})")
_FILED_RE = re.compile(r"Filed:\s*</b>\s*(\d{4}-\d{2}-\d{2})|Filed:\s*(\d{4}-\d{2}-\d{2})")


# --- live "getcurrent" Atom feed ---------------------------------------------------


def current_feed_url(form: str, start: int = 0, count: int = 100) -> str:
    return (
        f"{urls.WWW}/cgi-bin/browse-edgar?action=getcurrent&type={form.replace('/', '%2F')}"
        f"&company=&dateb=&owner=include&start={start}&count={count}&output=atom"
    )


def parse_current_feed(xml: bytes, forms: Iterable[str] = WATCHED_FORMS) -> list[FilingRef]:
    """Parse one page of the "latest filings" Atom feed.

    The feed lists a filing once per related entity (Filer, Subject, Reporting); we keep
    the issuer ("Filer") entry. 8-K entries list their items in the summary, so Item 4.02
    can be detected without downloading anything else.
    """
    wanted = set(forms)
    root = etree.fromstring(xml, etree.XMLParser(recover=True, no_network=True))
    refs: dict[str, FilingRef] = {}
    for e in root.iterfind("a:entry", ATOM):
        title = (e.findtext("a:title", namespaces=ATOM) or "").strip()
        m = _TITLE_RE.match(title)
        acc = _ACC_RE.search(e.findtext("a:id", namespaces=ATOM) or "")
        if not m or not acc:
            continue
        form = m["form"].strip()
        if form not in wanted or (acc[1] in refs and m["role"] != "Filer"):
            continue
        summary = html.unescape(e.findtext("a:summary", namespaces=ATOM) or "")
        filed = _FILED_RE.search(summary)
        updated = e.findtext("a:updated", namespaces=ATOM)
        accepted = datetime.fromisoformat(updated) if updated else None
        refs[acc[1]] = FilingRef(
            accession=acc[1],
            cik=int(m["cik"]),
            form=form,
            company=m["company"].strip(),
            filed_date=date.fromisoformat(filed[1] or filed[2])
            if filed
            else (accepted.date() if accepted else date.today()),
            accepted_at=accepted,
            items=parse_items(_ITEM_RE.findall(summary)) if form.startswith("8-K") else None,
            source="feed",
        )
    return list(refs.values())


# --- daily form index -----------------------------------------------------------------

_IDX_RE = re.compile(
    r"^(?P<form>\S(?:.*?\S)?)\s{2,}(?P<company>\S.*?)\s{2,}(?P<cik>\d+)\s+(?P<date>\d{8})\s+"
    r"(?P<file>edgar/data/\S+)\s*$"
)


def daily_index_url(day: date) -> str:
    q = (day.month - 1) // 3 + 1
    return f"{urls.WWW}/Archives/edgar/daily-index/{day.year}/QTR{q}/form.{day:%Y%m%d}.idx"


def parse_daily_index(
    text: str, forms: Iterable[str] = WATCHED_FORMS, source: str = "daily_index"
) -> list[FilingRef]:
    """Parse ``form.YYYYMMDD.idx``. One filing can appear once per filer; the first wins."""
    wanted = set(forms)
    refs: dict[str, FilingRef] = {}
    for line in text.splitlines():
        m = _IDX_RE.match(line)
        if not m or m["form"] not in wanted:
            continue
        acc = m["file"].rsplit("/", 1)[-1].removesuffix(".txt")
        if acc in refs:
            continue
        refs[acc] = FilingRef(
            accession=urls.normalize_accession(acc),
            cik=int(m["cik"]),
            form=m["form"],
            company=m["company"].strip(),
            filed_date=datetime.strptime(m["date"], "%Y%m%d").date(),
            source=source,
        )
    return list(refs.values())


def fetch_daily_index(
    edgar: EdgarClient, day: date, forms: Iterable[str] = WATCHED_FORMS
) -> list[FilingRef] | None:
    """Filings for ``day``; None when EDGAR has no index for that day (weekend, holiday,
    or not published yet)."""
    resp = edgar.get_optional(daily_index_url(day))
    return None if resp is None else parse_daily_index(resp.text, forms)


def iter_days(start: date, end: date) -> Iterator[date]:
    from datetime import timedelta

    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)
