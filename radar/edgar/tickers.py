"""Ticker symbols for known companies (www.sec.gov/files/company_tickers.json)."""

from __future__ import annotations

from collections import defaultdict

import psycopg

from radar.edgar import urls
from radar.edgar.client import EdgarClient

TICKERS_URL = f"{urls.WWW}/files/company_tickers.json"


def parse_tickers(data: dict) -> dict[int, list[str]]:
    out: dict[int, list[str]] = defaultdict(list)
    for entry in data.values():
        out[int(entry["cik_str"])].append(entry["ticker"].upper())
    return dict(out)


def refresh_tickers(conn: psycopg.Connection, edgar: EdgarClient) -> int:
    """Update company.tickers for every company we know; returns rows changed."""
    by_cik = parse_tickers(edgar.get_json(TICKERS_URL))
    known = [r["cik"] for r in conn.execute("SELECT cik FROM company")]
    changed = 0
    with conn.transaction():
        for cik in known:
            tickers = by_cik.get(cik, [])
            cur = conn.execute(
                "UPDATE company SET tickers = %s, updated_at = now() "
                "WHERE cik = %s AND tickers IS DISTINCT FROM %s",
                (tickers, cik, tickers),
            )
            changed += cur.rowcount
    return changed
