"""Small, polite client for SEC EDGAR JSON APIs with an on-disk cache.

* One ``companyfacts`` call returns every XBRL fact a company has filed, so
  a study needs one request per company instead of one per concept.
* Responses are cached under ``data/cache`` so reruns are free and offline.
* Requests are throttled below the SEC's 10 requests/second fair-access limit.
* For full-universe studies, use the SEC bulk archives instead of the API:
  https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip
  https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip
"""
from __future__ import annotations

import json
import time
import zipfile
from pathlib import Path
from typing import Iterator

import requests

from fsds.config import sec_headers

DEFAULT_CACHE = Path("data/cache")


def cik10(cik) -> str:
    """Zero-pad a CIK to the 10 digits EDGAR URLs use."""
    return str(int(str(cik).strip())).zfill(10)


class SecClient:
    def __init__(self, cache_dir: Path | str = DEFAULT_CACHE, min_interval: float = 0.12,
                 max_retries: int = 4, session: requests.Session | None = None):
        self.cache_dir = Path(cache_dir)
        self.min_interval = min_interval
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self._last_request = 0.0
        self._headers = None

    # -- low level -------------------------------------------------------
    def _throttle(self):
        wait = self.min_interval - (time.monotonic() - self._last_request)
        if wait > 0:
            time.sleep(wait)
        self._last_request = time.monotonic()

    def get_json(self, url: str, cache_name: str | None = None, refresh: bool = False):
        """GET a JSON document. Returns None on 404 (company has no such data)."""
        path = self.cache_dir / cache_name if cache_name else None
        if path and path.exists() and not refresh:
            return json.loads(path.read_text())

        if self._headers is None:
            self._headers = sec_headers()
        for attempt in range(self.max_retries + 1):
            self._throttle()
            resp = self.session.get(url, headers=self._headers, timeout=30)
            if resp.status_code == 404:
                return None
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                time.sleep(2 ** (attempt + 1))
                continue
            resp.raise_for_status()
            data = resp.json()
            if path:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(data))
            return data
        raise RuntimeError(f"unreachable: retries exhausted for {url}")

    # -- endpoints -------------------------------------------------------
    def company_tickers(self) -> dict:
        return self.get_json("https://www.sec.gov/files/company_tickers.json",
                             "company_tickers.json")

    def company_facts(self, cik) -> dict | None:
        c = cik10(cik)
        return self.get_json(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{c}.json",
                             f"companyfacts/CIK{c}.json")

    def submissions(self, cik) -> dict | None:
        c = cik10(cik)
        return self.get_json(f"https://data.sec.gov/submissions/CIK{c}.json",
                             f"submissions/CIK{c}.json")

    def ticker_to_cik(self, tickers) -> dict[str, str]:
        table = {v["ticker"].upper(): cik10(v["cik_str"]) for v in self.company_tickers().values()}
        missing = [t for t in tickers if t.upper() not in table]
        if missing:
            raise KeyError(f"Unknown tickers: {', '.join(missing)}")
        return {t.upper(): table[t.upper()] for t in tickers}


def iter_bulk_json(zip_path: Path | str) -> Iterator[dict]:
    """Yield each company's JSON from an SEC bulk archive (companyfacts.zip or
    submissions.zip). Supplementary ``-submissions-NNN.json`` pages are skipped."""
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if not name.endswith(".json") or "-submissions-" in name:
                continue
            with zf.open(name) as fh:
                yield json.load(fh)
