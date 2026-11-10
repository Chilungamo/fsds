"""Rate-limited, retrying HTTP client for SEC EDGAR."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

import httpx

from radar import config
from radar.edgar import urls

RETRY_STATUSES = {403, 429, 500, 502, 503, 504}


class TokenBucket:
    """Thread-safe rate limiter: at most ``rate`` requests/second, bursts of ``capacity``.

    Implemented as a "next allowed time" scheduler (GCRA), which needs a single
    sleep per request and cannot spin on floating-point rounding.
    """

    def __init__(
        self,
        rate: float,
        capacity: int = 1,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if rate <= 0 or capacity < 1:
            raise ValueError("rate must be > 0 and capacity >= 1")
        self.interval = 1.0 / rate
        self.burst = (capacity - 1) * self.interval
        self._clock = clock
        self._sleep = sleep
        self._next = clock()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = self._clock()
            start = max(self._next, now - self.burst)
            if start > now:
                self._sleep(start - now)
            self._next = start + self.interval


class EdgarError(RuntimeError):
    pass


class EdgarClient:
    """Synchronous EDGAR client honouring the SEC fair-access policy.

    * declares a User-Agent with contact details (``RADAR_SEC_USER_AGENT``)
    * stays under the configured requests/second (default 5, SEC maximum 10)
    * retries 403/429/5xx with exponential backoff
    """

    def __init__(
        self,
        user_agent: str | None = None,
        rate: float | None = None,
        max_retries: int = 5,
        backoff: float = 1.0,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.user_agent = user_agent or config.sec_user_agent()
        self.bucket = TokenBucket(rate or config.sec_rate_limit(), sleep=sleep)
        self.max_retries = max_retries
        self.backoff = backoff
        self._sleep = sleep
        self.http = httpx.Client(
            headers={"User-Agent": self.user_agent, "Accept-Encoding": "gzip, deflate"},
            timeout=30.0,
            follow_redirects=True,
            transport=transport,
        )

    def __enter__(self) -> EdgarClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self.http.close()

    def get(self, url: str) -> httpx.Response:
        last: httpx.Response | Exception | None = None
        for attempt in range(self.max_retries):
            self.bucket.acquire()
            try:
                resp = self.http.get(url)
            except httpx.TransportError as exc:
                last = exc
            else:
                if resp.status_code not in RETRY_STATUSES:
                    resp.raise_for_status()
                    return resp
                last = resp
            self._sleep(self.backoff * 2**attempt)
        detail = f"HTTP {last.status_code}" if isinstance(last, httpx.Response) else repr(last)
        raise EdgarError(
            f"EDGAR request failed after {self.max_retries} attempts ({detail}): {url}"
        )

    def get_json(self, url: str) -> Any:
        return self.get(url).json()

    # --- convenience wrappers -------------------------------------------------

    def submissions(self, cik: int | str) -> dict[str, Any]:
        return self.get_json(urls.submissions_url(cik))

    def companyfacts(self, cik: int | str) -> dict[str, Any]:
        return self.get_json(urls.companyfacts_url(cik))

    def filing_index(self, cik: int | str, accession: str) -> list[dict[str, Any]]:
        data = self.get_json(urls.filing_index_url(cik, accession))
        return data["directory"]["item"]

    def filing_document(self, cik: int | str, accession: str, filename: str) -> bytes:
        return self.get(urls.filing_document_url(cik, accession, filename)).content

    def primary_document_name(self, cik: int | str, accession: str) -> str:
        """Primary document of a filing, looked up in the company's submissions."""
        acc = urls.normalize_accession(accession)
        recent = self.submissions(cik)["filings"]["recent"]
        for i, a in enumerate(recent["accessionNumber"]):
            if a == acc:
                return recent["primaryDocument"][i]
        raise EdgarError(f"{acc} not in recent filings of CIK {cik}; pass the filename explicitly")
