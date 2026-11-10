"""Live watcher: poll EDGAR's "latest filings" feed, enqueue new filings, process them."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import psycopg

from radar.db import repo
from radar.edgar.client import EdgarClient
from radar.edgar.feeds import current_feed_url, parse_current_feed
from radar.models import FilingRef
from radar.pipeline import Outcome, Pipeline

log = logging.getLogger(__name__)

FEED_TYPES = ("8-K", "10-K/A", "10-Q/A")  # "8-K" also returns 8-K/A
PAGE_SIZE = 100
MAX_PAGES = 5


@dataclass
class PollResult:
    seen: int = 0
    new: list[FilingRef] = field(default_factory=list)

    @property
    def new_non_reliance(self) -> list[FilingRef]:
        return [r for r in self.new if r.is_non_reliance]


def poll_once(conn: psycopg.Connection, edgar: EdgarClient) -> PollResult:
    """Read each feed newest-first, paging back until a page holds a filing we already
    know (so bursts of more than 100 filings per minute are not lost)."""
    result = PollResult()
    for feed in FEED_TYPES:
        for page in range(MAX_PAGES):
            refs = parse_current_feed(edgar.get(current_feed_url(feed, page * PAGE_SIZE)).content)
            result.seen += len(refs)
            known = repo.known_accessions(conn, (r.accession for r in refs))
            for ref in refs:
                if ref.accession not in known and repo.insert_filing(conn, ref):
                    result.new.append(ref)
            if known or len(refs) < PAGE_SIZE // 2:
                break
    for ref in result.new_non_reliance:
        log.warning("Item 4.02 filed: %s %s %s", ref.company, ref.form, ref.accession)
    return result


def run(
    conn: psycopg.Connection,
    edgar: EdgarClient,
    interval: float = 60.0,
    once: bool = False,
    on_cycle: Callable[[PollResult, list[Outcome]], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    pipeline = Pipeline(conn, edgar)
    while True:
        started = time.monotonic()
        try:
            polled = poll_once(conn, edgar)
            outcomes = pipeline.drain()
        except Exception:  # keep watching through transient EDGAR/DB errors
            log.exception("watch cycle failed")
            polled, outcomes = PollResult(), []
        if on_cycle:
            on_cycle(polled, outcomes)
        if once:
            return
        sleep(max(0.0, interval - (time.monotonic() - started)))
