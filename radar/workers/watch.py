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


WATCHER_LOCK = 4_020_402  # pg advisory lock id: one watcher per database


def run_singleton(
    database_url: str,
    interval: float = 60.0,
    stop: Callable[[], bool] = lambda: False,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Run the watcher only while holding a Postgres advisory lock, so any number of
    web instances can embed it and exactly one polls EDGAR. If the holder dies, its
    session ends, the lock is released and another instance takes over."""
    while not stop():
        try:
            with repo.connect(database_url) as conn:
                got = conn.execute(
                    "SELECT pg_try_advisory_lock(%s) AS ok", (WATCHER_LOCK,)
                ).fetchone()["ok"]
                if not got:
                    sleep(interval)
                    continue
                log.warning("embedded watcher acquired the lock; polling EDGAR")
                with EdgarClient() as edgar:
                    run(conn, edgar, interval=interval, stop=stop, sleep=sleep)
        except Exception:
            log.exception("embedded watcher crashed; retrying")
            sleep(interval)


def run(
    conn: psycopg.Connection,
    edgar: EdgarClient,
    interval: float = 60.0,
    once: bool = False,
    on_cycle: Callable[[PollResult, list[Outcome]], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    stop: Callable[[], bool] = lambda: False,
) -> None:
    pipeline = Pipeline(conn, edgar)
    # latency is only meaningful for filings accepted while the watcher is running;
    # anything earlier is a catch-up backlog
    from datetime import UTC, datetime

    repo.set_cursor(conn, "watch:started", datetime.now(UTC).isoformat())
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
        if once or stop():
            return
        sleep(max(0.0, interval - (time.monotonic() - started)))
