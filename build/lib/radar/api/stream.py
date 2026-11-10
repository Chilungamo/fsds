"""Live event stream: one Postgres LISTEN per process, fanned out to every SSE client.

The pipeline appends to event_log and NOTIFYs ``radar_events`` with the new row's id. A
single Broadcaster task listens, loads each event row and puts it on every subscriber's
queue. Each browser stream subscribes first, then replays what it missed (EventSource
resends Last-Event-ID on reconnect), then relays live events; ids already replayed are
skipped, so nothing is lost or duplicated in between.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

from radar.api.queries import jsonable
from radar.db.repo import EVENTS_CHANNEL

log = logging.getLogger(__name__)

KEEPALIVE_SECONDS = 15
REPLAY_LIMIT = 200
QUEUE_SIZE = 500


def sse(event: dict[str, Any]) -> str:
    data = json.dumps(jsonable(event), separators=(",", ":"))
    return f"id: {event['id']}\nevent: {event['type']}\ndata: {data}\n\n"


class Broadcaster:
    def __init__(self, url: str) -> None:
        self.url = url
        self.subscribers: set[asyncio.Queue] = set()
        self.ready = asyncio.Event()

    @contextmanager
    def subscribe(self):
        queue: asyncio.Queue = asyncio.Queue(QUEUE_SIZE)
        self.subscribers.add(queue)
        try:
            yield queue
        finally:
            self.subscribers.discard(queue)

    def publish(self, event: dict[str, Any]) -> None:
        for q in list(self.subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:  # a stuck client; it will replay on reconnect
                log.warning("dropping event %s for a slow subscriber", event["id"])

    async def run(self) -> None:
        """Listen forever, reconnecting after database errors."""
        while True:
            try:
                async with (
                    await psycopg.AsyncConnection.connect(self.url, autocommit=True) as listen,
                    await psycopg.AsyncConnection.connect(
                        self.url, autocommit=True, row_factory=dict_row
                    ) as query,
                ):
                    await listen.execute(f"LISTEN {EVENTS_CHANNEL}")
                    self.ready.set()
                    async for note in listen.notifies():
                        event_id = json.loads(note.payload)["id"]
                        # a separate connection: the listening one is busy inside notifies()
                        cur = await query.execute(
                            "SELECT * FROM event_log WHERE id = %s", (event_id,)
                        )
                        row = await cur.fetchone()
                        if row:
                            self.publish(row)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.ready.clear()
                log.exception("event listener failed; reconnecting in 2 s")
                await asyncio.sleep(2)


async def replay(url: str, since: int) -> list[dict[str, Any]]:
    async with await psycopg.AsyncConnection.connect(
        url, autocommit=True, row_factory=dict_row
    ) as c:
        cur = await c.execute(
            "SELECT * FROM event_log WHERE id > %s ORDER BY id LIMIT %s", (since, REPLAY_LIMIT)
        )
        return await cur.fetchall()


async def event_stream(
    broadcaster: Broadcaster,
    since: int | None,
    max_seconds: float,
    is_disconnected: Callable[[], Awaitable[bool]],
) -> AsyncIterator[str]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max_seconds
    with broadcaster.subscribe() as queue:  # before replaying, so nothing slips between
        yield "retry: 3000\n: connected\n\n"
        last = since or 0
        if since is not None:
            for row in await replay(broadcaster.url, since):
                last = row["id"]
                yield sse(row)
        while not await is_disconnected():
            remaining = deadline - loop.time()
            if remaining <= 0:
                return
            try:
                event = await asyncio.wait_for(queue.get(), min(KEEPALIVE_SECONDS, remaining))
            except TimeoutError:
                yield ": keep-alive\n\n"
                continue
            if event["id"] > last:
                last = event["id"]
                yield sse(event)
