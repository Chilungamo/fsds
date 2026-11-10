"""Dashboard API (design doc §11): REST for queries, Server-Sent Events for the live feed.

The built frontend (radar/web/dist) is served from the same process, so the whole
dashboard is one service.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from radar import config
from radar.api import queries
from radar.api.stream import Broadcaster, event_stream

WEB_DIST = Path(__file__).resolve().parent.parent / "web" / "dist"
# Streams end after this long; EventSource reconnects with Last-Event-ID and the replay
# fills the gap, so connections are recycled without losing events.
STREAM_MAX_SECONDS = int(os.environ.get("RADAR_STREAM_MAX_SECONDS", "300"))


def create_app(database_url: str | None = None, web_dist: Path | None = WEB_DIST) -> FastAPI:
    url = database_url or config.database_url()
    pool = ConnectionPool(
        url,
        min_size=1,
        max_size=int(os.environ.get("RADAR_DB_POOL", "10")),
        kwargs={"row_factory": dict_row, "autocommit": True},
        open=False,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool.open(wait=True)
        broadcaster = Broadcaster(url)
        app.state.broadcaster = broadcaster
        task = asyncio.create_task(broadcaster.run())
        try:
            await asyncio.wait_for(broadcaster.ready.wait(), 10)
        except TimeoutError:
            pass  # keep serving REST; the listener keeps retrying
        yield
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        pool.close()

    app = FastAPI(title="Restatement Radar", version="0.3", lifespan=lifespan)
    app.state.pool = pool
    app.state.database_url = url

    def db():
        return pool.connection()

    @app.get("/api/health")
    def health():
        with db() as conn:
            conn.execute("SELECT 1")
        return {"ok": True}

    @app.get("/api/cases")
    def cases(
        status: str | None = None,
        q: str | None = None,
        sort: str = "recent",
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
    ):
        with db() as conn:
            return queries.list_cases(conn, status, q, sort, limit, offset)

    @app.get("/api/cases/{case_id}")
    def case(case_id: int):
        with db() as conn:
            c = queries.get_case(conn, case_id)
        if c is None:
            raise HTTPException(404, "case not found")
        return c

    @app.get("/api/cases/{case_id}/deltas")
    def deltas(case_id: int, all: bool = False):
        with db() as conn:
            return queries.get_deltas(conn, case_id, all)

    @app.get("/api/cases/{case_id}/graph")
    def graph(case_id: int, start: str | None = None, end: str | None = None):
        with db() as conn:
            return queries.get_graph(conn, case_id, start or None, end)

    @app.get("/api/cases/{case_id}/checks")
    def checks(case_id: int):
        with db() as conn:
            return queries.get_checks(conn, case_id)

    @app.get("/api/companies/{cik}")
    def company(cik: int):
        with db() as conn:
            c = queries.get_company(conn, cik)
        if c is None:
            raise HTTPException(404, "company not found")
        return c

    @app.get("/api/stats")
    def stats():
        with db() as conn:
            return queries.get_stats(conn)

    @app.get("/api/stream")
    async def stream(
        request: Request,
        last_event_id: int | None = Query(None),
        max_seconds: int = Query(STREAM_MAX_SECONDS, ge=1, le=3600),
    ):
        header = request.headers.get("last-event-id")
        since = int(header) if header and header.isdigit() else last_event_id
        return StreamingResponse(
            event_stream(
                request.app.state.broadcaster, since, max_seconds, request.is_disconnected
            ),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    if web_dist and (web_dist / "index.html").exists():
        app.mount("/assets", StaticFiles(directory=web_dist / "assets"), name="assets")

        @app.get("/{path:path}", include_in_schema=False)
        def spa(path: str):
            if path.startswith("api/"):
                return JSONResponse({"detail": "Not Found"}, status_code=404)
            file = (web_dist / path).resolve()
            if path and file.is_file() and web_dist in file.parents:
                return FileResponse(file)
            return FileResponse(web_dist / "index.html")

    return app
