"""Database access for the detection pipeline (psycopg 3, Postgres)."""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from radar import config
from radar.models import FilingRef

MAX_ATTEMPTS = 3
EVENTS_CHANNEL = "radar_events"


def connect(url: str | None = None) -> psycopg.Connection:
    return psycopg.connect(url or config.database_url(), row_factory=dict_row, autocommit=True)


# --- companies & filings -------------------------------------------------------


def upsert_company(conn: psycopg.Connection, cik: int, name: str) -> None:
    conn.execute(
        """INSERT INTO company (cik, name) VALUES (%s, %s)
           ON CONFLICT (cik) DO UPDATE SET name = EXCLUDED.name, updated_at = now()
           WHERE company.name IS DISTINCT FROM EXCLUDED.name""",
        (cik, name or f"CIK {cik}"),
    )


def insert_filing(conn: psycopg.Connection, ref: FilingRef) -> bool:
    """Insert a discovered filing (idempotent). Returns True if it was new.

    A filing seen again from another source only fills in fields that were unknown.
    """
    upsert_company(conn, ref.cik, ref.company)
    row = conn.execute(
        """INSERT INTO filing (accession, cik, form, items, filed_date, accepted_at,
                               primary_doc, source, meta)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (accession) DO UPDATE SET
             items = COALESCE(filing.items, EXCLUDED.items),
             accepted_at = COALESCE(filing.accepted_at, EXCLUDED.accepted_at),
             primary_doc = COALESCE(filing.primary_doc, EXCLUDED.primary_doc)
           RETURNING (xmax = 0) AS inserted""",
        (
            ref.accession,
            ref.cik,
            ref.form,
            ref.items,
            ref.filed_date,
            ref.accepted_at,
            ref.primary_doc,
            ref.source,
            Jsonb(ref.meta),
        ),
    ).fetchone()
    return bool(row and row["inserted"])


def known_accessions(conn: psycopg.Connection, accessions: Iterable[str]) -> set[str]:
    accs = list(accessions)
    if not accs:
        return set()
    rows = conn.execute("SELECT accession FROM filing WHERE accession = ANY(%s)", (accs,))
    return {r["accession"] for r in rows}


def claim_filings(
    conn: psycopg.Connection, limit: int = 10, stale_minutes: int = 15
) -> list[dict[str, Any]]:
    """Atomically claim work. Safe with many concurrent workers (SKIP LOCKED);
    rows stuck in PROCESSING longer than ``stale_minutes`` are reclaimed."""
    return conn.execute(
        """UPDATE filing f SET state = 'PROCESSING', attempts = f.attempts + 1, claimed_at = now()
           FROM (SELECT accession FROM filing
                 WHERE state = 'NEW'
                    OR (state = 'RETRY' AND (not_before IS NULL OR not_before <= now()))
                    OR (state = 'PROCESSING' AND claimed_at < now() - make_interval(mins => %s))
                 -- live alerts never wait behind a historical backfill
                 ORDER BY CASE source WHEN 'feed' THEN 0 WHEN 'daily_index' THEN 1
                                     WHEN 'followup' THEN 2 ELSE 3 END,
                          discovered_at, accession
                 LIMIT %s FOR UPDATE SKIP LOCKED) todo
           WHERE f.accession = todo.accession
           RETURNING f.*, (SELECT name FROM company c WHERE c.cik = f.cik) AS company""",
        (stale_minutes, limit),
    ).fetchall()


def finish_filing(
    conn: psycopg.Connection,
    accession: str,
    state: str,
    meta: dict | None = None,
    parser_version: str | None = None,
    primary_doc: str | None = None,
) -> None:
    conn.execute(
        """UPDATE filing SET state = %s, meta = meta || %s, parser_version = %s,
                             primary_doc = COALESCE(primary_doc, %s),
                             last_error = NULL, processed_at = now()
           WHERE accession = %s""",
        (state, Jsonb(meta or {}), parser_version, primary_doc, accession),
    )


def fail_filing(conn: psycopg.Connection, accession: str, error: str) -> str:
    """Record a failure; retry with exponential backoff (2, 4, ... minutes) up to
    MAX_ATTEMPTS, then park the filing as FAILED for a human to look at."""
    row = conn.execute(
        """UPDATE filing SET last_error = %s,
                  state = CASE WHEN attempts >= %s THEN 'FAILED' ELSE 'RETRY' END,
                  not_before = now() + make_interval(mins => power(2, attempts)::int)
           WHERE accession = %s RETURNING state""",
        (error[:2000], MAX_ATTEMPTS, accession),
    ).fetchone()
    return row["state"] if row else "FAILED"


def requeue_if_ignored(conn: psycopg.Connection, accession: str) -> bool:
    """Send an IGNORED filing back through the pipeline (e.g. new context exists)."""
    row = conn.execute(
        """UPDATE filing SET state = 'NEW', attempts = 0, not_before = NULL
           WHERE accession = %s AND state = 'IGNORED' RETURNING accession""",
        (accession,),
    ).fetchone()
    return row is not None


def queue_depth(conn: psycopg.Connection) -> dict[str, int]:
    rows = conn.execute("SELECT state, count(*) AS n FROM filing GROUP BY state")
    return {r["state"]: r["n"] for r in rows}


# --- cases ------------------------------------------------------------------------


def find_related_case(
    conn: psycopg.Connection, cik: int, filed_date, days_before: int, days_after: int
) -> dict[str, Any] | None:
    """Open case of the same company with a linked filing dated within
    [filed_date - days_before, filed_date + days_after].

    Matching on filing dates (not processing order) lets a backfill link an 8-K and its
    amendment whichever is processed first.
    """
    return conn.execute(
        """SELECT rc.* FROM restatement_case rc
           JOIN case_filing cf ON cf.case_id = rc.id
           JOIN filing f ON f.accession = cf.accession
           WHERE rc.cik = %s AND rc.status <> 'CLOSED'
             AND f.filed_date BETWEEN %s::date - %s AND %s::date + %s
           ORDER BY abs(f.filed_date - %s::date), rc.id DESC LIMIT 1""",
        (cik, filed_date, days_before, filed_date, days_after, filed_date),
    ).fetchone()


def create_case(
    conn: psycopg.Connection,
    cik: int,
    type_: str,
    status: str,
    sub_item: str | None = None,
    confidence: float | None = None,
    summary: dict | None = None,
) -> int:
    row = conn.execute(
        """INSERT INTO restatement_case (cik, type, status, sub_item, confidence, summary)
           VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
        (cik, type_, status, sub_item, confidence, Jsonb(summary or {})),
    ).fetchone()
    return row["id"]


def update_case(conn: psycopg.Connection, case_id: int, **fields: Any) -> None:
    allowed = {"type", "status", "sub_item", "confidence"}
    sets, args = [], []
    for k, v in fields.items():
        if k == "summary":
            sets.append("summary = summary || %s")
            args.append(Jsonb(v))
        elif k in allowed:
            sets.append(f"{k} = %s")
            args.append(v)
        else:
            raise ValueError(f"cannot update {k}")
    conn.execute(
        f"UPDATE restatement_case SET {', '.join(sets)}, updated_at = now() WHERE id = %s",
        (*args, case_id),
    )


def link_filing(conn: psycopg.Connection, case_id: int, accession: str, role: str) -> None:
    conn.execute(
        "INSERT INTO case_filing (case_id, accession, role) VALUES (%s, %s, %s) "
        "ON CONFLICT DO NOTHING",
        (case_id, accession, role),
    )


def case_for_filing(conn: psycopg.Connection, accession: str) -> dict[str, Any] | None:
    return conn.execute(
        """SELECT rc.* FROM restatement_case rc JOIN case_filing cf ON cf.case_id = rc.id
           WHERE cf.accession = %s""",
        (accession,),
    ).fetchone()


def add_periods(
    conn: psycopg.Connection, case_id: int, periods: Iterable[tuple[str, str]], source: str
) -> None:
    for period_end, kind in periods:
        conn.execute(
            "INSERT INTO affected_period (case_id, period_end, kind, source) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (case_id, period_end, kind, source),
        )


def log_event(
    conn: psycopg.Connection,
    type_: str,
    case_id: int | None = None,
    accession: str | None = None,
    payload: dict | None = None,
) -> int:
    """Append to event_log and NOTIFY listeners (delivered when the transaction commits)."""
    row = conn.execute(
        "INSERT INTO event_log (type, case_id, accession, payload) VALUES (%s, %s, %s, %s) "
        "RETURNING id",
        (type_, case_id, accession, Jsonb(payload or {})),
    ).fetchone()
    event = {"id": row["id"], "type": type_, "case_id": case_id, "accession": accession}
    conn.execute("SELECT pg_notify(%s, %s)", (EVENTS_CHANNEL, json.dumps(event)))
    return row["id"]


# --- cursors ------------------------------------------------------------------------


def get_cursor(conn: psycopg.Connection, name: str) -> str | None:
    row = conn.execute("SELECT value FROM job_cursor WHERE name = %s", (name,)).fetchone()
    return row["value"] if row else None


def set_cursor(conn: psycopg.Connection, name: str, value: str) -> None:
    conn.execute(
        """INSERT INTO job_cursor (name, value) VALUES (%s, %s)
           ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, updated_at = now()""",
        (name, value),
    )
