"""Forward-only SQL migrations (radar/db/migrations/NNN_name.sql)."""

from __future__ import annotations

from pathlib import Path

import psycopg
from psycopg.rows import tuple_row

MIGRATIONS = Path(__file__).parent / "migrations"


def migrate(conn: psycopg.Connection) -> list[str]:
    """Apply pending migrations in order; returns the versions applied."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        " version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
    )
    cur = conn.cursor(row_factory=tuple_row)
    done = {r[0] for r in cur.execute("SELECT version FROM schema_migrations")}
    applied = []
    for path in sorted(MIGRATIONS.glob("*.sql")):
        version = path.stem
        if version in done:
            continue
        with conn.transaction():
            conn.execute(path.read_text())
            conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (version,))
        applied.append(version)
    return applied
