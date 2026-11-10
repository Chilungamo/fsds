"""M2 enrichment: store a restatement's line-item deltas and analyse the case.

enrich_filing  download (or reuse) the amendment, extract XBRL restatement rows (HTML
               tables as fallback), map them to canonical items, store in fact_delta
enrich_case    integrity checks, impact graph and severity from the case's latest deltas
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from radar.db import repo
from radar.edgar.client import EdgarClient
from radar.edgar.submissions import SubmissionsCache
from radar.extract.companyfacts import companyfacts_deltas
from radar.extract.html_tables import extract_html
from radar.extract.ixbrl import RestatementExtraction, extract, fmt_decimal
from radar.normalize.normalize import catalog
from radar.propagate import engine
from radar.storage import RawStore

log = logging.getLogger(__name__)

ENRICH_VERSION = "enrich/2"


@dataclass
class FilingDeltas:
    accession: str
    source: str | None  # xbrl | html | None (nothing found)
    rows: int
    mapped: int


def fetch_document(
    edgar: EdgarClient, store: RawStore, subs: SubmissionsCache, row: dict[str, Any]
) -> tuple[str, bytes]:
    name = row.get("primary_doc")
    if not name:
        info = subs.lookup(row["cik"], row["accession"])
        if info is None:
            raise LookupError(f"{row['accession']} not in submissions of {row['cik']}")
        name = info.primary_doc
    raw = store.get(row["cik"], row["accession"], name)
    if raw is None:
        raw = edgar.filing_document(row["cik"], row["accession"], name)
        store.put(row["cik"], row["accession"], name, raw)
    return name, raw


def _xbrl_records(extraction: RestatementExtraction) -> list[dict[str, Any]]:
    cat = catalog()
    out = []
    for r in extraction.rows:
        item = cat.item_for_concept(r.concept) if not r.dims else None
        out.append(
            {
                "concept": r.concept,
                "label": None,
                "canonical_item": item.key if item else None,
                "statement": item.statement if item else ("BS" if r.period_start is None else None),
                "period_start": r.period_start,
                "period_end": r.period_end,
                "dims": r.dims,
                "unit": r.unit,
                "previously_reported": r.previously_reported,
                "adjustment": r.adjustment,
                "restated": r.restated,
                "tolerance": r.tolerance,
                "check_ok": r.check_ok,
                "error_member": r.error_member,
            }
        )
    return out


def _html_records(raw: bytes) -> list[dict[str, Any]]:
    return [
        {
            "concept": r.concept,
            "label": r.label,
            "canonical_item": r.canonical_item,
            "statement": r.statement,
            "period_start": r.period_start,
            "period_end": r.period_end,
            "dims": {},
            "unit": r.unit,
            "previously_reported": r.previously_reported,
            "adjustment": r.adjustment,
            "restated": r.restated,
            "tolerance": r.tolerance,
            "check_ok": r.check_ok,
            "error_member": None,
        }
        for r in extract_html(raw)
    ]


def deltas_from_document(raw: bytes) -> tuple[str | None, list[dict[str, Any]], set[str]]:
    """(source, records, items reported on the face of the filing)."""
    extraction = extract(raw)
    cat = catalog()
    face_items = {
        item.key for c in extraction.face_concepts if (item := cat.item_for_concept(c)) is not None
    }
    records = _xbrl_records(extraction)
    if any(r["adjustment"] for r in records):
        return "xbrl", records, face_items
    records = _html_records(raw)
    if records:
        return "html", records, face_items
    return None, [], face_items


def store_deltas(conn: psycopg.Connection, accession: str, records: list[dict[str, Any]]) -> None:
    with conn.transaction():
        conn.execute("DELETE FROM fact_delta WHERE accession = %s", (accession,))
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO fact_delta (accession, concept, label, canonical_item, statement,
                     period_start, period_end, dims, unit, previously_reported, adjustment,
                     restated, tolerance, source, check_ok, error_member)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT DO NOTHING""",
                [
                    (
                        accession,
                        r["concept"][:500],
                        (r["label"] or None) and r["label"][:500],
                        r["canonical_item"],
                        r["statement"],
                        r["period_start"],
                        r["period_end"],
                        Jsonb(r["dims"]),
                        r["unit"],
                        r["previously_reported"],
                        r["adjustment"],
                        r["restated"],
                        r["tolerance"],
                        r["source"],
                        r["check_ok"],
                        r["error_member"],
                    )
                    for r in records
                ],
            )


def enrich_filing(
    conn: psycopg.Connection,
    edgar: EdgarClient,
    row: dict[str, Any],
    store: RawStore | None = None,
    subs: SubmissionsCache | None = None,
    raw: bytes | None = None,
) -> FilingDeltas:
    store = store or RawStore()
    if raw is None:
        _, raw = fetch_document(edgar, store, subs or SubmissionsCache(edgar), row)
    source, records, face_items = deltas_from_document(raw)
    if source is None and row.get("filed_date"):
        # no restatement table at all: compare with the values originally filed
        facts = edgar.companyfacts(row["cik"])
        records = [
            r
            for r in companyfacts_deltas(facts, row["accession"], row["filed_date"])
            if r["adjustment"]
        ]
        source = "companyfacts" if records else None
    for r in records:
        r["source"] = source
    store_deltas(conn, row["accession"], records)
    mapped = sum(1 for r in records if r["canonical_item"] and r["adjustment"])
    conn.execute(
        "UPDATE filing SET meta = meta || %s WHERE accession = %s",
        (
            Jsonb(
                {
                    "deltas": {
                        "version": ENRICH_VERSION,
                        "source": source,
                        "rows": len(records),
                        "mapped_changed": mapped,
                        "face_items": sorted(face_items),
                    }
                }
            ),
            row["accession"],
        ),
    )
    return FilingDeltas(row["accession"], source, len(records), mapped)


def _unchanged_items(conn: psycopg.Connection, case_id: int) -> set[str]:
    """Items a case's amendments report (face statements, or listed in an HTML table)
    without restating them; a missing restatement row then means "unchanged"."""
    rows = conn.execute(
        """SELECT f.meta->'deltas'->'face_items' AS items FROM case_filing cf
           JOIN filing f USING (accession)
           WHERE cf.case_id = %s AND cf.role = 'amendment'""",
        (case_id,),
    ).fetchall()
    items: set[str] = set()
    for r in rows:
        items |= set(r["items"] or [])
    html_zero = conn.execute(
        """SELECT DISTINCT canonical_item FROM case_delta
           WHERE case_id = %s AND source = 'html' AND adjustment = 0
             AND canonical_item IS NOT NULL""",
        (case_id,),
    ).fetchall()
    return items | {r["canonical_item"] for r in html_zero}


def enrich_case(conn: psycopg.Connection, case_id: int) -> dict[str, Any]:
    case = conn.execute("SELECT * FROM restatement_case WHERE id = %s", (case_id,)).fetchone()
    rows = conn.execute("SELECT * FROM case_delta WHERE case_id = %s", (case_id,)).fetchall()
    view = engine.build_view(rows)
    checks = engine.integrity_checks(view, _unchanged_items(conn, case_id))
    edges = engine.impact_edges(view)
    periods = conn.execute(
        "SELECT count(DISTINCT period_end) AS n FROM affected_period WHERE case_id = %s",
        (case_id,),
    ).fetchone()["n"]
    sev = engine.severity(view, case["type"] == "BIG_R", case["sub_item"] == "b", periods)
    headline = _headline(view)
    summary = {
        "deltas": {
            "periods": len(view),
            "items_changed": sum(
                1 for cells in view.values() for c in cells.values() if c.adjustment
            ),
            "checks_passed": sum(c.status == "pass" for c in checks),
            "checks_failed": sum(c.status == "fail" for c in checks),
            "headline": headline,
        }
    }
    with conn.transaction():
        conn.execute("DELETE FROM integrity_check WHERE case_id = %s", (case_id,))
        conn.execute("DELETE FROM impact_edge WHERE case_id = %s", (case_id,))
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO integrity_check (case_id, period_start, period_end, name, status,
                     gap, details) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                [
                    (
                        case_id,
                        c.period.start,
                        c.period.end,
                        c.name,
                        c.status,
                        c.gap,
                        Jsonb(c.details),
                    )
                    for c in checks
                ],
            )
            cur.executemany(
                """INSERT INTO impact_edge (case_id, period_start, period_end, src_item, dst_item,
                     relationship, sign, src_delta, dst_delta)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                [
                    (
                        case_id,
                        e.period.start,
                        e.period.end,
                        e.src,
                        e.dst,
                        e.relationship,
                        e.sign,
                        e.src_delta,
                        e.dst_delta,
                    )
                    for e in edges
                ],
            )
        conn.execute(
            """UPDATE restatement_case SET severity = %s, enriched_at = now(),
                      summary = summary || %s, updated_at = now() WHERE id = %s""",
            (sev if view else None, Jsonb(summary), case_id),
        )
        if view:
            repo.log_event(
                conn, "case.enriched", case_id, None, {"severity": sev, **summary["deltas"]}
            )
    return {"severity": sev, **summary["deltas"]}


def _headline(view: engine.View) -> list[dict[str, Any]]:
    """Largest changes to key items, one per item, for the dashboard card."""
    key_items = (
        "net_income",
        "revenue",
        "total_equity",
        "total_assets",
        "total_liabilities",
        "cfo",
    )
    out = []
    for item in key_items:
        best = None
        for p, cells in view.items():
            c = cells.get(item)
            if c and c.adjustment and (best is None or abs(c.adjustment) > abs(best[1].adjustment)):
                best = (p, c)
        if best:
            p, c = best
            out.append(
                {
                    "item": item,
                    "period_start": p.start,
                    "period_end": p.end,
                    "previously_reported": fmt_decimal(c.previously_reported),
                    "adjustment": fmt_decimal(c.adjustment),
                    "restated": fmt_decimal(c.restated),
                }
            )
    return out
