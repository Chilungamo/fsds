"""Read-side queries for the dashboard API. Plain SQL returning JSON-ready dicts."""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg

from radar.edgar.urls import accession_nodash
from radar.normalize.normalize import catalog

SORTS = {
    "recent": "last_filed DESC NULLS LAST, rc.id DESC",
    "severity": "rc.severity DESC NULLS LAST, last_filed DESC NULLS LAST, rc.id DESC",
    "opened": "first_filed DESC NULLS LAST, rc.id DESC",
}


def jsonable(v: Any) -> Any:
    if isinstance(v, Decimal):
        return float(v) if v != v.to_integral_value() else int(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [jsonable(x) for x in v]
    return v


def filing_url(cik: int, accession: str, doc: str | None) -> str:
    base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_nodash(accession)}"
    return f"{base}/{doc}" if doc else f"{base}/{accession}-index.htm"


_CASE_COLUMNS = """
    rc.id, rc.cik, c.name AS company, c.tickers, rc.status, rc.type, rc.sub_item,
    rc.severity, rc.opened_at, rc.updated_at, rc.enriched_at,
    rc.summary->'deltas'->'headline' AS headline,
    (rc.summary->'deltas'->>'checks_passed')::int AS checks_passed,
    (rc.summary->'deltas'->>'checks_failed')::int AS checks_failed,
    fd.first_filed, fd.last_filed, fd.forms,
    (SELECT array_agg(DISTINCT ap.period_end ORDER BY ap.period_end)
       FROM affected_period ap WHERE ap.case_id = rc.id) AS periods
"""
_CASE_FROM = """
    FROM restatement_case rc
    JOIN company c USING (cik)
    LEFT JOIN LATERAL (
      SELECT min(f.filed_date) AS first_filed, max(f.filed_date) AS last_filed,
             array_agg(f.form ORDER BY f.filed_date, f.accession) AS forms
      FROM case_filing cf JOIN filing f USING (accession) WHERE cf.case_id = rc.id
    ) fd ON true
"""


def list_cases(
    conn: psycopg.Connection,
    status: str | None = None,
    q: str | None = None,
    sort: str = "recent",
    limit: int = 50,
    offset: int = 0,
    cik: int | None = None,
) -> dict[str, Any]:
    where, args = ["true"], []
    if status:
        where.append("rc.status::text = ANY(%s)")
        args.append(status.upper().split(","))
    if cik:
        where.append("rc.cik = %s")
        args.append(cik)
    if q:
        where.append("(c.name ILIKE %s OR %s = ANY(c.tickers) OR rc.cik::text = %s)")
        args += [f"%{q}%", q.upper(), q.lstrip("0")]
    sql_where = " AND ".join(where)
    total = conn.execute(
        "SELECT count(*) AS n FROM restatement_case rc JOIN company c USING (cik) "
        f"WHERE {sql_where}",
        args,
    ).fetchone()["n"]
    rows = conn.execute(
        f"SELECT {_CASE_COLUMNS} {_CASE_FROM} WHERE {sql_where} "
        f"ORDER BY {SORTS.get(sort, SORTS['recent'])} LIMIT %s OFFSET %s",
        [*args, min(limit, 200), offset],
    ).fetchall()
    return {"total": total, "items": jsonable(rows), "offset": offset}


def get_case(conn: psycopg.Connection, case_id: int) -> dict[str, Any] | None:
    case = conn.execute(
        f"SELECT {_CASE_COLUMNS}, rc.summary, rc.confidence {_CASE_FROM} WHERE rc.id = %s",
        (case_id,),
    ).fetchone()
    if case is None:
        return None
    filings = conn.execute(
        """SELECT f.accession, f.form, f.filed_date, f.accepted_at, f.primary_doc, cf.role,
                  f.items, f.state, (f.meta->'triage'->>'score')::float AS triage_score,
                  f.meta->'triage'->'signals' AS triage_signals,
                  f.meta->'deltas'->>'source' AS delta_source,
                  left(f.meta->'item402'->>'section_text', 3000) AS item402_text,
                  f.meta->'item402'->'amounts' AS item402_amounts,
                  left(f.meta->>'amendment_description', 1500) AS amendment_description
           FROM case_filing cf JOIN filing f USING (accession)
           WHERE cf.case_id = %s ORDER BY f.filed_date, f.accession""",
        (case_id,),
    ).fetchall()
    for f in filings:
        f["url"] = filing_url(case["cik"], f["accession"], f["primary_doc"])
    periods = conn.execute(
        """SELECT period_end, array_agg(DISTINCT kind) AS kinds,
                  array_agg(DISTINCT source) AS sources
           FROM affected_period WHERE case_id = %s GROUP BY 1 ORDER BY 1""",
        (case_id,),
    ).fetchall()
    out = jsonable(case)
    out["filings"] = jsonable(filings)
    out["affected_periods"] = jsonable(periods)
    return out


def humanize(concept: str) -> str:
    """us-gaap:TangibleAssetImpairmentCharges -> Tangible asset impairment charges."""
    local = concept.split(":", 2)[-1]
    words = re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", " ", local).split()
    text = " ".join(w if w.isupper() and len(w) > 1 else w.lower() for w in words)
    return text[:1].upper() + text[1:]


def _period_label(start: str | None, end: str) -> str:
    if start is None:
        return f"As of {end}"
    days = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
    months = round(days / 30.44)
    span = {3: "Quarter", 6: "Six months", 9: "Nine months", 12: "Year"}.get(months, f"{days} days")
    return f"{span} ended {end}"


def get_deltas(conn: psycopg.Connection, case_id: int, all_rows: bool = False) -> list[dict]:
    cat = catalog()
    rows = conn.execute(
        """SELECT concept, label, canonical_item, statement, period_start, period_end, unit,
                  previously_reported, adjustment, restated, source, check_ok, accession, filed_date
           FROM case_delta WHERE case_id = %s AND dims = '{}'
             AND (%s OR adjustment <> 0)
           ORDER BY period_end, period_start NULLS FIRST""",
        (case_id, all_rows),
    ).fetchall()
    periods: dict[tuple, list] = defaultdict(list)
    for r in rows:
        r = jsonable(r)
        item = cat.items.get(r["canonical_item"]) if r["canonical_item"] else None
        prev, adj = r["previously_reported"], r["adjustment"]
        r["name"] = item.name if item else (r["label"] or humanize(r["concept"]))
        r["statement"] = r["statement"] or (item.statement if item else None)
        r["pct"] = None if not prev or adj is None else adj / abs(prev)
        periods[(r["period_start"], r["period_end"])].append(r)
    order = {"IS": 0, "BS": 1, "CF": 2, "EQ": 3, None: 4}
    out = []
    for (start, end), items in sorted(periods.items(), key=lambda kv: (kv[0][1], kv[0][0] or "")):
        items.sort(
            key=lambda r: (order.get(r["statement"], 4), r["canonical_item"] is None, r["name"])
        )
        out.append({"start": start, "end": end, "label": _period_label(start, end), "rows": items})
    return out


def get_graph(conn: psycopg.Connection, case_id: int, start: str | None, end: str | None) -> dict:
    cat = catalog()
    available = conn.execute(
        """SELECT period_start, period_end, count(*) AS edges FROM impact_edge
           WHERE case_id = %s GROUP BY 1, 2 ORDER BY 2, 1""",
        (case_id,),
    ).fetchall()
    if not available:
        return {"periods": [], "nodes": [], "edges": []}
    if end is None:  # default: the period with the most connections
        best = max(available, key=lambda p: (p["edges"], p["period_end"]))
        start, end = jsonable(best["period_start"]), jsonable(best["period_end"])
    edges = jsonable(
        conn.execute(
            """SELECT src_item, dst_item, sign, relationship, src_delta, dst_delta
               FROM impact_edge WHERE case_id = %s AND period_end = %s
                 AND period_start IS NOT DISTINCT FROM %s""",
            (case_id, end, start),
        ).fetchall()
    )
    nodes: dict[str, dict] = {}
    for e in edges:
        for key, delta, measured in (
            (e["src_item"], e["src_delta"], True),
            (e["dst_item"], e["dst_delta"], e["dst_delta"] is not None),
        ):
            item = cat.items[key]
            node = nodes.setdefault(
                key,
                {
                    "id": key,
                    "name": item.name,
                    "statement": item.statement,
                    "delta": None,
                    "measured": False,
                },
            )
            if measured:
                node["delta"], node["measured"] = delta, True
    return {
        "period": {"start": start, "end": end, "label": _period_label(start, end)},
        "periods": [
            {
                "start": jsonable(p["period_start"]),
                "end": jsonable(p["period_end"]),
                "label": _period_label(jsonable(p["period_start"]), jsonable(p["period_end"])),
                "edges": p["edges"],
            }
            for p in available
        ],
        "nodes": list(nodes.values()),
        "edges": edges,
    }


def get_checks(conn: psycopg.Connection, case_id: int) -> list[dict]:
    return jsonable(
        conn.execute(
            """SELECT name, status, period_start, period_end, gap, details FROM integrity_check
               WHERE case_id = %s ORDER BY period_end, period_start NULLS FIRST, name""",
            (case_id,),
        ).fetchall()
    )


def get_stats(conn: psycopg.Connection) -> dict[str, Any]:
    by_status = conn.execute(
        "SELECT status, count(*) AS n FROM restatement_case GROUP BY 1"
    ).fetchall()
    warnings = conn.execute(
        """SELECT count(*) FILTER (WHERE f.filed_date > current_date - 30) AS last_30,
                  count(*) FILTER (WHERE f.filed_date > current_date - 90) AS last_90,
                  count(*) FILTER (WHERE f.filed_date > current_date - 365) AS last_365
           FROM filing f WHERE '4.02' = ANY(f.items) AND f.form IN ('8-K', '8-K/A')"""
    ).fetchone()
    by_year = conn.execute(
        """SELECT extract(year FROM f.filed_date)::int AS year, count(*) AS n
           FROM filing f WHERE '4.02' = ANY(f.items) AND f.form = '8-K' GROUP BY 1 ORDER BY 1"""
    ).fetchall()
    lag = conn.execute(
        """SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY days) AS median_days
           FROM (SELECT min(fa.filed_date) - min(fw.filed_date) AS days
                 FROM restatement_case rc
                 JOIN case_filing w ON w.case_id = rc.id AND w.role = 'warning'
                 JOIN filing fw ON fw.accession = w.accession
                 JOIN case_filing a ON a.case_id = rc.id AND a.role = 'amendment'
                 JOIN filing fa ON fa.accession = a.accession AND fa.filed_date >= fw.filed_date
                 GROUP BY rc.id) t"""
    ).fetchone()
    latency = conn.execute(
        """SELECT count(*) AS n,
                  percentile_cont(0.5) WITHIN GROUP (ORDER BY secs) AS p50_seconds,
                  percentile_cont(0.95) WITHIN GROUP (ORDER BY secs) AS p95_seconds
           FROM (SELECT extract(epoch FROM f.discovered_at - f.accepted_at) AS secs
                 FROM filing f, job_cursor jc
                 WHERE jc.name = 'watch:started' AND f.source = 'feed'
                   AND f.accepted_at >= jc.value::timestamptz
                   AND f.accepted_at > now() - interval '7 days') t"""
    ).fetchone()
    delta_coverage = conn.execute(
        """SELECT count(*) AS confirmed,
                  count(*) FILTER (WHERE EXISTS (
                      SELECT 1 FROM case_filing cf
                      JOIN fact_delta fd ON fd.accession = cf.accession
                      WHERE cf.case_id = rc.id)) AS with_deltas
           FROM restatement_case rc WHERE rc.status IN ('CONFIRMED', 'ENRICHED')"""
    ).fetchone()
    return jsonable(
        {
            "detection_latency": latency,
            "delta_coverage": delta_coverage,
            "cases": {r["status"]: r["n"] for r in by_status},
            "warnings": warnings,
            "warnings_by_year": by_year,
            "median_days_warning_to_amendment": lag["median_days"],
        }
    )


def get_company(conn: psycopg.Connection, cik: int) -> dict[str, Any] | None:
    company = conn.execute("SELECT * FROM company WHERE cik = %s", (cik,)).fetchone()
    if company is None:
        return None
    out = jsonable(company)
    out["cases"] = list_cases(conn, cik=cik, sort="opened", limit=200)["items"]
    return out
