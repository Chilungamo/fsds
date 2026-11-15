"""Build, validate and store a case's explanation (M4 step 5).

``explain_case`` gathers what the database knows about a case (the Item 4.02 notice, the
amendments' explanatory notes and the restated line items), classifies the cause, writes the
template explanation, optionally has Claude rewrite it, and stores whatever passed the
validator in ``case_explanation``. It is called by the pipeline after a warning or an
amendment is processed, and by ``radar explain`` for backfills.

Claude is used only when asked (``llm=True``, or RADAR_LLM=1 in the environment) and an API
key is available. A case whose inputs have not changed since its last explanation is skipped,
so re-running costs nothing.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import asdict
from datetime import date
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from radar.db import repo
from radar.explain import narrative
from radar.explain.causes import classify
from radar.explain.template import CaseFacts, Explanation, explain
from radar.explain.validate import enforce
from radar.extract.ixbrl import fmt_decimal
from radar.propagate import engine

log = logging.getLogger(__name__)

EXPLAIN_VERSION = 1  # bump to make every case re-explain


def llm_enabled() -> bool:
    return os.environ.get("RADAR_LLM", "0") == "1"


def _iso(d: Any) -> str | None:
    return d.isoformat() if isinstance(d, date) else d


def gather(conn: psycopg.Connection, case_id: int) -> dict[str, Any] | None:
    case = conn.execute(
        """SELECT rc.*, c.name AS company FROM restatement_case rc JOIN company c USING (cik)
           WHERE rc.id = %s""",
        (case_id,),
    ).fetchone()
    if case is None:
        return None
    filings = conn.execute(
        """SELECT cf.role, f.form, f.filed_date, f.meta->'item402'->>'section_text' AS item402,
                  f.meta->>'amendment_description' AS note
           FROM case_filing cf JOIN filing f USING (accession)
           WHERE cf.case_id = %s ORDER BY f.filed_date, f.accession""",
        (case_id,),
    ).fetchall()
    warnings = [f for f in filings if f["role"] in ("warning", "warning_amendment")]
    amendments = [f for f in filings if f["role"] == "amendment"]
    words = [f["item402"] for f in warnings if f["item402"]]
    words += [f["note"] for f in filings if f["role"] == "amendment" and f["note"]]
    periods = [
        _iso(r["period_end"])
        for r in conn.execute(
            "SELECT DISTINCT period_end FROM affected_period WHERE case_id = %s ORDER BY 1",
            (case_id,),
        )
    ]
    rows = conn.execute("SELECT * FROM case_delta WHERE case_id = %s", (case_id,)).fetchall()
    view = engine.build_view(rows)
    facts = CaseFacts(
        company=case["company"],
        warning_date=_iso(warnings[0]["filed_date"]) if warnings else None,
        sub_item=case["sub_item"],
        case_type=case["type"],
        period_ends=periods if warnings else [],
        amendment_form=amendments[0]["form"] if amendments else None,
        amendment_date=_iso(amendments[0]["filed_date"]) if amendments else None,
    )
    return {"case": facts, "words": "\n\n".join(words), "view": view}


def _explanation_json(e: Explanation) -> dict[str, Any]:
    return {
        "headline": e.headline,
        "sentences": e.sentences,
        "facts": [{**asdict(f), "value": fmt_decimal(f.value)} for f in e.facts],  # Decimal -> str
        "causes": [asdict(c) for c in e.causes],
    }


def explain_case(
    conn: psycopg.Connection,
    case_id: int,
    llm: bool | None = None,
    client: Any | None = None,
    force: bool = False,
) -> dict[str, Any] | None:
    """Explain one case and store it. Returns a short summary, or None if the case is gone."""
    g = gather(conn, case_id)
    if g is None:
        return None
    view = g["view"]
    changed = {i for cells in view.values() for i, c in cells.items() if c.adjustment}
    causes = classify(g["words"], changed)
    draft = explain(g["case"], causes, view, engine.impact_edges(view))
    template, tv = enforce(draft, view, f"{g['case'].company} restates its financial statements")
    use_llm = llm_enabled() if llm is None else llm

    inputs = json.dumps(
        [EXPLAIN_VERSION, template.headline, template.sentences, g["words"][:6000], use_llm],
        sort_keys=True,
    )
    input_hash = hashlib.sha256(inputs.encode()).hexdigest()
    old = conn.execute(
        "SELECT input_hash, method, llm_reason FROM case_explanation WHERE case_id = %s",
        (case_id,),
    ).fetchone()
    # same inputs: nothing to do, unless an earlier Claude call failed for a passing reason
    # (network, rate limit, missing key); a rewrite that failed validation is not retried
    settled = old and (
        not use_llm or old["method"] == "llm" or old["llm_reason"] == "failed validation"
    )
    if old and old["input_hash"] == input_hash and settled and not force:
        return {"case_id": case_id, "method": old["method"], "skipped": True}

    shown, reason, usage, v = template, "not requested", None, tv
    if use_llm:
        r = narrative.narrate(template, view, g["words"], client=client)
        shown, reason, usage = r.explanation, r.reason, r.usage
        v = r.validation if r.used_llm and r.validation else tv
    body = _explanation_json(shown)
    validation = {
        "figures_checked": v.figures_checked,
        "facts_checked": v.facts_checked,
        "dates_checked": v.dates_checked,
        "template_problems_dropped": [p.detail for p in tv.problems],
    }
    with conn.transaction():
        conn.execute(
            """INSERT INTO case_explanation (case_id, method, headline, sentences, facts,
                 causes, template, validation, input_hash, model, llm_reason, usage)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (case_id) DO UPDATE SET
                 method = EXCLUDED.method, headline = EXCLUDED.headline,
                 sentences = EXCLUDED.sentences, facts = EXCLUDED.facts,
                 causes = EXCLUDED.causes, template = EXCLUDED.template,
                 validation = EXCLUDED.validation, input_hash = EXCLUDED.input_hash,
                 model = EXCLUDED.model, llm_reason = EXCLUDED.llm_reason,
                 usage = EXCLUDED.usage, updated_at = now()""",
            (
                case_id,
                shown.method,
                body["headline"],
                Jsonb(body["sentences"]),
                Jsonb(body["facts"]),
                Jsonb(body["causes"]),
                Jsonb({"headline": template.headline, "sentences": template.sentences}),
                Jsonb(validation),
                input_hash,
                narrative.MODEL if shown.method == "llm" else None,
                reason,
                Jsonb(usage) if usage else None,
            ),
        )
        repo.log_event(
            conn,
            "case.explained",
            case_id,
            None,
            {
                "method": shown.method,
                "headline": shown.headline,
                "causes": [c.code for c in causes],
            },
        )
    return {"case_id": case_id, "method": shown.method, "reason": reason, "usage": usage}


# --- M4 exit check ----------------------------------------------------------------------


def audit(conn: psycopg.Connection, limit: int | None = None) -> dict[str, Any]:
    """Re-validate every stored explanation against the data as it is now.

    The M4 exit criterion is that 100% of the figures (and dates) the dashboard displays
    match the stored data. Explanations were validated when written; this re-checks them in
    case the data changed since (e.g. a later amendment) and the case was not re-explained.
    """
    from decimal import Decimal

    from radar.explain.template import Fact
    from radar.explain.validate import dates, validate

    total_cases = conn.execute("SELECT count(*) AS n FROM restatement_case").fetchone()["n"]
    rows = conn.execute(
        "SELECT * FROM case_explanation ORDER BY case_id LIMIT %s", (limit or 10**9,)
    ).fetchall()
    figures = figures_bad = dates_n = dates_bad = fact_bad = 0
    methods: dict[str, int] = {}
    reasons: dict[str, int] = {}
    failing: list[dict[str, Any]] = []
    tokens = {"input_tokens": 0, "output_tokens": 0}
    for row in rows:
        methods[row["method"]] = methods.get(row["method"], 0) + 1
        reasons[row["llm_reason"] or "-"] = reasons.get(row["llm_reason"] or "-", 0) + 1
        for k in tokens:
            tokens[k] += (row["usage"] or {}).get(k, 0)
        g = gather(conn, row["case_id"])
        if g is None:
            continue
        c = g["case"]
        known = {d for d in [c.warning_date, c.amendment_date, *c.period_ends] if d}
        known |= {d for p in g["view"] for d in (p.start, p.end) if d}
        known |= {iso for _s, iso in dates(g["words"]) if iso}
        e = Explanation(
            headline=row["headline"],
            sentences=row["sentences"],
            facts=[
                Fact(
                    f["shown"],
                    Decimal(f["value"]),
                    f["item"],
                    f["period_start"],
                    f["period_end"],
                    f["field"],
                )
                for f in row["facts"]
            ],
            causes=[],
            method=row["method"],
            known_dates=known,
            names={c.company},
        )
        v = validate(e, g["view"])
        bad_fig = sum(p.kind == "unmatched_number" for p in v.problems)
        bad_date = sum(p.kind == "unmatched_date" for p in v.problems)
        bad_fact = sum(p.kind in ("fact_mismatch", "missing_cell") for p in v.problems)
        figures += v.figures_checked
        figures_bad += bad_fig
        dates_n += v.dates_checked
        dates_bad += bad_date
        fact_bad += bad_fact
        if v.problems and len(failing) < 25:
            failing.append(
                {"case_id": row["case_id"], "problems": [p.detail for p in v.problems][:5]}
            )

    def pct(bad: int, n: int) -> float:
        return round(100.0 * (n - bad) / n, 2) if n else 100.0

    cost = (tokens["input_tokens"] * 4 + tokens["output_tokens"] * 20) / 1e6
    return {
        "cases": total_cases,
        "explained": len(rows),
        "coverage_pct": round(100.0 * len(rows) / total_cases, 2) if total_cases else 0.0,
        "by_method": methods,
        "llm_reasons": reasons,
        "figures_displayed": figures,
        "figures_validated_pct": pct(figures_bad, figures),
        "dates_displayed": dates_n,
        "dates_validated_pct": pct(dates_bad, dates_n),
        "facts_no_longer_matching_data": fact_bad,
        "failing_cases": failing,
        "tokens": tokens,
        "approx_cost_usd": round(cost, 2),
    }
