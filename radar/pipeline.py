"""Turn discovered filings into restatement cases (design doc §5).

8-K / 8-K/A with Item 4.02 -> PENDING case (the amber alert), periods from the text.
10-K/A / 10-Q/A            -> triage; restatements confirm the related case or open one.
Everything else             -> IGNORED.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg

from radar.db import repo
from radar.detect.triage import TRIAGE_VERSION, triage
from radar.edgar.client import EdgarClient
from radar.edgar.submissions import SubmissionsCache
from radar.extract.ixbrl import extract, fmt_decimal
from radar.models import AMENDMENT_FORMS, NON_RELIANCE_ITEM, WARNING_FORMS, FilingRef
from radar.parse.item402 import PARSER_VERSION, document_text, parse_item402
from radar.storage import RawStore

log = logging.getLogger(__name__)

# How far apart (in filing dates) an 8-K and its amendments may be and still be one case.
FOLLOWUP_DAYS = 365  # after a warning, queue the company's amendments filed this soon after
WARNING_LINK_DAYS = (30, 365)  # an 8-K joins a case with filings 30 days before .. 365 after
AMENDMENT_LINK_DAYS = (365, 30)  # an amendment joins a case with filings 365 before .. 30 after
ANNUAL_MIN_DAYS = 300


@dataclass
class Outcome:
    accession: str
    form: str
    state: str
    case_id: int | None = None
    detail: str = ""


class Pipeline:
    def __init__(
        self,
        conn: psycopg.Connection,
        edgar: EdgarClient,
        submissions: SubmissionsCache | None = None,
        store: RawStore | None = None,
        enrich: bool = True,
    ) -> None:
        self.conn = conn
        self.edgar = edgar
        self.subs = submissions or SubmissionsCache(edgar)
        self.store = store or RawStore()
        self.enrich = enrich

    # --- queue -----------------------------------------------------------------------

    def drain(self, batch: int = 10, max_filings: int | None = None) -> list[Outcome]:
        outcomes: list[Outcome] = []
        while max_filings is None or len(outcomes) < max_filings:
            rows = repo.claim_filings(self.conn, limit=batch)
            if not rows:
                break
            for row in rows:
                outcomes.append(self.process(row))
        return outcomes

    def process(self, row: dict[str, Any]) -> Outcome:
        acc, form = row["accession"], row["form"]
        try:
            if form in WARNING_FORMS:
                return self._warning(row)
            if form in AMENDMENT_FORMS:
                return self._amendment(row)
            repo.finish_filing(self.conn, acc, "IGNORED", {"reason": "form not watched"})
            return Outcome(acc, form, "IGNORED")
        except Exception as exc:  # recorded on the row; retried up to MAX_ATTEMPTS
            log.exception("processing %s failed", acc)
            state = repo.fail_filing(self.conn, acc, f"{type(exc).__name__}: {exc}")
            return Outcome(acc, form, state, detail=str(exc))

    # --- helpers ---------------------------------------------------------------------

    def _primary_doc(self, row: dict[str, Any]) -> str:
        if row.get("primary_doc"):
            return row["primary_doc"]
        info = self.subs.lookup(row["cik"], row["accession"])
        if info is None:
            raise LookupError(f"{row['accession']} not found in submissions of {row['cik']}")
        return info.primary_doc

    def _download(self, row: dict[str, Any]) -> tuple[str, bytes]:
        """Primary document, from the raw store when already downloaded."""
        name = self._primary_doc(row)
        raw = self.store.get(row["cik"], row["accession"], name)
        if raw is None:
            raw = self.edgar.filing_document(row["cik"], row["accession"], name)
            self.store.put(row["cik"], row["accession"], name, raw)
        return name, raw

    # --- 8-K: Item 4.02 warning ------------------------------------------------------

    def _warning(self, row: dict[str, Any]) -> Outcome:
        acc = row["accession"]
        items = row["items"]
        if items is None:  # discovered without items (e.g. daily index): look them up
            info = self.subs.lookup(row["cik"], acc)
            items = info.items if info else []
            self.conn.execute("UPDATE filing SET items = %s WHERE accession = %s", (items, acc))
        if NON_RELIANCE_ITEM not in items:
            repo.finish_filing(self.conn, acc, "IGNORED", {"reason": "no Item 4.02"})
            return Outcome(acc, row["form"], "IGNORED")

        name, raw = self._download(row)
        parsed = parse_item402(raw)
        periods = [(p.period_end, p.kind) for p in parsed.periods]
        role = "warning_amendment" if row["form"] == "8-K/A" else "warning"
        summary = {
            "company": row.get("company"),
            "warning_accession": acc,
            "warning_filed": row["filed_date"].isoformat(),
            "periods": [p for p, _ in periods],
            "amounts_mentioned": parsed.amounts[:20],
            "auditor_discussed": parsed.auditor_discussed,
            "amendment_promised": parsed.amendment_promised,
        }
        with self.conn.transaction():
            case = repo.case_for_filing(self.conn, acc) or repo.find_related_case(
                self.conn, row["cik"], row["filed_date"], *WARNING_LINK_DAYS
            )
            if case is None:
                case_id = repo.create_case(
                    self.conn,
                    row["cik"],
                    "BIG_R",  # Item 4.02 means prior statements are unreliable: a reissuance
                    "PENDING",
                    sub_item=parsed.sub_item,
                    confidence=1.0,
                    summary=summary,
                )
                event = "case.created"
            else:
                case_id = case["id"]
                fields: dict[str, Any] = {"summary": summary, "sub_item": parsed.sub_item}
                if case["type"] == "UNKNOWN":
                    fields["type"] = "BIG_R"
                repo.update_case(self.conn, case_id, **fields)
                event = "case.updated"
            repo.link_filing(self.conn, case_id, acc, role)
            repo.add_periods(self.conn, case_id, periods, "item402")
            repo.finish_filing(
                self.conn,
                acc,
                "DONE",
                {"item402": parsed.to_dict()},
                parser_version=PARSER_VERSION,
                primary_doc=name,
            )
            repo.log_event(
                self.conn,
                event,
                case_id,
                acc,
                {"form": row["form"], "role": role, "company": row.get("company"), **summary},
            )
        queued = self._queue_followups(row)
        return Outcome(
            acc,
            row["form"],
            "DONE",
            case_id,
            f"{event}: {len(periods)} periods"
            + (f", {queued} amendments queued" if queued else ""),
        )

    def _queue_followups(self, row: dict[str, Any]) -> int:
        """Queue the company's 10-K/As and 10-Q/As filed within FOLLOWUP_DAYS after the
        warning, and re-triage ones already IGNORED (triage counts an open case as
        evidence, and backfills can process an amendment before its 8-K).

        Live, the watcher would see these amendments anyway; for history this closes the
        gap left by full-text search, which misses many amendments.
        """
        try:
            filings = self.subs.company(row["cik"])
        except Exception as exc:  # follow-ups are best effort; the warning is recorded
            log.warning("follow-up lookup failed for %s: %s", row["accession"], exc)
            return 0
        start = row["filed_date"]
        queued = 0
        for acc, info in filings.items():
            if (
                info.form not in AMENDMENT_FORMS
                or not 0 <= (info.filed - start).days <= FOLLOWUP_DAYS
            ):
                continue
            ref = FilingRef(
                accession=acc,
                cik=row["cik"],
                form=info.form,
                company=row.get("company") or "",
                filed_date=info.filed,
                accepted_at=info.accepted_at,
                primary_doc=info.primary_doc,
                source="followup",
            )
            if repo.insert_filing(self.conn, ref):
                queued += 1
            else:
                queued += repo.requeue_if_ignored(self.conn, acc)
        return queued

    # --- 10-K/A, 10-Q/A: amendment triage ----------------------------------------------

    def _amendment(self, row: dict[str, Any]) -> Outcome:
        acc = row["accession"]
        name, raw = self._download(row)
        extraction = extract(raw)
        related = repo.case_for_filing(self.conn, acc) or repo.find_related_case(
            self.conn, row["cik"], row["filed_date"], *AMENDMENT_LINK_DAYS
        )
        result = triage(extraction, document_text(raw), has_open_case=related is not None)
        changed = [r for r in extraction.rows if r.adjustment and not r.dims]
        meta = {
            "triage": result.to_dict(),
            "cover": {k: v for k, v in extraction.cover.items() if k != "AmendmentDescription"},
            "amendment_description": extraction.cover.get("AmendmentDescription", "")[:4000],
            "top_adjustments": [
                {
                    "concept": r.concept,
                    "period_start": r.period_start,
                    "period_end": r.period_end,
                    "previously_reported": fmt_decimal(r.previously_reported),
                    "adjustment": fmt_decimal(r.adjustment),
                    "restated": fmt_decimal(r.restated),
                }
                for r in sorted(changed, key=lambda r: abs(r.adjustment or 0), reverse=True)[:25]
            ],
        }
        if result.decision == "IGNORE":
            repo.finish_filing(
                self.conn, acc, "IGNORED", meta, parser_version=TRIAGE_VERSION, primary_doc=name
            )
            return Outcome(acc, row["form"], "IGNORED", detail=f"triage {result.score}")

        status = "CONFIRMED" if result.decision == "CONFIRMED" else "NEEDS_REVIEW"
        periods = _xbrl_periods(changed)
        summary = {
            "company": row.get("company"),
            "amendment_accessions": [acc],
            "triage_score": result.score,
        }
        with self.conn.transaction():
            if related is None:
                case_id = repo.create_case(
                    self.conn,
                    row["cik"],
                    "BIG_R" if status == "CONFIRMED" else "UNKNOWN",
                    status,
                    confidence=result.score,
                    summary=summary,
                )
                event = "case.created"
            else:
                case_id = related["id"]
                fields: dict[str, Any] = {"confidence": max(result.score, 0)}
                if status == "CONFIRMED" and related["status"] in ("PENDING", "NEEDS_REVIEW"):
                    fields["status"] = "CONFIRMED"
                    event = "case.confirmed"
                else:
                    event = "case.updated"
                prior = (related["summary"] or {}).get("amendment_accessions", [])
                summary["amendment_accessions"] = sorted({*prior, acc})
                fields["summary"] = summary
                repo.update_case(self.conn, case_id, **fields)
            repo.link_filing(self.conn, case_id, acc, "amendment")
            repo.add_periods(self.conn, case_id, periods, "xbrl")
            repo.finish_filing(
                self.conn, acc, "DONE", meta, parser_version=TRIAGE_VERSION, primary_doc=name
            )
            repo.log_event(
                self.conn,
                event,
                case_id,
                acc,
                {
                    "form": row["form"],
                    "role": "amendment",
                    "company": row.get("company"),
                    "triage_score": result.score,
                    "status": status,
                },
            )
        detail = f"{event} (triage {result.score})"
        if self.enrich:
            detail += self._enrich(row, raw, case_id)
        return Outcome(acc, row["form"], "DONE", case_id, detail)

    def _enrich(self, row: dict[str, Any], raw: bytes, case_id: int) -> str:
        """M2: store line-item deltas and re-analyse the case. Detection has already been
        recorded, so a failure here is logged on the filing rather than retried."""
        from radar.enrich import enrich_case, enrich_filing

        try:
            deltas = enrich_filing(self.conn, self.edgar, row, raw=raw)
            enrich_case(self.conn, case_id)
            return f", {deltas.mapped} deltas ({deltas.source or 'none'})"
        except Exception as exc:
            log.exception("enrichment of %s failed", row["accession"])
            self.conn.execute(
                "UPDATE filing SET meta = meta || jsonb_build_object('enrich_error', %s::text) "
                "WHERE accession = %s",
                (f"{type(exc).__name__}: {exc}", row["accession"]),
            )
            return ", enrichment failed"


def _xbrl_periods(rows) -> list[tuple[str, str]]:
    """Restated reporting periods, from duration facts that changed."""
    out = set()
    for r in rows:
        if r.period_start and r.period_end:
            days = (date.fromisoformat(r.period_end) - date.fromisoformat(r.period_start)).days
            out.add((r.period_end, "annual" if days >= ANNUAL_MIN_DAYS else "interim"))
    return sorted(out)
