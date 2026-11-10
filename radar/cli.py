"""Command line entry point: ``radar <command>`` (or ``python -m radar``).

M0  fetch, extract          download a filing / print its restatement table
M1  migrate                 create or upgrade the database schema
    watch [--once]          poll EDGAR's live feed and process new filings
    process                 drain the work queue
    reconcile START [END]   enqueue filings the watcher missed, from daily indexes
    backfill START [END]    historical Item 4.02 8-Ks (and --amendments) via full-text search
    recall START [END]      detection recall vs full-text search
    cases                   list restatement cases
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from radar.edgar.client import EdgarClient
from radar.edgar.urls import normalize_accession
from radar.extract.ixbrl import extract_file


def _fmt(v: Decimal | None, unit: str | None) -> str:
    if v is None:
        return ""
    return f"{v:,.2f}" if unit and "/" in unit else f"{v:,.0f}"


def cmd_fetch(args: argparse.Namespace) -> int:
    acc = normalize_accession(args.accession)
    with EdgarClient() as edgar:
        name = args.document or edgar.primary_document_name(args.cik, acc)
        data = edgar.filing_document(args.cik, acc, name)
    out = Path(args.out) if args.out else Path(f"{acc}_{name}.gz")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(gzip.compress(data, mtime=0))
    print(f"saved {name} ({len(data):,} bytes) -> {out}")
    return 0


def cmd_extract(args: argparse.Namespace) -> int:
    result = extract_file(args.path)
    if args.json:
        json.dump(
            {
                "cover": result.cover,
                "axes_seen": {k: sorted(v) for k, v in result.axes_seen.items()},
                "rows": [r.to_dict() for r in result.rows],
            },
            sys.stdout,
            indent=2,
        )
        print()
        return 0

    c = result.cover
    print(
        f"{c.get('EntityRegistrantName', '?')}  {c.get('DocumentType', '?')}  "
        f"period {c.get('DocumentPeriodEndDate', '?')}"
    )
    correction = c.get("DocumentFinStmtErrorCorrectionFlag", "n/a")
    recovery = c.get("DocumentFinStmtRestatementRecoveryAnalysisFlag", "n/a")
    print(f"  error-correction flag: {correction}   recovery-analysis flag: {recovery}")
    if not result.has_restatement_dimension:
        print("  no RestatementAxis facts found")
        return 1
    rows = [
        r for r in result.rows if (args.all or not r.dims) and (not args.changed or r.adjustment)
    ]
    print(
        f"  {len(result.rows)} restatement rows, {len(rows)} shown "
        "(--all adds dimensional rows, --changed hides zero adjustments)\n"
    )
    head = f"{'concept':<58}{'period':<24}{'as reported':>16}{'adjustment':>16}{'restated':>16}  ok"
    print(head)
    print("-" * len(head))
    for r in rows:
        period = f"{r.period_start or ''}..{r.period_end}" if r.period_start else str(r.period_end)
        ok = {True: "✓", False: "✗", None: "·"}[r.check_ok]
        print(
            f"{r.concept[:57]:<58}{period:<24}{_fmt(r.previously_reported, r.unit):>16}"
            f"{_fmt(r.adjustment, r.unit):>16}{_fmt(r.restated, r.unit):>16}  {ok}"
        )
    return 0


def _db():
    from radar.db import repo

    return repo.connect()


def _day(s: str) -> date:
    return date.fromisoformat(s)


def cmd_migrate(args: argparse.Namespace) -> int:
    from radar.db.migrate import migrate

    with _db() as conn:
        applied = migrate(conn)
    print("applied: " + (", ".join(applied) if applied else "nothing (up to date)"))
    return 0


def _print_outcomes(outcomes) -> None:
    for o in outcomes:
        if o.state != "IGNORED":
            case = f" case {o.case_id}" if o.case_id else ""
            print(f"  {o.form:7} {o.accession} {o.state}{case} {o.detail}")


def cmd_watch(args: argparse.Namespace) -> int:
    from radar.workers import watch

    def report(polled, outcomes):
        ignored = sum(o.state == "IGNORED" for o in outcomes)
        print(
            f"polled {polled.seen} entries, {len(polled.new)} new, "
            f"{len(polled.new_non_reliance)} with Item 4.02; processed {len(outcomes)} "
            f"({ignored} ignored)",
            flush=True,
        )
        _print_outcomes(outcomes)

    with _db() as conn, EdgarClient() as edgar:
        watch.run(conn, edgar, interval=args.interval, once=args.once, on_cycle=report)
    return 0


def cmd_process(args: argparse.Namespace) -> int:
    from radar.db import repo
    from radar.pipeline import Pipeline

    with _db() as conn, EdgarClient() as edgar:
        outcomes = Pipeline(conn, edgar).drain(max_filings=args.limit)
        _print_outcomes(outcomes)
        print(f"processed {len(outcomes)}; queue: {repo.queue_depth(conn)}")
    return 0


def cmd_reconcile(args: argparse.Namespace) -> int:
    from radar.workers.reconcile import reconcile

    end = _day(args.end) if args.end else date.today() - timedelta(days=1)
    with _db() as conn, EdgarClient() as edgar:
        for r in reconcile(conn, edgar, _day(args.start), end):
            if r.available:
                print(
                    f"{r.day}: {r.listed} listed, {r.missing} new, "
                    f"Item 4.02: {', '.join(r.non_reliance) or '-'}",
                    flush=True,
                )
    return 0


def cmd_backfill(args: argparse.Namespace) -> int:
    from radar.workers.backfill import backfill

    end = _day(args.end) if args.end else date.today()
    with _db() as conn, EdgarClient() as edgar:
        for r in backfill(conn, edgar, _day(args.start), end, amendments=args.amendments):
            print(f"{r.window[0]}..{r.window[1]}: {r.hits} hits, {r.inserted} new", flush=True)
    return 0


def cmd_recall(args: argparse.Namespace) -> int:
    from radar.workers.recall import measure

    end = _day(args.end) if args.end else date.today()
    with _db() as conn, EdgarClient() as edgar:
        r = measure(conn, edgar, _day(args.start), end)
    pct = "n/a" if r.recall is None else f"{r.recall:.1%}"
    print(
        f"{r.start}..{r.end}: full-text search lists {len(r.reference)} Item 4.02 filings; "
        f"we detected {len(r.found)} of them (recall {pct}); "
        f"{len(r.extra)} more found only by us"
    )
    for acc in sorted(r.missed):
        print(f"  missed: {acc}")
    for acc in sorted(r.extra):
        print(f"  only ours: {acc}")
    return 0 if r.recall is None or r.recall >= args.target else 1


def cmd_cases(args: argparse.Namespace) -> int:
    with _db() as conn:
        rows = conn.execute(
            """SELECT rc.id, rc.status, rc.type, rc.sub_item, c.name, rc.opened_at::date AS opened,
                      (SELECT string_agg(cf.role || ' ' || f.form || ' ' || f.filed_date, '; '
                                         ORDER BY f.filed_date)
                         FROM case_filing cf JOIN filing f USING (accession)
                        WHERE cf.case_id = rc.id) AS filings,
                      (SELECT string_agg(ap.period_end::text, ', ' ORDER BY ap.period_end)
                         FROM (SELECT DISTINCT period_end FROM affected_period
                               WHERE case_id = rc.id) ap) AS periods
               FROM restatement_case rc JOIN company c USING (cik)
               WHERE %s::text IS NULL OR rc.status::text = %s
               ORDER BY rc.updated_at DESC LIMIT %s""",
            (args.status, args.status, args.limit),
        ).fetchall()
    for r in rows:
        print(
            f"#{r['id']:<5} {r['status']:<12} {r['type']:<8} {r['name'][:40]:<40} "
            f"periods: {r['periods'] or '-'}"
        )
        print(f"       {r['filings']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(
        prog="radar", description="Restatement Radar: fetch SEC filings and extract restatements."
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    f = sub.add_parser("fetch", help="download a filing document from EDGAR")
    f.add_argument("cik")
    f.add_argument("accession")
    f.add_argument("--document", help="file name inside the filing (default: primary document)")
    f.add_argument("--out", help="output path (.gz)")
    f.set_defaults(func=cmd_fetch)

    e = sub.add_parser("extract", help="extract the restatement table from a local iXBRL file")
    e.add_argument("path")
    e.add_argument("--json", action="store_true")
    e.add_argument("--all", action="store_true", help="also show rows with other dimensions")
    e.add_argument("--changed", action="store_true", help="only rows with a non-zero adjustment")
    e.set_defaults(func=cmd_extract)

    sub.add_parser("migrate", help="create/upgrade the database schema").set_defaults(
        func=cmd_migrate
    )
    w = sub.add_parser("watch", help="poll EDGAR's live feed and process new filings")
    w.add_argument("--interval", type=float, default=60.0, help="seconds between polls")
    w.add_argument("--once", action="store_true", help="poll and process once, then exit")
    w.set_defaults(func=cmd_watch)
    pr = sub.add_parser("process", help="drain the work queue")
    pr.add_argument("--limit", type=int, default=None)
    pr.set_defaults(func=cmd_process)
    for name, func, helptext in (
        ("reconcile", cmd_reconcile, "enqueue filings missing from daily indexes"),
        ("backfill", cmd_backfill, "historical Item 4.02 filings via full-text search"),
        ("recall", cmd_recall, "detection recall vs full-text search"),
    ):
        c = sub.add_parser(name, help=helptext)
        c.add_argument("start", help="YYYY-MM-DD")
        c.add_argument("end", nargs="?", help="YYYY-MM-DD (default: today/yesterday)")
        c.set_defaults(func=func)
        if name == "backfill":
            c.add_argument(
                "--amendments",
                action="store_true",
                help="also enqueue candidate restatement 10-K/As and 10-Q/As",
            )
        if name == "recall":
            c.add_argument("--target", type=float, default=0.99)
    cs = sub.add_parser("cases", help="list restatement cases")
    cs.add_argument("--status")
    cs.add_argument("--limit", type=int, default=30)
    cs.set_defaults(func=cmd_cases)

    args = p.parse_args(argv)
    try:
        return args.func(args)
    except BrokenPipeError:  # e.g. piped into `head`
        sys.stderr.close()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
