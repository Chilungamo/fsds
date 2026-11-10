"""Command line entry point: ``radar <command>`` (or ``python -m radar``).

M0  fetch, extract          download a filing / print its restatement table
M3  serve                   dashboard: REST API, live event stream, web app
    tickers                 refresh ticker symbols
M2  enrich                  line-item deltas, integrity checks, impact graph, severity
    deltas CASE / graph CASE show a case's restatement and how it propagates
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


def cmd_enrich(args: argparse.Namespace) -> int:
    """(Re)compute deltas for amendments of confirmed / needs-review cases."""
    from radar.edgar.submissions import SubmissionsCache
    from radar.enrich import ENRICH_VERSION, enrich_case, enrich_filing
    from radar.storage import RawStore

    with _db() as conn, EdgarClient() as edgar:
        todo = conn.execute(
            """SELECT f.*, cf.case_id FROM filing f
               JOIN case_filing cf ON cf.accession = f.accession AND cf.role = 'amendment'
               JOIN restatement_case rc ON rc.id = cf.case_id
               WHERE rc.status IN ('CONFIRMED', 'NEEDS_REVIEW')
                 AND (%s::bigint IS NULL OR rc.id = %s)
                 AND (%s OR f.meta->'deltas'->>'version' IS DISTINCT FROM %s)
                 AND (NOT %s OR f.meta->'deltas'->>'source' IS NULL)
                 AND abs(hashtext(f.accession)) %% %s = %s
               ORDER BY f.filed_date DESC LIMIT %s""",
            (
                args.case,
                args.case,
                args.force or args.missing,
                ENRICH_VERSION,
                args.missing,
                args.shards,
                args.shard,
                args.limit or 10**9,
            ),
        ).fetchall()
        store, subs = RawStore(), SubmissionsCache(edgar)
        cases: set[int] = set()
        by_source: dict[str, int] = {}
        for i, row in enumerate(todo, 1):
            try:
                d = enrich_filing(conn, edgar, row, store=store, subs=subs)
                by_source[d.source or "none"] = by_source.get(d.source or "none", 0) + 1
            except Exception as exc:  # keep going; the error is recorded on the filing
                by_source["error"] = by_source.get("error", 0) + 1
                conn.execute(
                    "UPDATE filing SET meta = meta || jsonb_build_object('enrich_error', %s::text)"
                    " WHERE accession = %s",
                    (f"{type(exc).__name__}: {exc}", row["accession"]),
                )
            cases.add(row["case_id"])
            if i % 50 == 0 or i == len(todo):
                print(f"filings {i}/{len(todo)} {by_source}", flush=True)
        for case_id in sorted(cases):
            enrich_case(conn, case_id)
        print(f"enriched {len(cases)} cases")
    return 0


def _money(v) -> str:
    if v is None:
        return ""
    return f"{v:,.2f}" if abs(v) < 100 and v != int(v) else f"{v:,.0f}"


def cmd_deltas(args: argparse.Namespace) -> int:
    from radar.normalize.normalize import catalog

    names = {k: i.name for k, i in catalog().items.items()}
    with _db() as conn:
        case = conn.execute(
            """SELECT rc.*, c.name FROM restatement_case rc JOIN company c USING (cik)
               WHERE rc.id = %s""",
            (args.case,),
        ).fetchone()
        if case is None:
            print(f"no case {args.case}")
            return 1
        rows = conn.execute(
            """SELECT * FROM case_delta WHERE case_id = %s AND dims = '{}'
                 AND (%s OR adjustment <> 0) AND (%s OR canonical_item IS NOT NULL)
               ORDER BY period_end, period_start NULLS FIRST, statement, canonical_item, concept""",
            (args.case, args.all, args.all),
        ).fetchall()
        checks = conn.execute(
            "SELECT * FROM integrity_check WHERE case_id = %s ORDER BY period_end, name",
            (args.case,),
        ).fetchall()
    sev = "-" if case["severity"] is None else f"{case['severity']:.2f}"
    print(f"#{case['id']} {case['name']}  {case['status']} {case['type']}  severity {sev}")
    current = None
    for r in rows:
        period = (r["period_start"], r["period_end"])
        if period != current:
            current = period
            label = (
                f"{r['period_start']} .. {r['period_end']}"
                if r["period_start"]
                else f"as of {r['period_end']}"
            )
            print(f"\n  {label}")
        name = names.get(r["canonical_item"]) or r["label"] or r["concept"]
        ok = {True: "✓", False: "✗", None: " "}[r["check_ok"]]
        print(
            f"    {r['statement'] or '?':2} {name[:44]:44} {_money(r['previously_reported']):>18}"
            f" {_money(r['adjustment']):>16} {_money(r['restated']):>18} {ok} {r['source']}"
        )
    if checks:
        print("\n  integrity checks")
        for c in checks:
            when = (
                f"{c['period_start']}..{c['period_end']}"
                if c["period_start"]
                else str(c["period_end"])
            )
            assumed = c["details"].get("assumed_unchanged") or []
            note = f"  (assumed unchanged: {', '.join(assumed)})" if assumed else ""
            print(
                f"    {c['status'].upper():4} {c['name']:40} {when:24} gap {_money(c['gap'])}{note}"
            )
    return 0


def cmd_graph(args: argparse.Namespace) -> int:
    from radar.normalize.normalize import catalog

    items = catalog().items
    with _db() as conn:
        edges = conn.execute(
            "SELECT * FROM impact_edge WHERE case_id = %s "
            "ORDER BY period_end, period_start, src_item",
            (args.case,),
        ).fetchall()
    current = None
    for e in edges:
        period = (e["period_start"], e["period_end"])
        if period != current:
            current = period
            print(f"\n{e['period_start'] or 'as of'} .. {e['period_end']}")
        dst = "(implied)" if e["dst_delta"] is None else _money(e["dst_delta"])
        src = items[e["src_item"]]
        print(
            f"  {src.statement} {src.name:28} {_money(e['src_delta']):>16}"
            f"  --{'+' if e['sign'] > 0 else '-'} {e['relationship']:<26}-->  "
            f"{items[e['dst_item']].statement} {items[e['dst_item']].name:28} {dst:>16}"
        )
    return 0


def cmd_tickers(args: argparse.Namespace) -> int:
    from radar.edgar.tickers import refresh_tickers

    with _db() as conn, EdgarClient() as edgar:
        print(f"updated tickers for {refresh_tickers(conn, edgar)} companies")
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    from radar.api.app import create_app

    uvicorn.run(create_app(), host=args.host, port=args.port, log_level="info")
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

    en = sub.add_parser("enrich", help="compute line-item deltas, checks and impact graphs")
    en.add_argument("--case", type=int)
    en.add_argument("--limit", type=int)
    en.add_argument("--force", action="store_true", help="recompute even if up to date")
    en.add_argument("--missing", action="store_true", help="only amendments without deltas")
    en.add_argument("--shard", type=int, default=0, help="this worker's shard (0-based)")
    en.add_argument("--shards", type=int, default=1, help="number of parallel workers")
    en.set_defaults(func=cmd_enrich)
    dl = sub.add_parser("deltas", help="show a case's restated line items and checks")
    dl.add_argument("case", type=int)
    dl.add_argument("--all", action="store_true", help="include unchanged and unmapped rows")
    dl.set_defaults(func=cmd_deltas)
    gr = sub.add_parser("graph", help="show a case's impact graph")
    gr.add_argument("case", type=int)
    gr.set_defaults(func=cmd_graph)

    sub.add_parser("tickers", help="refresh ticker symbols from the SEC").set_defaults(
        func=cmd_tickers
    )
    sv = sub.add_parser("serve", help="run the dashboard (API + web app)")
    sv.add_argument("--host", default="127.0.0.1")
    sv.add_argument("--port", type=int, default=8000)
    sv.set_defaults(func=cmd_serve)

    args = p.parse_args(argv)
    try:
        return args.func(args)
    except BrokenPipeError:  # e.g. piped into `head`
        sys.stderr.close()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
