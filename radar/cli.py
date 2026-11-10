"""Command line entry point: ``python -m radar <command>``.

Commands
--------
fetch    Download a filing's primary iXBRL document (gzipped) for offline work.
extract  Print the restatement table found in a local iXBRL file.
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
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


def main(argv: list[str] | None = None) -> int:
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

    args = p.parse_args(argv)
    try:
        return args.func(args)
    except BrokenPipeError:  # e.g. piped into `head`
        sys.stderr.close()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
