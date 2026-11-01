"""Run the Modified Jones replication.

Examples (from the repository root):

    # a handful of companies via the SEC API (cached under data/cache)
    python -m theories.modified_jones --tickers AAPL MSFT KO PEP WMT TGT HD LOW

    # the full listed universe via the API (~10k companies, ~30 min first run)
    python -m theories.modified_jones --all-tickers

    # the full universe from the SEC bulk archives (fastest for everything)
    python -m theories.modified_jones \\
        --companyfacts-zip data/companyfacts.zip --submissions-zip data/submissions.zip
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from fsds.sec_client import SecClient, cik10, iter_bulk_json

from .data import apply_sample_filters, build_panel, company_items, concat_items
from .model import estimate, summarize_coefficients

log = logging.getLogger("modified_jones")


def _progress(it, total=None, desc=""):
    try:
        from tqdm import tqdm
        return tqdm(it, total=total, desc=desc)
    except ImportError:  # pragma: no cover
        return it


def load_sic_from_submissions(docs) -> pd.Series:
    out = {}
    for d in docs:
        if d and d.get("cik") and d.get("sic"):
            out[cik10(d["cik"])] = d["sic"]
    return pd.to_numeric(pd.Series(out, dtype=object), errors="coerce")


def load_sic_from_db() -> pd.Series:
    from fsds.config import get_engine
    df = pd.read_sql("SELECT cik, sic FROM ciksic", get_engine())
    return pd.to_numeric(df.set_index(df["cik"].map(cik10))["sic"], errors="coerce")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--tickers", nargs="+")
    src.add_argument("--ciks", nargs="+")
    src.add_argument("--all-tickers", action="store_true", help="every company in company_tickers.json")
    src.add_argument("--companyfacts-zip", type=Path, help="SEC bulk companyfacts.zip")
    ap.add_argument("--submissions-zip", type=Path, help="SEC bulk submissions.zip (for SIC codes)")
    ap.add_argument("--sic-from-db", action="store_true", help="read SIC codes from the ciksic table")
    ap.add_argument("--limit", type=int, help="only process the first N companies")
    ap.add_argument("--policy", choices=["first", "latest"], default="first",
                    help="first-reported (default) or latest restated values")
    ap.add_argument("--variant", choices=["modified_jones", "kothari"], default="modified_jones")
    ap.add_argument("--min-obs", type=int, default=10, help="minimum firms per industry-year")
    ap.add_argument("--winsor", type=float, default=0.01)
    ap.add_argument("--cache", type=Path, default=Path("data/cache"))
    ap.add_argument("--out", type=Path, default=Path(__file__).parent / "results")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    client = SecClient(cache_dir=args.cache)

    # 1. universe + facts
    if args.companyfacts_zip:
        docs = iter_bulk_json(args.companyfacts_zip)
        ciks = None
    else:
        if args.tickers:
            ciks = list(client.ticker_to_cik(args.tickers).values())
        elif args.ciks:
            ciks = [cik10(c) for c in args.ciks]
        else:
            ciks = sorted({cik10(v["cik_str"]) for v in client.company_tickers().values()})
        if args.limit:
            ciks = ciks[: args.limit]
        docs = (client.company_facts(c) for c in ciks)

    per_company, seen = [], []
    for i, doc in enumerate(_progress(docs, total=len(ciks) if ciks else None, desc="companyfacts")):
        if args.limit and i >= args.limit:
            break
        if not doc:
            continue
        seen.append(cik10(doc["cik"]))
        per_company.append(company_items(doc, policy=args.policy))
    items = concat_items(per_company)
    log.info("parsed XBRL facts for %d companies", len(seen))

    # 2. industry codes
    if args.sic_from_db:
        sic = load_sic_from_db()
    elif args.submissions_zip:
        sic = load_sic_from_submissions(iter_bulk_json(args.submissions_zip))
    else:
        sic = load_sic_from_submissions(client.submissions(c) for c in _progress(seen, len(seen), "submissions"))

    # 3. panel, sample, estimation
    panel = build_panel(items, sic=sic)
    sample, attrition = apply_sample_filters(panel)
    firm_years, coefs = estimate(sample, variant=args.variant, min_obs=args.min_obs, winsor=args.winsor)
    attrition.loc[len(attrition)] = [f"in industry-years with >= {args.min_obs} firms", len(firm_years)]

    # 4. outputs
    args.out.mkdir(parents=True, exist_ok=True)
    keep = ["cik", "fyear", "start", "end", "sic", "sic2", "earnings", "earnings_tag", "cfo", "revenue",
            "revenue_tag", "revenue_lag", "receivables", "receivables_lag", "ppe", "ppe_tag", "assets_lag",
            "y_ta", "nda", "da", "da_jones"]
    firm_years[[c for c in keep if c in firm_years]].to_csv(args.out / "firm_year_da.csv", index=False)
    coefs.to_csv(args.out / "industry_year_coefficients.csv", index=False)
    write_summary(args, attrition, coefs, firm_years)
    log.info("wrote results to %s", args.out)


def write_summary(args, attrition: pd.DataFrame, coefs: pd.DataFrame, firm_years: pd.DataFrame):
    lines = [f"# Modified Jones results ({args.variant}, policy={args.policy})", "",
             "## Sample attrition", "", attrition.to_markdown(index=False), ""]
    if coefs.empty:
        lines += ["No industry-year had enough firms to estimate. Use a larger universe "
                  f"or lower --min-obs (currently {args.min_obs})."]
    else:
        lines += ["## Coefficients across industry-years", "",
                  summarize_coefficients(coefs).to_markdown(index=False, floatfmt=".4f"), "",
                  "## Accruals by fiscal year", ""]
        by_year = firm_years.groupby("fyear").agg(
            firm_years=("da", "size"), mean_ta=("y_ta", "mean"), mean_da=("da", "mean"),
            mean_abs_da=("da", lambda s: s.abs().mean()), median_abs_da=("da", lambda s: s.abs().median()))
        lines += [by_year.to_markdown(floatfmt=".4f"), "",
                  "## Tags used (share of firm-years)", ""]
        for col in ("revenue_tag", "earnings_tag", "ppe_tag"):
            share = firm_years[col].value_counts(normalize=True).mul(100).round(1)
            lines += [f"**{col}**", "", share.to_frame("%").to_markdown(), ""]
    (args.out / "summary.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
