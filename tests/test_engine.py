from decimal import Decimal

import pytest

from radar.enrich import deltas_from_document
from radar.extract.ixbrl import load_document
from radar.normalize.normalize import catalog
from radar.propagate.engine import Period, build_view, impact_edges, integrity_checks, severity

from .conftest import FIXTURES


def row(item, concept, start, end, prev, adj, rest, source="xbrl"):
    return {
        "canonical_item": item,
        "concept": f"us-gaap:{concept}" if source == "xbrl" else f"label:?:{concept}",
        "period_start": start,
        "period_end": end,
        "previously_reported": Decimal(str(prev)),
        "adjustment": Decimal(str(adj)),
        "restated": Decimal(str(rest)),
        "tolerance": Decimal("0.05"),
        "dims": {},
        "source": source,
    }


FY = ("2025-01-01", "2025-12-31")
# Design doc §8.4: inventory overstated by 8.4 at year end, 25% tax ($ millions)
WORKED = [
    row("inventory", "InventoryNet", None, "2025-12-31", 120, -8.4, 111.6),
    row("cogs", "CostOfRevenue", *FY, 400, 8.4, 408.4),
    row(
        "pretax_income",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
        *FY,
        60,
        -8.4,
        51.6,
    ),
    row("tax_expense", "IncomeTaxExpenseBenefit", *FY, 15, -2.1, 12.9),
    row("net_income", "NetIncomeLoss", *FY, 45, -6.3, 38.7),
    row(
        "retained_earnings",
        "RetainedEarningsAccumulatedDeficit",
        None,
        "2025-12-31",
        300,
        -6.3,
        293.7,
    ),
    row("retained_earnings", "RetainedEarningsAccumulatedDeficit", None, "2024-12-31", 250, 0, 250),
    row("total_assets", "Assets", None, "2025-12-31", 1000, -8.4, 991.6),
    row("total_liabilities", "Liabilities", None, "2025-12-31", 500, -2.1, 497.9),
    row("total_equity", "StockholdersEquity", None, "2025-12-31", 500, -6.3, 493.7),
]


def test_worked_example_passes_all_checks():
    checks = integrity_checks(build_view(WORKED))
    assert {(c.name, c.status) for c in checks} == {
        ("balance_sheet_balances", "pass"),
        ("net_income_rollup", "pass"),
        ("net_income_flows_to_retained_earnings", "pass"),
    }


def test_worked_example_impact_graph():
    edges = {(e.src, e.dst): e for e in impact_edges(build_view(WORKED))}
    assert edges[("cogs", "inventory")].dst_delta == Decimal("-8.4")  # cost relieved
    assert edges[("net_income", "retained_earnings")].dst_delta == Decimal("-6.3")
    assert edges[("net_income", "retained_earnings")].period == Period(*FY)
    assert edges[("inventory", "total_current_assets")].dst_delta is None  # implied, not reported
    assert ("total_assets", "net_income") not in edges  # balance sheet never flows into P&L


def test_broken_balance_sheet_fails():
    rows = [r for r in WORKED if r["canonical_item"] != "total_liabilities"]
    rows.append(row("total_liabilities", "Liabilities", None, "2025-12-31", 500, -1.0, 499))
    (bs,) = [c for c in integrity_checks(build_view(rows)) if c.name == "balance_sheet_balances"]
    assert bs.status == "fail" and bs.gap == Decimal("-1.1")


def test_missing_term_assumed_unchanged_only_if_reported():
    rows = [r for r in WORKED if r["canonical_item"] != "tax_expense"]
    view = build_view(rows)
    assert not [c for c in integrity_checks(view) if c.name == "net_income_rollup"]
    (c,) = [c for c in integrity_checks(view, {"tax_expense"}) if c.name == "net_income_rollup"]
    assert c.details["assumed_unchanged"] == ["tax_expense"]
    assert c.status == "fail"  # 8.4 pre-tax change cannot all flow to net income


def test_opening_balance_is_never_assumed_unchanged():
    # without the 2024 year-end retained earnings, the roll-forward cannot be checked even
    # though retained earnings is reported: the opening balance carries earlier errors
    rows = [r for r in WORKED if r["period_end"] != "2024-12-31"]
    names = {c.name for c in integrity_checks(build_view(rows), {"retained_earnings"})}
    assert "net_income_flows_to_retained_earnings" not in names


def test_xbrl_beats_html_and_preferred_concept_wins():
    rows = [
        row("revenue", "Total revenues", *FY, 10, 1, 11, source="html"),
        row("revenue", "RevenueFromContractWithCustomerExcludingAssessedTax", *FY, 10, 2, 12),
        row("revenue", "Revenues", *FY, 10, 3, 13),
    ]
    cell = build_view(rows)[Period(*FY)]["revenue"]
    assert cell.concept == "us-gaap:Revenues" and cell.adjustment == 3


def test_severity():
    s = severity(build_view(WORKED), big_r=True, auditor_initiated=False, periods_affected=1)
    # 0.40*6.3/45 + 0.25*6.3/500 + 0.15 + 0.10*1/8
    assert s == pytest.approx(0.40 * 0.14 + 0.25 * 0.0126 + 0.15 + 0.0125, abs=0.001)


def test_catalog_is_consistent():
    cat = catalog()
    assert cat.item_for_concept("us-gaap:InventoryNet").key == "inventory"
    for key, item in cat.items.items():
        assert item.statement in {"IS", "BS", "CF"}, key
    from radar.propagate.engine import EDGES

    for src, dst, sign, _ in EDGES:
        assert src in cat.items and dst in cat.items and sign in (1, -1)


# The M0 golden fixtures: every check the engine can run must pass.
GOLDEN = [
    ("0001193125-26-098732_ulh-20250927.htm.gz", {"balance_sheet_balances", "net_income_rollup"}),
    ("0001628280-26-013286_core-20250331.htm.gz", {"balance_sheet_balances", "net_income_rollup"}),
    ("0001992243-25-000029_t1-20250331.htm.gz", {"gross_profit_rollup"}),
    (
        "0001628280-24-008911_cutr-20230331.htm.gz",
        {"balance_sheet_balances", "gross_profit_rollup", "net_income_rollup", "cash_flow_sums"},
    ),
    ("0001144204-19-014450_tv515406_10qa.htm.gz", set()),  # HTML-only filing
]


@pytest.mark.parametrize("name,expected", GOLDEN, ids=lambda v: str(v)[:24])
def test_golden_filings_pass_their_checks(name, expected):
    source, records, face = deltas_from_document(load_document(FIXTURES / "filings" / name))
    for r in records:
        r["source"] = source
    checks = integrity_checks(build_view(records), face)
    assert {c.name for c in checks} >= expected
    assert [c for c in checks if c.status == "fail"] == []
