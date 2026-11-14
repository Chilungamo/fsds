"""Template explanations on real restatements and on the engine's worked example."""

import re
from decimal import Decimal

import pytest

from radar.enrich import deltas_from_document
from radar.explain.causes import classify
from radar.explain.template import CaseFacts, explain, money
from radar.extract.ixbrl import load_document
from radar.parse.item402 import parse_item402
from radar.propagate.engine import build_view, impact_edges

from .conftest import FIXTURES
from .test_engine import FY, WORKED, row

F = FIXTURES / "filings"


def real_case(warning, amendment, company, date):
    notice = parse_item402(load_document(F / warning))
    _src, records, _face = deltas_from_document(load_document(F / amendment))
    view = build_view(records)
    case = CaseFacts(
        company, date, notice.sub_item, "BIG_R", [p.period_end for p in notice.periods]
    )
    return explain(case, classify(notice.section_text), view, impact_edges(view))


def test_ulh_goodwill_impairment():
    e = real_case(
        "0001193125-26-098600_ulh-20260303.htm.gz",
        "0001193125-26-098732_ulh-20250927.htm.gz",
        "Universal Logistics",
        "2026-03-03",
    )
    assert e.headline == "Impairment or asset valuation lowers net income by $43.2 million"
    assert "for the period ended September 27, 2025 should no longer be relied upon" in e.text
    assert (
        "The net loss for the nine months ended September 27, 2025 widens from $60.4 million "
        "to $103.6 million, a change of $43.2 million (71.4%)."
    ) in e.sentences
    assert "the main balance sheet change is goodwill (down $43.2 million)" in e.text
    assert e.sentences[-1] == (
        "The change starts in impairment and carries through operating expenses → "
        "operating income → pre-tax income → net income → retained earnings → total equity."
    )


def test_core_cash_moves_between_sections():
    e = real_case(
        "0001628280-26-013214_core-20260302.htm.gz",
        "0001628280-26-013286_core-20250331.htm.gz",
        "Core Scientific",
        "2026-03-02",
    )
    assert "for 6 periods from June 30, 2024 to September 30, 2025" in e.text
    assert "also cites expense capitalisation or accruals" in e.text
    assert "falls by $127.3 million to a deficit of $308.8 million" in e.text
    assert "Cash itself is unchanged: $4.4 million moves from operating to investing" in e.text


def test_cutera_inventory():
    e = real_case(
        "0001628280-23-042507_cutr-20231221.htm.gz",
        "0001628280-24-008911_cutr-20230331.htm.gz",
        "Cutera",
        "2023-12-21",
    )
    assert e.headline.startswith("Inventory and cost of sales lowers net income by $3.0 million")
    assert "Revenue for the same period is $467,000 lower (0.8%)." in e.sentences
    assert "the main balance sheet change is inventory (down $2.0 million)" in e.text
    assert "starts in cost of revenue and carries through gross profit" in e.text


def test_t1_revenue_only():
    e = real_case(
        "0001213900-25-077935_ea0253661-8k_t1energy.htm.gz",
        "0001992243-25-000029_t1-20250331.htm.gz",
        "T1 Energy",
        "2025-06-01",
    )
    assert e.headline == "Revenue recognition cuts revenue by $11.2 million"
    assert (
        "Revenue for the quarter ended March 31, 2025 falls by $11.2 million (17.3%), "
        "from $64.6 million to $53.5 million."
    ) in e.sentences


def test_every_number_shown_is_a_recorded_fact():
    """What the step-3 validator relies on: no figure appears that isn't traceable."""
    e = real_case(
        "0001628280-23-042507_cutr-20231221.htm.gz",
        "0001628280-24-008911_cutr-20230331.htm.gz",
        "Cutera",
        "2023-12-21",
    )
    shown = {f.shown for f in e.facts}
    in_text = re.findall(r"\$[\d,.]+(?: (?:million|billion))?|[\d.]+%", e.text)
    assert in_text and set(in_text) <= shown
    for f in e.facts:
        assert f.item and f.period_end and f.field


def test_pending_case_has_no_numbers():
    case = CaseFacts("Acme Corp", "2026-11-09", "b", "UNKNOWN", ["2025-12-31"])
    e = explain(case, [], {}, [])
    assert e.headline == "Acme Corp warns its financial statements can't be relied on"
    assert e.sentences == [
        "Acme Corp said on November 9, 2026, after being notified by its auditor, that its "
        "financial statements for the period ended December 31, 2025 should no longer be "
        "relied upon.",
        "The filing does not say clearly what caused the error.",
        "Restated figures have not been filed yet; they will appear here when the amended "
        "report arrives.",
    ]
    assert e.facts == []


def test_worked_example_income_falls_and_little_r():
    view = build_view(WORKED)
    e = explain(CaseFacts("Acme", None, "a", "LITTLE_R"), [], view, impact_edges(view))
    assert "(a “little r” revision)" in e.text
    assert (
        "Net income for the year ended December 31, 2025 falls by $6 (14.0%), from $45 to $39."
    ) in e.sentences  # small worked-example numbers show as whole dollars
    assert "starts in cost of revenue" in e.text


@pytest.mark.parametrize(
    "before,adj,expected",
    [
        (-10_000_000, -5_000_000, "The net loss for the year ended December 31, 2025 widens"),
        (-10_000_000, 4_000_000, "The net loss for the year ended December 31, 2025 narrows"),
        (
            3_000_000,
            -5_000_000,
            "For the year ended December 31, 2025, the company now reports a net loss of "
            "$2.0 million instead of net income of $3.0 million.",
        ),
    ],
)
def test_net_income_wording_follows_the_sign(before, adj, expected):
    rows = [row("net_income", "NetIncomeLoss", *FY, before, adj, before + adj)]
    view = build_view(rows)
    e = explain(CaseFacts("Acme"), [], view, impact_edges(view))
    assert expected in e.text


def test_money_format():
    assert money(Decimal("-43166000")) == "$43.2 million"
    assert money(Decimal("1817108000")) == "$1.82 billion"
    assert money(Decimal("467000")) == "$467,000"
    assert money(Decimal("-1.64"), per_share=True) == "$1.64"
