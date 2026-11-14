"""Number validator: real explanations validate; any figure that drifts from the data is caught."""

from dataclasses import replace
from decimal import Decimal

import pytest

from radar.explain.template import CaseFacts, Fact, explain
from radar.explain.validate import check_text, enforce, figures, validate
from radar.propagate.engine import build_view, impact_edges

from .test_engine import FY, row
from .test_template import real_case

REAL = [
    (
        "0001193125-26-098600_ulh-20260303.htm.gz",
        "0001193125-26-098732_ulh-20250927.htm.gz",
        "Universal Logistics",
        "2026-03-03",
    ),
    (
        "0001628280-26-013214_core-20260302.htm.gz",
        "0001628280-26-013286_core-20250331.htm.gz",
        "Core Scientific",
        "2026-03-02",
    ),
    (
        "0001628280-23-042507_cutr-20231221.htm.gz",
        "0001628280-24-008911_cutr-20230331.htm.gz",
        "Cutera",
        "2023-12-21",
    ),
    (
        "0001213900-25-077935_ea0253661-8k_t1energy.htm.gz",
        "0001992243-25-000029_t1-20250331.htm.gz",
        "T1 Energy",
        "2025-06-01",
    ),
]


def real(i):
    from radar.enrich import deltas_from_document
    from radar.extract.ixbrl import load_document

    from .test_template import F

    e = real_case(*REAL[i])
    _s, records, _f = deltas_from_document(load_document(F / REAL[i][1]))
    return e, build_view(records)


@pytest.mark.parametrize("i", range(len(REAL)), ids=[r[2] for r in REAL])
def test_every_figure_in_real_explanations_validates(i):
    e, view = real(i)
    v = validate(e, view)
    assert v.ok, v.problems
    assert v.figures_checked >= 1 and v.facts_checked == len(e.facts)


def test_reading_figures():
    got = {
        (f.text, f.value, f.kind)
        for f in figures(
            "On September 27, 2025 (Item 4.02(a)), 6 periods in FY2024: net loss of $103.6 million"
            " (71.4%), $467,000, $1.82 billion, $1.64 per share, 43,166,000 and $5m."
        )
    }
    assert got == {
        ("$103.6 million", Decimal("103600000.0"), "money"),
        ("71.4%", Decimal("71.4"), "pct"),
        ("$467,000", Decimal(467000), "money"),
        ("$1.82 billion", Decimal("1820000000"), "money"),
        ("$1.64", Decimal("1.64"), "money"),
        ("43,166,000", Decimal(43166000), "number"),
        ("$5m", Decimal(5000000), "money"),
    }


def test_rounded_rewording_is_accepted_at_its_own_precision():
    """Step 4 may round differently; a coarser figure is fine if it is the same number."""
    e, view = real(0)  # ULH: adjustment -43,166,000; 71.4%
    facts = e.facts
    assert check_text("Net income fell by about $43 million, or 71%.", facts)[1] == []
    assert check_text("a $43,166,000 cut", facts)[1] == []
    assert check_text("Net income fell by $44 million.", facts)[1] == ["$44 million"]
    assert check_text("a 72% drop", facts)[1] == ["72%"]


def test_figure_not_in_data_is_caught_and_its_sentence_dropped():
    e, view = real(0)
    tampered = replace(e, sentences=[*e.sentences[:3], e.sentences[3].replace("103.6", "130.6")])
    v = validate(tampered, view)
    assert [p.kind for p in v.problems] == ["unmatched_number"] and v.problems[0].sentence == 3
    cleaned, _ = enforce(tampered, view, "Universal Logistics restates")
    assert "130.6" not in cleaned.text and len(cleaned.sentences) == 3
    assert validate(cleaned, view).ok


def test_fact_that_disagrees_with_the_database_is_caught():
    rows = [row("net_income", "NetIncomeLoss", *FY, 45_000_000, -6_000_000, 39_000_000)]
    view = build_view(rows)
    e = explain(CaseFacts("Acme"), [], view, impact_edges(view))
    assert validate(e, view).ok
    # the stored value changed after the text was written (e.g. a later amendment)
    newer = build_view([row("net_income", "NetIncomeLoss", *FY, 45_000_000, -7_000_000, 38e6)])
    v = validate(e, newer)
    kinds = {p.kind for p in v.problems}
    assert kinds == {"fact_mismatch", "unmatched_number"}
    cleaned, _ = enforce(e, newer, "Acme restates")
    assert cleaned.headline == "Acme restates" and "$6.0 million" not in cleaned.text
    assert validate(cleaned, newer).ok


def test_fact_for_a_cell_that_does_not_exist_is_caught():
    rows = [row("net_income", "NetIncomeLoss", *FY, 45, -6, 39)]
    view = build_view(rows)
    bogus = Fact("$9", Decimal(9), "revenue", *FY, "adjustment")
    e = explain(CaseFacts("Acme"), [], view, impact_edges(view))
    v = validate(replace(e, facts=[*e.facts, bogus]), view)
    assert [p.kind for p in v.problems] == ["missing_cell"]


def test_text_without_figures_validates():
    e = explain(CaseFacts("Acme Corp", "2026-11-09", "b"), [], {}, [])
    v = validate(e, {})
    assert v.ok and v.figures_checked == 0
