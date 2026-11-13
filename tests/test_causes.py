"""Root-cause classifier on real Item 4.02 8-Ks.

Expected causes were read from each filing's own description of the error.
"""

import pytest

from radar.explain.causes import classify
from radar.extract.ixbrl import load_document
from radar.parse.item402 import parse_item402

from .conftest import FIXTURES

F = FIXTURES / "filings"

CASES = [
    # file, primary cause, other causes that must also be found
    ("0001193125-26-098600_ulh-20260303.htm.gz", "IMPAIR", set()),  # goodwill impairment
    (
        "0001628280-26-013214_core-20260302.htm.gz",
        "IMPAIR",
        {"EXP"},
    ),  # capitalized, not written down
    ("0001213900-25-077935_ea0253661-8k_t1energy.htm.gz", "REV", set()),  # offset to revenue
    ("0001628280-23-042507_cutr-20231221.htm.gz", "INV", {"CF_CLASS"}),  # inventory count
    ("0001493152-26-050537_form8-k.htm.gz", "DEBT_EQ", {"REV"}),  # warrants; revenue
    ("0000824142-26-000083_aaon-20261109.htm.gz", "REV", set()),  # percent of completion
    ("0001726978-26-000072_gshd-20261106.htm.gz", "EXP", {"REV"}),  # costs to obtain contracts
    ("0001193125-26-453778_ck0001986817-20261109.htm.gz", "EPS", set()),  # per-share allocation
]


@pytest.mark.parametrize("name,primary,others", CASES, ids=[c[0][:24] for c in CASES])
def test_real_8k_causes(name, primary, others):
    text = parse_item402(load_document(F / name)).section_text
    causes = classify(text)
    codes = [c.code for c in causes]
    assert codes[0] == primary, codes
    assert others <= set(codes[1:]), codes
    for c in causes:  # every label is backed by the company's own words
        assert c.evidence
        for quote in c.evidence:
            core = quote.strip("…")[20:80]
            assert core in " ".join(text.split())


def test_no_guess_when_text_says_nothing():
    assert (
        classify("The Audit Committee concluded the statements should no longer be relied upon.")
        == []
    )


def test_changed_line_items_add_evidence():
    text = "The Company identified an error related to its goodwill."  # 1.0: below the threshold
    assert classify(text) == []
    (c,) = classify(text, changed_items={"goodwill", "net_income"})
    assert c.code == "IMPAIR" and c.from_items == ["goodwill"]


def test_weak_secondary_causes_are_dropped():
    text = (
        "The error relates to goodwill impairment of the reporting unit, which was written down. "
        "The carrying value included deferred tax liabilities."
    )
    assert [c.code for c in classify(text)] == ["IMPAIR"]
