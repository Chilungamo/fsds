import pytest

from radar.extract.ixbrl import load_document
from radar.parse.item402 import affected_periods, expand_dates, parse_item402

from .conftest import FIXTURES

F = FIXTURES / "filings"

# Expected values read from each 8-K's Item 4.02 text.
CASES = {
    "0001193125-26-098600_ulh-20260303.htm.gz": [("2025-09-27", "interim")],
    "0001213900-25-077935_ea0253661-8k_t1energy.htm.gz": [("2025-03-31", "interim")],
    "0001628280-23-042507_cutr-20231221.htm.gz": [
        ("2023-03-31", "interim"),
        ("2023-06-30", "interim"),
    ],
    "0001628280-26-013214_core-20260302.htm.gz": [
        ("2024-06-30", "interim"),
        ("2024-09-30", "interim"),
        ("2024-12-31", "annual"),
        ("2025-03-31", "interim"),
        ("2025-06-30", "interim"),
        ("2025-09-30", "interim"),
    ],
    # found live by the M1 watcher, November 2026
    "0000824142-26-000083_aaon-20261109.htm.gz": [
        ("2026-03-31", "interim"),
        ("2026-06-30", "interim"),
    ],
    "0001193125-26-453778_ck0001986817-20261109.htm.gz": [("2025-09-30", "interim")],
    "0001493152-26-050537_form8-k.htm.gz": [
        ("2024-12-31", "annual"),
        ("2025-12-31", "annual"),
        ("2026-03-31", "interim"),
        ("2026-06-30", "interim"),
    ],
    "0001726978-26-000072_gshd-20261106.htm.gz": [
        *[
            (f"{y}-{md}", "interim")
            for y in (2023, 2024, 2025)
            for md in ("03-31", "06-30", "09-30")
        ],
        ("2023-12-31", "annual"),
        ("2024-12-31", "annual"),
        ("2025-12-31", "annual"),
        ("2026-03-31", "interim"),
        ("2026-06-30", "interim"),
    ],
}


@pytest.mark.parametrize("name,periods", CASES.items(), ids=lambda v: str(v)[:24])
def test_real_8k_periods(name, periods):
    r = parse_item402(load_document(F / name))
    assert r.found
    assert r.sub_item == "a"
    assert [(p.period_end, p.kind) for p in r.periods] == sorted(periods)


@pytest.mark.parametrize("name", list(CASES)[:4])
def test_auditor_and_amendment_flags(name):
    r = parse_item402(load_document(F / name))
    assert r.auditor_discussed and r.amendment_promised


@pytest.mark.parametrize(
    "run,want",
    [
        ("December 31, 2025, 2024 and 2023", ["2025-12-31", "2024-12-31", "2023-12-31"]),
        (
            "September 30 and March 31 of each of 2025 and 2024, and June 30, 2026",
            ["2025-09-30", "2025-03-31", "2024-09-30", "2024-03-31", "2026-06-30"],
        ),
        (
            "March 31, 2025, June 30, 2025 and September 30, 2025",
            ["2025-03-31", "2025-06-30", "2025-09-30"],
        ),
        ("June 30 and March 31, 2026", ["2026-06-30", "2026-03-31"]),
        ("February 30, 2026", []),
    ],
)
def test_expand_dates(run, want):
    assert expand_dates(run) == want


def test_section_stops_at_next_item_and_keeps_amounts():
    r = parse_item402(load_document(F / "0001193125-26-098600_ulh-20260303.htm.gz"))
    assert r.amounts[:2] == [46_700_000.0, 43_200_000.0]
    assert "Item 9.01" not in r.section_text


def test_core_section_excludes_preceding_item_202():
    r = parse_item402(load_document(F / "0001628280-26-013214_core-20260302.htm.gz"))
    assert (
        r.section_text.lstrip().startswith("Non-Reliance")
        or "Non-Reliance Periods" in (r.section_text[:600])
    )
    assert "Results of Operations and Financial Condition" not in r.section_text[:300]


def test_sub_item_b_and_missing_section():
    html = (
        b"<html><body><p>Item 4.02(b) Non-Reliance on a Previously Issued Audit Report or "
        b"Completed Interim Review. On May 1, 2026 our independent registered public accounting "
        b"firm advised the Company that its report on the financial statements for the year "
        b"ended December 31, 2025 should no longer be relied upon.</p>"
        b"<p>Item 9.01 Financial Statements and Exhibits.</p></body></html>"
    )
    r = parse_item402(html)
    assert r.found and r.sub_item == "b"
    assert [(p.period_end, p.kind) for p in r.periods] == [("2025-12-31", "annual")]
    assert not parse_item402(b"<html><body>Item 2.02 Results</body></html>").found


@pytest.mark.parametrize(
    "text,want",
    [
        (
            "the quarterly periods ended March 31, 2025, June 30, 2025 and September 30, 2025, "
            "filed with the SEC on May 7, 2025",
            [("2025-03-31", "interim"), ("2025-06-30", "interim"), ("2025-09-30", "interim")],
        ),
        (
            "for the three and nine months ended September 30, 2024, the year ended "
            "December 31, 2024",
            [("2024-09-30", "interim"), ("2024-12-31", "annual")],
        ),
        ("In preparing its Form 10-Q for the period ended September 30, 2023, management", []),
        (  # SPAC warrant restatements (2021) declare balance sheets unreliable
            "its balance sheets as of March 31, 2021 and June 30, 2021 included in its Quarterly "
            "Reports should no longer be relied upon",
            [("2021-03-31", "unknown"), ("2021-06-30", "unknown")],
        ),
        ("all amounts as of December 31, 2025 in the press release", []),
        ("will file its Quarterly Report on Form 10-Q for the quarter ended June 30, 2023", []),
    ],
)
def test_period_heuristics(text, want):
    assert [(p.period_end, p.kind) for p in affected_periods(text)] == want
