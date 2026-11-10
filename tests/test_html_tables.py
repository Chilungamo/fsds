"""HTML restatement tables from real filings that lack XBRL restatement tags.

Expected values were read from each filing's printed table.
"""

from datetime import date
from decimal import Decimal

import pytest

from radar.extract.html_tables import _minus_months, extract_html, parse_number, parse_period
from radar.extract.ixbrl import load_document

from .conftest import FIXTURES

F = FIXTURES / "filings"
PROTALIX = "0001144204-19-014450_tv515406_10qa.htm.gz"  # header says "in thousands"
CASCADE = "0001213900-21-034531_f10k2020a1_cascadeacq.htm.gz"  # SPAC: periods in section rows
LSYN = "0001654954-20-007584_lsyn_10qa.htm.gz"  # periods inside the header cells
PCTL = "0001554795-21-000325_pctl0907form10qa.htm.gz"  # "... AND STOCKHOLDERS' EQUITY" caption

_cache: dict = {}


def rows(name):
    if name not in _cache:
        _cache[name] = extract_html(load_document(F / name))
    return _cache[name]


def find(name, label_start, start, end):
    hits = [
        r
        for r in rows(name)
        if r.label.lower().startswith(label_start.lower())
        and r.period_start == start
        and r.period_end == end
    ]
    assert hits, f"{label_start} {start}..{end} not found"
    return hits[0]


@pytest.mark.parametrize(
    "name,label,start,end,prev,adj,rest,item",
    [
        (
            PROTALIX,
            "Contracts liability",
            None,
            "2018-03-31",
            0,
            2_651_000,
            2_651_000,
            "deferred_revenue",
        ),
        (
            PROTALIX,
            "Total liabilities",
            None,
            "2018-03-31",
            104_236_000,
            -3_997_000,
            100_239_000,
            "total_liabilities",
        ),
        (
            PROTALIX,
            "REVENUES FROM LICENSE",
            "2018-01-01",
            "2018-03-31",
            0,
            2_161_000,
            2_161_000,
            "revenue",
        ),
        (
            PROTALIX,
            "Net loss per share",
            "2018-01-01",
            "2018-03-31",
            Decimal("-0.06"),
            Decimal("0.01"),
            Decimal("-0.05"),
            "eps_basic",
        ),
        (
            CASCADE,
            "Warrant Liabilities",
            None,
            "2020-11-24",
            0,
            17_317_000,
            17_317_000,
            "warrant_liabilities",
        ),
        (
            CASCADE,
            "Class A Common Stock Subject",
            None,
            "2020-12-31",
            221_916_560,
            -26_815_120,
            195_101_440,
            "temporary_equity",
        ),
        (
            CASCADE,
            "Net loss",
            "2020-08-14",
            "2020-12-31",
            -159_000,
            -7_669_675,
            -7_828_675,
            "net_income",
        ),
        (
            LSYN,
            "Net Income",
            "2019-01-01",
            "2019-09-30",
            3_194_996,
            -689_071,
            2_505_925,
            "net_income",
        ),
        (LSYN, "Net Income", "2019-07-01", "2019-09-30", 819_384, -178_129, 641_255, "net_income"),
        (
            PCTL,
            "Derivative liability",
            None,
            "2020-09-30",
            17_066_643,
            -6_572_227,
            10_494_416,
            "warrant_liabilities",
        ),
        (
            PCTL,
            "NET INCOME (LOSS)",
            "2020-07-01",
            "2020-09-30",
            -3_352_793,
            -453_165,
            -3_805_958,
            "net_income",
        ),
    ],
)
def test_rows_match_printed_tables(name, label, start, end, prev, adj, rest, item):
    r = find(name, label, start, end)
    assert (r.previously_reported, r.adjustment, r.restated) == (
        Decimal(prev),
        Decimal(adj),
        Decimal(rest),
    )
    assert r.canonical_item == item
    assert r.check_ok is True


def test_filer_sign_error_is_flagged():
    # Liberated Syndication prints the accumulated-deficit adjustment with the wrong sign
    r = find(LSYN, "Accumulated Deficit", None, "2019-09-30")
    assert r.check_ok is False


def test_liabilities_section_is_balance_sheet_not_equity_statement():
    assert {r.statement for r in rows(PCTL) if r.period_start is None} == {"BS"}
    assert find(PCTL, "Accounts payable", None, "2020-09-30").statement == "BS"


def test_flow_rows_without_a_duration_are_dropped():
    assert all(r.period_start for r in rows(LSYN) if r.statement in ("IS", "CF"))


@pytest.mark.parametrize(
    "text,want",
    [
        ("$ (6,648 )", Decimal(-6648)),
        ("—", Decimal(0)),
        ("1,752,979‬‬", Decimal(1752979)),
        ("(0.06", Decimal("-0.06")),
        ("", None),
        ("n/a", None),
    ],
)
def test_parse_number(text, want):
    assert parse_number(text) == want


@pytest.mark.parametrize(
    "text,want",
    [
        ("Nine months ended September 30, 2019 As Reported", ("2019-01-01", "2019-09-30")),
        ("Three Months Ended March 31, 2018", ("2018-01-01", "2018-03-31")),
        ("Year ended December 31, 2024", ("2024-01-01", "2024-12-31")),
        (
            "for the Period from August 14, 2020 (inception) to December 31, 2020",
            ("2020-08-14", "2020-12-31"),
        ),
        ("Balance sheet as of November 24, 2020", (None, "2020-11-24")),
        ("no date here", None),
    ],
)
def test_parse_period(text, want):
    assert parse_period(text) == want


def test_minus_months_handles_month_ends():
    assert _minus_months(date(2019, 9, 30), 9) == date(2018, 12, 31)
    assert _minus_months(date(2024, 2, 29), 12) == date(2023, 2, 28)
    assert _minus_months(date(2025, 9, 27), 3) == date(2025, 6, 27)


def test_difference_column_last():
    """Altimeter Growth (2021): As Reported | As Restated | Difference."""
    html = b"""<html><body><table>
      <tr><td></td><td colspan="2">As of December 31, 2020</td><td colspan="2">As of December 31, 2020</td><td colspan="2">As of December 31, 2020</td></tr>
      <tr><td></td><td colspan="2">As Reported</td><td colspan="2">As Restated</td><td colspan="2">Difference</td></tr>
      <tr><td>Balance Sheet</td></tr>
      <tr><td>Total Liabilities</td><td></td><td>17,564,100</td><td>$</td><td>174,754,111</td><td>$</td><td>157,190,011</td></tr>
    </table></body></html>"""
    (r,) = extract_html(html)
    assert (r.previously_reported, r.adjustment, r.restated) == (
        17_564_100,
        157_190_011,
        174_754_111,
    )
    assert r.period_end == "2020-12-31" and r.check_ok


def test_band_with_two_numbers_is_ambiguous():
    assert parse_number("17,475,411 1,139,625,911") is None
