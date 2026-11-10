"""M0 exit criterion: extracted deltas match the restatement tables printed in real filings.

Expected values in ``fixtures/expected`` were copied by hand from the HTML tables of each
filing, independently of the XBRL tags the extractor reads.
"""

import json
from decimal import Decimal

import pytest

from radar.extract.ixbrl import extract_file

from .conftest import FIXTURES

EXPECTED = sorted((FIXTURES / "expected").glob("*.json"))
_cache: dict = {}


def _load(path):
    spec = json.loads(path.read_text())
    if path not in _cache:
        _cache[path] = extract_file(FIXTURES / "filings" / spec["file"])
    return spec, _cache[path]


def _cases():
    for path in EXPECTED:
        spec = json.loads(path.read_text())
        for row in spec["rows"]:
            yield pytest.param(path, row, id=f"{spec['company'].split()[0]}-{row['label']}")


def test_fixture_set_is_present():
    assert len(EXPECTED) >= 3


@pytest.mark.parametrize("path", EXPECTED, ids=lambda p: p.stem)
def test_cover_page(path):
    spec, result = _load(path)
    assert int(result.cover["EntityCentralIndexKey"]) == spec["cik"]
    assert result.cover["DocumentType"] == spec["form"]
    assert result.cover["AmendmentFlag"] == "true"
    assert result.has_restatement_dimension


@pytest.mark.parametrize("path,expected", list(_cases()))
def test_row_matches_html_table(path, expected):
    _, result = _load(path)
    row = result.find(
        expected["concept"], expected["period_end"], expected["period_start"], **expected["dims"]
    )
    assert row is not None, f"no extracted row for {expected['label']}"
    scale = Decimal(expected["scale"])
    for col in ("previously_reported", "adjustment", "restated"):
        want = Decimal(str(expected[col])) * scale
        assert getattr(row, col) == want, f"{expected['label']} {col}"
    assert row.check_ok is expected["expected_check_ok"]
    assert not row.conflicts


@pytest.mark.parametrize("path", EXPECTED, ids=lambda p: p.stem)
def test_face_statement_rows_are_internally_consistent(path):
    """Every undimensioned row satisfies previously reported + adjustment = restated,
    except rows the fixture documents as filer errors."""
    spec, result = _load(path)
    allowed = {r["concept"] for r in spec["rows"] if r["expected_check_ok"] is False}
    bad = [
        r.concept
        for r in result.rows
        if not r.dims
        and r.check_ok is False
        and r.concept not in allowed
        and "WeightedAverage" not in r.concept
    ]
    assert bad == []
