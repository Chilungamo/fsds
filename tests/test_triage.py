import pytest

from radar.detect.triage import triage
from radar.extract.ixbrl import extract, load_document
from radar.parse.item402 import document_text

from .conftest import FIXTURES

F = FIXTURES / "filings"

RESTATEMENTS = [
    "0001193125-26-098732_ulh-20250927.htm.gz",
    "0001628280-24-008911_cutr-20230331.htm.gz",
    "0001628280-26-013286_core-20250331.htm.gz",
    "0001992243-25-000029_t1-20250331.htm.gz",
]
NOT_RESTATEMENTS = [
    "0001193125-23-130742_d492883d10ka.htm.gz",  # Cutera 10-K/A adding Part III
    "0001992243-25-000015_t1energy-20241231.htm.gz",  # T1 Energy 10-K/A, no restatement
]


def run(name, open_case=False):
    raw = load_document(F / name)
    return triage(extract(raw), document_text(raw), has_open_case=open_case)


@pytest.mark.parametrize("name", RESTATEMENTS)
def test_real_restatements_are_confirmed(name):
    r = run(name)
    assert r.decision == "CONFIRMED"
    assert r.signals["restatement_axis"] and r.signals["full_statements"]
    assert run(name, open_case=True).score == pytest.approx(r.score + 0.10)


@pytest.mark.parametrize("name", NOT_RESTATEMENTS)
def test_other_amendments_are_ignored(name):
    r = run(name, open_case=True)  # even with a pending 8-K nearby
    assert r.decision == "IGNORE"
    assert not r.signals["restatement_axis"]


def test_part_iii_amendment_is_recognised():
    r = run(NOT_RESTATEMENTS[0])
    assert r.signals["part_iii_only"]
    assert "Part III" in r.evidence["limited_purpose"]


def test_cover_checkbox_text_is_not_restatement_evidence():
    # the cover page of every 10-K asks about "correction of an error to previously issued
    # financial statements"; that boilerplate must not count
    assert not run(NOT_RESTATEMENTS[1]).signals["restatement_text"]


@pytest.mark.parametrize(
    "note,expected",
    [
        (
            "The Company is filing this Amendment No. 1 to the Quarterly Report on Form 10-Q for "
            "the quarterly period ended September 30, 2024 to restate its unaudited condensed "
            "consolidated interim financial statements as of and for the nine months ended "
            "September 30, 2024.",
            True,
        ),
        (
            "This Amendment amends and restates Part III of the Original Filing in its entirety.",
            False,
        ),
        ("This Amendment is filed to amend and restate Item 9A, Controls and Procedures.", False),
    ],
)
def test_states_restatement_signal(note, expected):
    from radar.detect.triage import _STATES_RESTATEMENT

    assert bool(_STATES_RESTATEMENT.search(note)) is expected
