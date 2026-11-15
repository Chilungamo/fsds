"""M4 step 5: explanations produced by the pipeline, stored, re-used and served by the API."""

import pytest

from radar.db import repo
from radar.explain.service import explain_case
from radar.pipeline import Pipeline

from .fake_edgar import FakeEdgar
from .test_narrative import GOOD, FakeClient
from .test_pipeline_db import conn, ref  # noqa: F401  (conn is a fixture)


@pytest.fixture
def cases(conn):  # noqa: F811
    """ULH: Item 4.02 8-K + restated 10-Q/A (confirmed); T1: 8-K only (pending)."""
    edgar = FakeEdgar(not_yet_filed={"0001992243-25-000029"}).client()
    repo.insert_filing(conn, ref("0001193125-26-098600"))
    repo.insert_filing(conn, ref("0001213900-25-077935"))
    Pipeline(conn, edgar).drain()
    ids = {r["cik"]: r["id"] for r in conn.execute("SELECT id, cik FROM restatement_case")}
    return conn, ids[1308208], ids[1992243]


def row(db, case_id):
    return db.execute("SELECT * FROM case_explanation WHERE case_id = %s", (case_id,)).fetchone()


def test_pipeline_explains_every_case_with_the_template(cases):
    db, ulh, t1 = cases
    e = row(db, ulh)
    assert e["method"] == "template" and e["llm_reason"] == "not requested"
    assert e["headline"] == "Impairment or asset valuation lowers net income by $43.2 million"
    assert e["sentences"][0] == (
        "Company 1308208 said on March 9, 2026 that its financial statements for the period "
        "ended September 27, 2025 should no longer be relied upon."
    )  # the company's name is not mistaken for a figure
    assert e["validation"]["template_problems_dropped"] == []
    assert e["validation"]["figures_checked"] >= 8 and e["validation"]["dates_checked"] >= 3
    assert [c["code"] for c in e["causes"]] == ["IMPAIR"]
    assert {f["field"] for f in e["facts"]} >= {"previously_reported", "restated", "adjustment"}

    p = row(db, t1)  # pending: no numbers yet, the cause still comes from the 8-K
    assert [c["code"] for c in p["causes"]] == ["REV"]
    assert "Restated figures have not been filed yet" in " ".join(p["sentences"])
    assert p["facts"] == []
    events = [r["type"] for r in db.execute("SELECT type FROM event_log ORDER BY id")]
    assert events.count("case.explained") == 3  # T1 once; ULH on warning and on amendment


def test_unchanged_inputs_are_not_rewritten(cases):
    db, ulh, _ = cases
    before = row(db, ulh)["updated_at"]
    assert explain_case(db, ulh)["skipped"]
    assert row(db, ulh)["updated_at"] == before


def test_claude_rewrite_is_stored_when_it_validates(cases):
    db, ulh, _ = cases
    r = explain_case(db, ulh, llm=True, client=FakeClient(GOOD))
    assert r["method"] == "llm" and r["reason"] == "ok"
    e = row(db, ulh)
    assert e["method"] == "llm" and e["headline"] == GOOD["headline"]
    assert e["model"] and e["usage"] == {"input_tokens": 1200, "output_tokens": 180}
    assert e["template"]["headline"].startswith("Impairment or asset valuation")  # kept
    # same inputs again: no second API call
    client = FakeClient(GOOD)
    assert explain_case(db, ulh, llm=True, client=client)["skipped"] and client.calls == []


def test_rejected_rewrite_falls_back_and_is_not_retried(cases):
    db, ulh, _ = cases
    bad = {**GOOD, "sentences": ["The loss widens to $106.3 million."]}
    assert explain_case(db, ulh, llm=True, client=FakeClient(bad))["method"] == "template"
    e = row(db, ulh)
    assert e["method"] == "template" and e["llm_reason"] == "failed validation"
    assert "$106.3" not in " ".join(e["sentences"])
    client = FakeClient(GOOD)
    assert explain_case(db, ulh, llm=True, client=client)["skipped"] and client.calls == []


def test_api_error_is_retried_next_time(cases):
    db, ulh, _ = cases
    explain_case(db, ulh, llm=True, client=FakeClient(error=TimeoutError()))
    assert row(db, ulh)["llm_reason"] == "API error: TimeoutError"
    assert explain_case(db, ulh, llm=True, client=FakeClient(GOOD))["method"] == "llm"


def test_rewrite_with_a_date_not_in_the_filings_is_rejected(cases):
    db, ulh, _ = cases
    bad = {**GOOD, "sentences": [*GOOD["sentences"], "It was first flagged on July 4, 2019."]}
    explain_case(db, ulh, llm=True, client=FakeClient(bad))
    assert row(db, ulh)["llm_reason"] == "failed validation"


def test_api_serves_the_explanation(cases):
    from fastapi.testclient import TestClient

    from radar.api.app import create_app

    from .test_pipeline_db import TEST_DB

    db, ulh, t1 = cases
    with TestClient(create_app(TEST_DB, web_dist=None)) as c:
        e = c.get(f"/api/cases/{ulh}/explanation").json()
        assert e["method"] == "template" and len(e["sentences"]) >= 4
        assert c.get(f"/api/cases/{ulh}").json()["explanation"]["headline"] == e["headline"]
        heads = {x["id"]: x["explanation_headline"] for x in c.get("/api/cases").json()["items"]}
        assert heads[ulh] == e["headline"] and heads[t1]
        assert c.get("/api/cases/999999/explanation").status_code == 404
