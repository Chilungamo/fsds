"""End-to-end detection tests against a real Postgres database and a fake EDGAR.

Needs a database the tests may wipe: RADAR_TEST_DATABASE_URL
(default postgresql://radar:radar@localhost:5432/radar_test). Skipped if unreachable.
"""

import json
import os
import threading
from datetime import date

import psycopg
import pytest
from psycopg.rows import dict_row

from radar.db import repo
from radar.db.migrate import migrate
from radar.edgar.submissions import SubmissionsCache
from radar.models import FilingRef
from radar.pipeline import Pipeline
from radar.workers import watch
from radar.workers.reconcile import reconcile_day

from .fake_edgar import FILINGS, FakeEdgar, FakeFiling, by_accession, feed

TEST_DB = os.environ.get(
    "RADAR_TEST_DATABASE_URL", "postgresql://radar:radar@localhost:5432/radar_test"
)


@pytest.fixture
def conn():
    try:
        c = psycopg.connect(TEST_DB, row_factory=dict_row, autocommit=True, connect_timeout=3)
    except psycopg.OperationalError as exc:
        pytest.skip(f"test database unavailable: {exc}")
    c.execute("DROP SCHEMA public CASCADE")
    c.execute("CREATE SCHEMA public")
    migrate(c)
    yield c
    c.close()


def ref(acc: str, source="manual", with_items=True) -> FilingRef:
    f = by_accession(acc)
    return FilingRef(
        accession=f.accession,
        cik=f.cik,
        form=f.form,
        company=f"Company {f.cik}",
        filed_date=date.fromisoformat(f.filed),
        items=(f.items.split(",") if f.items else []) if with_items and f.form == "8-K" else None,
        source=source,
    )


def events(conn):
    return [
        (r["type"], r["accession"])
        for r in conn.execute("SELECT type, accession FROM event_log ORDER BY id")
    ]


def case_rows(conn):
    return conn.execute("SELECT * FROM restatement_case ORDER BY id").fetchall()


def links(conn, case_id):
    return {
        r["accession"]: r["role"]
        for r in conn.execute("SELECT * FROM case_filing WHERE case_id = %s", (case_id,))
    }


def periods(conn, case_id, source):
    return [
        (r["period_end"].isoformat(), r["kind"])
        for r in conn.execute(
            "SELECT * FROM affected_period WHERE case_id = %s AND source = %s ORDER BY 1, 2",
            (case_id, source),
        )
    ]


def test_migrate_is_idempotent(conn):
    assert migrate(conn) == []


def test_live_feed_8k_then_amendment(conn):
    """The watcher sees ULH's Item 4.02 8-K (amber alert), then its 10-Q/A (confirmed)."""
    noise = FakeFiling(1, "0000000001-26-000001", "8-K", "2026-03-09", "x.htm", "2.02,9.01")
    fake = FakeEdgar(
        feeds={"8-K": feed(by_accession("0001193125-26-098600"), noise)},
        not_yet_filed={"0001193125-26-098732"},  # the 10-Q/A comes 21 minutes later
    )
    edgar = fake.client()

    polled = watch.poll_once(conn, edgar)
    assert [r.accession for r in polled.new_non_reliance] == ["0001193125-26-098600"]
    out = {o.accession: o for o in Pipeline(conn, edgar).drain()}
    assert out["0000000001-26-000001"].state == "IGNORED"
    assert out["0001193125-26-098600"].state == "DONE"

    (case,) = case_rows(conn)
    assert (case["status"], case["type"], case["sub_item"]) == ("PENDING", "BIG_R", "a")
    assert periods(conn, case["id"], "item402") == [("2025-09-27", "interim")]
    assert case["summary"]["amendment_promised"] is True

    # the feed carried the items, so only the Item 4.02 filer was looked up (for its
    # primary document); the other 8-K cost no requests at all
    looked_up = {u.rsplit("CIK", 1)[1] for u in fake.requests if "submissions" in u}
    assert looked_up == {"0001308208.json"}

    # the amendment arrives in a later poll
    fake.not_yet_filed.clear()
    fake.feeds = {"10-Q/A": feed(by_accession("0001193125-26-098732"))}
    watch.poll_once(conn, edgar)
    Pipeline(conn, edgar).drain()
    (case,) = case_rows(conn)
    assert case["status"] == "CONFIRMED"
    assert links(conn, case["id"]) == {
        "0001193125-26-098600": "warning",
        "0001193125-26-098732": "amendment",
    }
    assert ("2025-09-27", "interim") in periods(conn, case["id"], "xbrl")
    assert events(conn) == [
        ("case.created", "0001193125-26-098600"),
        ("case.confirmed", "0001193125-26-098732"),
    ]
    meta = conn.execute(
        "SELECT meta FROM filing WHERE accession = '0001193125-26-098732'"
    ).fetchone()["meta"]
    assert meta["triage"]["decision"] == "CONFIRMED"
    assert meta["top_adjustments"][0]["adjustment"] == "-43166000"


def test_backfill_order_amendment_before_8k(conn):
    """Backfill may process Core Scientific's 10-Q/A before its 8-K; they still join up."""
    edgar = FakeEdgar().client()
    repo.insert_filing(conn, ref("0001628280-26-013286", "backfill"))
    Pipeline(conn, edgar).drain()
    (case,) = case_rows(conn)
    assert (case["status"], case["type"]) == ("CONFIRMED", "BIG_R")

    repo.insert_filing(conn, ref("0001628280-26-013214", "backfill"))
    Pipeline(conn, edgar).drain()
    (case,) = case_rows(conn)  # still one case
    assert case["status"] == "CONFIRMED" and case["sub_item"] == "a"
    assert set(links(conn, case["id"]).values()) == {"warning", "amendment"}
    assert len(periods(conn, case["id"], "item402")) == 6
    assert [e for e, _ in events(conn)] == ["case.created", "case.updated"]


def test_cases_are_kept_apart_by_company_and_time(conn):
    edgar = FakeEdgar().client()
    for acc in [f.accession for f in FILINGS]:
        repo.insert_filing(conn, ref(acc, "backfill"))
    out = Pipeline(conn, edgar).drain()
    assert {o.state for o in out} <= {"DONE", "IGNORED"}
    cases = case_rows(conn)
    assert len(cases) == 4 and {c["status"] for c in cases} == {"CONFIRMED"}
    by_cik = {c["cik"]: links(conn, c["id"]) for c in cases}
    assert set(by_cik) == {1308208, 1839341, 1162461, 1992243}
    # Cutera's Part III 10-K/A (May 2023) is not part of its restatement case
    assert "0001193125-23-130742" not in by_cik[1162461]
    state = conn.execute(
        "SELECT state, meta FROM filing WHERE accession = '0001193125-23-130742'"
    ).fetchone()
    assert state["state"] == "IGNORED" and state["meta"]["triage"]["signals"]["part_iii_only"]


def test_daily_index_reconcile_fills_items_from_submissions(conn):
    idx = (
        "Form Type   Company Name   CIK   Date Filed  File Name\n"
        "8-K              T1 Energy Inc.                                    1992243     20250818"
        "    edgar/data/1992243/0001213900-25-077935.txt\n"
        "10-Q/A           T1 Energy Inc.                                    1992243     20250818"
        "    edgar/data/1992243/0001992243-25-000029.txt\n"
    )
    fake = FakeEdgar(daily_index={"20250818": idx})
    edgar = fake.client()
    repo.insert_filing(conn, ref("0001992243-25-000029", "feed"))  # watcher saw this one
    r = reconcile_day(conn, edgar, SubmissionsCache(edgar), date(2025, 8, 18))
    assert (r.listed, r.missing, r.non_reliance) == (2, 1, ["0001213900-25-077935"])
    row = conn.execute("SELECT * FROM filing WHERE accession = '0001213900-25-077935'").fetchone()
    assert row["items"] == ["4.02", "9.01"] and row["source"] == "daily_index"
    assert row["primary_doc"] == "ea0253661-8k_t1energy.htm"
    assert reconcile_day(conn, edgar, SubmissionsCache(edgar), date(2025, 8, 17)).available is False


def test_8k_without_items_is_looked_up(conn):
    edgar = FakeEdgar(not_yet_filed={"0001992243-25-000029"}).client()
    repo.insert_filing(conn, ref("0001213900-25-077935", with_items=False))
    (o,) = Pipeline(conn, edgar).drain()
    assert o.state == "DONE" and case_rows(conn)[0]["status"] == "PENDING"


def test_failures_retry_then_fail(conn):
    fake = FakeEdgar(broken_docs={"0001193125-26-098600"})
    edgar = fake.client()
    repo.insert_filing(conn, ref("0001193125-26-098600"))
    pipeline = Pipeline(conn, edgar)
    states = []
    for _ in range(3):
        states += [o.state for o in pipeline.drain()]
        assert pipeline.drain() == []  # backoff: not retried straight away
        conn.execute("UPDATE filing SET not_before = now()")  # let time pass
    assert states == ["RETRY", "RETRY", "FAILED"]
    assert pipeline.drain() == []  # FAILED filings are not claimed again
    assert case_rows(conn) == []
    row = conn.execute("SELECT * FROM filing").fetchone()
    assert "500" in row["last_error"]


def test_reprocessing_is_idempotent(conn):
    edgar = FakeEdgar().client()
    repo.insert_filing(conn, ref("0001193125-26-098600"))
    Pipeline(conn, edgar).drain()
    before = conn.execute("SELECT * FROM case_filing ORDER BY accession").fetchall()
    conn.execute("UPDATE filing SET state = 'NEW'")
    Pipeline(conn, edgar).drain()
    assert len(case_rows(conn)) == 1
    assert conn.execute("SELECT * FROM case_filing ORDER BY accession").fetchall() == before
    assert len(before) == 2  # the 8-K and the 10-Q/A it queued


def test_insert_filing_merges_sources(conn):
    r = ref("0001213900-25-077935", "daily_index", with_items=False)
    assert repo.insert_filing(conn, r) is True
    r2 = ref("0001213900-25-077935", "feed")
    r2.primary_doc = "ea0253661-8k_t1energy.htm"
    assert repo.insert_filing(conn, r2) is False
    row = conn.execute("SELECT * FROM filing").fetchone()
    assert row["source"] == "daily_index"  # first discovery is kept
    assert row["items"] == ["4.02", "9.01"] and row["primary_doc"]  # gaps filled in


def test_concurrent_workers_claim_disjoint_filings(conn):
    for f in FILINGS:
        repo.insert_filing(conn, ref(f.accession))
    claimed: list[list[str]] = []
    barrier = threading.Barrier(3)

    def worker():
        with psycopg.connect(TEST_DB, row_factory=dict_row, autocommit=True) as c:
            barrier.wait()
            claimed.append([r["accession"] for r in repo.claim_filings(c, limit=3)])

    threads = [threading.Thread(target=worker) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    flat = [a for batch in claimed for a in batch]
    assert len(flat) == 9 and len(set(flat)) == 9


def test_warning_queues_the_companys_later_amendments(conn):
    """Processing T1's 8-K queues its 10-Q/A from submissions, even though no feed, index
    or search ever reported it, and re-triages an amendment that was ignored earlier."""
    edgar = FakeEdgar().client()
    repo.insert_filing(conn, ref("0001213900-25-077935", "backfill"))
    (o,) = Pipeline(conn, edgar).drain(max_filings=1)
    assert "1 amendments queued" in o.detail
    queued = conn.execute(
        "SELECT source, state FROM filing WHERE accession = '0001992243-25-000029'"
    ).fetchone()
    assert queued == {"source": "followup", "state": "NEW"}
    Pipeline(conn, edgar).drain()
    (case,) = case_rows(conn)
    assert case["status"] == "CONFIRMED"
    assert set(links(conn, case["id"]).values()) == {"warning", "amendment"}


def test_ignored_amendment_is_retriaged_after_warning(conn):
    edgar = FakeEdgar().client()
    repo.insert_filing(conn, ref("0001992243-25-000029"))
    conn.execute("UPDATE filing SET state = 'IGNORED'")  # e.g. triaged before the 8-K existed
    repo.insert_filing(conn, ref("0001213900-25-077935"))
    Pipeline(conn, edgar).drain()
    assert case_rows(conn)[0]["status"] == "CONFIRMED"


def test_live_filings_jump_the_backfill_queue(conn):
    repo.insert_filing(conn, ref("0001628280-23-042507", "backfill"))
    repo.insert_filing(conn, ref("0001213900-25-077935", "daily_index"))
    repo.insert_filing(conn, ref("0001193125-26-098600", "feed"))
    order = [repo.claim_filings(conn, limit=1)[0]["source"] for _ in range(3)]
    assert order == ["feed", "daily_index", "backfill"]


def test_events_are_notified(conn):
    edgar = FakeEdgar().client()
    with psycopg.connect(TEST_DB, autocommit=True) as listener:
        listener.execute(f"LISTEN {repo.EVENTS_CHANNEL}")
        repo.insert_filing(conn, ref("0001213900-25-077935"))
        Pipeline(conn, edgar).drain()
        note = next(listener.notifies(timeout=5))
    payload = json.loads(note.payload)
    assert payload["type"] == "case.created"
    assert payload["accession"] == "0001213900-25-077935"
