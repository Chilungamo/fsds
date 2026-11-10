"""Dashboard API against a real Postgres test database, seeded through the pipeline."""

import json
import os
import shutil
import socket
import subprocess
import threading
import time
from pathlib import Path

import pytest

from radar.db import repo
from radar.pipeline import Pipeline

from .fake_edgar import FakeEdgar
from .test_pipeline_db import TEST_DB, conn, ref  # noqa: F401  (conn is a fixture)

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from radar.api.app import create_app  # noqa: E402

WEB_DIST = Path(__file__).resolve().parent.parent / "radar" / "web" / "dist"


@pytest.fixture
def seeded(conn):  # noqa: F811
    """ULH: Item 4.02 8-K + restated 10-Q/A (confirmed, enriched); T1: 8-K only (pending)."""
    edgar = FakeEdgar(not_yet_filed={"0001992243-25-000029"}).client()
    repo.insert_filing(conn, ref("0001193125-26-098600"))
    repo.insert_filing(conn, ref("0001213900-25-077935"))
    Pipeline(conn, edgar).drain()
    conn.execute("UPDATE company SET tickers = '{ULH}' WHERE cik = 1308208")
    return conn


@pytest.fixture
def client(seeded):
    with TestClient(create_app(TEST_DB, web_dist=None)) as c:
        yield c


def ulh_id(client):
    return next(c["id"] for c in client.get("/api/cases").json()["items"] if c["cik"] == 1308208)


def test_health(client):
    assert client.get("/api/health").json() == {"ok": True}


def test_case_list_filters_and_search(client):
    body = client.get("/api/cases").json()
    assert body["total"] == 2
    statuses = {c["cik"]: c["status"] for c in body["items"]}
    assert statuses == {1308208: "CONFIRMED", 1992243: "PENDING"}
    assert [c["cik"] for c in client.get("/api/cases?status=PENDING").json()["items"]] == [1992243]
    assert [c["cik"] for c in client.get("/api/cases?q=ulh").json()["items"]] == [1308208]  # ticker
    assert client.get("/api/cases?q=1992243").json()["items"][0]["cik"] == 1992243  # CIK
    ulh = next(c for c in body["items"] if c["cik"] == 1308208)
    assert ulh["tickers"] == ["ULH"] and ulh["severity"] is not None
    assert {"item": "net_income"}.items() <= ulh["headline"][0].items() or ulh["headline"]


def test_case_detail_has_filings_with_sec_links(client):
    c = client.get(f"/api/cases/{ulh_id(client)}").json()
    assert [f["role"] for f in c["filings"]] == ["warning", "amendment"]
    assert c["filings"][1]["url"] == (
        "https://www.sec.gov/Archives/edgar/data/1308208/000119312526098732/ulh-20250927.htm"
    )
    assert c["filings"][0]["item402_text"].startswith("Non-Reliance")
    assert client.get("/api/cases/999999").status_code == 404


def test_deltas_grouped_by_period(client):
    periods = client.get(f"/api/cases/{ulh_id(client)}/deltas").json()
    labels = [p["label"] for p in periods]
    assert "Quarter ended 2025-09-27" in labels and "As of 2025-09-27" in labels
    q3 = next(p for p in periods if p["label"] == "Quarter ended 2025-09-27")
    ni = next(r for r in q3["rows"] if r["canonical_item"] == "net_income")
    assert (ni["previously_reported"], ni["adjustment"], ni["restated"]) == (
        -74772000,
        -43166000,
        -117938000,
    )
    assert ni["name"] == "Net income" and ni["source"] == "xbrl"
    assert q3["rows"][0]["statement"] == "IS"  # income statement first


def test_graph_and_checks(client):
    cid = ulh_id(client)
    g = client.get(f"/api/cases/{cid}/graph").json()
    nodes = {n["id"]: n for n in g["nodes"]}
    assert nodes["goodwill"]["measured"] and nodes["goodwill"]["delta"] == -43166000
    assert {"src_item": "impairment", "dst_item": "goodwill"}.items() <= next(
        e for e in g["edges"] if e["dst_item"] == "goodwill"
    ).items()
    assert g["period"]["end"] == "2025-09-27" and len(g["periods"]) >= 2
    other = client.get(
        f"/api/cases/{cid}/graph", params={"start": "2025-06-29", "end": "2025-09-27"}
    ).json()
    assert other["period"]["start"] == "2025-06-29"
    checks = client.get(f"/api/cases/{cid}/checks").json()
    assert checks and {c["status"] for c in checks} == {"pass"}


def test_stats_and_company(client):
    s = client.get("/api/stats").json()
    assert s["cases"] == {"CONFIRMED": 1, "PENDING": 1}
    assert {"year": 2026, "n": 1} in s["warnings_by_year"]
    assert s["median_days_warning_to_amendment"] == 0
    c = client.get("/api/companies/1308208").json()
    assert len(c["cases"]) == 1
    assert client.get("/api/companies/1").status_code == 404


def test_stream_replays_events_after_last_event_id(client, seeded):
    first = seeded.execute("SELECT min(id) AS id FROM event_log").fetchone()["id"]
    expected = [
        r["type"]
        for r in seeded.execute("SELECT type FROM event_log WHERE id > %s ORDER BY id", (first,))
    ]
    got = []
    with client.stream(
        "GET", "/api/stream", params={"max_seconds": 1}, headers={"Last-Event-ID": str(first)}
    ) as r:
        assert r.headers["content-type"].startswith("text/event-stream")
        for line in r.iter_lines():
            if line.startswith("event: "):
                got.append(line[7:])
            if len(got) == len(expected):
                break
    assert got == expected


# --- end to end: browser + live stream ------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.skipif(
    not (WEB_DIST / "index.html").exists(), reason="web app not built (npm run build)"
)
@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_browser_receives_live_alert(seeded):
    import uvicorn

    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(create_app(TEST_DB), host="127.0.0.1", port=port, log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    while not server.started:
        time.sleep(0.05)
    try:
        script = Path(__file__).with_name("e2e_live.cjs")
        env = {
            **os.environ,
            "NODE_PATH": subprocess.run(
                ["npm", "root", "-g"], capture_output=True, text=True
            ).stdout.strip(),
        }
        proc = subprocess.Popen(
            ["node", str(script), f"http://127.0.0.1:{port}", "Company 1839341"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
        )
        # wait until the page is open and subscribed, then a new Item 4.02 filing arrives
        assert proc.stdout.readline().strip() == "ready"
        edgar = FakeEdgar().client()
        repo.insert_filing(seeded, ref("0001628280-26-013214"))  # Core Scientific 8-K
        Pipeline(seeded, edgar, enrich=False).drain(max_filings=1)
        out, err = proc.communicate(timeout=60)
        result = json.loads(out.strip().splitlines()[-1])
        assert result["appeared"], (result, err)
        assert result["errors"] == []
        assert "Company 1839341" in result["firstCard"]  # fixture companies are named by CIK
    finally:
        server.should_exit = True
        thread.join(timeout=10)
