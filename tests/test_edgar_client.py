import httpx
import pytest

from radar.edgar import urls
from radar.edgar.client import EdgarClient, EdgarError, TokenBucket

UA = "RestatementRadar/test test@example.com"


def client(handler, **kw):
    return EdgarClient(
        user_agent=UA, rate=1000, transport=httpx.MockTransport(handler), sleep=lambda s: None, **kw
    )


def test_sends_user_agent_and_gzip():
    seen = {}

    def handler(request):
        seen.update(request.headers)
        return httpx.Response(200, json={"ok": True})

    assert client(handler).get_json("https://data.sec.gov/x") == {"ok": True}
    assert seen["user-agent"] == UA
    assert "gzip" in seen["accept-encoding"]


def test_retries_on_429_then_succeeds():
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(429 if len(calls) < 3 else 200, text="ok")

    assert client(handler).get("https://www.sec.gov/x").text == "ok"
    assert len(calls) == 3


def test_gives_up_after_max_retries():
    with pytest.raises(EdgarError, match="HTTP 503"):
        client(lambda r: httpx.Response(503), max_retries=2).get("https://www.sec.gov/x")


def test_404_is_not_retried():
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(404)

    with pytest.raises(httpx.HTTPStatusError):
        client(handler).get("https://www.sec.gov/missing")
    assert len(calls) == 1


def test_missing_user_agent_is_a_config_error(monkeypatch):
    from radar.config import ConfigError

    monkeypatch.delenv("RADAR_SEC_USER_AGENT", raising=False)
    with pytest.raises(ConfigError):
        EdgarClient()


def test_primary_document_lookup():
    def handler(request):
        assert str(request.url) == urls.submissions_url(1308208)
        return httpx.Response(
            200,
            json={
                "filings": {
                    "recent": {
                        "accessionNumber": ["0001193125-26-098732"],
                        "primaryDocument": ["ulh-20250927.htm"],
                    }
                }
            },
        )

    assert client(handler).primary_document_name(1308208, "000119312526098732") == (
        "ulh-20250927.htm"
    )


def test_token_bucket_spaces_requests():
    now = [0.0]
    slept = []

    def sleep(s):
        slept.append(s)
        now[0] += s

    bucket = TokenBucket(rate=5, capacity=1, clock=lambda: now[0], sleep=sleep)
    for _ in range(6):
        bucket.acquire()
    assert now[0] == pytest.approx(1.0)  # 6 requests at 5/s take 1 s after the first


def test_urls():
    assert urls.normalize_accession("000119312526098732") == "0001193125-26-098732"
    assert urls.cik10(1308208) == "0001308208"
    assert urls.filing_document_url(1308208, "0001193125-26-098732", "a.htm") == (
        "https://www.sec.gov/Archives/edgar/data/1308208/000119312526098732/a.htm"
    )
    with pytest.raises(ValueError):
        urls.normalize_accession("12-34")
