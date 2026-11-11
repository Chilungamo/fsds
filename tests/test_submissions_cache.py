import resource

import httpx

from radar.edgar.client import EdgarClient
from radar.edgar.submissions import SubmissionsCache


def big_company(n=1000):
    return {
        "filings": {
            "recent": {
                "accessionNumber": [f"0000000001-26-{i:06d}" for i in range(n)],
                "form": ["8-K"] * n,
                "filingDate": ["2026-11-02"] * n,
                "items": ["2.02,9.01"] * n,
                "primaryDocument": ["doc.htm"] * n,
                "acceptanceDateTime": ["2026-11-02T21:00:00.000Z"] * n,
            }
        }
    }


def client(calls):
    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(200, json=big_company())

    return EdgarClient(
        user_agent="t t@example.com",
        rate=10_000,
        transport=httpx.MockTransport(handler),
        sleep=lambda s: None,
    )


def test_cache_is_bounded_and_lru():
    calls = []
    cache = SubmissionsCache(client(calls), max_companies=3)
    for cik in (1, 2, 3):
        cache.company(cik)
    cache.company(1)  # touch 1: now 2 is the least recently used
    cache.company(4)
    assert len(cache) == 3
    assert len(calls) == 4
    cache.company(1)  # still cached
    assert len(calls) == 4
    cache.company(2)  # was evicted: fetched again
    assert len(calls) == 5


def test_memory_stays_flat_across_many_companies():
    """2,000 busy filers (1,000 filings each) through a default-size cache."""
    cache = SubmissionsCache(client([]))
    for cik in range(200):  # warm up to the cap
        cache.company(cik)
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    for cik in range(200, 2200):
        cache.company(cik)
    grown_mb = (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - before) / 1024
    assert len(cache) == 128
    assert grown_mb < 40, f"peak memory grew {grown_mb:.0f} MB"


def test_lookup_finds_items():
    cache = SubmissionsCache(client([]))
    info = cache.lookup(7, "0000000001-26-000005")
    assert info.items == ["2.02", "9.01"] and info.accepted_at.year == 2026
