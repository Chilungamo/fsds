from datetime import date
from urllib.parse import parse_qs, urlparse

import httpx

from radar.edgar import fulltext
from radar.edgar.client import EdgarClient


def hit(acc, doc, form, items=("4.02",), file_type=None):
    return {
        "_id": f"{acc}:{doc}",
        "_source": {
            "ciks": ["0001308208"],
            "form": form,
            "file_type": file_type or form,
            "display_names": ["UNIVERSAL LOGISTICS HOLDINGS, INC.  (ULH)  (CIK 0001308208)"],
            "file_date": "2026-03-09",
            "items": list(items),
        },
    }


def test_queries_each_form_separately_pages_and_dedupes():
    seen_forms = []

    def handler(request):
        q = parse_qs(urlparse(str(request.url)).query)
        seen_forms.append(q["forms"][0])
        start = int(q["from"][0])
        if q["forms"][0] == "8-K":
            hits = [
                hit(f"0001193125-26-{n:06d}", "a.htm", "8-K") for n in range(start, start + 100)
            ]
            hits = hits if start < 200 else []
            total = 150
            hits = hits[: max(0, total - start)]
        else:  # the same 8-K/A as the 8-K query already returned, plus its exhibit
            hits = [
                hit("0001193125-26-000001", "a.htm", "8-K"),
                hit("0001193125-26-000001", "ex99.htm", "8-K", file_type="EX-99.1"),
            ]
            total = 2
        return httpx.Response(200, json={"hits": {"total": {"value": total}, "hits": hits}})

    edgar = EdgarClient(
        user_agent="t t@example.com", transport=httpx.MockTransport(handler), sleep=lambda s: None
    )
    hits = list(
        fulltext.search(edgar, '"Item 4.02"', ["8-K", "8-K/A"], date(2026, 1, 1), date(2026, 3, 31))
    )
    assert seen_forms == ["8-K", "8-K", "8-K/A"]  # two pages, then the second form
    assert len(hits) == 151  # 150 + the exhibit document; the duplicate is dropped
    first = hits[0]
    assert first.company == "UNIVERSAL LOGISTICS HOLDINGS, INC." and first.cik == 1308208
    assert first.items == ["4.02"] and first.is_primary
    assert not hits[-1].is_primary
