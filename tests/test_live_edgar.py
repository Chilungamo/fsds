"""Live smoke test against SEC EDGAR. Needs network access and RADAR_SEC_USER_AGENT.

RADAR_SEC_USER_AGENT="RestatementRadar/0.1 you@example.com" pytest -m live
"""

import pytest

from radar.edgar.client import EdgarClient
from radar.extract.ixbrl import extract


@pytest.mark.live
def test_fetch_and_extract_universal_logistics():
    with EdgarClient() as edgar:
        name = edgar.primary_document_name(1308208, "0001193125-26-098732")
        result = extract(edgar.filing_document(1308208, "0001193125-26-098732", name))
    row = result.find("us-gaap:Goodwill", "2025-09-27")
    assert row is not None and row.adjustment == -43_166_000
