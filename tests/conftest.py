from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


def pytest_collection_modifyitems(config, items):
    """Skip tests marked ``live`` (SEC EDGAR) or ``llm`` (Anthropic API, costs money) unless
    explicitly selected with ``-m live`` / ``-m llm``."""
    selected = config.getoption("-m") or ""
    reasons = {"live": "hits SEC EDGAR; run with -m live", "llm": "calls Claude; run with -m llm"}
    for marker, reason in reasons.items():
        if marker in selected:
            continue
        for item in items:
            if marker in item.keywords:
                item.add_marker(pytest.mark.skip(reason=reason))


@pytest.fixture(autouse=True)
def _raw_store_in_tmp(tmp_path, monkeypatch):
    """Never let tests write downloaded documents into the user's cache."""
    monkeypatch.setenv("RADAR_RAW_DIR", str(tmp_path / "raw"))
