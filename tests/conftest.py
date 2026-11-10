from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


def pytest_collection_modifyitems(config, items):
    """Skip tests marked ``live`` unless explicitly selected with ``-m live``."""
    if "live" in (config.getoption("-m") or ""):
        return
    skip = pytest.mark.skip(reason="hits SEC EDGAR; run with -m live")
    for item in items:
        if "live" in item.keywords:
            item.add_marker(skip)
