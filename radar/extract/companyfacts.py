"""Third source of deltas: compare an amendment's tagged facts with the values first
filed for the same periods (design doc §5.3, "silent value change").

Some restatements print only the restated statements, with no "as previously reported"
table and no restatement dimension. The SEC's companyfacts API keeps every undimensioned
fact with the accession that reported it, so for each concept and period the amendment's
value can be compared with the earliest value filed before it.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from radar.normalize.normalize import catalog

USD_UNITS = {"USD", "USD/shares"}


def companyfacts_deltas(facts: dict[str, Any], accession: str, filed: date) -> list[dict[str, Any]]:
    """Rows for every mapped concept whose value in ``accession`` differs from the value
    originally filed for the same period."""
    cat = catalog()
    out = []
    for taxonomy in ("us-gaap", "ifrs-full"):
        for concept, body in facts.get("facts", {}).get(taxonomy, {}).items():
            item = cat.item_for_concept(concept)
            if item is None:
                continue
            for unit, values in body.get("units", {}).items():
                if unit not in USD_UNITS:
                    continue
                by_period: dict[tuple, list[dict]] = {}
                for v in values:
                    by_period.setdefault((v.get("start"), v["end"]), []).append(v)
                for (start, end), vs in by_period.items():
                    mine = [v for v in vs if v["accn"] == accession]
                    cutoff = filed.isoformat()
                    earlier = sorted(
                        (v for v in vs if v["accn"] != accession and v["filed"] < cutoff),
                        key=lambda v: (v["filed"], v["accn"]),
                    )
                    if not mine or not earlier:
                        continue
                    restated = Decimal(str(mine[0]["val"]))
                    original = Decimal(str(earlier[0]["val"]))
                    out.append(
                        {
                            "concept": f"{taxonomy}:{concept}",
                            "label": None,
                            "canonical_item": item.key,
                            "statement": item.statement,
                            "period_start": start,
                            "period_end": end,
                            "dims": {},
                            "unit": unit,
                            "previously_reported": original,
                            "adjustment": restated - original,
                            "restated": restated,
                            "tolerance": Decimal("0.01") if "/" in unit else Decimal(1),
                            "check_ok": None,
                            "error_member": None,
                            "original_accession": earlier[0]["accn"],
                        }
                    )
    return out
