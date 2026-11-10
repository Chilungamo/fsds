from datetime import date
from decimal import Decimal

from radar.extract.companyfacts import companyfacts_deltas


def fact(val, accn, filed, end="2024-09-30", start="2024-07-01", form="10-Q"):
    return {"val": val, "accn": accn, "filed": filed, "end": end, "start": start, "form": form}


FACTS = {
    "facts": {
        "us-gaap": {
            "NetIncomeLoss": {
                "units": {
                    "USD": [
                        fact(1000, "0000000001-24-000010", "2024-11-01"),  # original 10-Q
                        fact(
                            1000, "0000000001-25-000002", "2025-02-20", form="10-K"
                        ),  # comparative
                        fact(800, "0000000001-25-000005", "2025-03-14", form="10-Q/A"),  # restated
                        fact(
                            500,
                            "0000000001-25-000005",
                            "2025-03-14",
                            start="2023-07-01",
                            end="2023-09-30",
                        ),
                    ]
                }
            },
            "Assets": {
                "units": {"USD": [fact(9, "0000000001-25-000005", "2025-03-14", start=None)]}
            },
            "SomethingUnmapped": {
                "units": {"USD": [fact(1, "0000000001-24-000010", "2024-11-01")]}
            },
        }
    }
}


def test_amendment_value_is_compared_with_the_first_filed_value():
    rows = companyfacts_deltas(FACTS, "0000000001-25-000005", date(2025, 3, 14))
    (ni,) = [r for r in rows if r["canonical_item"] == "net_income"]
    assert (ni["previously_reported"], ni["adjustment"], ni["restated"]) == (
        Decimal(1000),
        Decimal(-200),
        Decimal(800),
    )
    assert ni["original_accession"] == "0000000001-24-000010"
    assert (ni["period_start"], ni["period_end"]) == ("2024-07-01", "2024-09-30")


def test_periods_without_an_earlier_value_are_skipped():
    rows = companyfacts_deltas(FACTS, "0000000001-25-000005", date(2025, 3, 14))
    assert {r["period_end"] for r in rows} == {"2024-09-30"}  # 2023 and Assets have no original
