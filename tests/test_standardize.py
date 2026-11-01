import pandas as pd
import pytest

from fsds.facts import facts_to_frame
from fsds.standardize import (annual_durations, coalesce_concepts, fiscal_year, point_in_time,
                              standardized_items)
from tests.fixtures import FactsBuilder


def _rev_facts():
    b = FactsBuilder(1)
    # FY2022 first reported in the FY2022 10-K ...
    b.add("Revenues", 100, "2022-12-31", "2022-01-01", accn="a22", filed="2023-02-15", fy=2022)
    # ... and restated as a comparative in the FY2023 10-K
    b.add("Revenues", 110, "2022-12-31", "2022-01-01", accn="a23", filed="2024-02-15", fy=2023)
    # FY2023 after ASC 606 uses a different tag
    b.add("RevenueFromContractWithCustomerExcludingAssessedTax", 130, "2023-12-31", "2023-01-01",
          accn="a23", filed="2024-02-15", fy=2023)
    # a quarter inside the 10-K and a 10-Q value must both be ignored
    b.add("Revenues", 35, "2023-12-31", "2023-10-01", accn="a23", filed="2024-02-15", fy=2023, fp="FY")
    b.add("Revenues", 999, "2023-09-30", "2023-01-01", accn="q3", form="10-Q", filed="2023-11-01")
    return facts_to_frame(b.build())


def test_facts_to_frame_shape_and_types():
    df = _rev_facts()
    assert len(df) == 5
    assert df["cik"].iloc[0] == "0000000001"
    assert pd.api.types.is_datetime64_any_dtype(df["end"])
    assert df.loc[df["concept"] == "Revenues", "start"].notna().all()


@pytest.mark.parametrize("policy,expected", [("first", 100), ("latest", 110)])
def test_point_in_time_policy(policy, expected):
    df = point_in_time(_rev_facts(), policy=policy)
    fy22 = df[(df["concept"] == "Revenues") & (df["end"] == "2022-12-31")]
    assert fy22["val"].tolist() == [expected]


def test_annual_durations_drops_quarters_and_keeps_52_53_weeks():
    df = _rev_facts()
    b = FactsBuilder(2)
    b.add("Revenues", 1, "2023-09-30", "2022-09-25")  # 53-week year (371 days)
    df = pd.concat([df, facts_to_frame(b.build())])
    kept = annual_durations(df)
    assert set(kept["val"]) == {100, 110, 130, 1}  # quarter (35) and 9-month YTD (999) dropped


def test_coalesce_prefers_chain_order_and_records_tag():
    b = FactsBuilder(1)
    b.add("Revenues", 50, "2023-12-31", "2023-01-01")
    b.add("RevenueFromContractWithCustomerExcludingAssessedTax", 49, "2023-12-31", "2023-01-01")
    b.add("RevenueFromContractWithCustomerExcludingAssessedTax", 40, "2022-12-31", "2022-01-01")
    out = coalesce_concepts(facts_to_frame(b.build()),
                            ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax"], "rev")
    out = out.set_index(out["end"].dt.year)
    assert out.loc[2023, "rev"] == 50 and out.loc[2023, "rev_tag"] == "Revenues"
    assert out.loc[2022, "rev"] == 40
    assert out.loc[2022, "rev_tag"] == "RevenueFromContractWithCustomerExcludingAssessedTax"


def test_standardized_items_end_to_end():
    items = standardized_items(_rev_facts(), ["revenue"], policy="first")
    rev = items["revenue"].set_index(items["revenue"]["end"].dt.year)["revenue"]
    assert rev.to_dict() == {2022: 100, 2023: 130}  # 10-Q and quarter excluded, ASC 606 tag picked up


def test_fiscal_year_convention():
    ends = pd.to_datetime(pd.Series(["2023-12-31", "2023-06-30", "2023-05-31", "2024-01-28"]))
    assert fiscal_year(ends).tolist() == [2023, 2023, 2022, 2023]
