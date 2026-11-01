import numpy as np
import pandas as pd
import pytest

from tests.fixtures import FactsBuilder, annual_report
from theories.modified_jones.data import apply_sample_filters, build_panel, company_items, concat_items
from theories.modified_jones.model import estimate, ols, summarize_coefficients, winsorize


def _company(cik, rev_tag_2023="Revenues"):
    b = FactsBuilder(cik)
    y21 = dict(ni=8, cfo=10, rev=100, ar=20, ppe=50, assets=200)
    y22 = dict(ni=9, cfo=12, rev=120, ar=25, ppe=55, assets=220)
    y23 = dict(ni=12, cfo=11, rev=150, ar=40, ppe=60, assets=250)
    annual_report(b, 2022, "2023-02-15", "a22", y22, y21)
    # FY2023 10-K restates FY2022 revenue as a comparative and switches tag (ASC 606)
    annual_report(b, 2023, "2024-02-15", "a23", y23, {**y22, "rev": 118}, revenue_tag=rev_tag_2023)
    return b.build()


def test_panel_variables_from_xbrl():
    items = concat_items([company_items(_company(1, "RevenueFromContractWithCustomerExcludingAssessedTax"))])
    panel = build_panel(items, sic=pd.Series({"0000000001": 2000}))
    row = panel[panel["fyear"] == 2023].iloc[0]
    assert row["revenue_tag"] == "RevenueFromContractWithCustomerExcludingAssessedTax"
    assert row["ta"] == 12 - 11
    assert row["revenue_lag"] == 120          # first-reported, not the restated 118
    assert row["d_rev"] == 30
    assert row["d_rec"] == 15
    assert row["assets_lag"] == 220
    assert row["x_drev_drec"] == pytest.approx((30 - 15) / 220)
    assert row["x_ppe"] == pytest.approx(60 / 220)
    assert row["sic2"] == 20

    # first XBRL year still has lags, taken from the comparative column
    row22 = panel[panel["fyear"] == 2022].iloc[0]
    assert row22["assets_lag"] == 200 and row22["d_rev"] == 20


def test_latest_policy_uses_restated_comparative():
    items = concat_items([company_items(_company(1), policy="latest")])
    panel = build_panel(items)
    assert panel.loc[panel["fyear"] == 2023, "revenue_lag"].item() == 118


def test_sample_filters_exclude_financials_and_utilities():
    docs = [_company(c) for c in (1, 2, 3)]
    items = concat_items([company_items(d) for d in docs])
    sic = pd.Series({"0000000001": 2834, "0000000002": 6022, "0000000003": 4911})
    sample, attrition = apply_sample_filters(build_panel(items, sic))
    assert set(sample["cik"]) == {"0000000001"}
    assert attrition["firm_years"].is_monotonic_decreasing


def test_ols_recovers_known_coefficients():
    rng = np.random.default_rng(0)
    X = rng.normal(size=(500, 3))
    y = X @ np.array([0.5, -1.0, 2.0]) + rng.normal(scale=0.01, size=500)
    res = ols(X, y)
    np.testing.assert_allclose(res["coef"], [0.5, -1.0, 2.0], atol=0.01)
    assert res["r2"] > 0.99


def _synthetic_sample(n_per_group=40, years=(2021, 2022), seed=1, a=(0.02, 0.05, -0.08)):
    rng = np.random.default_rng(seed)
    rows = []
    for sic2 in (20, 35):
        for fy in years:
            for i in range(n_per_group):
                assets_lag = rng.uniform(50, 5000)
                drev = rng.normal(0.08, 0.15)
                drec = rng.normal(0.02, 0.04)
                ppe = rng.uniform(0.1, 0.9)
                y = a[0] / assets_lag + a[1] * drev + a[2] * ppe + rng.normal(0, 0.001)
                rows.append(dict(cik=f"{sic2}{i:04d}", fyear=fy, sic2=sic2, x_inv_a=1 / assets_lag,
                                 x_drev=drev, x_drev_drec=drev - drec, x_ppe=ppe, x_roa=0.0,
                                 y_ta=y, d_rec_scaled=drec))
    return pd.DataFrame(rows)


def test_estimate_recovers_jones_coefficients_and_modified_da():
    s = _synthetic_sample()
    firm_years, coefs = estimate(s, min_obs=10, winsor=0)
    assert len(coefs) == 4 and coefs["n"].eq(40).all()
    np.testing.assert_allclose(coefs["a2_d_rev"], 0.05, atol=3e-3)
    np.testing.assert_allclose(coefs["a3_ppe"], -0.08, atol=3e-3)
    # Jones residual ~ noise; Modified Jones DA = a2 * dREC (credit sales treated as discretionary)
    assert firm_years["da_jones"].abs().max() < 0.01
    np.testing.assert_allclose(firm_years["da"], coefs["a2_d_rev"].mean() * firm_years["d_rec_scaled"],
                               atol=0.005)


def test_estimate_skips_small_groups():
    s = _synthetic_sample(n_per_group=5)
    firm_years, coefs = estimate(s, min_obs=10, winsor=0)
    assert coefs.empty and firm_years.empty


def test_kothari_variant_has_intercept_and_roa():
    _, coefs = estimate(_synthetic_sample().assign(x_roa=lambda d: np.random.default_rng(2).normal(size=len(d))),
                        variant="kothari", winsor=0)
    assert {"a0_const", "a4_roa"} <= set(coefs.columns)
    summary = summarize_coefficients(coefs)
    assert "a4_roa" in summary["coefficient"].tolist()


def test_winsorize_clips_within_year():
    df = pd.DataFrame({"fyear": [1] * 100, "v": list(range(99)) + [10_000]})
    out = winsorize(df, ["v"], p=0.01)
    assert out["v"].max() < 10_000
