# Modified Jones model — discretionary accruals from XBRL

## The theory

Accrual accounting lets earnings differ from cash flow. **Earnings-management
research** splits total accruals into a *non-discretionary* part (driven by the
firm's economics) and a *discretionary* part (the residual, attributed to
managerial choice).

| Paper | Contribution |
|---|---|
| Healy (1985), DeAngelo (1986) | Total accruals / changes in accruals as a proxy for manipulation |
| **Jones (1991)** | Model normal accruals from revenue growth (working capital) and PPE (depreciation) |
| **Dechow, Sloan & Sweeney (1995)** | *Modified* Jones: subtract receivables growth from revenue growth, since credit sales are where managers have discretion. Shown to have the most power of the models they tested to detect SEC-enforcement firms |
| DeFond & Jiambalvo (1994) | Estimate cross-sectionally by industry-year instead of per-firm time series |
| Hribar & Collins (2002) | Measure total accruals from the cash-flow statement, not balance-sheet changes |
| Kothari, Leone & Wasley (2005) | Add ROA and an intercept to control for performance |

## The model

Per 2-digit SIC industry and fiscal year:

$$\frac{TA_{it}}{A_{i,t-1}} = \alpha_1\frac{1}{A_{i,t-1}} + \alpha_2\frac{\Delta REV_{it}}{A_{i,t-1}} + \alpha_3\frac{PPE_{it}}{A_{i,t-1}} + \varepsilon_{it}$$

$$NDA_{it} = \hat\alpha_1\frac{1}{A_{i,t-1}} + \hat\alpha_2\frac{\Delta REV_{it}-\Delta REC_{it}}{A_{i,t-1}} + \hat\alpha_3\frac{PPE_{it}}{A_{i,t-1}}, \qquad DA_{it} = \frac{TA_{it}}{A_{i,t-1}} - NDA_{it}$$

Theory predicts **α₂ > 0** (growth needs working capital) and **α₃ < 0**
(PPE generates depreciation, a negative accrual). Mean total accruals are
typically negative, and DA averages ≈ 0 within each industry-year by construction.

## XBRL mapping

| Variable | us-gaap tags (fallback order) | Compustat analogue |
|---|---|---|
| Earnings | `IncomeLossFromContinuingOperations` → `NetIncomeLoss` → `ProfitLoss` | IB |
| CFO | `NetCashProvidedByUsedInOperatingActivitiesContinuingOperations` → `NetCashProvidedByUsedInOperatingActivities` | OANCF |
| Revenue | `Revenues` → `RevenueFromContractWithCustomerExcludingAssessedTax` → `…IncludingAssessedTax` → `SalesRevenueNet` → … | SALE |
| Receivables | `AccountsReceivableNetCurrent` → `ReceivablesNetCurrent` → `AccountsNotesAndLoansReceivableNetCurrent` | RECT |
| PPE | `PropertyPlantAndEquipmentGross` → `PropertyPlantAndEquipmentNet` → PPE incl. finance-lease ROU | PPEGT |
| Total assets | `Assets` | AT |
| Industry | SIC from EDGAR submissions | SICH |

Chains live in [`fsds/standardize.py`](../../fsds/standardize.py); the tag that
supplied each value is kept in the output (`*_tag` columns) and summarised.

### Design decisions

* **One value per period.** Every period appears in several filings
  (comparatives, restatements). The default `--policy first` takes the
  first-reported value, i.e. what investors actually saw — the right choice for
  an earnings-management study. `--policy latest` uses restated values instead;
  the difference between the two is itself a restatement measure.
* **Annual periods only.** Durations of 350–380 days from 10-K/20-F/40-F
  filings (52/53-week years included). Fiscal year follows the Compustat rule
  (ends Jan–May → previous year).
* **Lags from comparatives.** Prior-year values are read at
  `start − 1 day`, the previous fiscal year-end, which every 10-K reports as a
  comparative. The first XBRL year therefore still has lagged data.
* **Sample screens.** Financials (SIC 60–69) and utilities (49) excluded;
  positive lagged assets; all variables present; ≥ 10 firms per
  industry-year; variables winsorised at 1%/99% by year.

## Running it

```bash
# small sample through the API (responses cached in data/cache)
python -m theories.modified_jones --tickers AAPL MSFT KO PEP WMT TGT HD LOW

# full universe from SEC bulk archives (recommended for real results)
#   https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip
#   https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip
python -m theories.modified_jones \
    --companyfacts-zip data/companyfacts.zip --submissions-zip data/submissions.zip

# performance-matched variant / restated data
python -m theories.modified_jones --companyfacts-zip ... --variant kothari --policy latest
```

A handful of tickers rarely fills an industry-year with 10 firms; use the full
universe (or `--min-obs` for a quick look).

Outputs in `results/`:

* `firm_year_da.csv` — inputs, tags used, NDA, Modified Jones DA, Jones residual
* `industry_year_coefficients.csv` — α̂, t-stats, R² per industry-year
* `summary.md` — sample attrition, coefficient distribution (with Fama-MacBeth
  t), accruals by year, tag usage

## What XBRL can and cannot do here

**Can:** every input of the model is in the SEC's XBRL financial data
(US filers, FY2009+), which makes this a free, transparent replacement for
Compustat for this model. The `*_tag` columns make every number traceable to a
filing.

**Limitations (to discuss in results):**

* Coverage starts 2009–2011 (XBRL phase-in), so the original 1980s samples cannot be replicated directly.
* Tag choice is filer-specific; a firm's revenue can switch tag across years (handled per period, but definitions can differ).
* `companyfacts` excludes dimensional facts (segments), so segment-level analysis needs the full instance documents.
* Gross PPE is often only in the notes, so many firm-years fall back to net PPE, which is not what Jones (1991) used.
* SIC codes are current, not historical (Compustat SICH is historical).
* Firms without a receivables tag drop out; check attrition before generalising.

## Next steps

* Validation à la Dechow et al. (1995): compare DA of firms subject to SEC
  AAERs against matched controls.
* Accrual quality (Dechow & Dichev 2002) and the Sloan (1996) accrual anomaly
  reuse this panel.
* Compare first-reported against restated DA as a measure of restatements.
