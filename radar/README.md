# Restatement Radar — Milestone M0

The first milestone of the Restatement Radar dashboard (see the living document
*Restatement Radar — Technical Implementation Guide*, §17 Roadmap). M0 proves that
before/after restatement numbers can be pulled reliably out of real SEC filings.

**M0 exit criterion:** deltas extracted from 3+ real restatement filings match the
filings' own printed tables. **Status: met** (4 filings, 46 hand-checked line items).

## What's here

| Module | Purpose |
|---|---|
| `radar/edgar/client.py` | Rate-limited (default 5 req/s, SEC max 10), retrying EDGAR client with the required User-Agent |
| `radar/edgar/urls.py` | Accession/CIK normalisation and EDGAR URL builders |
| `radar/extract/ixbrl.py` | Inline XBRL restatement extractor (`srt:RestatementAxis`) |
| `radar/cli.py` | `radar fetch` and `radar extract` commands |
| `tests/fixtures/filings` | Four real 10-Q/A filings (gzipped iXBRL) |
| `tests/fixtures/expected` | Values copied by hand from each filing's HTML restatement tables |

## Quick start

```bash
pip install -e ".[dev]"
```

```bash
export RADAR_SEC_USER_AGENT="RestatementRadar/0.1 you@example.com"
```

```bash
radar fetch 1308208 0001193125-26-098732 --out ulh.htm.gz
```

```bash
radar extract --changed ulh.htm.gz
```

```text
UNIVERSAL LOGISTICS HOLDINGS, INC.  10-Q/A  period September 27, 2025
concept                                 period                   as reported   adjustment      restated  ok
us-gaap:Goodwill                        2025-09-27               148,784,000  -43,166,000   105,618,000  ✓
us-gaap:NetIncomeLoss                   2025-06-29..2025-09-27   -74,772,000  -43,166,000  -117,938,000  ✓
...
```

`radar extract --json` emits every row (including dimensional ones) for downstream use.

## Tests

```bash
pytest
```

```bash
RADAR_SEC_USER_AGENT="RestatementRadar/0.1 you@example.com" pytest -m live
```

The default run is offline. `-m live` downloads one filing from EDGAR.

## Golden fixtures

| Filing | Company | Error | Statements hit |
|---|---|---|---|
| 0001193125-26-098732 (10-Q/A Q3 2025) | Universal Logistics | Goodwill impairment understated by $43.2M | IS, BS |
| 0001628280-26-013286 (10-Q/A Q1 2025) | Core Scientific | Demolition-bound assets left capitalised; $127.3M cumulative PP&E, $4.4M Q1 SG&A, cash reclassified investing → operating | IS, BS, CF |
| 0001992243-25-000029 (10-Q/A Q1 2025) | T1 Energy | Contract-intangible amortisation shown in SG&A instead of against revenue (presentation only) | IS |
| 0001628280-24-008911 (10-Q/A Q1 2023) | Cutera | Inventory overstated (count shortfall) plus revenue/AR/deferred-revenue errors | IS, BS, CF |

## Findings from real filings (feed back into the design doc)

1. **Adjustment members vary.** Older filings use `srt:RestatementAdjustmentMember`;
   newer ones use `srt:RevisionOfPriorPeriodErrorCorrectionAdjustmentMember` on the same
   `srt:RestatementAxis`. Members are matched by local name, so custom filer members work too.
2. **The same fact is tagged more than once at different precisions.** For example, the table
   says `124,411` (thousands) and the narrative says `$124.4 million`. The extractor keeps
   the most precise value and flags a conflict only when the rounded copy disagrees.
3. **Filers break adjustments down by cause.** Cutera uses
   `us-gaap:ErrorCorrectionsAndPriorPeriodAdjustmentsRestatementByRestatementPeriodAndAmountAxis`
   with one member per error (e.g. `PhysicalInventoryCountShortfallMember`). These rows are
   kept, and `RestatementRow.error_member` exposes them. They are ready-made input for the
   explanation layer.
4. **Filers make mistakes, and the check catches them.** Cutera prints the share count in the
   Adjustments column for weighted-average shares. Previously reported + adjustment ≠
   restated, so `check_ok` is `False`.
5. **Cover-page flags are tagged on the checkbox glyph** (`☒`/`☐`) and are normalised to
   `true`/`false`. Only 10-K-family forms carry them, not 10-Q/As.
6. **The SEC's companyfacts API has no dimensional facts**, as expected. The filing itself
   has to be parsed.
