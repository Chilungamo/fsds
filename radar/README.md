# Restatement Radar

Backend for a real-time dashboard of SEC financial restatements (see the living document
*Restatement Radar — Technical Implementation Guide*, §17 Roadmap).

| Milestone | Scope | Status |
|---|---|---|
| M0 | EDGAR client, iXBRL restatement extractor, golden fixtures | done — 46/46 hand-checked line items match |
| M1 | Live detection: feed watcher, Item 4.02 parser, amendment triage, Postgres schema, reconciliation, backfill | done — 100% recall (14/14) over 30 days; 1,998 cases since 2019 |
| M2 | Line-item deltas (XBRL, HTML tables, companyfacts), normaliser, propagation engine, integrity checks, severity | done — deltas for 97.1% of confirmed cases (target 80%); 89.9% of 5,070 checks pass |

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

---

# M1 — Detection

```
EDGAR "latest filings" Atom feed ──► radar watch ──┐
EDGAR daily form index ───────────► radar reconcile ├──► filing (Postgres, also the work queue)
EDGAR full-text search ───────────► radar backfill ─┘            │  claim … FOR UPDATE SKIP LOCKED
                                                                 ▼
                                      radar.pipeline: 8-K Item 4.02 → parse_item402 → PENDING case
                                                      10-K/A, 10-Q/A → extract + triage → CONFIRMED
                                                                 │
                                       restatement_case, case_filing, affected_period, event_log
                                                                 │  pg_notify('radar_events')
                                                                 ▼
                                                      (M3: API + live dashboard)
```

| Module | Purpose |
|---|---|
| `radar/edgar/feeds.py` | Atom "getcurrent" feed (8-K entries carry their item numbers) and daily `form.YYYYMMDD.idx` |
| `radar/edgar/submissions.py` | Per-company metadata cache: 8-K items, primary document, acceptance time |
| `radar/edgar/fulltext.py` | efts.sec.gov full-text search (backfill candidates and recall only) |
| `radar/parse/item402.py` | Item 4.02 section: affected periods, 4.02(a)/(b), amounts, auditor discussion, promised amendment |
| `radar/detect/triage.py` | Is a 10-K/A or 10-Q/A really a restatement? Weighted, explainable signals |
| `radar/pipeline.py` | Turns filings into cases; links 8-Ks and amendments by filing date in either order |
| `radar/db/` | Schema migrations and repository (Postgres; the `filing` table doubles as the queue) |
| `radar/workers/` | `watch`, `reconcile`, `backfill`, `recall` |

## Running it

```bash
export RADAR_SEC_USER_AGENT="RestatementRadar/0.1 you@example.com"
```

```bash
export RADAR_DATABASE_URL="postgresql://radar:radar@localhost:5432/radar"
```

```bash
radar migrate
```

```bash
radar watch
```

```bash
radar reconcile 2026-10-09 2026-11-09
```

```bash
radar backfill 2019-01-01 2026-10-08
```

```bash
radar process
```

```bash
radar recall 2026-10-09 2026-11-09
```

```bash
radar cases
```

`radar watch` polls every 60 s and processes what it finds; run `radar reconcile` nightly
for the previous business day. Several `radar process` workers can run side by side.

## Tests

The database tests need a Postgres database they may wipe (default
`postgresql://radar:radar@localhost:5432/radar_test`, override with
`RADAR_TEST_DATABASE_URL`); they are skipped if it is unreachable.

## Design decisions made in M1

* **Postgres is the queue** (ADR-009, supersedes the Redis Streams proposal ADR-007):
  `FOR UPDATE SKIP LOCKED` for workers, `LISTEN/NOTIFY` for live events, exponential
  retry backoff, failed filings parked for review. One less service to run.
* **The live feed carries 8-K item numbers**, so detecting Item 4.02 needs no extra
  request; only actual 4.02 filers are looked up (for the primary document).
* **Cases are linked by filing date, not processing order**, so a backfill that processes
  an amendment before its 8-K still produces one case.
* **Full-text search is only for backfill and measurement.** It also has a quirk: a
  multi-form filter such as `forms=8-K,8-K/A` silently returns only the 8-K/As, so the
  client queries one form at a time.

## Findings from real filings

1. Affected periods are best read from the sentence that says "should no longer be relied
   upon"; background paragraphs mention many other dates. Filers write compact lists
   ("fiscal years ended December 31, 2025, 2024 and 2023"; "September 30, June 30 and
   March 31 of each of 2025, 2024 and 2023"), which the parser expands.
2. The 10-K cover page's check-box question contains "correction of an error to previously
   issued financial statements" verbatim, so it must be stripped before text matching.
3. Part III amendments say "to include the information required by Items 10 through 14
   of Part III", often without "solely".
4. 2021's Item 4.02 wave (839 filings, about 9× a normal year) is the SPAC warrant/
   temporary-equity restatements. Those 8-Ks declare *balance sheets* "as of" an IPO date
   unreliable, so "as of" is an anchor when it follows "balance sheet". This raised
   period coverage from 84.5% to 92.2% of all Item 4.02 8-Ks (2021: 77% → 93%).
5. Full-text search misses many restatement amendments. In a sample of 15 unlinked 2025
   cases, 7 had a 10-K/A or 10-Q/A on EDGAR that search never returned. Processing a
   warning therefore queues the company's own amendments from its submissions record and
   re-triages any it ignored before the case existed. This raised confirmed cases from 92
   to 1,073 (of 1,998). Confirmations are backed by XBRL restatement tags (1,344
   amendments) or an explanatory note stating the restatement (370 more). The remaining
   unlinked warnings are mostly restatements corrected in the next *regular* 10-K/10-Q
   (7 of the 15 sampled), which a later milestone has to triage.
6. EDGAR answers both throttling and a missing Archives file (e.g. a weekend daily index)
   with HTTP 403; only throttling (identified by its message) is retried.

---

# M2 — Deltas and propagation

```
amendment ──► raw store ──► XBRL restatement axis ──┐
                         └► HTML "as reported | adjustment | as restated" tables (fallback)
                         └► companyfacts: amendment value vs first-filed value (fallback)
                                                     │
                       canonical.toml (62 line items)│ normalise
                                                     ▼
              fact_delta ──► case_delta view (latest filing wins per line item and period)
                                                     │
                       radar.propagate.engine ───────┤ integrity checks, impact graph, severity
                                                     ▼
                       integrity_check, impact_edge, restatement_case.severity
```

| Module | Purpose |
|---|---|
| `radar/normalize/canonical.toml` | Canonical line items: us-gaap concepts (in order of preference) and row-label patterns |
| `radar/extract/html_tables.py` | Reads printed restatement tables (colspan grid, role column bands, periods from header, header cells or section rows, "in thousands" captions, presentation signs) |
| `radar/extract/companyfacts.py` | Deltas for amendments that print only restated statements |
| `radar/propagate/engine.py` | Checks: balance sheet balances, gross profit and net income roll-ups, cash flow sums, net income → retained earnings; impact graph; severity |
| `radar/enrich.py` | Stores deltas and analyses cases; also runs inside the pipeline for new amendments |
| `radar/storage.py` | Raw documents (gzip, `RADAR_RAW_DIR`) so every parse can be re-run offline |

```bash
radar enrich
```

```bash
radar deltas 1956
```

```bash
radar graph 167
```

## Results (all confirmed cases, Nov 2026)

| Measure | Result |
|---|---|
| Confirmed cases with line-item deltas | 1,042 of 1,073 (97.1%) |
| Source | XBRL only 773 · HTML 176 · companyfacts 43 · combinations 50 |
| Integrity checks passing | 89.9% of 5,070 — balance sheet 98.6%, gross profit 93.5%, cash flow 93.1%, net income roll-up 87.1%, retained earnings 80.5% |
| Changed rows mapped to a canonical item | XBRL 68.6%, HTML 65.1%, companyfacts 100% |

## Findings

1. **A case is more than one filing.** Core Scientific restated through a 10-K/A and
   three 10-Q/As; only together do they give the opening balances that let the retained-
   earnings roll-forward be checked (the PP&E error grows from $97.3M to $127.3M).
2. **Opening balances are never "unchanged".** They carry every earlier error; assuming
   otherwise produced a false $122.9M failure for Core's Q1 2025.
3. **A missing line is not an unchanged line.** Restatement tables often omit lines that
   did change (e.g. pre-tax income). A check that fails only under such an assumption is
   dropped as inconclusive.
4. **Retained earnings often moves without net income.** SPAC restatements remeasure
   redeemable shares directly against accumulated deficit: cases that change temporary
   equity or APIC fail the roll-forward 26% of the time (11% otherwise). Such failures are
   annotated with that likely cause instead of being reported as errors.
5. **Printed tables use presentation signs** (expenses in parentheses); they are converted
   to XBRL's natural signs before checks run.
6. **Some amendments print no reconciliation at all**, only restated statements. Comparing
   the amendment's tagged values with the first-filed values (SEC companyfacts) recovered
   105 such filings.
7. **Never guess a number.** A column band holding two numbers once produced values like
   $5.5 × 10²⁰; ambiguous cells now yield nothing.
