# Restatement Radar

**Real-time tracking of SEC financial restatements: detected from EDGAR, quantified line by line, traced through the financial statements and explained in plain English.**

**Live:** https://restatement-radar.onrender.com

![Restatement Radar dashboard](docs/screenshots/dashboard-overview.png)

When a US-listed company finds that its published financial statements were wrong, it files an **8-K Item 4.02** ("non-reliance") warning and later a restated **10-K/A** or **10-Q/A**. Restatement Radar watches EDGAR for both and, within about two minutes of a filing appearing:

- **Detects** the warning or restated filing and groups related filings into one *case*.
- **Extracts** every restated figure (previously reported → adjustment → restated) from the filing's XBRL or HTML tables.
- **Traces** each change through the income statement, balance sheet and cash flow statement, and checks the restated numbers still add up.
- **Explains** what went wrong and why, citing the company's own words, with every figure and date checked against the data.
- **Pushes** the case to the dashboard live.

Coverage: every Item 4.02 warning and restating amendment since 2019, about 1,800 cases.

---

## How it works

```mermaid
flowchart TB
    subgraph EDGAR["SEC EDGAR"]
        direction LR
        F1["Live feed<br/>every 60 s"]
        F2["Daily index<br/>nightly"]
        F3["Filing documents<br/>and XBRL"]
    end

    Q[("Filing queue<br/>(Postgres)")]

    subgraph Pipeline["Processing pipeline"]
        direction LR
        D["1 · Detect<br/>Item 4.02 parser<br/>amendment triage"]
        X["2 · Extract<br/>restated line items<br/>XBRL · HTML · companyfacts"]
        P["3 · Propagate<br/>impact graph<br/>checks · severity"]
        E["4 · Explain<br/>cause classifier<br/>template · Claude rewrite"]
        D --> X --> P --> E
    end

    V{"5 · Validate<br/>every figure and date<br/>matches the data?"}
    DB[("PostgreSQL<br/>cases · deltas · checks<br/>explanations · event log")]
    API["FastAPI<br/>REST API + live event stream"]
    UI["Dashboard (React)<br/>feed · case pages · impact graph"]

    F1 --> Q
    F2 --> Q
    Q --> Pipeline
    F3 -. "documents" .-> Pipeline
    Pipeline --> V
    V -- "yes: Claude's text" --> DB
    V -- "no: template text" --> DB
    DB -- "LISTEN / NOTIFY" --> API
    API -- "Server-Sent Events" --> UI
```

- **Watch.** `radar watch` polls EDGAR's live feed every 60 seconds. A nightly job (`radar nightly`) compares against EDGAR's daily index, so nothing is missed if the watcher was down.
- **Detect.** Every 8-K with Item 4.02 opens or updates a case: the affected periods, whether the auditor initiated it (4.02(b)), and the company's own description. Amendments are scored by triage, and the restating ones confirm the case.
- **Extract.** Restated figures are read from inline XBRL (`srt:RestatementAxis`). If a filing has none, they come from its printed restatement tables, or from comparing amended values with the originally filed ones. Everything is mapped onto one chart of accounts.
- **Propagate.** An impact graph links each change to the totals it feeds (for example impairment → operating income → net income → retained earnings → equity). Accounting checks then confirm the restated figures still add up: the balance sheet balances, subtotals roll up, and net income flows into retained earnings. Each case also gets a severity score.
- **Explain and validate.**
   - A rule-based classifier finds the likely cause (revenue recognition, impairment, inventory and others) and quotes the sentences that show it.
   - A template writes a plain-English summary from the stored figures.
   - Optionally, Claude rewrites it more readably.
   - The **validator** then reads every amount, percentage and date back out of the text and checks each one against the database. Any rewrite that fails is discarded, and the template text is shown instead.
- **Serve.** Postgres `LISTEN/NOTIFY` feeds a Server-Sent Events stream, so open dashboards show new cases live without reloading.

PostgreSQL is both the work queue (`FOR UPDATE SKIP LOCKED`) and the event bus, so there is no separate queue or broker to run.

---

## The dashboard

- **Feed:** every case, newest first, with status (pending, restated, needs review), severity and headline. Searchable by company, ticker or CIK.
- **Case page:**
  - the explanation, with each figure highlighted and traceable to its source;
  - the filing timeline, with links to EDGAR;
  - restated figures by period, with a net income bridge;
  - the accounting checks;
  - an interactive graph of how the adjustments flow between statements.
- **Stats:** warnings per year, pending and confirmed counts, median days from warning to restated filing, and alert latency.

---

## Quick start (local)

Requirements: Python 3.11+, PostgreSQL 16, Node 22 (to build the web app).

```bash
python3 -m venv .venv && source .venv/bin/activate
```

```bash
pip install -e ".[dev,llm]"
```

```bash
export RADAR_SEC_USER_AGENT="YourApp/1.0 contact@example.com"
```

```bash
export RADAR_DATABASE_URL="postgresql://radar:radar@localhost:5432/radar"
```

```bash
radar migrate
```

```bash
cd radar/web && npm ci && npm run build && cd ../..
```

```bash
radar serve --port 8000
```

Open http://localhost:8000. To load history, run `radar backfill 2019-01-01 2026-11-01 --amendments` (Item 4.02 warnings plus candidate amendments), then `radar process`.

Or run everything with Docker (database, migrations, web app and watcher):

```bash
docker compose up
```

SEC requires every client to identify itself with a contact in the User-Agent, and to stay under 10 requests per second (the client defaults to 5).

### Commands

| Command | What it does |
|---|---|
| `radar watch` | Poll the live feed and process new filings |
| `radar nightly --days 3` | Scheduled job: reconcile recent days against the daily index, then process |
| `radar backfill FROM TO [--amendments]` · `radar process` | Queue historical filings · drain the queue |
| `radar enrich` | (Re)compute line-item deltas, checks and impact graphs |
| `radar explain [--llm] [--missing] [--workers N]` | Write explanations; resumable |
| `radar explain-report` | Re-check every stored explanation against the data (coverage, % validated) |
| `radar cases` · `radar deltas CASE` · `radar graph CASE` | Inspect cases from the terminal |
| `radar fetch` · `radar extract` | Download a filing · extract its restatement table |
| `radar serve` | Run the API and dashboard |

### Configuration

| Variable | Default | Purpose |
|---|---|---|
| `RADAR_DATABASE_URL` | local `radar` database | PostgreSQL connection string (`DATABASE_URL` also works) |
| `RADAR_SEC_USER_AGENT` | — | Required by SEC: app name and contact email |
| `RADAR_SEC_RATE` | `5` | EDGAR requests per second |
| `RADAR_EMBEDDED_WATCHER` | `0` | `1` runs the watcher inside the web process (single-instance hosting) |
| `RADAR_DB_POOL` | `10` | Database connections for the API |
| `ANTHROPIC_API_KEY` | — | Enables Claude rewrites of explanations |
| `RADAR_LLM` | `0` | `1` rewrites new cases with Claude automatically |
| `RADAR_LLM_MODEL` / `RADAR_LLM_EFFORT` | `claude-opus-5-5` / `low` | Model and effort for rewrites |
| `RADAR_RAW_DIR` | `~/.cache/restatement-radar/raw` | Local cache for downloaded filings |

---

## Tests

```bash
pytest
```

The default run is offline. Database tests use `RADAR_TEST_DATABASE_URL` (default `postgresql://radar:radar@localhost:5432/radar_test`) and are skipped if it is unreachable.

Opt-in suites:
- `pytest -m live` downloads from EDGAR.
- `pytest -m llm` calls Claude (a few cents per run). It can also run on GitHub: **Actions → LLM narrative check**.

Real filings in `tests/fixtures` are the golden set: hand-checked restated values, and Item 4.02 notices with known causes.

---

## Deployment

The app runs on **Render** from `render.yaml` (Blueprint): one web service with the embedded watcher, a nightly cron job, and a Postgres database, at about $13 a month. Migrations run before each deploy. Fly.io (`fly.toml`) and a plain VM (`deploy/vm/`) are documented alternatives. See [docs/DEPLOY.md](docs/DEPLOY.md) for costs, scaling and the runbook.

---

## Repository layout

```
radar/
  edgar/       EDGAR client (rate limit, retries), feeds, submissions, full-text search
  parse/       8-K Item 4.02 parser
  detect/      amendment triage
  extract/     iXBRL, HTML-table and companyfacts extractors
  normalize/   canonical chart of accounts (canonical.toml)
  propagate/   impact graph, integrity checks, severity
  explain/     cause classifier, template, validator, Claude narrative, service
  db/          migrations and queries (Postgres as queue and event log)
  workers/     watch, reconcile, backfill, recall
  api/         FastAPI app and live event stream
  web/         React + TypeScript dashboard (Vite)
  pipeline.py  per-filing processing; cli.py  the `radar` command
tests/         offline tests, golden fixtures, database and browser tests
docs/          deployment guide and screenshots
```

The `.py` files at the repository root (`income.py`, `revenues.py`, `mda_scraper.py` and others) are earlier exploratory XBRL analysis scripts, not part of the Radar app.

## Status

| Milestone | Scope | Status |
|---|---|---|
| M0 | EDGAR client, iXBRL extractor, golden fixtures | Done |
| M1 | Live detection, Item 4.02 parser, triage, reconciliation, backfill | Done: 100% recall over 30 days |
| M2 | Line-item deltas, normalisation, propagation, checks, severity | Done: deltas for 97% of confirmed cases |
| M3 | API, live stream, dashboard, deployment | Live on Render |
| M4 | Plain-English explanations with validated figures | Built; history backfill in progress |

Design decisions and findings from real filings, milestone by milestone, are in [radar/README.md](radar/README.md).

---

Data from SEC EDGAR, extracted automatically. Not investment advice; check figures against the source filings.
