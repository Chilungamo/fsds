# fsds — Accounting theory, replicated with XBRL

Can the classic results of empirical accounting research be reproduced using
only free, machine-readable financial reporting data (SEC XBRL), instead of
commercial databases such as Compustat? This repository revisits major
accounting theories one at a time. Each one gets the original model, a
documented mapping to XBRL tags, a reproducible pipeline, and an honest
account of what XBRL can and cannot capture.

## Theories

| # | Theory / model | Status |
|---|---|---|
| 1 | [Modified Jones discretionary accruals](theories/modified_jones/) — Jones (1991); Dechow, Sloan & Sweeney (1995); Kothari et al. (2005) | ✅ implemented |
| 2 | Accrual anomaly — Sloan (1996) | planned |
| 3 | Accrual quality — Dechow & Dichev (2002) | planned |
| 4 | Benford's law on reported XBRL facts | planned |
| 5 | Agency theory via SEC Pay-versus-Performance (`ecd` taxonomy) | planned |

## Layout

```
fsds/                 shared data layer
  config.py           credentials + SEC User-Agent from .env
  sec_client.py       rate-limited, cached EDGAR client; bulk-archive reader
  facts.py            companyfacts JSON -> tidy long DataFrame
  standardize.py      one value per period, tag fallback chains, annual periods
theories/<name>/      one package per theory: README, data.py, model.py, CLI
tests/                pytest suite with synthetic companyfacts fixtures
*.py (root)           original single-concept ETL scripts into Postgres
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env             # then edit: SEC_USER_AGENT and DB_* values
python -m pytest -q
```

Run everything from the repository root (e.g. `python -m theories.modified_jones --tickers AAPL MSFT`,
`python revenues.py`) so the `fsds` package is importable.

### Note for existing `revenues2` / `income2` tables

`revenues.py` and `income.py` stored the period end
date in `start`, so rows loaded before this fix have zero-length periods.
Truncate and reload them:

```sql
TRUNCATE revenues2; TRUNCATE income2;
```

## Data source

SEC EDGAR XBRL APIs (`data.sec.gov/api/xbrl/companyfacts`) and bulk archives
(`companyfacts.zip`, `submissions.zip`). The SEC requires a descriptive
User-Agent with contact details and limits clients to 10 requests/second.
