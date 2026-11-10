-- Restatement Radar — initial schema (M1: detection).
-- Later milestones add fact_delta, impact_edge, integrity_check and explanation.

CREATE TABLE company (
  cik         BIGINT PRIMARY KEY,
  name        TEXT NOT NULL,
  tickers     TEXT[] NOT NULL DEFAULT '{}',
  sic         TEXT,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per EDGAR accession. Doubles as the work queue: workers claim NEW/RETRY rows
-- with FOR UPDATE SKIP LOCKED (see radar.db.repo.claim_filings).
CREATE TABLE filing (
  accession      TEXT PRIMARY KEY CHECK (accession ~ '^\d{10}-\d{2}-\d{6}$'),
  cik            BIGINT NOT NULL REFERENCES company(cik),
  form           TEXT NOT NULL,
  items          TEXT[],                         -- 8-K items, e.g. {4.02,9.01}; NULL = unknown
  filed_date     DATE NOT NULL,
  accepted_at    TIMESTAMPTZ,
  primary_doc    TEXT,
  source         TEXT NOT NULL,                  -- feed | daily_index | backfill | manual
  state          TEXT NOT NULL DEFAULT 'NEW'
                 CHECK (state IN ('NEW','PROCESSING','RETRY','DONE','IGNORED','FAILED')),
  attempts       INT NOT NULL DEFAULT 0,
  claimed_at     TIMESTAMPTZ,
  not_before     TIMESTAMPTZ,                    -- RETRY backoff
  last_error     TEXT,
  parser_version TEXT,
  meta           JSONB NOT NULL DEFAULT '{}',
  discovered_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  processed_at   TIMESTAMPTZ
);
CREATE INDEX filing_queue_idx ON filing (discovered_at) WHERE state IN ('NEW','RETRY','PROCESSING');
CREATE INDEX filing_cik_idx ON filing (cik, filed_date DESC);
CREATE INDEX filing_items_idx ON filing USING GIN (items);

CREATE TYPE case_type   AS ENUM ('BIG_R', 'LITTLE_R', 'UNKNOWN');
CREATE TYPE case_status AS ENUM ('PENDING', 'CONFIRMED', 'ENRICHED', 'NEEDS_REVIEW', 'CLOSED');

CREATE TABLE restatement_case (
  id          BIGSERIAL PRIMARY KEY,
  cik         BIGINT NOT NULL REFERENCES company(cik),
  type        case_type   NOT NULL DEFAULT 'UNKNOWN',
  status      case_status NOT NULL DEFAULT 'PENDING',
  sub_item    CHAR(1) CHECK (sub_item IN ('a','b')),
  confidence  NUMERIC(4,3),
  summary     JSONB NOT NULL DEFAULT '{}',
  opened_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX case_cik_idx ON restatement_case (cik, opened_at DESC);
CREATE INDEX case_status_idx ON restatement_case (status, updated_at DESC);

CREATE TABLE case_filing (
  case_id    BIGINT NOT NULL REFERENCES restatement_case(id) ON DELETE CASCADE,
  accession  TEXT NOT NULL REFERENCES filing(accession),
  role       TEXT NOT NULL CHECK (role IN ('warning','warning_amendment','amendment','revision')),
  PRIMARY KEY (case_id, accession)
);
CREATE INDEX case_filing_acc_idx ON case_filing (accession);

CREATE TABLE affected_period (
  case_id      BIGINT NOT NULL REFERENCES restatement_case(id) ON DELETE CASCADE,
  period_end   DATE NOT NULL,
  kind         TEXT NOT NULL CHECK (kind IN ('annual','interim','unknown')),
  source       TEXT NOT NULL,                  -- item402 | xbrl
  PRIMARY KEY (case_id, period_end, kind, source)
);

-- Append-only event stream: powers the live feed (LISTEN radar_events) and replay.
CREATE TABLE event_log (
  id         BIGSERIAL PRIMARY KEY,
  ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
  type       TEXT NOT NULL,
  case_id    BIGINT REFERENCES restatement_case(id) ON DELETE SET NULL,
  accession  TEXT,
  payload    JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX event_log_case_idx ON event_log (case_id, id);

-- Resumable cursors for backfill and reconciliation jobs.
CREATE TABLE job_cursor (
  name        TEXT PRIMARY KEY,
  value       TEXT NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
