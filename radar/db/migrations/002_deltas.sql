-- M2: line-item deltas, integrity checks, impact graph, severity.

CREATE TABLE fact_delta (
  id                  BIGSERIAL PRIMARY KEY,
  accession           TEXT NOT NULL REFERENCES filing(accession) ON DELETE CASCADE,
  concept             TEXT NOT NULL,     -- us-gaap:InventoryNet, or label:BS:inventories (HTML)
  label               TEXT,              -- row label as printed (HTML source)
  canonical_item      TEXT,              -- inventory, net_income, ... (NULL = not mapped)
  statement           TEXT CHECK (statement IN ('IS','BS','CF','EQ')),
  period_start        DATE,              -- NULL for instants (balance sheet)
  period_end          DATE NOT NULL,
  dims                JSONB NOT NULL DEFAULT '{}',
  unit                TEXT,
  previously_reported NUMERIC(28,6),
  adjustment          NUMERIC(28,6),
  restated            NUMERIC(28,6),
  tolerance           NUMERIC(28,6) NOT NULL DEFAULT 1,
  source              TEXT NOT NULL CHECK (source IN ('xbrl','html')),
  check_ok            BOOLEAN,
  error_member        TEXT,
  UNIQUE NULLS NOT DISTINCT (accession, concept, period_start, period_end, dims)
);
CREATE INDEX fact_delta_item_idx ON fact_delta (canonical_item, period_end);

-- The case's current view: for each line item and period, the most recently filed value.
CREATE VIEW case_delta AS
SELECT DISTINCT ON (cf.case_id, fd.concept, fd.period_start, fd.period_end, fd.dims)
       cf.case_id, f.filed_date, fd.*
FROM fact_delta fd
JOIN case_filing cf ON cf.accession = fd.accession
JOIN filing f ON f.accession = fd.accession
ORDER BY cf.case_id, fd.concept, fd.period_start, fd.period_end, fd.dims,
         f.filed_date DESC, f.accession DESC;

CREATE TABLE integrity_check (
  case_id      BIGINT NOT NULL REFERENCES restatement_case(id) ON DELETE CASCADE,
  period_start DATE,
  period_end   DATE NOT NULL,
  name         TEXT NOT NULL,
  status       TEXT NOT NULL CHECK (status IN ('pass','fail')),
  gap          NUMERIC(28,6),
  details      JSONB NOT NULL DEFAULT '{}',
  UNIQUE NULLS NOT DISTINCT (case_id, period_start, period_end, name)
);

CREATE TABLE impact_edge (
  case_id      BIGINT NOT NULL REFERENCES restatement_case(id) ON DELETE CASCADE,
  period_start DATE,
  period_end   DATE NOT NULL,
  src_item     TEXT NOT NULL,
  dst_item     TEXT NOT NULL,
  relationship TEXT NOT NULL,
  sign         SMALLINT NOT NULL,
  src_delta    NUMERIC(28,6) NOT NULL,
  dst_delta    NUMERIC(28,6),             -- NULL: the effect is implied, not reported
  UNIQUE NULLS NOT DISTINCT (case_id, period_start, period_end, src_item, dst_item)
);

ALTER TABLE restatement_case
  ADD COLUMN severity NUMERIC(4,3),
  ADD COLUMN enriched_at TIMESTAMPTZ;
