-- M5: every annual report checked for the cover-page error-correction check boxes.
-- One row per 10-K / 10-KT scanned, ticked or not, so scans are never repeated and the
-- share of filers reporting corrections can be measured.
CREATE TABLE cover_scan (
  accession          TEXT PRIMARY KEY,
  cik                BIGINT NOT NULL REFERENCES company(cik),
  form               TEXT NOT NULL,
  filed_date         DATE NOT NULL,
  error_correction   BOOLEAN,           -- NULL: not reported on the cover
  recovery_analysis  BOOLEAN,
  source             TEXT NOT NULL,     -- report read (e.g. R1.htm), or 'missing'
  scanned_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX cover_scan_flagged_idx ON cover_scan (filed_date DESC) WHERE error_correction;
CREATE INDEX cover_scan_cik_idx ON cover_scan (cik, filed_date DESC);
