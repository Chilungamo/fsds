-- M2: deltas can also come from comparing an amendment's XBRL facts with the values
-- originally filed for the same periods (SEC companyfacts API).
ALTER TABLE fact_delta DROP CONSTRAINT fact_delta_source_check;
ALTER TABLE fact_delta ADD CONSTRAINT fact_delta_source_check
  CHECK (source IN ('xbrl', 'html', 'companyfacts'));
