-- M4: plain-English explanation per case (design doc §5.4). One row per case, replaced
-- when its inputs change. What is shown always passed the number and date validator.
CREATE TABLE case_explanation (
  case_id      BIGINT PRIMARY KEY REFERENCES restatement_case(id) ON DELETE CASCADE,
  method       TEXT NOT NULL CHECK (method IN ('template', 'llm')),
  headline     TEXT NOT NULL,
  sentences    JSONB NOT NULL,          -- ["...", ...] as shown
  facts        JSONB NOT NULL,          -- each figure shown and the stored value behind it
  causes       JSONB NOT NULL,          -- classifier output with the company's own sentences
  template     JSONB NOT NULL,          -- {headline, sentences}: always kept as the fallback
  validation   JSONB NOT NULL,          -- counts checked, and anything dropped
  input_hash   TEXT NOT NULL,           -- unchanged inputs => no new API call
  model        TEXT,
  llm_reason   TEXT,                    -- "ok" or why the template is shown
  usage        JSONB,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX case_explanation_method_idx ON case_explanation (method);
