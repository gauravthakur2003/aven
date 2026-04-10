-- migration 0004_create_extraction_log.sql
-- M2 audit log — one row per normalisation attempt. Retained 30 days.

CREATE TABLE extraction_log (
  id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  payload_id              UUID NOT NULL,
  listing_id              UUID REFERENCES listings(id),
  source_id               VARCHAR(64) NOT NULL,
  extraction_method       extraction_method_enum NOT NULL,
  extraction_model        VARCHAR(64),
  normalisation_version   VARCHAR(16),
  llm_prompt_tokens       INTEGER,
  llm_completion_tokens   INTEGER,
  llm_latency_ms          INTEGER,
  confidence_score        SMALLINT,
  confidence_details      JSONB,
  fields_extracted        TEXT[],
  fields_null             TEXT[],
  price_pattern_flag      VARCHAR(32),
  pii_items_redacted      INTEGER DEFAULT 0,
  pii_redaction_failed    BOOLEAN NOT NULL DEFAULT FALSE,
  outcome                 VARCHAR(16),  -- 'published', 'review', 'rejected'
  error_code              VARCHAR(32),
  error_message           TEXT
);

CREATE INDEX idx_extraction_log_payload    ON extraction_log (payload_id);
CREATE INDEX idx_extraction_log_created    ON extraction_log (created_at);
CREATE INDEX idx_extraction_log_outcome    ON extraction_log (outcome);
CREATE INDEX idx_extraction_log_pii_failed ON extraction_log (pii_redaction_failed)
  WHERE pii_redaction_failed = TRUE;
