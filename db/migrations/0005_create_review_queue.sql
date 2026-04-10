-- migration 0005_create_review_queue.sql
-- Listings held for manual review (confidence 50-69, or PII validation failure).

CREATE TABLE review_queue (
  id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  listing_id       UUID NOT NULL REFERENCES listings(id),
  confidence_score SMALLINT NOT NULL,
  reason           TEXT,
  reviewed_by      VARCHAR(128),
  reviewed_at      TIMESTAMPTZ,
  decision         VARCHAR(16),  -- 'approve', 'reject', 'edit'
  notes            TEXT
);

CREATE INDEX idx_review_queue_listing    ON review_queue (listing_id);
CREATE INDEX idx_review_queue_decision   ON review_queue (decision) WHERE decision IS NULL;
CREATE INDEX idx_review_queue_created    ON review_queue (created_at);
