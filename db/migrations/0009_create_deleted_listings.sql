-- migration 0009_create_deleted_listings.sql
-- Archive table for listings removed via the dashboard REMOVE action.
-- Rows are auto-purged after 48 hours by the dashboard's periodic cleanup task.
-- Keeps a full JSONB snapshot of the listings row at the moment of deletion for audit purposes.

CREATE TABLE deleted_listings (
  id            UUID PRIMARY KEY,
  deleted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  original_data JSONB NOT NULL
);

CREATE INDEX idx_deleted_listings_deleted_at ON deleted_listings (deleted_at);
