-- migration 0011_add_liveness_columns.sql
-- Liveness checker support: track when each listing was last re-verified against
-- its source URL, how many times it has been checked, and (when removed) why.
--
-- last_seen_at  = last time we CONFIRMED the listing is still live (200 + valid data)
-- last_checked_at = last time we ATTEMPTED a liveness check (success or not)
-- These differ: a check that errors out updates last_checked_at but not last_seen_at.

ALTER TABLE listings ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMPTZ;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS check_count     INTEGER NOT NULL DEFAULT 0;
ALTER TABLE listings ADD COLUMN IF NOT EXISTS removal_reason  VARCHAR(32);

-- Partial index to find active listings due for a liveness check quickly.
-- The checker filters on status='active' and orders by last_checked_at NULLS FIRST.
CREATE INDEX IF NOT EXISTS idx_listings_liveness
  ON listings (last_checked_at NULLS FIRST)
  WHERE status = 'active';
