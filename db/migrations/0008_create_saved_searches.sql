-- migration 0008: saved searches / car alerts
-- Users subscribe to alerts for specific makes/price ranges.
-- When a new matching listing goes active, we email them.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS public.saved_searches (
  id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  email      VARCHAR(256) NOT NULL,
  make       VARCHAR(128),
  model      VARCHAR(256),
  max_price  INTEGER,
  min_year   SMALLINT,
  is_active  BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_saved_searches_active ON public.saved_searches (is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_saved_searches_email  ON public.saved_searches (email);
