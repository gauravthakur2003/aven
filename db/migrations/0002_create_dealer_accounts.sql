-- migration 0002_create_dealer_accounts.sql
-- Stub dealer accounts table. Required before listings (FK reference).
-- Full dealer account fields are built in M9.

CREATE TABLE IF NOT EXISTS dealer_accounts (
  id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  name         VARCHAR(256) NOT NULL,
  website      TEXT,
  city         VARCHAR(128),
  province     CHAR(2),
  active       BOOLEAN NOT NULL DEFAULT TRUE
);
