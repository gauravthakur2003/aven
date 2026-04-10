-- migration 0001_create_schema_setup.sql
-- Extensions, enums, and shared types used across all tables.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ── Enums ──────────────────────────────────────────────────────────────────

CREATE TYPE price_type_enum AS ENUM (
  'PURCHASE_PRICE',
  'BIWEEKLY_PAYMENT',
  'MONTHLY_PAYMENT',
  'CALL_FOR_PRICE',
  'AUCTION_ESTIMATE',
  'AUCTION_HAMMER',
  'UNKNOWN'
);

CREATE TYPE condition_enum AS ENUM (
  'New',
  'Used',
  'Certified Pre-Owned',
  'Demo',
  'Salvage',
  'Unknown'
);

CREATE TYPE seller_type_enum AS ENUM (
  'Private',
  'Dealer',
  'Auction',
  'Fleet',
  'Unknown'
);

CREATE TYPE listing_status_enum AS ENUM (
  'active',
  'sold',
  'expired',
  'review',
  'rejected',
  'archived'
);

CREATE TYPE extraction_method_enum AS ENUM (
  'LLM',
  'STRUCTURED',
  'HYBRID'
);
