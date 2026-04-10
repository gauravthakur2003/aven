-- migration 0003_create_listings.sql
-- Canonical listings table — the central data store of the Aven platform.

CREATE TABLE listings (
  -- ── PRIMARY KEY & IDENTITY ──────────────────────────────────────────────
  id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- ── SOURCE METADATA ──────────────────────────────────────────────────────
  source_id           VARCHAR(64) NOT NULL,
  source_category     VARCHAR(64) NOT NULL,
  source_url          TEXT NOT NULL,
  source_listing_id   VARCHAR(256),
  payload_id          UUID,
  scrape_run_id       UUID,

  -- ── VEHICLE IDENTITY ──────────────────────────────────────────────────────
  vin                 CHAR(17),
  make                VARCHAR(128) NOT NULL,
  model               VARCHAR(256) NOT NULL,
  year                SMALLINT NOT NULL CHECK (year >= 1900 AND year <= 2030),
  trim                VARCHAR(256),
  body_type           VARCHAR(64),
  drivetrain          VARCHAR(16),
  fuel_type           VARCHAR(32),
  transmission        VARCHAR(32),
  colour_exterior     VARCHAR(64),
  colour_interior     VARCHAR(64),
  engine              VARCHAR(128),
  doors               SMALLINT,
  seats               SMALLINT,

  -- ── CONDITION & HISTORY ───────────────────────────────────────────────────
  condition           condition_enum NOT NULL DEFAULT 'Unknown',
  mileage_km          INTEGER CHECK (mileage_km >= 0),
  safetied            BOOLEAN,
  accidents           SMALLINT,
  owners              SMALLINT,
  last_service_km     INTEGER,

  -- ── PRICING ───────────────────────────────────────────────────────────────
  price               INTEGER,
  price_type          price_type_enum NOT NULL DEFAULT 'UNKNOWN',
  price_qualifier     VARCHAR(32),
  price_raw           VARCHAR(128) NOT NULL,
  price_currency_orig VARCHAR(8) DEFAULT 'CAD',
  price_exchange_rate NUMERIC(8,4),
  payment_amount      INTEGER,
  payment_frequency   VARCHAR(16),

  -- ── LOCATION ──────────────────────────────────────────────────────────────
  city                VARCHAR(128) NOT NULL,
  province            CHAR(2),
  postal_code_prefix  CHAR(3),

  -- ── SELLER ────────────────────────────────────────────────────────────────
  seller_type         seller_type_enum NOT NULL DEFAULT 'Unknown',
  dealer_id           UUID REFERENCES dealer_accounts(id),
  dealer_name         VARCHAR(256),

  -- ── LISTING CONTENT ───────────────────────────────────────────────────────
  description         TEXT,
  photo_urls          TEXT[],
  photo_hashes        TEXT[],

  -- ── LISTING LIFECYCLE ─────────────────────────────────────────────────────
  status              listing_status_enum NOT NULL DEFAULT 'active',
  listed_date         DATE,
  first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  sold_at             TIMESTAMPTZ,
  -- days_on_market is computed in queries: CURRENT_DATE - listed_date

  -- ── ENRICHMENT (set by M4) ────────────────────────────────────────────────
  fair_price_tag      VARCHAR(16),
  fair_price_delta    INTEGER,
  market_median_price INTEGER,
  trust_score         SMALLINT,
  embedding           vector(1536),

  -- ── AUCTION METADATA ──────────────────────────────────────────────────────
  auction_lot             VARCHAR(64),
  auction_date            TIMESTAMPTZ,
  auction_location        VARCHAR(256),
  auction_reserve_met     BOOLEAN,
  auction_current_bid     INTEGER,
  auction_condition_grade VARCHAR(8),

  -- ── NORMALISATION METADATA ────────────────────────────────────────────────
  confidence_score        SMALLINT NOT NULL DEFAULT 0 CHECK (confidence_score >= 0 AND confidence_score <= 100),
  confidence_details      JSONB,
  extraction_method       extraction_method_enum NOT NULL DEFAULT 'LLM',
  extraction_model        VARCHAR(64),
  normalisation_version   VARCHAR(16),
  needs_review            BOOLEAN NOT NULL DEFAULT FALSE,
  review_notes            TEXT,

  -- ── DEDUP METADATA (set by M3) ────────────────────────────────────────────
  canonical_id            UUID,
  duplicate_source_urls   TEXT[],

  CONSTRAINT ck_vin_format CHECK (
    vin IS NULL OR (LENGTH(vin) = 17 AND vin ~ '^[A-HJ-NPR-Z0-9]{17}$')
  )
);

-- Auto-update updated_at on every row change
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER listings_updated_at
  BEFORE UPDATE ON listings
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();
