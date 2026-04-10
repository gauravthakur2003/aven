-- migration 0006_create_listings_index.sql
-- Search and operational indexes on the listings table.

-- Primary search filters
CREATE INDEX idx_listings_make_model   ON listings (make, model);
CREATE INDEX idx_listings_year         ON listings (year);
CREATE INDEX idx_listings_price        ON listings (price) WHERE price IS NOT NULL;
CREATE INDEX idx_listings_mileage      ON listings (mileage_km) WHERE mileage_km IS NOT NULL;
CREATE INDEX idx_listings_city         ON listings (city);
CREATE INDEX idx_listings_province     ON listings (province);
CREATE INDEX idx_listings_safetied     ON listings (safetied);
CREATE INDEX idx_listings_condition    ON listings (condition);
CREATE INDEX idx_listings_seller_type  ON listings (seller_type);
CREATE INDEX idx_listings_status       ON listings (status);

-- Dedup lookups (M3)
CREATE INDEX idx_listings_vin          ON listings (vin) WHERE vin IS NOT NULL;
CREATE UNIQUE INDEX idx_listings_source_url ON listings (source_url);

-- Lifecycle queries
CREATE INDEX idx_listings_last_seen    ON listings (last_seen_at);
CREATE INDEX idx_listings_status_src   ON listings (status, source_id);

-- Normalisation maintenance
CREATE INDEX idx_listings_confidence   ON listings (confidence_score);
CREATE INDEX idx_listings_needs_review ON listings (needs_review) WHERE needs_review = TRUE;
CREATE INDEX idx_listings_norm_version ON listings (normalisation_version);

-- pgvector HNSW index for persona embedding search (M6)
-- Built after initial data load — expensive during bulk insert.
CREATE INDEX idx_listings_embedding ON listings
  USING hnsw (embedding vector_cosine_ops)
  WITH (m = 16, ef_construction = 64);
