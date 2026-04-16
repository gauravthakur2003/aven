// Module 2 — Normalisation Engine types
// Shared across M2a–M2e sub-modules.

// ── Input: from M1 Redis stream ────────────────────────────

export interface RawPayload {
  payload_id:        string;
  source_id:         string;
  source_category:   string;
  listing_url:       string;
  scrape_timestamp:  string;
  connector_version: string;
  raw_content:       string;
  raw_content_type:  'json' | 'html' | 'xml';
  listing_images:    string[];
  geo_region:        string;
  scrape_run_id:     string;
  http_status:       number;
  proxy_used:        boolean;
  requires_auth:     boolean;
  is_dealer_listing: boolean | null;
  /** Set to true when the listing's detail page was fetched to fill in missing fields. */
  _advancedScrape?:  boolean;
}

// ── M2a output: raw LLM extraction result ─────────────────

export type ConfidenceLevel = 'high' | 'medium' | 'low' | 'none';

export interface ExtractedFields {
  make:                  string | null;
  model:                 string | null;
  year:                  number | null;
  trim:                  string | null;
  body_type:             string | null;
  drivetrain:            'FWD' | 'RWD' | 'AWD' | '4WD' | null;
  fuel_type:             string | null;
  transmission:          'Automatic' | 'Manual' | 'CVT' | null;
  colour_exterior:       string | null;
  colour_interior:       string | null;
  engine:                string | null;
  doors:                 number | null;
  seats:                 number | null;
  vin:                   string | null;
  condition:             'New' | 'Used' | 'Certified Pre-Owned' | 'Demo' | 'Salvage' | 'Unknown' | null;
  mileage_km:            number | null;
  mileage_unit_original: 'km' | 'miles' | null;
  safetied:              boolean | null;
  accidents:             number | null;
  owners:                number | null;
  price:                 number | null;
  price_type:            'PURCHASE_PRICE' | 'BIWEEKLY_PAYMENT' | 'MONTHLY_PAYMENT' | 'CALL_FOR_PRICE' | 'UNKNOWN';
  price_qualifier:       'OAC' | 'FIRM' | 'OBO' | 'NEGOTIABLE' | null;
  price_raw:             string;
  price_currency_orig:   'CAD' | 'USD';
  payment_amount:        number | null;
  payment_frequency:     'BIWEEKLY' | 'MONTHLY' | null;
  city:                  string | null;
  province:              string | null;
  seller_type:           'Private' | 'Dealer' | 'Auction' | 'Fleet' | null;
  dealer_name:           string | null;
  listed_date:           string | null;
  description:           string | null;
  confidence: {
    make:       ConfidenceLevel;
    model:      ConfidenceLevel;
    year:       ConfidenceLevel;
    price:      ConfidenceLevel;
    mileage_km: ConfidenceLevel;
    safetied:   ConfidenceLevel;
    city:       ConfidenceLevel;
  };
}

// ── M2b output: validated + standardised fields ────────────

export type ValidatedFields = ExtractedFields & {
  _validationWarnings: string[];
};

// ── M2c output: scored record ──────────────────────────────

export interface ScoredRecord {
  fields:           ValidatedFields;
  confidence_score: number;             // 0–100
  confidence_details: Record<string, number>;
  outcome:          'published' | 'review' | 'rejected';
  needs_review:     boolean;
}

// ── M2e: what gets written to Postgres ────────────────────

export interface NormalisedListing {
  // Source
  source_id:             string;
  source_category:       string;
  source_url:            string;
  payload_id:            string;
  scrape_run_id:         string;
  // Vehicle identity
  make:                  string;
  model:                 string;
  year:                  number;
  trim:                  string | null;
  body_type:             string | null;
  drivetrain:            string | null;
  fuel_type:             string | null;
  transmission:          string | null;
  colour_exterior:       string | null;
  colour_interior:       string | null;
  engine:                string | null;
  doors:                 number | null;
  seats:                 number | null;
  vin:                   string | null;
  // Condition
  condition:             string;
  mileage_km:            number | null;
  safetied:              boolean | null;
  accidents:             number | null;
  owners:                number | null;
  // Pricing
  price:                 number | null;
  price_type:            string;
  price_qualifier:       string | null;
  price_raw:             string;
  price_currency_orig:   string;
  price_exchange_rate:   number | null;
  payment_amount:        number | null;
  payment_frequency:     string | null;
  // Location
  city:                  string;
  province:              string | null;
  // Seller
  seller_type:           string;
  dealer_name:           string | null;
  // Content
  description:           string | null;
  photo_urls:            string[];
  // Lifecycle
  status:                string;
  listed_date:           string | null;
  // Normalisation metadata
  confidence_score:      number;
  confidence_details:    Record<string, number>;
  extraction_method:     string;
  extraction_model:      string;
  normalisation_version: string;
  needs_review:          boolean;
}

// ── Extraction log entry ───────────────────────────────────

export interface ExtractionLogEntry {
  payload_id:            string;
  listing_id:            string | null;
  source_id:             string;
  extraction_method:     string;
  extraction_model:      string;
  normalisation_version: string;
  llm_prompt_tokens:     number;
  llm_completion_tokens: number;
  llm_latency_ms:        number;
  confidence_score:      number;
  confidence_details:    Record<string, number>;
  fields_extracted:      string[];
  fields_null:           string[];
  pii_items_redacted:    number;
  pii_redaction_failed:  boolean;
  outcome:               string;
  error_code:            string | null;
  error_message:         string | null;
}
