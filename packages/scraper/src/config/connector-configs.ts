// Connector configuration registry.
// In production these are stored in the Postgres connector_configs table.
// For MVP, they are defined here and can be overridden by environment variables.

import { ConnectorConfig } from '../types';

// ── Kijiji Autos ─────────────────────────────────────────────────────────────
// Priority 1. RSS-based. No proxy required. Stealth off.
// Rate limit per PRD §7.3: 20 req/min → 500–1500ms delay.
// ─────────────────────────────────────────────────────────────────────────────
export const KIJIJI_CONFIG: ConnectorConfig = {
  connector_id:        'kijiji-ca',
  enabled:             true,
  schedule_cron:       '0 */6 * * *',     // every 6 hours
  peak_schedule_cron:  '0 */3 * * *',     // every 3 hours (7am–11pm ET)
  geographic_scope:    ['ON-GTA'],
  vehicle_categories:  ['cars-trucks'],
  max_listings_per_run: 100_000,
  request_delay_ms:    [500, 1_500],
  max_retries:         3,
  timeout_ms:          30_000,
  pagination_depth:    5_000,             // 5000 pages × 20 listings = 100K cap
  proxy_pool_id:       'none',
  stealth_mode:        false,
  alert_threshold_pct: 20,
};

// ── Facebook Marketplace ──────────────────────────────────────────────────────
// Priority 1. Playwright. Residential proxy required. Stealth on.
// Rate limit per PRD §7.3: 6 req/min → 8000–15000ms delay.
// Highest-risk source — most likely to require maintenance.
// ─────────────────────────────────────────────────────────────────────────────
export const FACEBOOK_CONFIG: ConnectorConfig = {
  connector_id:        'facebook-mp-ca',
  enabled:             true,
  schedule_cron:       '0 */6 * * *',
  peak_schedule_cron:  '0 */3 * * *',
  geographic_scope:    ['ON-GTA'],
  vehicle_categories:  ['vehicles'],
  max_listings_per_run: 80_000,
  request_delay_ms:    [8_000, 15_000],
  max_retries:         3,
  timeout_ms:          60_000,
  pagination_depth:    200,               // 200 scrolls × ~40 listings/scroll
  proxy_pool_id:       'brightdata-residential-ca',
  stealth_mode:        true,
  alert_threshold_pct: 20,
};

// ── AutoTrader.ca ─────────────────────────────────────────────────────────────
// BLOCKED — data partnership required. Config here for completeness only.
// ─────────────────────────────────────────────────────────────────────────────
export const AUTOTRADER_CONFIG: ConnectorConfig = {
  connector_id:        'autotrader-ca',
  enabled:             false,             // never enable without signed agreement
  schedule_cron:       '0 */12 * * *',
  peak_schedule_cron:  '0 */6 * * *',
  geographic_scope:    ['ON-GTA'],
  vehicle_categories:  ['cars-trucks'],
  max_listings_per_run: 150_000,
  request_delay_ms:    [3_000, 7_000],
  max_retries:         3,
  timeout_ms:          30_000,
  pagination_depth:    7_500,
  proxy_pool_id:       'brightdata-residential-ca',
  stealth_mode:        true,
  alert_threshold_pct: 20,
};

export const ALL_CONFIGS: ConnectorConfig[] = [
  KIJIJI_CONFIG,
  FACEBOOK_CONFIG,
  AUTOTRADER_CONFIG,
];
