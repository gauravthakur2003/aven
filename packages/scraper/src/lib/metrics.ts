// Shared Prometheus metrics collector.
// All connectors use this module — never create their own registries.
// Label names must match exactly across all connectors.

import { Counter, Gauge, Registry } from 'prom-client';

export const register = new Registry();

register.setDefaultLabels({ service: 'aven-scraper' });

// ── Per-connector counters (BS-06 requirement) ──────────────

export const requestsMade = new Counter({
  name: 'aven_scraper_requests_made_total',
  help: 'Total HTTP requests made',
  labelNames: ['connector_id'] as const,
  registers: [register],
});

export const requestsSucceeded = new Counter({
  name: 'aven_scraper_requests_succeeded_total',
  help: 'Total HTTP requests that returned 2xx',
  labelNames: ['connector_id'] as const,
  registers: [register],
});

export const requestsFailed = new Counter({
  name: 'aven_scraper_requests_failed_total',
  help: 'Total HTTP requests that failed or returned non-2xx',
  labelNames: ['connector_id'] as const,
  registers: [register],
});

export const listingsCollected = new Counter({
  name: 'aven_scraper_listings_collected_total',
  help: 'Total listing payloads collected',
  labelNames: ['connector_id'] as const,
  registers: [register],
});

export const queuePushes = new Counter({
  name: 'aven_scraper_queue_pushes_total',
  help: 'Total successful ingestion queue pushes',
  labelNames: ['connector_id'] as const,
  registers: [register],
});

export const queuePushFailures = new Counter({
  name: 'aven_scraper_queue_push_failures_total',
  help: 'Total failed ingestion queue pushes',
  labelNames: ['connector_id'] as const,
  registers: [register],
});

// ── Per-connector gauges ─────────────────────────────────────

export const runDurationSeconds = new Gauge({
  name: 'aven_scraper_run_duration_seconds',
  help: 'Duration of the last completed scrape run',
  labelNames: ['connector_id'] as const,
  registers: [register],
});

export const lastSuccessfulRunAgeSeconds = new Gauge({
  name: 'aven_scraper_last_successful_run_age_seconds',
  help: 'Seconds elapsed since the last successful scrape run',
  labelNames: ['connector_id'] as const,
  registers: [register],
});

export const selectorFailureRate = new Gauge({
  name: 'aven_scraper_selector_failure_rate',
  help: 'Fraction of pages where expected CSS selectors did not match',
  labelNames: ['connector_id'] as const,
  registers: [register],
});

export const proxyBanRate = new Gauge({
  name: 'aven_scraper_proxy_ban_rate',
  help: 'Fraction of proxy IPs that received a ban signal in the last run',
  labelNames: ['connector_id'] as const,
  registers: [register],
});

export const queuePushFailureRate = new Gauge({
  name: 'aven_scraper_queue_push_failure_rate',
  help: 'Fraction of queue push attempts that failed',
  labelNames: ['connector_id'] as const,
  registers: [register],
});
