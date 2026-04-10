// Standalone connector test — no Redis required.
// Run: npx ts-node test-connector.ts
//
// Tests: health check, scrape (first 2 pages), payload schema validation.

import { KijijiConnector } from './src/connectors/kijiji-ca';
import { KIJIJI_CONFIG }   from './src/config/connector-configs';
import { RawPayload }      from './src/types';
import { randomUUID }      from 'crypto';

const REQUIRED_FIELDS: (keyof RawPayload)[] = [
  'payload_id', 'source_id', 'source_category', 'listing_url',
  'scrape_timestamp', 'connector_version', 'raw_content',
  'raw_content_type', 'listing_images', 'geo_region',
  'scrape_run_id', 'http_status', 'proxy_used', 'requires_auth',
];

function validate(payload: RawPayload): string[] {
  const errs: string[] = [];
  for (const f of REQUIRED_FIELDS) {
    if (payload[f] === undefined) errs.push(`missing: ${f}`);
  }
  if (!Array.isArray(payload.listing_images)) errs.push('listing_images must be array');
  if (!payload.listing_url.startsWith('http'))  errs.push('listing_url invalid');
  return errs;
}

async function main(): Promise<void> {
  // Use a shallow config — only scrape 2 pages for the test.
  const testConfig = { ...KIJIJI_CONFIG, pagination_depth: 2, request_delay_ms: [200, 400] as [number, number] };
  const connector  = new KijijiConnector(testConfig);

  // ── Health check ─────────────────────────────────────────
  console.log('\n[1] Running health check...');
  const health = await connector.healthCheck();
  console.log(`    Status : ${health.status}`);
  if (health.reason) console.log(`    Reason : ${health.reason}`);
  if (health.status === 'FAILED') {
    console.error('    FAIL: health check failed — aborting test');
    process.exit(1);
  }
  console.log('    PASS');

  // ── Scrape ───────────────────────────────────────────────
  console.log('\n[2] Scraping (2 pages)...');
  const payloads: RawPayload[] = [];
  const runId = randomUUID();

  for await (const payload of connector.scrape({ runId, geoRegion: 'ON-GTA' })) {
    payloads.push(payload);
    if (payloads.length === 1) {
      console.log(`    First listing URL : ${payload.listing_url}`);
    }
  }
  console.log(`    Total payloads collected: ${payloads.length}`);
  if (payloads.length === 0) {
    console.error('    FAIL: no payloads returned');
    process.exit(1);
  }
  console.log('    PASS');

  // ── Schema validation ────────────────────────────────────
  console.log('\n[3] Validating payload schema on all payloads...');
  let failures = 0;
  for (const p of payloads) {
    const errs = validate(p);
    if (errs.length > 0) {
      console.error(`    FAIL [${p.payload_id}]: ${errs.join(', ')}`);
      failures++;
    }
  }
  if (failures > 0) {
    console.error(`    ${failures}/${payloads.length} payloads failed validation`);
    process.exit(1);
  }
  console.log(`    All ${payloads.length} payloads passed schema validation`);
  console.log('    PASS');

  // ── Per-run dedup ────────────────────────────────────────
  console.log('\n[4] Checking per-run deduplication...');
  const urls = payloads.map((p) => p.listing_url);
  const unique = new Set(urls);
  if (unique.size !== urls.length) {
    console.error(`    FAIL: ${urls.length - unique.size} duplicate URLs in single run`);
    process.exit(1);
  }
  console.log(`    ${unique.size} unique URLs — no duplicates`);
  console.log('    PASS');

  // ── Source attribution ───────────────────────────────────
  console.log('\n[5] Checking source attribution fields...');
  const sample = payloads[0];
  console.log(`    source_id        : ${sample.source_id}`);
  console.log(`    source_category  : ${sample.source_category}`);
  console.log(`    connector_version: ${sample.connector_version}`);
  console.log(`    geo_region       : ${sample.geo_region}`);
  console.log(`    scrape_run_id    : ${sample.scrape_run_id}`);
  console.log(`    raw_content_type : ${sample.raw_content_type}`);
  console.log(`    requires_auth    : ${sample.requires_auth}`);
  console.log(`    proxy_used       : ${sample.proxy_used}`);
  console.log('    PASS');

  console.log('\n✓ All tests passed\n');
}

main().catch((err: Error) => {
  console.error('\nFatal error:', err.message);
  process.exit(1);
});
