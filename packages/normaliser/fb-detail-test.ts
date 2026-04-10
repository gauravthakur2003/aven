/**
 * Verify that the updated normaliseFBListing extracts vehicle_* fields.
 */
import { chromium } from 'playwright';
import * as fs from 'fs';
import * as path from 'path';
import { } from './src/fb-scraper'; // just to check compile

// Replicate key extraction logic inline to test
function extractListingsFromGQL(obj: unknown): unknown[] {
  if (typeof obj !== 'object' || obj === null) return [];
  const o = obj as Record<string, unknown>;
  if (typeof o['marketplace_listing_title'] === 'string' && typeof o['id'] === 'string') return [o];
  const r: unknown[] = [];
  for (const v of Object.values(o)) {
    if (typeof v === 'object' && v !== null) r.push(...extractListingsFromGQL(v));
  }
  return r;
}

const SESSION_FILE = path.join(__dirname, 'fb-session.json');
const LISTING_URL  = 'https://www.facebook.com/marketplace/item/1311042720310138/';

(async () => {
  const storageState = JSON.parse(fs.readFileSync(SESSION_FILE, 'utf-8'));
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ storageState, locale: 'en-CA' });
  const page = await context.newPage();
  await page.goto(LISTING_URL, { waitUntil: 'networkidle', timeout: 30_000 });

  const ssrText = await page.evaluate(`
    (function() {
      const scripts = Array.from(document.querySelectorAll('script'));
      let best = '';
      for (const s of scripts) {
        const t = s.textContent || '';
        if (t.includes('marketplace_listing_title') && t.length > best.length) best = t;
      }
      return best;
    })()
  `);

  const candidates: unknown[] = [];
  for (const line of String(ssrText).split('\n').filter(Boolean)) {
    try { candidates.push(...extractListingsFromGQL(JSON.parse(line))); } catch {}
  }

  console.log(`Found ${candidates.length} listing objects in SSR`);
  const target = (candidates as Record<string, unknown>[]).find(c => c['id'] === '1311042720310138');
  if (target) {
    const keys = Object.keys(target).filter(k => k.startsWith('vehicle_') || ['id','marketplace_listing_title','redacted_description','location_text','listing_price'].includes(k));
    for (const k of keys) {
      console.log(`  ${k}: ${JSON.stringify(target[k]).slice(0, 100)}`);
    }
  } else {
    console.log('Target listing not found. IDs found:', (candidates as Record<string, unknown>[]).map(c => c['id']));
  }

  await browser.close();
})();
