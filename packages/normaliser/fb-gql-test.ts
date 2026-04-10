/**
 * Extract listing data embedded in FB's SSR HTML script tags.
 */
import { chromium } from 'playwright';
import * as fs from 'fs';
import * as path from 'path';

const SESSION_FILE = path.join(__dirname, 'fb-session.json');

(async () => {
  const storageState = JSON.parse(fs.readFileSync(SESSION_FILE, 'utf-8'));
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    storageState,
    viewport: { width: 1280, height: 900 },
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    locale: 'en-CA',
  });

  // Also intercept GQL for scroll batches
  let gqlListingBuffers: string[] = [];
  context.on('response', async (res) => {
    if (!res.url().includes('/api/graphql')) return;
    if (res.request().method() !== 'POST') return;
    if (res.status() !== 200) return;
    try {
      const text = await res.text();
      if (text.includes('marketplace_listing_title')) {
        gqlListingBuffers.push(text);
        console.log(`[gql] Found listings GQL response (${text.length} bytes)`);
      }
    } catch {}
  });

  const page = await context.newPage();
  await page.goto('https://www.facebook.com', { waitUntil: 'domcontentloaded', timeout: 30_000 });
  await new Promise(r => setTimeout(r, 2000));

  await page.goto('https://www.facebook.com/marketplace/toronto/vehicles', { waitUntil: 'networkidle', timeout: 60_000 });
  await new Promise(r => setTimeout(r, 3000));

  // Extract all script tag content looking for marketplace_listing_title
  const scriptData = await page.evaluate(`
    (function() {
      const scripts = Array.from(document.querySelectorAll('script'));
      const results = [];
      for (const s of scripts) {
        const t = s.textContent || '';
        if (t.includes('marketplace_listing_title')) {
          results.push(t.slice(0, 200));
        }
      }
      return results;
    })()
  `);

  const scripts = scriptData as string[];
  console.log(`Script tags with marketplace_listing_title: ${scripts.length}`);
  for (let i = 0; i < Math.min(3, scripts.length); i++) {
    console.log(`  [${i}] ${scripts[i].replace(/\s+/g, ' ').slice(0, 150)}`);
  }

  // Get full listing data from page's __SSR_DATA__ or similar
  // FB stores data in window.__INITIAL_STATE__ or in require() calls
  const fullScript = await page.evaluate(`
    (function() {
      const scripts = Array.from(document.querySelectorAll('script'));
      for (const s of scripts) {
        const t = s.textContent || '';
        if (t.includes('marketplace_listing_title')) {
          return t.slice(0, 100000);
        }
      }
      return '';
    })()
  `);

  if (fullScript) {
    const sample = String(fullScript);
    fs.writeFileSync('/tmp/fb-ssr-script.txt', sample);
    console.log(`Saved first SSR script to /tmp/fb-ssr-script.txt (${sample.length} bytes)`);

    // Count listing titles
    const titles = sample.match(/"marketplace_listing_title":"[^"]+"/g) ?? [];
    console.log(`Listing titles in first script: ${titles.length}`);
    titles.slice(0, 5).forEach(t => console.log(`  ${t}`));
  }

  // Try 3 scrolls to see if GQL fires
  for (let i = 0; i < 3; i++) {
    await page.evaluate('window.scrollBy({ top: 1200, behavior: "smooth" })');
    await new Promise(r => setTimeout(r, 4000));
    console.log(`Scroll ${i+1} — GQL listing batches: ${gqlListingBuffers.length}`);
  }

  await browser.close();
})();
