// FB scraper debugger — run once to see what's happening
// npx ts-node fb-debug.ts

import * as dotenv from 'dotenv';
import * as path   from 'path';
dotenv.config({ path: path.join(__dirname, '.env'), override: true });

import { chromium } from 'playwright';
import * as fs from 'fs';

const SESSION_FILE = path.join(__dirname, 'fb-session.json');

async function main() {
  const storageState = JSON.parse(fs.readFileSync(SESSION_FILE, 'utf-8'));

  console.log('Launching browser...');
  const browser = await chromium.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
           '--disable-blink-features=AutomationControlled'],
  });

  const context = await browser.newContext({
    storageState: storageState as any,
    viewport: { width: 1280, height: 900 },
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    locale: 'en-CA', timezoneId: 'America/Toronto',
  });

  await context.addInitScript(`
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    window.chrome = { runtime: {} };
  `);

  // Track ALL responses
  const gqlResponses: string[] = [];
  let listingsFound = 0;

  context.on('response', async (res) => {
    const url = res.url();
    if (url.includes('/api/graphql')) {
      try {
        const text = await res.text();
        // Check if it contains marketplace listings
        if (text.includes('marketplace_listing_title') || text.includes('marketplace_listing')) {
          listingsFound++;
          gqlResponses.push(`[HAS LISTINGS] ${url.slice(0, 80)}`);
          // Save first one for inspection
          if (listingsFound === 1) {
            fs.writeFileSync('/tmp/fb-gql-sample.json', text);
            console.log('  Saved sample GraphQL response to /tmp/fb-gql-sample.json');
          }
        } else if (text.includes('marketplace')) {
          gqlResponses.push(`[marketplace, no listings] ${url.slice(0, 60)}`);
        }
      } catch { /* skip */ }
    }
  });

  const page = await context.newPage();

  // Step 1: Check if session is valid
  console.log('Navigating to facebook.com...');
  await page.goto('https://www.facebook.com', { waitUntil: 'domcontentloaded', timeout: 30_000 });
  await page.waitForTimeout(3000);

  const url1 = page.url();
  const title1 = await page.title();
  console.log(`  URL: ${url1}`);
  console.log(`  Title: ${title1}`);

  const isLoggedIn = !url1.includes('login') && !url1.includes('checkpoint') && !title1.includes('Log in');
  console.log(`  Logged in: ${isLoggedIn ? 'YES ✓' : 'NO ✗'}`);

  if (!isLoggedIn) {
    console.log('\n⚠ Session is NOT valid. You need to re-run fb-auth-setup.ts');
    await browser.close();
    return;
  }

  // Step 2: Navigate to Marketplace
  console.log('\nNavigating to Marketplace vehicles...');
  try {
    await page.goto('https://www.facebook.com/marketplace/toronto/vehicles', {
      waitUntil: 'domcontentloaded', timeout: 30_000,
    });
  } catch (e) {
    console.log('  Navigation timeout (expected for FB) — continuing anyway');
  }

  const url2 = page.url();
  const title2 = await page.title();
  console.log(`  URL: ${url2}`);
  console.log(`  Title: ${title2}`);

  // Wait for content to load and GraphQL to fire
  console.log('  Waiting 8s for GraphQL responses...');
  await page.waitForTimeout(8000);

  // Step 3: Check what's on the page
  const listingLinks = await page.evaluate(
    `Array.from(document.querySelectorAll('a[href*="/marketplace/item/"]')).length`
  ) as number;
  console.log(`  Listing links found in DOM: ${listingLinks}`);

  // Step 4: Scroll once to trigger more data
  console.log('  Scrolling to trigger GraphQL...');
  await page.evaluate('window.scrollBy(0, 1500)');
  await page.waitForTimeout(5000);

  // Report
  console.log(`\n── Results ──────────────────────────`);
  console.log(`GraphQL responses with listing data: ${listingsFound}`);
  if (gqlResponses.length > 0) {
    gqlResponses.slice(0, 10).forEach(r => console.log('  ', r));
  } else {
    console.log('  NO GraphQL responses captured at all!');
    // Check all network requests
    console.log('\n  All /api/graphql requests made (checking interceptor):');
  }

  // Take a screenshot to visually verify
  await page.screenshot({ path: '/tmp/fb-debug-screenshot.png', fullPage: false });
  console.log('\nScreenshot saved to /tmp/fb-debug-screenshot.png');

  // Also dump page HTML snippet to check for login wall
  const bodyText = await page.evaluate(`document.body.innerText.slice(0, 500)`) as string;
  console.log('\nPage text preview:');
  console.log(bodyText.slice(0, 300));

  await browser.close();
  console.log('\nDone.');
}

main().catch(console.error);
