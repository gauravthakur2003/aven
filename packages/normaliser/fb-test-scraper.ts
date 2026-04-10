// Quick test: run FB scraper for 3 scrolls and print what it yields
// npx ts-node fb-test-scraper.ts

import * as dotenv from 'dotenv';
import * as path from 'path';
dotenv.config({ path: path.join(__dirname, '.env'), override: true });

import { scrapeFacebook } from './src/fb-scraper';

async function main() {
  console.log('Starting FB scraper test (will stop after 5 listings or 60s)...\n');

  const seenUrls = new Set<string>();
  let count = 0;
  const start = Date.now();

  for await (const payload of scrapeFacebook(seenUrls, (msg) => console.log(msg), () => false)) {
    count++;
    const raw = JSON.parse(payload.raw_content);
    console.log(`\nListing #${count}:`);
    console.log(`  Title:  ${raw.title}`);
    console.log(`  Price:  $${raw.priceAmount}`);
    console.log(`  Year:   ${raw.year}`);
    console.log(`  Make:   ${raw.make}`);
    console.log(`  Model:  ${raw.model}`);
    console.log(`  Loc:    ${raw.location}`);
    console.log(`  Images: ${raw.allImages?.length ?? 0}`);
    console.log(`  URL:    ${payload.listing_url}`);

    if (count >= 5 || Date.now() - start > 60_000) {
      console.log('\nGot enough — stopping.');
      break;
    }
  }

  if (count === 0) {
    console.log('⚠ NO listings yielded. Check if FB session is valid and GQL interception works.');
  } else {
    console.log(`\n✓ Scraper yielded ${count} listings in ${Math.round((Date.now() - start)/1000)}s`);
  }
}

main().catch(console.error);
