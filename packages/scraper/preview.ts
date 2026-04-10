// Live preview — first 10 listings with full structured fields.
// Run: npx ts-node preview.ts

import { KijijiConnector } from './src/connectors/kijiji-ca';
import { KIJIJI_CONFIG }   from './src/config/connector-configs';
import { randomUUID }      from 'crypto';

function formatPrice(cents: number | null): string {
  if (cents === null) return 'Price not listed';
  return `$${(cents / 100).toLocaleString('en-CA', { minimumFractionDigits: 0 })}`;
}

async function main(): Promise<void> {
  const connector = new KijijiConnector({
    ...KIJIJI_CONFIG,
    pagination_depth: 1,
    request_delay_ms: [200, 400] as [number, number],
  });

  console.log('\nAven — Kijiji GTA Live Feed');
  console.log('============================\n');

  let count = 0;
  for await (const payload of connector.scrape({ runId: randomUUID(), geoRegion: 'ON-GTA' })) {
    count++;
    if (count > 200) break;

    const d = JSON.parse(payload.raw_content);

    const price     = formatPrice(d.priceCents);
    const mileage   = d.mileageKm != null ? `${d.mileageKm.toLocaleString()} km` : 'N/A';
    const seller    = d._sellerType === 'delr' ? 'Dealer' : d._sellerType === 'ownr' ? 'Private' : 'Unknown';
    const condition = d.vehicleType === 'used' ? 'Used' : d.vehicleType === 'new' ? 'New' : 'N/A';

    console.log(`#${count}  ${d.title}`);
    console.log(`    Price      : ${price}${d.priceRating ? `  [${d.priceRating}]` : ''}`);
    console.log(`    Year/Make  : ${d.year ?? 'N/A'} ${d.make ?? ''} ${d.model ?? ''} ${d.trim ?? ''}`.trim());
    console.log(`    Mileage    : ${mileage}  |  Colour: ${d.colour ?? 'N/A'}  |  ${condition}`);
    console.log(`    Drivetrain : ${d.drivetrain?.toUpperCase() ?? 'N/A'}  |  Fuel: ${d.fuelType ?? 'N/A'}  |  Body: ${d.bodyType ?? 'N/A'}`);
    console.log(`    VIN        : ${d.vin ?? 'not listed'}`);
    console.log(`    Seller     : ${seller}${d.posterRating ? `  (${d.posterRating}★)` : ''}`);
    console.log(`    Location   : ${d.location ?? 'N/A'}`);
    console.log(`    Images     : ${d.imageUrls?.length ?? 0}`);
    console.log(`    URL        : ${payload.listing_url}`);
    console.log(`    Payload ID : ${payload.payload_id}`);
    console.log();
  }

  console.log(`Showed ${count} listings.`);
  console.log('Each payload is queued to Redis stream aven:ingestion:raw for M2 to normalize.\n');
}

main().catch((err: Error) => {
  console.error('Error:', err.message);
  process.exit(1);
});
