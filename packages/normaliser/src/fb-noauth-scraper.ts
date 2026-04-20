/**
 * Facebook Marketplace no-auth scraper
 *
 * How it works:
 *   Stage 1 — Discovery:  Hit FB Marketplace grid pages (city/vehicles) without any auth.
 *             Each page returns ~1,000 listing IDs embedded in data-sjs scripts.
 *             Rotate cities × price bands to maximise unique coverage.
 *
 *   Stage 2 — Enrichment: Fetch each listing's detail page (also public/no-auth).
 *             Parse the data-sjs scripts for full vehicle data:
 *             make, model, year (from title), price, mileage, colour, transmission,
 *             fuel type, condition, seller type, description, images, location.
 *
 * No session file. No Playwright. No cookies. Runs forever on Railway.
 */

import axios from 'axios';
import { randomUUID as uuidv4 } from 'crypto';
import { RawPayload } from './types';

// ── Residential proxy (required on Railway — FB blocks datacenter IPs) ────────
// Set FB_PROXY_URL in Railway env vars, e.g.:
//   http://user:pass@proxy.example.com:8080
// Supports Webshare, PacketStream, Bright Data, or any HTTP/HTTPS proxy.
const FB_PROXY_URL = process.env.FB_PROXY_URL ?? null;
// eslint-disable-next-line @typescript-eslint/no-var-requires
const proxyAgent = FB_PROXY_URL ? new (require('https-proxy-agent').HttpsProxyAgent)(FB_PROXY_URL) : undefined;
if (FB_PROXY_URL) {
  const masked = FB_PROXY_URL.replace(/:\/\/[^@]+@/, '://*****@');
  console.log(`[fb] Using residential proxy: ${masked}`);
} else {
  console.log('[fb] No FB_PROXY_URL set — FB requests go direct (will fail on Railway)');
}

// ── Cities to scrape ─────────────────────────────────────
export interface FbCity { label: string; slug: string; province: string; }

export const FB_CITIES: FbCity[] = [
  // Ontario — 9 cities
  { label: 'Toronto',     slug: 'toronto',             province: 'ON' },
  { label: 'Ottawa',      slug: 'ottawa',              province: 'ON' },
  { label: 'Hamilton',    slug: 'hamilton',            province: 'ON' },
  { label: 'London ON',   slug: 'london-on',           province: 'ON' },
  { label: 'Kitchener',   slug: 'kitchener-waterloo',  province: 'ON' },
  { label: 'Windsor',     slug: 'windsor-on',          province: 'ON' },
  { label: 'Barrie',      slug: 'barrie',              province: 'ON' },
  { label: 'Oshawa',      slug: 'oshawa',              province: 'ON' },
  { label: 'Niagara',     slug: 'st-catharines',       province: 'ON' },
  // West
  { label: 'Vancouver',   slug: 'vancouver',           province: 'BC' },
  { label: 'Calgary',     slug: 'calgary',             province: 'AB' },
  { label: 'Edmonton',    slug: 'edmonton',            province: 'AB' },
  // East
  { label: 'Montreal',    slug: 'montreal',            province: 'QC' },
  { label: 'Winnipeg',    slug: 'winnipeg',            province: 'MB' },
];

// ── Price bands — overlap intentionally to catch listings near boundaries ──
const FB_PRICE_BANDS = [
  { label: 'all',   params: '' },
  { label: '0-8k',  params: '&minPrice=500&maxPrice=8000' },
  { label: '8-25k', params: '&minPrice=8000&maxPrice=25000' },
  { label: '25k+',  params: '&minPrice=25000' },
] as const;

const BASE_HEADERS = {
  'User-Agent':      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'en-CA,en;q=0.9',
  'Accept-Encoding': 'gzip, deflate, br',
  'Sec-Fetch-Dest':  'document',
  'Sec-Fetch-Mode':  'navigate',
  'Sec-Fetch-Site':  'none',
};

// ── Non-car keywords to reject from title ─────────────────
const REJECT_TITLE_KEYWORDS = [
  'motorcycle', 'motorbike', 'moto', 'harley', 'ducati', 'kawasaki', 'yamaha moto',
  'honda cbr', 'honda crf', 'suzuki gsx', 'bmw r1', 'dirt bike', 'atv', 'quad',
  'utv', 'polaris', 'can-am', 'sea-doo', 'jet ski', 'seadoo', 'boat', 'trailer',
  'rv ', 'motorhome', 'camper', 'snowmobile', 'ski-doo', 'skidoo',
  'kenworth', 'peterbilt', 'mack truck', 'semi truck', 'transport truck',
  'bus ', 'school bus', 'cargo van wanted', 'parts only', 'for parts',
];

function isLikelyCar(title: string): boolean {
  const t = title.toLowerCase();
  return !REJECT_TITLE_KEYWORDS.some(kw => t.includes(kw));
}

// ── Parse one data-sjs JSON blob, return all 12-16 digit IDs ─
function extractIdsFromScript(scriptContent: string): string[] {
  try {
    const str = JSON.stringify(JSON.parse(scriptContent));
    return [...str.matchAll(/"id":"(\d{12,16})"/g)].map(m => m[1]);
  } catch {
    return [];
  }
}

// ── Fetch grid page, return unique listing IDs ────────────
async function discoverListingIds(
  citySlug: string,
  priceBandParams: string,
): Promise<string[]> {
  const url = `https://www.facebook.com/marketplace/${citySlug}/vehicles?sortBy=creation_time_descend${priceBandParams}`;
  const res = await axios.get(url, { headers: BASE_HEADERS, timeout: 20_000, maxRedirects: 3, ...(proxyAgent ? { httpsAgent: proxyAgent, proxy: false } : {}) });
  const html = res.data as string;

  const allIds = new Set<string>();
  for (const match of html.matchAll(/<script[^>]+data-sjs[^>]*>([\s\S]*?)<\/script>/g)) {
    for (const id of extractIdsFromScript(match[1])) {
      allIds.add(id);
    }
  }
  return [...allIds];
}

// ── Parse vehicle data from a detail page's data-sjs scripts ─
function parseDetailPage(html: string, listingId: string): {
  title: string | null;
  price: number | null;
  currency: string;
  city: string | null;
  province: string | null;
  description: string | null;
  make: string | null;
  model: string | null;
  year: number | null;
  mileageKm: number | null;
  colourExt: string | null;
  colourInt: string | null;
  transmission: string | null;
  fuelType: string | null;
  condition: string | null;
  sellerType: string | null;
  vin: string | null;
  dealerName: string | null;
  images: string[];
  isSold: boolean;
  isLive: boolean;
} | null {
  // Find the main listing script — largest data-sjs script containing this listing's price
  const scripts = [...html.matchAll(/<script[^>]+data-sjs[^>]*>([\s\S]*?)<\/script>/g)];

  let target: Record<string, any> | null = null;
  let images: string[] = [];

  for (const s of scripts) {
    if (!s[1].includes(listingId) || !s[1].includes('listing_price')) continue;
    if (s[1].length < 20_000) continue; // skip tiny related-listings scripts

    let data: any;
    try { data = JSON.parse(s[1]); } catch { continue; }

    // Recursively find object with listing_price whose id matches
    function find(obj: any, depth = 0): any {
      if (depth > 20 || !obj || typeof obj !== 'object') return null;
      if (obj.listing_price && obj.id === listingId) return obj;
      for (const v of Object.values(obj)) {
        const r = find(v, depth + 1);
        if (r) return r;
      }
      return null;
    }

    target = find(data);
    if (target) break;
  }

  // Photos come from a separate (smaller) script
  for (const s of scripts) {
    if (!s[1].includes(listingId) || !s[1].includes('listing_photos')) continue;
    try {
      const data = JSON.parse(s[1]);
      function findPhotos(obj: any, depth = 0): string[] {
        if (depth > 20 || !obj || typeof obj !== 'object') return [];
        if (Array.isArray(obj.listing_photos)) {
          return obj.listing_photos
            .map((p: any) => p?.image?.uri as string)
            .filter(Boolean);
        }
        for (const v of Object.values(obj)) {
          const r = findPhotos(v, depth + 1);
          if (r.length) return r;
        }
        return [];
      }
      const found = findPhotos(data);
      if (found.length) { images = found; break; }
    } catch { }
  }

  if (!target) {
    // Fallback: try og tags only
    const ogTitle = html.match(/property="og:title" content="([^"]+)"/)?.[1] ?? null;
    const priceText = html.match(/"listing_price":\{"amount":"([\d.]+)","[^"]*":"[^"]*","currency":"([^"]+)"/);
    const city = html.match(/"city":"([^"]+)"/)?.[1] ?? null;
    const state = html.match(/"state":"([^"]+)"/)?.[1] ?? null;
    if (!ogTitle) return null;
    return {
      title: ogTitle, price: priceText ? parseFloat(priceText[1]) : null,
      currency: priceText?.[2] ?? 'CAD', city, province: state,
      description: html.match(/property="og:description" content="([^"]+)"/)?.[1] ?? null,
      make: null, model: null, year: null, mileageKm: null,
      colourExt: null, colourInt: null, transmission: null, fuelType: null,
      condition: null, sellerType: null, vin: null, dealerName: null,
      images, isSold: false, isLive: true,
    };
  }

  // Extract year from title e.g. "2018 Audi S5 ..."
  const titleStr: string = target.marketplace_listing_title ?? '';
  const yearMatch = titleStr.match(/\b(19[5-9]\d|20[0-3]\d)\b/);
  const year = yearMatch ? parseInt(yearMatch[1], 10) : null;

  // Mileage: unit is KILOMETERS or MILES
  const odo = target.vehicle_odometer_data;
  let mileageKm: number | null = null;
  if (odo?.value) {
    mileageKm = odo.unit === 'MILES'
      ? Math.round(odo.value * 1.60934)
      : odo.value;
  }

  const loc = target.location?.reverse_geocode;

  return {
    title:        titleStr || null,
    price:        target.listing_price?.amount ? parseFloat(target.listing_price.amount) : null,
    currency:     target.listing_price?.currency ?? 'CAD',
    city:         loc?.city ?? null,
    province:     loc?.state ?? null,
    description:  target.redacted_description?.text ?? null,
    make:         target.vehicle_make_display_name ?? null,
    model:        target.vehicle_model_display_name ?? null,
    year,
    mileageKm,
    colourExt:    target.vehicle_exterior_color ?? null,
    colourInt:    target.vehicle_interior_color ?? null,
    transmission: target.vehicle_transmission_type ?? null,
    fuelType:     target.vehicle_fuel_type ?? null,
    condition:    target.vehicle_condition ?? null,
    sellerType:   target.vehicle_seller_type ?? null,
    vin:          target.vehicle_identification_number ?? null,
    dealerName:   target.dealership_name ?? null,
    images,
    isSold:       target.is_sold ?? false,
    isLive:       target.is_live ?? true,
  };
}

// ── Fetch a listing detail page and build a RawPayload ────
async function fetchListingDetail(
  listingId: string,
  province: string,
): Promise<RawPayload | null> {
  const listingUrl = `https://www.facebook.com/marketplace/item/${listingId}/`;
  try {
    const res = await axios.get(listingUrl, {
      headers: BASE_HEADERS,
      timeout: 20_000,
      maxRedirects: 3,
      ...(proxyAgent ? { httpsAgent: proxyAgent, proxy: false } : {}),
    });
    const parsed = parseDetailPage(res.data as string, listingId);
    if (!parsed) return null;
    if (parsed.isSold || !parsed.isLive) return null;
    if (!parsed.price || parsed.price < 500) return null;
    if (parsed.title && !isLikelyCar(parsed.title)) return null;

    return {
      payload_id:        uuidv4(),
      source_id:         'facebook-mp-ca',
      source_category:   'social',
      listing_url:       listingUrl,
      scrape_timestamp:  new Date().toISOString(),
      connector_version: '2.0.0',
      raw_content:       JSON.stringify({
        title:           parsed.title,
        description:     parsed.description,
        priceCents:      parsed.price ? Math.round(parsed.price * 100) : null,
        year:            parsed.year,
        make:            parsed.make,
        model:           parsed.model,
        mileageKm:       parsed.mileageKm,
        colour:          parsed.colourExt,
        colourInterior:  parsed.colourInt,
        transmission:    parsed.transmission,
        fuelType:        parsed.fuelType,
        condition:       parsed.condition,
        _sellerType:     parsed.sellerType,
        dealerName:      parsed.dealerName,
        vin:             parsed.vin,
        location:        parsed.city && parsed.province
          ? `${parsed.city}, ${parsed.province}`
          : parsed.city ?? null,
      }),
      raw_content_type:  'json' as const,
      listing_images:    parsed.images.slice(0, 10),
      geo_region:        parsed.province ?? province,
      scrape_run_id:     uuidv4(),
      http_status:       200,
      proxy_used:        false,
      requires_auth:     false,
      is_dealer_listing: parsed.sellerType === 'DEALER' || parsed.dealerName != null,
      // Always true for FB — we always fetch the full detail page, so vision & CARFAX enrichment apply
      _advancedScrape:   true,
    };
  } catch {
    return null;
  }
}

// ── Main loop ─────────────────────────────────────────────
// Yields RawPayload objects continuously.
// Sweeps all city × price-band combinations, pauses between full sweeps.

export async function* fbNoAuthScraperLoop(
  seenUrls: Set<string>,
  log: (msg: string) => void,
  isStopping: () => boolean,
  cities: FbCity[] = FB_CITIES,
): AsyncGenerator<RawPayload> {
  const DETAIL_DELAY_MS   = 1_200;  // between detail page fetches
  const GRID_DELAY_MS     = 3_000;  // between grid page fetches
  const SWEEP_PAUSE_MS    = 5 * 60_000; // 5 min between full sweeps

  let sweepCount = 0;

  while (!isStopping()) {
    sweepCount++;
    let sweepNew = 0;

    for (const city of cities) {
      if (isStopping()) break;

      for (const band of FB_PRICE_BANDS) {
        if (isStopping()) break;

        // Discover listing IDs from grid page
        let ids: string[] = [];
        try {
          ids = await discoverListingIds(city.slug, band.params);
          log(`[fb-noauth] ${city.label}/${band.label} → ${ids.length} IDs discovered`);
        } catch (err) {
          log(`[fb-noauth] ${city.label}/${band.label} grid error: ${(err as Error).message.slice(0, 60)}`);
          await new Promise(r => setTimeout(r, GRID_DELAY_MS));
          continue;
        }

        await new Promise(r => setTimeout(r, GRID_DELAY_MS));

        // Filter to unseen
        const fresh = ids.filter(id => {
          const u = `https://www.facebook.com/marketplace/item/${id}/`;
          return !seenUrls.has(u);
        });

        log(`[fb-noauth] ${city.label}/${band.label} → ${fresh.length} new (${ids.length - fresh.length} already seen)`);

        // Fetch detail pages
        for (const id of fresh) {
          if (isStopping()) break;

          const listingUrl = `https://www.facebook.com/marketplace/item/${id}/`;
          seenUrls.add(listingUrl);

          const payload = await fetchListingDetail(id, city.province);
          await new Promise(r => setTimeout(r, DETAIL_DELAY_MS));

          if (!payload) continue;

          sweepNew++;
          yield payload;
        }
      }
    }

    const cityNames = cities.map(c => c.label).join('+');
    log(`[fb:${cityNames}] Sweep ${sweepCount} complete — ${sweepNew} new listings. Pausing ${SWEEP_PAUSE_MS / 60000}min…`);
    if (!isStopping()) {
      await new Promise(r => setTimeout(r, SWEEP_PAUSE_MS));
    }
  }

  log('[fb-noauth] scraper stopped');
}
