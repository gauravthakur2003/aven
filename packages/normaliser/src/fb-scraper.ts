/**
 * Facebook Marketplace Scraper — GraphQL Network Interception
 *
 * TWO-STEP STRATEGY (mirrors the Kijiji scraper pattern):
 *
 *   Step 1 — Grid scroll (fast discovery):
 *     Navigate to the FB Marketplace vehicles search URL and scroll down.
 *     FB fires internal GraphQL POST requests (/api/graphql) as the user scrolls,
 *     each returning ~10 listing summaries. We intercept these responses with a
 *     Playwright 'response' event listener and extract FBListing objects from the
 *     JSON. This gives us IDs, titles, prices, and thumbnail images quickly.
 *
 *   Step 2 — Detail page (full enrichment):
 *     For each new listing ID, navigate to /marketplace/item/<id>/ and intercept
 *     the detail GQL response. This contains vehicle_info (VIN, mileage, colour,
 *     drivetrain, all images, description) that the grid summary omits. If the
 *     SSR data from the grid is already rich enough (all core fields present),
 *     we skip the detail page entirely to save ~8s per listing.
 *
 * ONE VARIANT PER SESSION:
 *   FB detects multi-page automation patterns within a single browser context.
 *   To stay under the radar we run one city×price-band URL per browser session
 *   and restart the browser between variants. fbScraperLoop() in test-pipeline.ts
 *   manages this restart loop.
 *
 * AUTH:
 *   Reads fb-session.json (created by fb-auth-setup.ts) at startup.
 *   The session file contains Playwright storage state (cookies + localStorage).
 *   Also supports FB_STORAGE_STATE env var (base64-encoded JSON) for cloud deploys.
 *
 * OUTPUT:
 *   Yields RawPayload objects in the same shape as the Kijiji scraper,
 *   so the normaliser pipeline treats both sources identically.
 */

import { chromium, Browser, BrowserContext, Page, Response as PWResponse } from 'playwright';
import { v4 as uuidv4 } from 'uuid';
import * as fs from 'fs';
import * as path from 'path';
import { RawPayload } from './types';

// ── Config ────────────────────────────────────────────────

const SESSION_FILE        = path.join(__dirname, '..', 'fb-session.json');
const CONNECTOR_VERSION   = '1.1.0';
const SOURCE_ID           = 'facebook-mp-ca';

// Multi-city × price-band URL variants.
// Each city × price band shows a completely different set of listings.
// FB serves ~18 listings per URL in SSR. 8 cities × 5 bands = up to 720 listings per session.
export const FB_CITIES: Array<{ label: string; slug: string }> = [
  { label: 'toronto',      slug: 'toronto' },
  { label: 'mississauga',  slug: 'mississauga' },
  { label: 'brampton',     slug: 'brampton' },
  { label: 'markham',      slug: 'markham' },
  { label: 'hamilton',     slug: 'hamilton' },
  { label: 'oakville',     slug: 'oakville' },
  { label: 'scarborough',  slug: 'scarborough' },
  { label: 'vaughan',      slug: 'vaughan' },
];

export const FB_PRICE_BANDS: Array<{ label: string; params: string }> = [
  { label: 'all',      params: '' },
  { label: '$1-8k',    params: '&minPrice=500&maxPrice=8000' },
  { label: '$8-20k',   params: '&minPrice=8000&maxPrice=20000' },
  { label: '$20-45k',  params: '&minPrice=20000&maxPrice=45000' },
  { label: '$45k+',    params: '&minPrice=45000' },
];

export const FB_URL_VARIANTS: Array<{ label: string; url: string }> = FB_CITIES.flatMap(city =>
  FB_PRICE_BANDS.map(band => ({
    label: `${city.label}/${band.label}`,
    url:   `https://www.facebook.com/marketplace/${city.slug}/vehicles?sortBy=creation_time_descend${band.params}`,
  }))
);

// Grid scroll config
const MAX_SCROLLS         = 40;
const SCROLL_DELAY_MIN    = 1_000;   // reduced from 1200 — FB GQL fires reliably at this cadence
const SCROLL_DELAY_MAX    = 1_600;   // reduced from 2000
const IDLE_SCROLLS_LIMIT  = 4;       // reduced from 6 — abandon depleted variants faster

// Detail page config
const DETAIL_GQL_TIMEOUT  = 8_000;
const DETAIL_PAGE_DELAY   = 500;     // reduced from 700ms — still polite

// ── Types ─────────────────────────────────────────────────

export interface FBListing {
  id:            string;
  title:         string;
  priceAmount:   number | null;        // dollars (not cents)
  priceCurrency: string | null;
  paymentAmount: number | null;        // monthly/biweekly payment if shown
  paymentFreq:   string | null;        // 'monthly' | 'biweekly' | null
  description:   string | null;
  location:      string | null;
  primaryImage:  string | null;
  allImages:     string[];
  year:          number | null;
  make:          string | null;
  model:         string | null;
  trim:          string | null;
  mileageKm:     number | null;
  vin:           string | null;
  bodyType:      string | null;
  drivetrain:    string | null;
  fuelType:      string | null;
  transmission:  string | null;
  colour:        string | null;
  colourInterior: string | null;
  doors:         number | null;
  seats:         number | null;
  condition:     string | null;
  accidents:     number | null;
  owners:        number | null;
  sellerName:    string | null;
  sellerType:    'private' | 'dealer' | null;
}

// ── Session loading ───────────────────────────────────────

function loadStorageState(): object | null {
  if (process.env.FB_STORAGE_STATE) {
    try {
      const json = Buffer.from(process.env.FB_STORAGE_STATE, 'base64').toString('utf-8');
      return JSON.parse(json);
    } catch {
      console.error('[fb] FB_STORAGE_STATE env var is not valid base64 JSON — ignoring');
    }
  }
  if (fs.existsSync(SESSION_FILE)) {
    try {
      return JSON.parse(fs.readFileSync(SESSION_FILE, 'utf-8'));
    } catch {
      console.error('[fb] fb-session.json is corrupt — ignoring');
    }
  }
  return null;
}

// ── Session expiry check ──────────────────────────────────
// Inspects fb-session.json cookies for the earliest expiry.
// Returns the min expiry timestamp (seconds since epoch) among auth-critical
// cookies (c_user, xs, datr), or null if no session file exists.
// Logs a warning if any cookie expires within 24 hours.

function checkSessionExpiry(log: (msg: string) => void): void {
  if (!fs.existsSync(SESSION_FILE)) return;
  try {
    const state = JSON.parse(fs.readFileSync(SESSION_FILE, 'utf-8')) as {
      cookies?: Array<{ name: string; expires?: number }>;
    };
    const AUTH_COOKIES = new Set(['c_user', 'xs', 'datr', 'sb']);
    const nowSec = Date.now() / 1000;
    const WARN_WITHIN_SEC = 24 * 60 * 60; // 24 hours
    const REFRESH_WITHIN_SEC = 6 * 60 * 60; // 6 hours

    for (const cookie of state.cookies ?? []) {
      if (!AUTH_COOKIES.has(cookie.name)) continue;
      if (cookie.expires == null || cookie.expires <= 0) continue; // session cookie
      const expiresIn = cookie.expires - nowSec;
      if (expiresIn <= 0) {
        log(`[fb] SESSION WARNING: cookie '${cookie.name}' has already expired. Run: npx ts-node fb-auth-setup.ts`);
      } else if (expiresIn <= REFRESH_WITHIN_SEC) {
        const hrs = Math.round(expiresIn / 3600);
        log(`[fb] SESSION WARNING: cookie '${cookie.name}' expires in ~${hrs}h — refresh SOON: npx ts-node fb-auth-setup.ts`);
      } else if (expiresIn <= WARN_WITHIN_SEC) {
        const hrs = Math.round(expiresIn / 3600);
        log(`[fb] session notice: cookie '${cookie.name}' expires in ~${hrs}h`);
      }
    }
  } catch {
    // Non-fatal — session file may be in a different format
  }
}

// ── GraphQL listing extraction ────────────────────────────
// Facebook's GraphQL API returns deeply nested JSON. The schema varies between:
//   - Grid responses: listing objects nested under data.marketplace_search.feed_units[]
//   - Detail responses: listing object nested under data.marketplace_product_details_page
//
// Rather than hardcoding the path (which changes with FB deploys), we use a
// recursive approach: walk every object in the tree and identify listing nodes
// by the presence of 'marketplace_listing_title' (always a string on listing objects).
// This makes the extractor resilient to schema changes and works across both GQL types.

function extractListingsFromGQL(obj: unknown): FBListing[] {
  if (typeof obj !== 'object' || obj === null) return [];
  const o = obj as Record<string, unknown>;

  if (typeof o['marketplace_listing_title'] === 'string' && typeof o['id'] === 'string') {
    const listing = normaliseFBListing(o);
    return listing ? [listing] : [];
  }

  const results: FBListing[] = [];
  for (const val of Object.values(o)) {
    if (typeof val === 'object' && val !== null) {
      results.push(...extractListingsFromGQL(val));
    }
  }
  return results;
}

// Non-car leaf categories to reject — covers motorcycles, boats, RVs, quads, trailers, etc.
const REJECTED_CATEGORIES = new Set([
  'motorcycles', 'boats', 'rvs-campers', 'trailers', 'atvs-snowmobiles',
  'powersports', 'heavy-equipment', 'commercial-trucks', 'other-vehicles',
  'lawn-garden-tractors', 'golf-carts',
]);

// Body types that are definitely not passenger cars/trucks
const REJECTED_BODY_TYPES = new Set([
  'motorcycle', 'boat', 'rv', 'camper', 'trailer', 'atv', 'snowmobile',
  'golf cart', 'tractor', 'bus', 'motorhome',
]);

function isCarListing(o: Record<string, unknown>, title: string, bodyType: string | null): boolean {
  // Reject by FB leaf category
  const leafCat = String(o['marketplace_listing_leaf_vt_category_name'] ?? '').toLowerCase();
  const catId   = String(o['marketplace_listing_category_id'] ?? '');
  if (leafCat && !leafCat.includes('car') && !leafCat.includes('truck') && !leafCat.includes('suv') && !leafCat.includes('van')) {
    // Check if it's explicitly a non-car category
    for (const bad of REJECTED_CATEGORIES) {
      if (leafCat.includes(bad.replace(/-/g, ' '))) return false;
    }
  }

  // Reject by body type field
  if (bodyType) {
    const bt = bodyType.toLowerCase();
    for (const bad of REJECTED_BODY_TYPES) {
      if (bt.includes(bad)) return false;
    }
  }

  // Reject by title keywords — obvious non-car listings.
  // Why title-based: FB's leaf category filter catches most non-cars, but sellers
  // frequently miscategorize (e.g. listing a dirt bike under "Cars & Trucks").
  // Title matching is a fast second pass before we spend time on the detail page.
  //
  // NOTE: "polaris" and "ski-doo" appear twice — kept intentionally so each
  // category group is self-contained and readable without cross-referencing.
  const t = title.toLowerCase();
  const NON_CAR_TITLE_WORDS = [
    // ── Motorcycles ──
    'motorcycle', 'motorbike', 'dirt bike', 'sportbike', 'harley', 'kawasaki',
    'yamaha yz', 'honda cbr', 'suzuki gsxr',
    'triumph tt', 'triumph speed', 'triumph street', 'triumph tiger',
    'ducati', 'ktm duke', 'ktm exc', 'bmw gs', 'bmw r1', 'bmw s1',

    // ── Watercraft / boats ──
    'boat', 'jetski', 'jet ski', 'sea-doo', 'seadoo', 'waverunner', 'wave runner',
    'yamaha fx', 'fx cruiser', 'vx cruiser', 'ex deluxe', 'gp1800',   // Yamaha watercraft model numbers
    'kayak', 'canoe',
    'alumacraft', 'lund boat', 'tracker boat', 'princecraft', 'springbok', 'boston whaler',

    // ── Snowmobiles ──
    'snowmobile', 'ski-doo', 'skidoo', 'polaris sled', 'arctic cat', 'lynx sled',

    // ── ATVs / side-by-sides ──
    'atv', 'quad bike', 'side-by-side', 'rzr', 'utv',
    'can-am', 'can am', 'maverick x3', 'defender hd',   // Can-Am ATV / side-by-side model names

    // ── RVs / trailers / campers ──
    'trailer', 'camper', 'rv', 'motorhome',
    'fifth wheel', '5th wheel', 'toyhauler', 'toy hauler', 'westbrook',
    'coachmen', 'jayco', 'keystone', 'forest river', 'airstream', 'winnebago',
    'fleetwood', 'heartland', 'dutchmen', 'crossroads', 'palomino',
    'forester windchaser', 'windchaser', 'tiffin', 'newmar', 'monaco coach',

    // ── Heavy equipment / farm machinery ──
    'tractor', 'forklift', 'excavator', 'bulldozer',
    'mini excavator', 'mini dumper', 'dumper', 'skid steer', 'telehandler',
    'snow blower', 'snowblower', 'snow plow', 'blade attachment', 'scraper blade', 'snow blade',
    'john deere', 'kubota', 'case ih', 'new holland tractor',

    // ── Commercial / semi trucks ──
    // These brands/models never appear on passenger vehicles
    'kenworth', 'peterbilt', 'freightliner', 'mack truck', 'volvo truck', 'international truck',
    'volvo vnl', 'volvo vnr', 'volvo day cab', 'volvo t680', 'volvo t880',
    'gmc t7500', 'gmc t6500', 'gmc c5500', 'gmc c6500', 'gmc c7500', 'gmc topkick',
    'international maxforce', 'international max force', 'international lonestar',
    'semi truck', 'semi-truck', 'dump truck', 'flatbed truck', 'box truck',
    'day cab', 'sleeper cab', 'tandem axle',
    // Isuzu / GM commercial truck model codes (NPR, NRR, NQR = medium-duty trucks)
    ' nrr', ' npr', ' nqr', ' nls', ' nps', 'hooklift', 'hook lift', '5-ton', '5 ton truck',
    // Commercial trailer brands
    'wabash', 'stoughton', 'great dane trailer', 'utility trailer', 'flatdeck',

    // ── Light recreational / micro-mobility ──
    'go-kart', 'go kart', 'golf cart',
    'e-bike', 'electric bike', 'scooter',

    // ── Driving school / service ads ──
    // These appear in the vehicles category but are services, not vehicles for sale
    'g2 license', 'g license', 'g-license', 'g2-license', 'driving school', 'driving lesson', 'driver training',

    // ── Industrial equipment miscategorized as vehicles ──
    'sa-200', 'lincoln electric', 'welder', 'welding machine',

    // ── Parts / accessories (not a whole vehicle) ──
    'engine swap', 'car parts', 'auto parts', 'rims for sale', 'tires for sale',

    // NOTE: "Sprinter" is omitted intentionally — passenger Sprinter vans are valid listings.
    //       Cargo/fleet Sprinters are filtered by the LLM in M2a.
  ];
  for (const kw of NON_CAR_TITLE_WORDS) {
    if (t.includes(kw)) return false;
  }

  // Require a year in the title OR structured year field — services/junk rarely have years
  const hasYear = /\b(19[5-9]\d|20[012]\d)\b/.test(title);
  const structuredYear = bodyType != null || (o as Record<string, unknown>)['vehicle_info'] != null;
  if (!hasYear && !structuredYear) return false;

  return true;
}

function normaliseFBListing(o: Record<string, unknown>): FBListing | null {
  const id = String(o['id'] ?? '');
  if (!id) return null;
  const title = String(o['marketplace_listing_title'] ?? '').trim();
  if (!title) return null;

  // ── Price ───────────────────────────────────────────────
  let priceAmount: number | null = null;
  let priceCurrency: string | null = 'CAD';
  let paymentAmount: number | null = null;
  let paymentFreq:   string | null = null;

  const priceObj = (o['listing_price'] ?? o['price']) as Record<string, unknown> | null;
  if (priceObj) {
    const raw = (priceObj['amount'] ?? priceObj['amount_with_offset_dollars']) as number | string | null;
    if (raw != null) {
      const n = Number(raw);
      // Amounts > 100,000 are in cents (convert); otherwise already dollars
      priceAmount = n > 100_000 ? Math.round(n / 100) : n;
    }
    priceCurrency = String(priceObj['currency'] ?? 'CAD');
  }

  // Payment terms (dealer financing info): e.g. "$299/mo" or "$149 biweekly"
  // FB sometimes puts this in listing_payment_info or as a subtitle
  const payInfo = o['listing_payment_info'] as Record<string, unknown> | null;
  if (payInfo) {
    const pamt = Number(payInfo['monthly_payment'] ?? payInfo['payment_amount'] ?? 0);
    if (pamt > 0) {
      paymentAmount = pamt;
      paymentFreq   = String(payInfo['payment_frequency'] ?? 'monthly').toLowerCase();
    }
  }

  // Also check custom_sub_titles for payment info like "$299/mo" and mileage "125K km"
  const subtitles = o['custom_sub_titles_with_rendering_flags'] as unknown[] | null;
  if (Array.isArray(subtitles)) {
    for (const sub of subtitles) {
      const s   = sub as Record<string, unknown>;
      const txt = String(s['subtitle'] ?? '').trim();
      if (!txt) continue;

      // Mileage: "372K km", "125,000 km", "72K mi"
      if (!o['vehicle_info']) {                     // only use subtitle if vehicle_info absent
        const km = parseSubtitleMileage(txt);
        // store temporarily in the object so vehicle_info block below can find it
        if (km && !(o as Record<string, unknown>)['_subtitleMileageKm']) {
          (o as Record<string, unknown>)['_subtitleMileageKm'] = km;
        }
      }

      // Payment: "$299/mo" or "$149 biweekly"
      if (!paymentAmount) {
        const pm = parseSubtitlePayment(txt);
        if (pm) { paymentAmount = pm.amount; paymentFreq = pm.freq; }
      }

      // Estimated price from payment if no sticker price
      if (!priceAmount && paymentAmount) {
        // Rough reverse-calc: biweekly * 26 * 6yr  OR  monthly * 12 * 6yr
        const periods = paymentFreq === 'biweekly' ? 26 * 6 : 12 * 6;
        priceAmount = Math.round(paymentAmount * periods * 0.7); // 0.7 strips interest estimate
      }
    }
  }

  // ── Price fallback from description ──────────────────────
  // priceAmount=0 usually means FB returned the field but the value is 0 (free/error).
  // Also handles when priceAmount is null but description contains a price pattern.
  if (!priceAmount || priceAmount === 0) {
    const descRaw = (o['redacted_description'] ?? o['description']) as Record<string, unknown> | null;
    const descText = descRaw
      ? String(descRaw['text'] ?? descRaw['styled_text'] ?? '').trim()
      : typeof o['description'] === 'string' ? (o['description'] as string).trim() : '';
    if (descText) {
      const extracted = extractPriceFromText(descText);
      if (extracted) {
        priceAmount = extracted.price;
        // If a qualifier like OBO/Firm/Negotiable was found, note it via a temp field
        if (extracted.qualifier) {
          (o as Record<string, unknown>)['_priceQualifier'] = extracted.qualifier;
        }
      }
    }
  }

  // ── Location ─────────────────────────────────────────────
  // Detail pages: location_text.text ("Toronto, ON")
  // Grid/GQL:     location.reverse_geocode.{city,state}
  let location: string | null = null;
  const locationText = o['location_text'] as Record<string, unknown> | null;
  if (locationText) {
    location = toStr(locationText['text']);
  }
  if (!location) {
    const locationObj = o['location'] as Record<string, unknown> | null;
    if (locationObj) {
      const rg = locationObj['reverse_geocode_detailed'] ?? locationObj['reverse_geocode'] ?? locationObj['name'];
      if (typeof rg === 'string') {
        location = rg.trim() || null;
      } else if (rg && typeof rg === 'object') {
        const rgObj = rg as Record<string, unknown>;
        const city  = toStr(rgObj['city'] ?? rgObj['display_name']);
        const state = toStr(rgObj['state'] ?? rgObj['state_code']);
        location = [city, state].filter(Boolean).join(', ') || null;
      }
    }
  }

  // ── Description ──────────────────────────────────────────
  // Detail pages use 'redacted_description', grid uses 'description'
  const descObj = (o['redacted_description'] ?? o['description']) as Record<string, unknown> | null;
  const description = descObj
    ? String(descObj['text'] ?? descObj['styled_text'] ?? '').trim() || null
    : typeof o['description'] === 'string' ? (o['description'] as string).trim() || null : null;

  // ── Images ───────────────────────────────────────────────
  const primaryImageObj = (o['primary_listing_photo'] ?? o['listing_photo']) as Record<string, unknown> | null;
  const primaryImage = primaryImageObj ? extractImageUri(primaryImageObj) : null;

  const allImages: string[] = [];
  if (primaryImage) allImages.push(primaryImage);

  const photosArr = (o['listing_photos'] ?? o['photos']) as unknown[] | null;
  if (Array.isArray(photosArr)) {
    for (const p of photosArr) {
      const uri = extractImageUri(p as Record<string, unknown>);
      if (uri && !allImages.includes(uri)) allImages.push(uri);
    }
  }

  // ── Vehicle info ─────────────────────────────────────────
  // Two schemas depending on GQL query type:
  //   Detail page SSR: flat vehicle_* fields directly on the listing object
  //   Grid GQL / old detail: nested vehicle_info object
  const vi = o['vehicle_info'] as Record<string, unknown> | null;

  let year:          number | null = null;
  let make:          string | null = null;
  let model:         string | null = null;
  let trim:          string | null = null;
  let mileageKm:     number | null = null;
  let vin:           string | null = null;
  let bodyType:      string | null = null;
  let drivetrain:    string | null = null;
  let fuelType:      string | null = null;
  let transmission:  string | null = null;
  let colour:        string | null = null;
  let colourInterior: string | null = null;
  let doors:         number | null = null;
  let seats:         number | null = null;
  let accidents:     number | null = null;
  let owners:        number | null = null;

  // Schema A — nested vehicle_info (older GQL format)
  if (vi && typeof vi === 'object') {
    year         = toInt(vi['year'] ?? vi['model_year']);
    make         = toStr(vi['make']);
    model        = toStr(vi['model']);
    trim         = toStr(vi['trim'] ?? vi['sub_model']);
    vin          = toStr(vi['vehicle_identification_number'] ?? vi['vin']);
    bodyType     = toStr(vi['body_type'] ?? vi['body_style']);
    drivetrain   = toStr(vi['drivetrain'] ?? vi['drive_train'] ?? vi['drive_type']);
    fuelType     = toStr(vi['fuel_type'] ?? vi['fuel']);
    transmission = toStr(vi['transmission'] ?? vi['transmission_type']);
    colour       = toStr(vi['exterior_color'] ?? vi['color'] ?? vi['colour'] ?? vi['exterior_colour']);
    colourInterior = toStr(vi['interior_color'] ?? vi['interior_colour']);
    doors        = toInt(vi['doors'] ?? vi['num_doors']);
    seats        = toInt(vi['seats'] ?? vi['seating_capacity']);
    accidents    = toInt(vi['accident_count'] ?? vi['accidents']);
    owners       = toInt(vi['owner_count'] ?? vi['owners']);

    const mObj = (vi['mileage_data'] ?? vi['mileage']) as Record<string, unknown> | null;
    if (mObj) {
      const rawMileage = Number(mObj['value'] ?? mObj['distance_value'] ?? 0);
      const unit = String(mObj['unit'] ?? mObj['distance_unit'] ?? 'km').toLowerCase();
      if (rawMileage > 0) {
        mileageKm = unit.includes('mile') ? Math.round(rawMileage * 1.60934) : rawMileage;
      }
    }
    if (!mileageKm) mileageKm = toInt(vi['mileage_km'] ?? vi['odometer']);
  }

  // Schema B — flat vehicle_* fields (detail page SSR format)
  // These are direct keys on the listing object: vehicle_exterior_color, vehicle_odometer_data, etc.
  make         = make  ?? toStr(o['vehicle_make_display_name']);
  model        = model ?? toStr(o['vehicle_model_display_name']);
  trim         = trim  ?? toStr(o['vehicle_trim_display_name']);
  vin          = vin   ?? toStr(o['vehicle_identification_number']);
  bodyType     = bodyType  ?? toStr(o['vehicle_body_style'] ?? o['vehicle_body_type']);
  drivetrain   = drivetrain ?? toStr(o['vehicle_drivetrain_type'] ?? o['vehicle_drivetrain']);
  colour       = colour ?? toStr(o['vehicle_exterior_color']);
  colourInterior = colourInterior ?? toStr(o['vehicle_interior_color']);
  accidents    = accidents ?? toInt(o['vehicle_accident_count']);

  // fuel type: "GASOLINE" → "Gasoline"
  if (!fuelType) {
    const rawFuel = toStr(o['vehicle_fuel_type']);
    if (rawFuel) fuelType = rawFuel.charAt(0) + rawFuel.slice(1).toLowerCase();
  }

  // transmission: "AUTOMATIC" → "Automatic"
  if (!transmission) {
    const rawTrans = toStr(o['vehicle_transmission_type']);
    if (rawTrans) transmission = rawTrans.charAt(0) + rawTrans.slice(1).toLowerCase();
  }

  // owners: "ONE" → 1, "TWO" → 2, etc.
  if (owners == null) {
    const ownerStr = toStr(o['vehicle_number_of_owners']);
    if (ownerStr) {
      const ownerMap: Record<string, number> = { ONE: 1, TWO: 2, THREE: 3, FOUR: 4, FIVE: 5 };
      owners = ownerMap[ownerStr.toUpperCase()] ?? toInt(ownerStr);
    }
  }

  // seller type from vehicle_seller_type if not set from seller object
  // (will be overridden by the seller block below if that has better data)
  const vehicleSellerType = toStr(o['vehicle_seller_type']);

  // Mileage from vehicle_odometer_data: {unit: "KILOMETERS", value: 14700}
  if (!mileageKm) {
    const odomObj = o['vehicle_odometer_data'] as Record<string, unknown> | null;
    if (odomObj) {
      const rawKm   = Number(odomObj['value'] ?? 0);
      const unit    = String(odomObj['unit'] ?? 'KILOMETERS').toUpperCase();
      if (rawKm > 0) {
        mileageKm = unit.includes('MILE') ? Math.round(rawKm * 1.60934) : rawKm;
      }
    }
  }

  // Mileage fallback: subtitle text ("372K km", "125,000 km")
  if (!mileageKm) {
    const subtitleKm = toInt((o as Record<string, unknown>)['_subtitleMileageKm']);
    if (subtitleKm) mileageKm = subtitleKm;
  }

  // Year/make/model fallback: parse from title
  if (!year || !make || !model) {
    const tp = parseTitleFields(title);
    year  = year  ?? tp.year;
    make  = make  ?? tp.make;
    model = model ?? tp.model;
  }

  // Mileage final fallback: extract from title (e.g. "2019 BMW M240 🤘 100300 Miles")
  // Some FB sellers embed mileage in the listing title instead of a subtitle.
  if (!mileageKm) {
    const titleMileage = parseMileageAnywhere(title);
    if (titleMileage) mileageKm = titleMileage;
  }

  // ── Seller ───────────────────────────────────────────────
  let sellerName: string | null = null;
  let sellerType: 'private' | 'dealer' | null = null;

  const sellerObj = (o['marketplace_listing_seller'] ?? o['listing_owner']) as Record<string, unknown> | null;
  if (sellerObj) {
    sellerName = toStr(sellerObj['name']);
    const tn   = String(sellerObj['__typename'] ?? '').toLowerCase();
    sellerType = tn.includes('page') || tn.includes('business') ? 'dealer' : 'private';
  } else {
    const storyActors = (o['story']
      ? (o['story'] as Record<string, unknown>)['actors']
      : o['actors']) as unknown[] | null;
    if (Array.isArray(storyActors) && storyActors.length > 0) {
      const actor = storyActors[0] as Record<string, unknown>;
      sellerName  = toStr(actor['name']);
      sellerType  = String(actor['__typename'] ?? '').toLowerCase().includes('page') ? 'dealer' : 'private';
    }
  }

  // Seller type fallback from vehicle_seller_type
  if (!sellerType && vehicleSellerType) {
    sellerType = vehicleSellerType.toLowerCase().includes('dealer') ? 'dealer' : 'private';
  }

  // ── Condition ────────────────────────────────────────────
  const condition = toStr(o['condition_display_name'] ?? o['item_condition'] ?? o['condition']);

  const result: FBListing = {
    id, title, priceAmount, priceCurrency, paymentAmount, paymentFreq,
    description, location, primaryImage, allImages,
    year, make, model, trim, mileageKm, vin,
    bodyType, drivetrain, fuelType, transmission,
    colour, colourInterior, doors, seats,
    condition, accidents, owners,
    sellerName, sellerType,
  };

  // Geography filter: only keep Canadian listings (Ontario / GTA area).
  // FB sometimes surfaces US listings when it runs low on local inventory.
  // US state codes that appear as ", CA" ", NY" etc. — drop them.
  if (result.location) {
    const US_STATES = /,\s*(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\b/i;
    if (US_STATES.test(result.location)) return null;
  }

  // Price sanity: a real car listing is $800–$500,000 OR has no price (price negotiated)
  // Reject $0–$799 listings — spam, placeholder prices, or non-car items
  if (result.priceAmount !== null && result.priceAmount < 800) return null;

  // Dealer payment-as-price filter: some dealers list monthly payments (~$500–$2500) instead
  // of sale prices. If the price is under $3,000 but the listing has paymentAmount too,
  // the priceAmount is almost certainly the monthly payment — clear it and use the estimate.
  if (
    result.priceAmount !== null &&
    result.priceAmount < 3_000 &&
    result.paymentAmount !== null &&
    result.paymentAmount > 0
  ) {
    // The "price" is really the payment amount duplicated — clear it so the LLM
    // receives the payment info and can estimate the real price instead.
    result.priceAmount = null;
  }

  // Final gate: reject anything that isn't a car/truck/SUV/van
  if (!isCarListing(o, title, result.bodyType)) return null;

  return result;
}

// ── Helper: parse mileage from subtitle text ──────────────
// Subtitle text starts with the mileage number: "372K km", "372,000 km", "72K mi", "72,000 miles"

function parseSubtitleMileage(text: string): number | null {
  const m = text.match(/^([\d,]+)(K)?\s*(km|mi|miles?)/i);
  if (!m) return null;
  let n = parseFloat(m[1].replace(/,/g, ''));
  if (m[2]?.toUpperCase() === 'K') n *= 1000;
  return m[3].toLowerCase().startsWith('mi') ? Math.round(n * 1.60934) : Math.round(n);
}

// ── Helper: parse mileage from anywhere in a string ───────
// For titles where mileage appears mid-string: "BMW X5 🤘 100300 Miles"

function parseMileageAnywhere(text: string): number | null {
  // Look for number followed by K? km|mi|miles — anywhere in the string
  const m = text.match(/\b([\d,]+)(K)?\s*(km|mi|miles?)\b/i);
  if (!m) return null;
  let n = parseFloat(m[1].replace(/,/g, ''));
  if (n < 100 || n > 1_000_000) return null;  // sanity: 100–1,000,000
  if (m[2]?.toUpperCase() === 'K') n *= 1000;
  return m[3].toLowerCase().startsWith('mi') ? Math.round(n * 1.60934) : Math.round(n);
}

// ── Helper: parse payment from subtitle text ──────────────
// Handles: "$299/mo", "$149 biweekly", "$299/month"

function parseSubtitlePayment(text: string): { amount: number; freq: string } | null {
  const m = text.match(/\$?([\d,]+)\s*\/?\s*(mo(?:nth)?|biweekly|bi-weekly|week)/i);
  if (!m) return null;
  const amount = parseFloat(m[1].replace(/,/g, ''));
  const freq   = m[2].toLowerCase().startsWith('bi') ? 'biweekly' : 'monthly';
  return { amount, freq };
}

// ── Helper: extract price from description text ───────────
// Finds "$12,500", "$12500 OBO", "asking $12,500 firm", etc.

function extractPriceFromText(text: string): { price: number; qualifier: string | null } | null {
  const m = text.match(/\$\s*([\d,]+)(?:\s*(obo|firm|negotiable|neg\.?|or best offer))?/i);
  if (!m) return null;
  const price = parseFloat(m[1].replace(/,/g, ''));
  if (price < 500 || price > 500_000) return null; // sanity range for a vehicle
  return { price, qualifier: m[2] ? m[2].toLowerCase() : null };
}

// ── Helper: title parser ──────────────────────────────────
// Fallback for "2021 Toyota Camry SE" / "Toyota Camry 2021"

function parseTitleFields(title: string): { year: number | null; make: string | null; model: string | null } {
  const yearMatch = title.match(/\b(19[5-9]\d|20[012]\d)\b/);
  const year = yearMatch ? parseInt(yearMatch[1], 10) : null;
  const withoutYear = title.replace(/\b(19[5-9]\d|20[012]\d)\b/, '').trim();
  const words = withoutYear.split(/[\s|,–\-]+/).filter(Boolean);
  return { year, make: words[0] ?? null, model: words[1] ?? null };
}

// ── Helper: image URI extraction ─────────────────────────

function extractImageUri(obj: Record<string, unknown>): string | null {
  if (!obj) return null;
  const inner = (obj['image'] ?? obj) as Record<string, unknown>;
  const uri = String(inner['uri'] ?? inner['src'] ?? '').trim();
  return uri.startsWith('http') ? uri : null;
}

// ── Helpers: type coercions ───────────────────────────────

function toInt(val: unknown): number | null {
  if (val == null) return null;
  const n = parseInt(String(val), 10);
  return isNaN(n) ? null : n;
}

function toStr(val: unknown): string | null {
  if (val == null) return null;
  const s = String(val).trim();
  return s || null;
}

// ── SSR completeness check ────────────────────────────────
// Returns true if the listing is missing critical fields that only the detail
// page can supply (description, VIN, colour, drivetrain).
// Returns false when the SSR data is already rich enough to skip the detail page —
// this avoids the 8s+ detail-page round-trip for the majority of listings.

function needsDetailPage(listing: FBListing): boolean {
  const missingCritical =
    !listing.description &&
    !listing.vin &&
    !listing.colour &&
    !listing.drivetrain;

  // Also require the core fields that make a listing useful
  const hasCoreData =
    !!listing.title &&
    (listing.priceAmount != null || listing.paymentAmount != null) &&
    listing.mileageKm != null &&
    !!listing.location &&
    !!listing.make &&
    !!listing.model &&
    listing.year != null &&
    listing.allImages.length >= 1;

  // Skip detail page only when core data is complete AND no critical fields are missing
  return !hasCoreData || missingCritical;
}

// Returns a list of field names missing from an FBListing (for logging).
function missingDetailFields(listing: FBListing): string[] {
  const missing: string[] = [];
  if (!listing.description) missing.push('description');
  if (!listing.vin)         missing.push('vin');
  if (!listing.colour)      missing.push('colour');
  if (!listing.drivetrain)  missing.push('drivetrain');
  if (!listing.make)        missing.push('make');
  if (!listing.model)       missing.push('model');
  if (listing.year == null) missing.push('year');
  if (listing.mileageKm == null) missing.push('mileageKm');
  if (!listing.location)    missing.push('location');
  if (listing.allImages.length === 0) missing.push('images');
  return missing;
}

// ── Detail page fetcher ───────────────────────────────────
// Visits a single listing detail page and returns an enriched FBListing.
// The detail GQL contains full vehicle_info, all images, description, VIN, etc.
// Returns null on timeout or error — caller falls back to grid data.

async function fetchListingDetail(
  context: BrowserContext,
  gridListing: FBListing,
  log: (msg: string) => void,
): Promise<FBListing> {
  const detailUrl = `https://www.facebook.com/marketplace/item/${gridListing.id}/`;
  const detailPage = await context.newPage();

  // Collect all GQL responses that contain marketplace_listing_title
  const candidates: FBListing[] = [];

  const responseHandler = async (response: PWResponse) => {
    if (!response.url().includes('/api/graphql')) return;
    if (response.request().method() !== 'POST') return;
    if (response.status() !== 200) return;
    try {
      const text = await response.text();
      for (const line of text.split('\n').filter(Boolean)) {
        try {
          const parsed = JSON.parse(line);
          const found  = extractListingsFromGQL(parsed);
          candidates.push(...found.filter(l => l.id === gridListing.id));
        } catch { /* skip */ }
      }
    } catch { /* skip */ }
  };

  detailPage.on('response', responseHandler);

  try {
    await detailPage.goto(detailUrl, { waitUntil: 'domcontentloaded', timeout: 20_000 });

    // ── Parse SSR script tags (FB embeds full listing data server-side) ──────
    try {
      const ssrText = await detailPage.evaluate(`
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
      if (ssrText) {
        for (const line of String(ssrText).split('\n').filter(Boolean)) {
          try {
            const parsed = JSON.parse(line);
            candidates.push(...extractListingsFromGQL(parsed).filter(l => l.id === gridListing.id));
          } catch { /* skip */ }
        }
      }
    } catch { /* skip */ }

    // Also wait briefly for any live GQL responses (up to DETAIL_GQL_TIMEOUT)
    const deadline = Date.now() + DETAIL_GQL_TIMEOUT;
    while (Date.now() < deadline) {
      await detailPage.waitForTimeout(500);
      const best = pickBestListing(candidates, gridListing);
      if (best.mileageKm != null || best.colour != null || (best.description && best.description.length > 50)) {
        return best;
      }
    }

    return pickBestListing(candidates, gridListing);

  } catch (err) {
    log(`[fb] detail fetch error for ${gridListing.id}: ${(err as Error).message}`);
    return gridListing;
  } finally {
    detailPage.off('response', responseHandler);
    await detailPage.close().catch(() => {});
  }
}

// ── Merge helper ──────────────────────────────────────────
// Given multiple GQL responses for the same listing, pick the richest one.
// Falls back to grid data for any null field.

function pickBestListing(candidates: FBListing[], fallback: FBListing): FBListing {
  if (candidates.length === 0) return fallback;

  // Score = number of non-null fields
  const score = (l: FBListing) =>
    Object.values(l).filter(v => v != null && v !== '' && !(Array.isArray(v) && v.length === 0)).length;

  const best = candidates.reduce((a, b) => (score(a) >= score(b) ? a : b));

  // Merge: use best for each field, fall back to grid for any null
  return {
    id:             best.id,
    title:          best.title            || fallback.title,
    priceAmount:    best.priceAmount      ?? fallback.priceAmount,
    priceCurrency:  best.priceCurrency    ?? fallback.priceCurrency,
    paymentAmount:  best.paymentAmount    ?? fallback.paymentAmount,
    paymentFreq:    best.paymentFreq      ?? fallback.paymentFreq,
    description:    best.description      ?? fallback.description,
    location:       best.location         ?? fallback.location,
    primaryImage:   best.primaryImage     ?? fallback.primaryImage,
    allImages:      best.allImages.length > fallback.allImages.length ? best.allImages : fallback.allImages,
    year:           best.year             ?? fallback.year,
    make:           best.make             ?? fallback.make,
    model:          best.model            ?? fallback.model,
    trim:           best.trim             ?? fallback.trim,
    mileageKm:      best.mileageKm        ?? fallback.mileageKm,
    vin:            best.vin              ?? fallback.vin,
    bodyType:       best.bodyType         ?? fallback.bodyType,
    drivetrain:     best.drivetrain       ?? fallback.drivetrain,
    fuelType:       best.fuelType         ?? fallback.fuelType,
    transmission:   best.transmission     ?? fallback.transmission,
    colour:         best.colour           ?? fallback.colour,
    colourInterior: best.colourInterior   ?? fallback.colourInterior,
    doors:          best.doors            ?? fallback.doors,
    seats:          best.seats            ?? fallback.seats,
    condition:      best.condition        ?? fallback.condition,
    accidents:      best.accidents        ?? fallback.accidents,
    owners:         best.owners           ?? fallback.owners,
    sellerName:     best.sellerName       ?? fallback.sellerName,
    sellerType:     best.sellerType       ?? fallback.sellerType,
  };
}

// ── Payload builder ───────────────────────────────────────

function buildPayload(listing: FBListing): RawPayload {
  const listingUrl = `https://www.facebook.com/marketplace/item/${listing.id}/`;
  return {
    payload_id:        uuidv4(),
    source_id:         SOURCE_ID,
    source_category:   'social',
    listing_url:       listingUrl,
    scrape_timestamp:  new Date().toISOString(),
    connector_version: CONNECTOR_VERSION,
    raw_content:       JSON.stringify(listing),
    raw_content_type:  'json',
    listing_images:    listing.allImages,
    geo_region:        'ON-GTA',
    scrape_run_id:     uuidv4(),
    http_status:       200,
    proxy_used:        false,
    requires_auth:     true,
    is_dealer_listing: listing.sellerType === 'dealer' ? true
                     : listing.sellerType === 'private' ? false
                     : null,
  };
}

// ── Concurrent detail-page fetcher ───────────────────────
// Fetches detail pages for a batch of listings in parallel (CONCURRENCY at a time).
// Each fetch opens its own page, so they don't block each other.
// A 200ms stagger between concurrent page opens avoids FB detecting simultaneous requests.

async function* fetchDetailsConcurrent(
  context: BrowserContext,
  listings: FBListing[],
  log: (msg: string) => void,
  CONCURRENCY = 3,
): AsyncGenerator<FBListing> {
  for (let i = 0; i < listings.length; i += CONCURRENCY) {
    const batch = listings.slice(i, i + CONCURRENCY);
    const results = await Promise.allSettled(
      batch.map((listing, batchIdx) =>
        new Promise<FBListing>((resolve, reject) => {
          // Stagger page opens by 200ms per slot to avoid simultaneous requests
          setTimeout(
            () => fetchListingDetail(context, listing, log).then(resolve, reject),
            batchIdx * 200,
          );
        })
      )
    );
    for (const result of results) {
      if (result.status === 'fulfilled' && result.value) {
        yield result.value;
      } else if (result.status === 'rejected') {
        log(`[fb] detail fetch failed: ${result.reason}`);
      }
    }
  }
}

// ── Main scraper ──────────────────────────────────────────

export async function* scrapeFacebook(
  seenUrls: Set<string>,
  log: (msg: string) => void,
  isStopping: () => boolean,
  variant?: { label: string; url: string },  // if provided, scan only this one URL
): AsyncGenerator<RawPayload> {
  let browser: Browser | null = null;
  let context: BrowserContext | null = null;

  try {
    browser = await chromium.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-blink-features=AutomationControlled',
      ],
    });

    const contextOptions = {
      viewport:     { width: 1280, height: 900 },
      userAgent:    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
      locale:       'en-CA',
      timezoneId:   'America/Toronto',
      extraHTTPHeaders: { 'Accept-Language': 'en-CA,en;q=0.9' },
    };
    context = await browser.newContext(contextOptions);

    await context.addInitScript(`
      Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
      Object.defineProperty(navigator, 'plugins', {
        get: () => [{ name: 'Chrome PDF Plugin' }, { name: 'Chrome PDF Viewer' }, { name: 'Native Client' }],
      });
      window.chrome = { runtime: {}, loadTimes: () => ({}), csi: () => ({}) };
    `);

    // ── Shared GQL buffer and response listener ───────────
    // Attached to the context (not a page) so it captures GQL from all pages.
    const gridBuffer: FBListing[] = [];

    const gqlResponseHandler = async (response: PWResponse) => {
      if (!response.url().includes('/api/graphql')) return;
      if (response.request().method() !== 'POST') return;
      if (response.status() !== 200) return;
      try {
        const text = await response.text();
        for (const line of text.split('\n').filter(Boolean)) {
          try {
            const parsed = JSON.parse(line);
            gridBuffer.push(...extractListingsFromGQL(parsed));
          } catch { /* skip */ }
        }
      } catch { /* skip */ }
    };
    context.on('response', gqlResponseHandler);

    let totalYielded = 0;

    // ── Helper: hash a string quickly for script dedup ────
    const quickHash = (s: string) => {
      let h = 0;
      for (let i = 0; i < Math.min(s.length, 200); i++) h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
      return h;
    };

    // ── Scan URL variant(s) ───────────────────────────────
    // If a specific variant was passed in, scan only that one.
    // Otherwise scan all variants (legacy behaviour).
    // One-variant-per-session is strongly preferred: FB closes the connection
    // after detecting multiple page navigations within a single browser context.
    const variantsToScan = variant ? [variant] : FB_URL_VARIANTS;

    for (const currentUrl of variantsToScan) {
      {  // block to keep indentation consistent with old batch code
      if (isStopping()) break;

      log(`[fb] Loading variant: ${currentUrl.label}`);

      const gridPage = await context.newPage();
      // Per-variant script hash dedup (reset for each URL so new scripts are parsed)
      const parsedScriptHashes = new Set<number>();

      // Parse all listing-bearing script tags on the current page state.
      // FB embeds JSON inside requireLazy/ScheduledServerJS wrappers.
      const parsePageScripts = async () => {
        try {
          const allScripts = await gridPage.evaluate(`
            (function() {
              return Array.from(document.querySelectorAll('script'))
                .map(s => s.textContent || '')
                .filter(t => t.includes('marketplace_listing_title'));
            })()
          `);
          for (const text of allScripts as string[]) {
            const hash = quickHash(text);
            if (parsedScriptHashes.has(hash)) continue;
            parsedScriptHashes.add(hash);

            // Strategy 1: line-by-line JSON
            for (const line of text.split('\n').filter(Boolean)) {
              try { gridBuffer.push(...extractListingsFromGQL(JSON.parse(line))); } catch { /* skip */ }
            }
            // Strategy 2: brace-matched JSON blobs (for requireLazy wrappers)
            for (const blob of extractJsonBlobs(text)) {
              try { gridBuffer.push(...extractListingsFromGQL(JSON.parse(blob))); } catch { /* skip */ }
            }
          }
        } catch { /* skip */ }
      };

      let loaded = false;
      for (let attempt = 1; attempt <= 2 && !loaded; attempt++) {
        try {
          await gridPage.goto(currentUrl.url, { waitUntil: 'domcontentloaded', timeout: 90_000 });
          loaded = true;
        } catch (err) {
          const msg = (err as Error).message;
          if (attempt < 2) {
            log(`[fb] ${currentUrl.label}: load timeout (attempt ${attempt}) — retrying...`);
            await randomSleep(3_000, 5_000);
          } else {
            log(`[fb] Failed to load ${currentUrl.label} after 2 attempts: ${msg}`);
          }
        }
      }
      if (!loaded) {
        await gridPage.close().catch(() => {});
        continue;
      }

      // Give FB time to fire initial GQL batch and inject script tags
      await randomSleep(5_000, 7_000);

      // Initial SSR parse
      await parsePageScripts();
      const ssrTotal = gridBuffer.length;
      const ssrNew   = gridBuffer.filter(l => !seenUrls.has(`https://www.facebook.com/marketplace/item/${l.id}/`)).length;
      log(`[fb] ${currentUrl.label}: SSR ${ssrTotal} listings (${ssrNew} new, ${ssrTotal - ssrNew} already seen)`);

      let scrollCount = 0;
      let idleScrolls = 0;
      let variantYielded = 0;

      // Listings that need detail pages are collected here during the scroll phase,
      // then batch-fetched concurrently after scrolling finishes.
      const pendingDetail: FBListing[] = [];

      while (scrollCount < MAX_SCROLLS && !isStopping()) {
        // Process buffered listings
        const snapshot = gridBuffer.splice(0);
        const rawCount = snapshot.length;

        let newThisScroll = 0;
        for (const gridListing of snapshot) {
          if (isStopping()) break;

          const listingUrl = `https://www.facebook.com/marketplace/item/${gridListing.id}/`;
          if (seenUrls.has(listingUrl)) continue;
          seenUrls.add(listingUrl);
          newThisScroll++;

          if (!needsDetailPage(gridListing)) {
            // SSR data is complete — yield immediately without visiting detail page
            log(`[fb] ${gridListing.title}: SSR data complete — skipping detail page`);
            totalYielded++;
            variantYielded++;
            log(`[fb] +1 listing: "${gridListing.title}" | ${gridListing.mileageKm ? gridListing.mileageKm + 'km' : 'no km'} | $${gridListing.priceAmount ?? '?'} | ${gridListing.location ?? '?'}`);
            yield buildPayload(gridListing);
          } else {
            // Queue for concurrent detail-page fetch after scrolling finishes
            const missing = missingDetailFields(gridListing);
            log(`[fb] ${gridListing.title}: queued for detail page (missing: ${missing.join(', ')})`);
            pendingDetail.push(gridListing);
          }
        }

        // Track idle (no GQL data from FB) vs. all-dupes (GQL data but all in seenUrls)
        if (rawCount === 0) {
          idleScrolls++;
          if (idleScrolls % 5 === 0) {
            log(`[fb] ${currentUrl.label}: ${idleScrolls} idle scrolls (FB not sending GQL)`);
          }
          if (idleScrolls >= IDLE_SCROLLS_LIMIT) {
            log(`[fb] ${currentUrl.label}: exhausted after ${scrollCount} scrolls`);
            break;
          }
        } else {
          if (newThisScroll === 0) {
            log(`[fb] ${currentUrl.label}: scroll ${scrollCount}: ${rawCount} GQL, all dupes — scrolling deeper`);
          }
          idleScrolls = 0;
        }

        // ── Scroll mechanics ──────────────────────────────
        // Why mouse.move() before wheel(): Playwright's wheel() fires a CDP WheelEvent
        // which requires the page to have a focused scroll target. Moving the mouse
        // to a safe coordinate first gives focus to the page body without landing on
        // a listing card (which would trigger navigation to that listing's detail page).
        //
        // Why (30, 500) specifically: the left margin at x=30 is always empty whitespace
        // on FB Marketplace's grid layout. Clicking anywhere around x=200–1000 risks
        // hitting a listing card, a filter button, or a navigation link.
        //
        // Why double scroll (wheel + scrollBy): wheel() alone sometimes doesn't trigger
        // FB's infinite-scroll sentinel; scrollBy() as a JS fallback ensures the page
        // position actually changes, which fires the IntersectionObserver that loads more.
        //
        // When idle (no new GQL for 5+ scrolls) we increase wheelDelta to 4000 to jump
        // past duplicate content and reach listings FB hasn't shown yet.
        const wheelDelta = idleScrolls >= 5 ? 4000 : 1600;
        try {
          await gridPage.mouse.move(30, 500);  // left margin — no clickable elements here
          await gridPage.mouse.wheel(0, wheelDelta);
          await gridPage.evaluate(`window.scrollBy(0, ${wheelDelta})`);
        } catch {
          // Page navigated away or context died — bail out of this variant
          log(`[fb] ${currentUrl.label}: scroll error — skipping to next variant`);
          break;
        }
        await randomSleep(SCROLL_DELAY_MIN, SCROLL_DELAY_MAX);
        await parsePageScripts();
        scrollCount++;
      }

      // ── Concurrent detail-page fetch phase ────────────────
      // After scrolling is complete, fetch all queued detail pages 3 at a time.
      if (pendingDetail.length > 0 && !isStopping()) {
        log(`[fb] ${currentUrl.label}: fetching ${pendingDetail.length} detail pages (3 concurrent)`);
        for await (const listing of fetchDetailsConcurrent(context, pendingDetail, log)) {
          if (isStopping()) break;
          totalYielded++;
          variantYielded++;
          log(`[fb] +1 listing: "${listing.title}" | ${listing.mileageKm ? listing.mileageKm + 'km' : 'no km'} | $${listing.priceAmount ?? '?'} | ${listing.location ?? '?'}`);
          yield buildPayload(listing);
        }
      }

      log(`[fb] ${currentUrl.label}: done — ${variantYielded} new listings`);
      await gridPage.close().catch(() => {});
      } // end inner block
    } // end for (variantsToScan)

    log(`[fb] session done — ${totalYielded} new listings`);

  } finally {
    if (context) await context.close().catch(() => {});
    if (browser) await browser.close().catch(() => {});
  }
}

// ── Helper: extract embedded JSON blobs from script text ──
// Scans script text for outermost {...} blocks that contain a target token.
// Returns up to MAX_BLOBS candidate strings for JSON.parse() attempts.
// This handles FB's requireLazy/ScheduledServerJS wrapper format.

function extractJsonBlobs(text: string, token = 'marketplace_listing_title', maxBlobs = 20): string[] {
  const results: string[] = [];
  let i = 0;
  while (i < text.length && results.length < maxBlobs) {
    const start = text.indexOf('{', i);
    if (start === -1) break;

    // Walk forward matching braces
    let depth = 0;
    let j = start;
    while (j < text.length) {
      const ch = text[j];
      if (ch === '{') depth++;
      else if (ch === '}') { depth--; if (depth === 0) break; }
      else if (ch === '"') {
        // Skip over quoted strings to avoid counting braces inside strings
        j++;
        while (j < text.length && text[j] !== '"') {
          if (text[j] === '\\') j++; // skip escaped char
          j++;
        }
      }
      j++;
    }

    if (depth === 0 && j > start) {
      const blob = text.slice(start, j + 1);
      if (blob.includes(token)) {
        results.push(blob);
        i = j + 1;
      } else {
        // Not relevant — advance past this '{' and try the next one
        i = start + 1;
      }
    } else {
      i = start + 1;
    }
  }
  return results;
}

// ── Helper ────────────────────────────────────────────────

function randomSleep(min: number, max: number): Promise<void> {
  const ms = min + Math.floor(Math.random() * (max - min));
  return new Promise(r => setTimeout(r, ms));
}
