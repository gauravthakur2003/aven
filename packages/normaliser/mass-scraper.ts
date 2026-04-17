/**
 * Aven — 48-Hour Full Ontario Scrape
 *
 * Strategy: all workers focus on ONE region at a time (no parallel regions).
 * When the current region is exhausted (Kijiji returns empty pages), ALL
 * workers immediately shift to the next region. No cooldowns. No idle workers.
 *
 * Region order: Northern Ontario first (least scraped) → GTA last.
 * Within each region: pages 1 → N until Kijiji returns empty results.
 * Cursor-based stopping is DISABLED — we sweep every page of every region.
 *
 * Run:  npx ts-node mass-scraper.ts
 * Stop: Ctrl+C  (drains normaliser queue before exiting)
 */

import * as dotenv from 'dotenv';
import * as path from 'path';
dotenv.config({ path: path.join(__dirname, '.env'), override: true });

import axios from 'axios';
import { randomUUID as uuidv4 } from 'crypto';
import { getPool, closePool }        from './src/lib/db';
import { extractFields, ExtractionResult } from './src/m2a-extractor';
import { isFastPathEligible, fastPathExtract } from './src/m2a-fast-path';
import { validateAndStandardise }    from './src/m2b-validator';
import { redactPII }                 from './src/m2d-redactor';
import { computeConfidence }         from './src/m2c-scorer';
import { routeAndWrite }             from './src/m2e-router';
import { detectColour }              from './src/m2g-vision';
import { lookupCarfax }              from './src/m2f-carfax';
import { RawPayload }                from './src/types';

// ── Config ────────────────────────────────────────────────────────────────────

const KIJIJI_RPM        = 25;
const KIJIJI_MIN_GAP_MS = Math.ceil(60_000 / KIJIJI_RPM); // 2400ms
const MAX_PAGES         = 100;   // Kijiji hard limit per region
const EMPTY_PAGE_STOP   = 2;     // consecutive Kijiji-empty pages → region done
const QUEUE_MAX         = 800;
const QUEUE_PAUSE_MS    = 1_000;
const PROGRESS_EVERY    = 10;

const RETRY_429_MIN_MS  = 90_000;
const RETRY_429_JITTER  = 90_000;

// Regions ordered: Northern first (fewest scraped) → GTA last
const REGIONS = [
  // ── Northern Ontario (barely scraped) ────────────────────
  { label: 'Thunder Bay',     url: 'https://www.kijiji.ca/b-cars-trucks/thunder-bay/c174l1700126' },
  { label: 'Sault Ste Marie', url: 'https://www.kijiji.ca/b-cars-trucks/sault-ste-marie/c174l1700244' },
  { label: 'Sudbury',         url: 'https://www.kijiji.ca/b-cars-trucks/sudbury/c174l1700245' },
  { label: 'North Bay',       url: 'https://www.kijiji.ca/b-cars-trucks/north-bay/c174l1700243' },
  // ── Southwest Ontario ─────────────────────────────────────
  { label: 'Sarnia',          url: 'https://www.kijiji.ca/b-cars-trucks/sarnia/c174l1700191' },
  { label: 'Brantford',       url: 'https://www.kijiji.ca/b-cars-trucks/brantford/c174l1700206' },
  { label: 'Niagara',         url: 'https://www.kijiji.ca/b-cars-trucks/st-catharines/c174l80016' },
  { label: 'Windsor',         url: 'https://www.kijiji.ca/b-cars-trucks/windsor-area-on/c174l1700220' },
  { label: 'London',          url: 'https://www.kijiji.ca/b-cars-trucks/london/c174l1700214' },
  // ── Eastern Ontario ───────────────────────────────────────
  { label: 'Belleville',      url: 'https://www.kijiji.ca/b-cars-trucks/belleville/c174l1700130' },
  { label: 'Kingston',        url: 'https://www.kijiji.ca/b-cars-trucks/kingston-on/c174l1700183' },
  { label: 'Peterborough',    url: 'https://www.kijiji.ca/b-cars-trucks/peterborough/c174l1700218' },
  { label: 'Ottawa',          url: 'https://www.kijiji.ca/b-cars-trucks/ottawa/c174l1700185' },
  // ── Central Ontario ───────────────────────────────────────
  { label: 'Barrie',          url: 'https://www.kijiji.ca/b-cars-trucks/barrie/c174l1700006' },
  { label: 'Cambridge',       url: 'https://www.kijiji.ca/b-cars-trucks/cambridge/c174l1700210' },
  { label: 'Kitchener',       url: 'https://www.kijiji.ca/b-cars-trucks/kitchener-waterloo/c174l1700212' },
  { label: 'Guelph',          url: 'https://www.kijiji.ca/b-cars-trucks/guelph/c174l1700242' },
  // ── GTA / Hamilton (most scraped — saved for last) ────────
  { label: 'Hamilton',        url: 'https://www.kijiji.ca/b-cars-trucks/hamilton/c174l80014' },
  { label: 'Halton',          url: 'https://www.kijiji.ca/b-cars-trucks/oakville-halton-region/c174l1700277' },
  { label: 'Durham',          url: 'https://www.kijiji.ca/b-cars-trucks/oshawa-durham-region/c174l1700275' },
  { label: 'York',            url: 'https://www.kijiji.ca/b-cars-trucks/markham-york-region/c174l1700274' },
  { label: 'Peel',            url: 'https://www.kijiji.ca/b-cars-trucks/mississauga-peel-region/c174l1700276' },
  { label: 'Toronto',         url: 'https://www.kijiji.ca/b-cars-trucks/city-of-toronto/c174l1700273' },
];

type Region = typeof REGIONS[0];

const WORKER_PROVIDERS = [
  'cerebras',
  'groq',
  'gemini',
  ...(process.env.TOGETHER_API_KEY ? ['together', 'together', 'together', 'together', 'together'] : []),
  ...(process.env.GROQ_API_KEY_2   ? ['groq2'] : []),
] as const;

// ── Shared state ───────────────────────────────────────────────────────────────

const normQueue: RawPayload[] = [];
const seenUrls  = new Set<string>();
let stopping    = false;

const stats = {
  scraped: 0, queued: 0, normalised: 0,
  published: 0, review: 0, rejected: 0,
  errors: 0, totalMs: 0, fastPathed: 0,
};

// ── Region / page state (shared, synchronous = thread-safe in Node.js) ────────

let regionIdx      = 0;          // which region all workers are currently on
let nextPage       = 1;          // next page to be claimed
let emptyStreak    = 0;          // consecutive pages Kijiji returned 0 listings
let reportRegion   = 0;          // regionIdx at time of claim — stale reports ignored
const startTime    = Date.now();

/** Atomically claim the next page. Returns null when all regions are done. */
function claimPage(): { region: Region; page: number; rIdx: number } | null {
  // Advance region if exhausted
  while (emptyStreak >= EMPTY_PAGE_STOP || nextPage > MAX_PAGES) {
    regionIdx++;
    nextPage     = 1;
    emptyStreak  = 0;
    reportRegion = regionIdx;
    if (regionIdx >= REGIONS.length) return null;
    log(`\n━━━ [${regionIdx + 1}/${REGIONS.length}] Moving all workers → ${REGIONS[regionIdx].label} ━━━\n`);
  }
  const region = REGIONS[regionIdx];
  const page   = nextPage++;
  return { region, page, rIdx: regionIdx };
}

/** Workers call this after fetching a page to update the empty streak. */
function reportPage(rIdx: number, kijiji_count: number): void {
  if (rIdx !== reportRegion) return;  // stale — region already advanced
  if (kijiji_count === 0) emptyStreak++;
  else emptyStreak = 0;
}

// ── Kijiji rate gate ───────────────────────────────────────────────────────────

let _lastKijijiReq    = 0;
const _kijijiQueue: Array<() => void> = [];
let   _kijijiRunning  = false;

function _runGate() {
  if (_kijijiRunning) return;
  _kijijiRunning = true;
  const tick = () => {
    if (_kijijiQueue.length === 0) { _kijijiRunning = false; return; }
    const wait = Math.max(0, _lastKijijiReq + KIJIJI_MIN_GAP_MS - Date.now());
    setTimeout(() => {
      _lastKijijiReq = Date.now();
      _kijijiQueue.shift()!();
      tick();
    }, wait);
  };
  tick();
}

function kijijiRequest(): Promise<void> {
  return new Promise(r => { _kijijiQueue.push(r); _runGate(); });
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> { return new Promise(r => setTimeout(r, ms)); }

function log(msg: string): void {
  process.stdout.write(`\r${' '.repeat(120)}\r`);
  console.log(`  ${msg}`);
}

function printStatus(): void {
  const elapsedH = ((Date.now() - startTime) / 3_600_000).toFixed(1);
  const region   = REGIONS[regionIdx]?.label ?? 'DONE';
  const avg      = stats.normalised > 0 ? Math.round(stats.totalMs / stats.normalised) : 0;
  const rate     = stats.normalised > 0 ? Math.round(stats.normalised / ((Date.now() - startTime) / 3_600_000)) : 0;
  process.stdout.write(
    `\r  [${elapsedH}h] ${region} pg${nextPage - 1}  ` +
    `scraped:${stats.scraped} queue:${normQueue.length} ` +
    `norm:${stats.normalised}(${rate}/h) pub:${stats.published} rev:${stats.review} rej:${stats.rejected}  `
  );
}

const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
];

// ── Scraper worker ─────────────────────────────────────────────────────────────
// All N scraper workers share the same region/page state via claimPage().
// They compete for the rate gate, so in practice only 25 req/min hit Kijiji.

async function scraperWorker(workerId: number, pool: any): Promise<void> {
  // Stagger startup by 200ms so workers don't all hit claimPage() simultaneously
  if (workerId > 0) await sleep(workerId * 200);

  while (!stopping) {
    // Backpressure — pause if normaliser queue is too large
    while (normQueue.length >= QUEUE_MAX && !stopping) await sleep(QUEUE_PAUSE_MS);
    if (stopping) break;

    const job = claimPage();
    if (!job) {
      log(`[scraper-${workerId}] all regions done — worker exiting`);
      break;
    }

    const { region, page, rIdx } = job;
    const url = page === 1 ? region.url : `${region.url}?page=${page}`;
    const ua  = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];

    await kijijiRequest();

    let payloads: RawPayload[] = [];

    try {
      const res = await axios.get(url, {
        headers: {
          'User-Agent': ua,
          'Accept-Language': 'en-CA,en;q=0.9',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        },
        timeout: 20_000,
      });

      const scriptMatch = (res.data as string).match(/<script id="__NEXT_DATA__"[^>]*>(.*?)<\/script>/s);
      if (!scriptMatch) { reportPage(rIdx, 0); continue; }

      const apollo = (JSON.parse(scriptMatch[1]) as any)?.props?.pageProps?.__APOLLO_STATE__ as Record<string, unknown>;
      if (!apollo) { reportPage(rIdx, 0); continue; }

      const scrapeRunId = uuidv4();
      payloads = Object.entries(apollo)
        .filter(([k]) => k.startsWith('AutosListing:'))
        .map(([, raw]) => {
          const l = raw as any;
          const listingUrl = l.url
            ? (l.url.startsWith('http') ? l.url : `https://www.kijiji.ca${l.url}`)
            : `https://www.kijiji.ca/v-cars/${l.id}`;

          const attrs: Record<string, string> = {};
          for (const a of (l.attributes?.all ?? [])) {
            if (a.canonicalValues?.[0] != null) attrs[a.canonicalName] = a.canonicalValues[0];
          }

          const TRANS_MAP: Record<string, string> = { '1': 'Manual', '2': 'Automatic', '3': 'CVT' };
          const BODY_MAP:  Record<string, string> = {
            sedan: 'Sedan', suvcrossover: 'SUV/Crossover', hatchback: 'Hatchback',
            pickuptruck: 'Pickup Truck', convertible: 'Convertible', coupe: 'Coupe',
            minivan: 'Minivan', wagon: 'Wagon', van: 'Van',
          };

          const attrYear   = attrs['caryear'] ? parseInt(attrs['caryear'], 10) : undefined;
          const titleMatch = (l.title as string | undefined)?.match(/\b(19[5-9]\d|20[012]\d)\b/);
          const year = attrYear ?? l.vehicleInformation?.years?.[0] ?? (titleMatch ? parseInt(titleMatch[1], 10) : undefined);
          const make  = (attrs['carmake']  && attrs['carmake']  !== 'othrmake') ? attrs['carmake']  : (l.vehicleInformation?.make  ?? undefined);
          const model = (attrs['carmodel'] && attrs['carmodel'] !== 'othrmdl')  ? attrs['carmodel'] : (l.vehicleInformation?.model ?? undefined);
          const sellerType = attrs['forsaleby'] ?? l.forsaleby;

          return {
            payload_id:        uuidv4(),
            source_id:         'kijiji-ca',
            source_category:   'classifieds',
            listing_url:       listingUrl,
            scrape_timestamp:  new Date().toISOString(),
            connector_version: '1.3.0',
            raw_content: JSON.stringify({
              title: l.title, description: l.description, url: l.url,
              priceCents: l.price?.amount, priceRating: attrs['pricerating'] ?? l.price?.priceAnalysis?.label,
              year, make, model,
              trim:           attrs['cartrim']          ?? l.vehicleInformation?.trim,
              mileageKm:      attrs['carmileageinkms']  ? parseInt(attrs['carmileageinkms'], 10) : l.vehicleInformation?.mileage,
              colour:         attrs['carcolor']         ?? l.vehicleInformation?.colour,
              colourInterior: attrs['carinteriorcolor'] ?? undefined,
              bodyType:       BODY_MAP[attrs['carbodytype'] ?? ''] ?? attrs['carbodytype'] ?? l.vehicleInformation?.bodyType,
              drivetrain:     (attrs['drivetrain'] && attrs['drivetrain'] !== 'other') ? attrs['drivetrain'] : l.vehicleInformation?.drivetrain,
              fuelType:       attrs['carfueltype']      ?? l.vehicleInformation?.fuelType,
              transmission:   TRANS_MAP[attrs['cartransmission'] ?? ''] ?? l.vehicleInformation?.transmission,
              doors:          attrs['noofdoors']  ? parseInt(attrs['noofdoors'], 10)  : undefined,
              seats:          attrs['noofseats']  ? parseInt(attrs['noofseats'], 10)  : undefined,
              vin:            attrs['vin']               ?? l.vehicleInformation?.vin,
              condition:      attrs['vehicletype']       ?? undefined,
              _sellerType:    sellerType,
              location:       l.location?.address ?? l.location?.name,
            }),
            raw_content_type:  'json' as const,
            listing_images:    (l.imageUrls ?? []).slice(0, 5),
            geo_region:        'ON',
            scrape_run_id:     scrapeRunId,
            http_status:       200,
            proxy_used:        false,
            requires_auth:     false,
            is_dealer_listing: sellerType === 'delr',
          } satisfies RawPayload;
        });

    } catch (err) {
      const msg  = (err as Error).message ?? '';
      const is429 = msg.includes('429') || msg.includes('Too Many');
      const wait  = is429 ? RETRY_429_MIN_MS + Math.floor(Math.random() * RETRY_429_JITTER) : 8_000;
      log(`[scraper-${workerId}] ${region.label} p${page} failed${is429 ? ' (rate-limited)' : ''} — waiting ${Math.round(wait / 1000)}s`);
      // Return the page claim so another worker doesn't skip it — simplest: just sleep and retry
      if (is429) await sleep(wait);
      reportPage(rIdx, 1);  // treat as non-empty so we don't advance region on 429
      continue;
    }

    reportPage(rIdx, payloads.length);
    if (payloads.length === 0) {
      log(`[scraper] ${region.label} p${page} → empty (Kijiji end) streak=${emptyStreak}`);
      continue;
    }

    stats.scraped += payloads.length;

    // Dedup: skip listings seen this run or already in DB
    const runFresh = payloads.filter(p => !seenUrls.has(p.listing_url));
    for (const p of payloads) seenUrls.add(p.listing_url);

    if (runFresh.length === 0) { printStatus(); continue; }

    const urls = runFresh.map(p => p.listing_url);
    const { rows } = await pool.query(
      `SELECT source_url FROM listings WHERE source_url = ANY($1)`, [urls]
    );
    const inDb  = new Set(rows.map((r: any) => r.source_url as string));
    const fresh = runFresh.filter(p => !inDb.has(p.listing_url));

    if (fresh.length > 0) {
      normQueue.push(...fresh);
      stats.queued += fresh.length;
      log(`[scraper] ${region.label} p${page}  ${payloads.length} kj → ${fresh.length} new  (queue:${normQueue.length})`);
    }

    printStatus();
  }
}

// ── Normaliser worker ─────────────────────────────────────────────────────────
// Exact same pipeline as test-pipeline.ts. Processes normQueue.

const PROVIDER_DELAY_MS: Record<string, number> = {
  cerebras: 2_000, groq: 2_000, groq2: 2_000,
  gemini: 4_000, together: 500, anthropic: 1_500,
};

async function normaliserWorker(id: number, pool: any, provider: string): Promise<void> {
  if (id > 0) await sleep(id * 8_000);

  const delay = PROVIDER_DELAY_MS[provider] ?? 2_000;
  let lastReq = 0;

  while (!stopping || normQueue.length > 0) {
    const payload = normQueue.shift();
    if (!payload) { await sleep(500); continue; }

    const gap = Date.now() - lastReq;
    if (gap < delay) await sleep(delay - gap);
    lastReq = Date.now();

    const raw   = JSON.parse(payload.raw_content) as any;
    const label = `${raw.year ?? '?'} ${raw.make ?? '?'} ${raw.model ?? '?'}`;
    const t0    = Date.now();

    // Pre-LLM price filter
    const hasPriceCents   = raw.priceCents != null && raw.priceCents > 0;
    const hasPriceInDesc  = /\$\s*[\d,]+|\b\d{4,}\s*(?:obo|firm|asking|neg)/i.test(raw.description ?? '');
    const hasPaymentInDesc = /\$\s*\d+\s*\/\s*(?:mo|month|week|bi-?week|bw)/i.test(raw.description ?? '');
    if (!hasPriceCents && !hasPriceInDesc && !hasPaymentInDesc) {
      stats.rejected++;
      continue;
    }

    try {
      const noImages = payload.listing_images.length === 0;
      let extraction: ExtractionResult;
      if (isFastPathEligible(payload)) {
        extraction = fastPathExtract(payload);
        stats.fastPathed++;
      } else {
        extraction = await extractFields(payload, provider);
      }

      const validated  = validateAndStandardise(extraction.fields);

      if (payload._advancedScrape && !noImages && !validated.colour_exterior) {
        const vision = await detectColour(payload.listing_images);
        if (vision.colour) validated.colour_exterior = vision.colour;
      }

      if (validated.vin && (validated.accidents == null || validated.owners == null)) {
        const carfax = await lookupCarfax(validated.vin);
        if (carfax) {
          if (validated.accidents == null && carfax.accidents !== null) validated.accidents = carfax.accidents;
          if (validated.owners    == null && carfax.owners    !== null) validated.owners    = carfax.owners;
        }
      }

      const redaction = redactPII(validated.description);
      validated.description = redaction.text;

      const scored = computeConfidence(validated, noImages);
      await routeAndWrite(pool, payload, scored, extraction, redaction, redaction.failed);

      const ms = Date.now() - t0;
      stats.normalised++;
      stats.totalMs += ms;
      if (scored.outcome === 'published') stats.published++;
      else if (scored.outcome === 'review') stats.review++;
      else stats.rejected++;

      if (stats.normalised % PROGRESS_EVERY === 0) {
        log(`[${provider}] [${stats.normalised}] ${label} → ${scored.outcome} (${scored.confidence_score}) ${ms}ms`);
      }
    } catch (err) {
      const msg = (err as Error).message ?? '';
      const retryMatch = msg.match(/try again in (\d+)m([\d.]+)s/i) ?? msg.match(/try again in ([\d.]+)s/i);
      const is429 = msg.includes('429');

      if (retryMatch) {
        const waitMs = retryMatch[2] != null
          ? (parseInt(retryMatch[1], 10) * 60 + parseFloat(retryMatch[2])) * 1000
          : parseFloat(retryMatch[1]) * 1000;
        normQueue.unshift(payload);
        log(`[${provider}] rate-limited — waiting ${Math.ceil(waitMs / 1000)}s`);
        await sleep(waitMs + 2_000);
      } else if (is429) {
        normQueue.unshift(payload);
        log(`[${provider}] rate-limited (no hint) — waiting 90s`);
        await sleep(90_000);
      } else {
        stats.errors++;
        log(`[${provider}] error on "${label}" — ${msg.slice(0, 120)}`);
      }
    }

    printStatus();
  }

  log(`[${provider}] worker stopped`);
}

// ── Main ──────────────────────────────────────────────────────────────────────

process.on('SIGINT', () => {
  if (!stopping) {
    stopping = true;
    process.stdout.write('\n');
    log('Ctrl+C — draining queue then stopping…');
  }
});

async function main() {
  const pool = await getPool();

  const totalRegions = REGIONS.length;
  const totalPages   = totalRegions * MAX_PAGES;

  console.log('');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('  Aven — 48h Ontario Full Scrape');
  console.log(`  ${totalRegions} regions  ×  up to ${MAX_PAGES} pages each  =  ${totalPages} max requests`);
  console.log(`  Rate: ${KIJIJI_RPM} RPM = ~${Math.round(totalPages / KIJIJI_RPM)} min to sweep all pages`);
  console.log(`  Workers: ${REGIONS.length} scrapers + ${WORKER_PROVIDERS.length} normaliser`);
  console.log(`  Starting region: ${REGIONS[0].label}`);
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('');

  // Seed seenUrls from DB so we don't re-enqueue existing listings
  log('Loading existing Kijiji URLs from DB…');
  const { rows: existingRows } = await pool.query(
    `SELECT source_url FROM listings WHERE source_id = 'kijiji-ca'`
  );
  for (const r of existingRows) seenUrls.add(r.source_url as string);
  log(`Loaded ${seenUrls.size} existing URLs — won't re-queue these`);

  await Promise.all([
    // All scraper workers focus on the same region via claimPage()
    ...REGIONS.map((_, i) => scraperWorker(i, pool)),
    // Normaliser workers drain the queue concurrently
    ...WORKER_PROVIDERS.map((p, i) => normaliserWorker(i, pool, p)),
  ]);

  // Final summary
  process.stdout.write('\n');
  log('All workers finished.');
  const { rows } = await pool.query(`
    SELECT COUNT(*) AS total,
           COUNT(*) FILTER (WHERE status = 'active')  AS published,
           COUNT(*) FILTER (WHERE status = 'review')  AS in_review,
           COUNT(*) FILTER (WHERE status = 'rejected') AS rejected
    FROM listings WHERE source_id = 'kijiji-ca'
  `);
  const s = rows[0];
  console.log('');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('  Final Kijiji counts:');
  console.log(`  Total: ${s.total}  Published: ${s.published}  Review: ${s.in_review}  Rejected: ${s.rejected}`);
  console.log(`  Run stats: scraped=${stats.scraped} queued=${stats.queued} norm=${stats.normalised} errors=${stats.errors}`);
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

  await closePool();
}

main().catch(err => { console.error('Fatal:', err); process.exit(1); });
