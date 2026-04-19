/**
 * Aven — End-to-End Scrape + Normalise Pipeline
 *
 * What this does:
 *   All scraper workers focus on ONE Kijiji region at a time. When that region
 *   is exhausted (Kijiji returns empty pages), ALL workers shift to the next
 *   region together. Runs FB scraper in parallel with a pool of LLM normaliser
 *   workers draining a shared in-memory queue:
 *
 *     scraperWorker(0..N)  → queue[] → normaliserWorker(0) → Postgres
 *     fbScraperLoop()      ↗           normaliserWorker(1) ↗
 *                                      normaliserWorker(2) ↗
 *
 *   Region order: Northern Ontario first (least scraped) → GTA last.
 *   Within each region: pages 1 → MAX_PAGES until Kijiji returns empty.
 *   Cursor-based stopping is DISABLED — we sweep every page of every region.
 *
 *   Each normaliser worker runs the full M2 pipeline:
 *     M2a (LLM extraction) → M2b (validation) → M2g (vision) → M2f (CARFAX)
 *     → M2d (PII redaction) → M2c (scoring) → M2e (DB write)
 *
 * How to run:
 *   npx ts-node test-pipeline.ts
 *   Ctrl+C to stop cleanly (drains the queue before exiting).
 *
 * Prerequisites:
 *   - DATABASE_URL set in .env
 *   - At least one LLM API key set (CEREBRAS_API_KEY, GROQ_API_KEY, GEMINI_API_KEY)
 *   - For Facebook: run `npx ts-node fb-auth-setup.ts` first to create fb-session.json
 */

import * as dotenv from 'dotenv';
import * as path from 'path';
dotenv.config({ path: path.join(__dirname, '.env'), override: true });

import axios from 'axios';
import { randomUUID as uuidv4 } from 'crypto';
import { getPool, closePool } from './src/lib/db';
import { extractFields, ExtractionResult } from './src/m2a-extractor';
import { isFastPathEligible, fastPathExtract } from './src/m2a-fast-path';
import { validateAndStandardise } from './src/m2b-validator';
import { redactPII }             from './src/m2d-redactor';
import { computeConfidence }     from './src/m2c-scorer';
import { routeAndWrite }         from './src/m2e-router';
import { detectColour }          from './src/m2g-vision';
import { lookupCarfax }          from './src/m2f-carfax';
import { scrapeFacebook, FB_URL_VARIANTS } from './src/fb-scraper';
import { runDeduplication }     from './src/deduplicator';
import { RawPayload }            from './src/types';

const RETRY_429_MIN_MS   = 90_000; // min wait after a 429 (90s)
const RETRY_429_JITTER   = 90_000; // + random 0–90s → total 90–180s

// ── Region definitions ────────────────────────────────────
interface RegionEntry { label: string; url: string; province: string; }

// Ontario regions: Northern first (least scraped) → GTA last.
const ON_REGIONS: RegionEntry[] = [
  // ── Northern Ontario (barely scraped) ────────────────────
  { label: 'Thunder Bay',     province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/thunder-bay/c174l1700126' },
  { label: 'Sault Ste Marie', province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/sault-ste-marie/c174l1700244' },
  { label: 'Sudbury',         province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/sudbury/c174l1700245' },
  { label: 'North Bay',       province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/north-bay/c174l1700243' },
  // ── Southwest Ontario ─────────────────────────────────────
  { label: 'Sarnia',          province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/sarnia/c174l1700191' },
  { label: 'Brantford',       province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/brantford/c174l1700206' },
  { label: 'Niagara',         province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/st-catharines/c174l80016' },
  { label: 'Windsor',         province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/windsor-area-on/c174l1700220' },
  { label: 'London',          province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/london/c174l1700214' },
  // ── Eastern Ontario ───────────────────────────────────────
  { label: 'Belleville',      province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/belleville/c174l1700130' },
  { label: 'Kingston',        province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/kingston-on/c174l1700183' },
  { label: 'Peterborough',    province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/peterborough/c174l1700218' },
  { label: 'Ottawa',          province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/ottawa/c174l1700185' },
  // ── Central Ontario ───────────────────────────────────────
  { label: 'Barrie',          province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/barrie/c174l1700006' },
  { label: 'Cambridge',       province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/cambridge/c174l1700210' },
  { label: 'Kitchener',       province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/kitchener-waterloo/c174l1700212' },
  { label: 'Guelph',          province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/guelph/c174l1700242' },
  // ── GTA / Hamilton (most scraped — saved for last) ────────
  { label: 'Hamilton',        province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/hamilton/c174l80014' },
  { label: 'Halton',          province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/oakville-halton-region/c174l1700277' },
  { label: 'Durham',          province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/oshawa-durham-region/c174l1700275' },
  { label: 'York',            province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/markham-york-region/c174l1700274' },
  { label: 'Peel',            province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/mississauga-peel-region/c174l1700276' },
  { label: 'Toronto',         province: 'ON', url: 'https://www.kijiji.ca/b-cars-trucks/city-of-toronto/c174l1700273' },
];

// BC regions — all sub-regions confirmed from the BC provincial page (c174l9007).
// Ordered small → large so fresh listings in smaller markets get captured first,
// GVA sub-regions (most listings) run last.
const BC_REGIONS: RegionEntry[] = [
  // ── Small northern / interior ─────────────────────────────
  { label: 'Williams Lake',        province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/williams-lake/c174l1700305' },
  { label: 'Dawson Creek',         province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/dawson-creek/c174l1700304' },
  { label: 'North Shore',          province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/north-shore/c174l1700289' },
  { label: 'Victoria',             province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/victoria-bc/c174l1700173' },
  { label: 'Nanaimo',              province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/nanaimo/c174l1700263' },
  { label: 'Comox/Courtenay',      province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/comox-valley/c174l1700315' },
  { label: 'Cowichan Valley',      province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/cowichan-valley/c174l1700300' },
  { label: 'Terrace',              province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/terrace/c174l1700309' },
  { label: 'Chilliwack',           province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/chilliwack/c174l1700141' },
  // ── Mid-size interior ─────────────────────────────────────
  { label: 'Kelowna',              province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/kelowna/c174l1700228' },
  { label: 'Kamloops',             province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/kamloops/c174l1700227' },
  { label: 'Cranbrook',            province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/cranbrook/c174l1700224' },
  { label: 'Abbotsford',           province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/abbotsford/c174l1700140' },
  { label: 'Fraser Valley',        province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/fraser-valley/c174l1700139' },
  { label: 'Prince George',        province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/prince-george/c174l1700143' },
  // ── GVA sub-regions (most listings) ──────────────────────
  { label: 'Burnaby/New West',     province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/burnaby-new-westminster/c174l1700286' },
  { label: 'North Shore GVA',      province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/north-shore/c174l1700289' },
  { label: 'Vancouver',            province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/vancouver/c174l1700287' },
  { label: 'Richmond',             province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/richmond-bc/c174l1700288' },
  { label: 'Tricities/Pitt/Maple', province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/tricities-pitt-maple/c174l1700290' },
  { label: 'Delta/Surrey/Langley', province: 'BC', url: 'https://www.kijiji.ca/b-cars-trucks/delta-surrey-langley/c174l1700285' },
];

// ── RegionCursor: atomic page counter for a set of regions ──
//  continuous=false → returns null when all regions exhausted (forward sweep)
//  continuous=true  → wraps around forever (ongoing sweep)
class RegionCursor {
  private regIdx      = 0;
  private nextPage    = 1;
  private emptyStreak = 0;
  private reportIdx   = 0;

  constructor(
    readonly regions:    RegionEntry[],
    readonly name:       string,
    readonly continuous: boolean = false,
  ) {}

  get currentLabel(): string { return this.regions[this.regIdx % this.regions.length]?.label ?? 'DONE'; }
  get currentPage():  number { return this.nextPage - 1; }
  get isDone():       boolean { return !this.continuous && this.regIdx >= this.regions.length; }

  claim(): { region: RegionEntry; page: number; rIdx: number } | null {
    if (!this.continuous && this.regIdx >= this.regions.length) return null;
    while (this.emptyStreak >= EMPTY_PAGE_STOP || this.nextPage > MAX_PAGES) {
      if (this.continuous) {
        this.regIdx = (this.regIdx + 1) % this.regions.length;
        if (this.regIdx === 0) log(`\n━━━ [${this.name}] Full sweep — restarting ━━━\n`);
      } else {
        this.regIdx++;
        if (this.regIdx >= this.regions.length) return null;
      }
      this.nextPage    = 1;
      this.emptyStreak = 0;
      this.reportIdx   = this.regIdx;
      const r = this.regions[this.regIdx % this.regions.length];
      log(`\n━━━ [${this.name} ${(this.regIdx % this.regions.length) + 1}/${this.regions.length}] → ${r.label} ━━━\n`);
    }
    const region = this.regions[this.regIdx % this.regions.length];
    const page   = this.nextPage++;
    return { region, page, rIdx: this.regIdx };
  }

  report(rIdx: number, count: number): void {
    if (rIdx !== this.reportIdx) return;
    if (count === 0) this.emptyStreak++;
    else this.emptyStreak = 0;
  }
}

// Phase 1: all 8 workers sweep Ontario forward
const onCursorP1 = new RegionCursor(ON_REGIONS, 'ON', false);
// Phase 2 cursors (created after phase 1 completes)
let bcCursor:   RegionCursor;
let onCursorP2: RegionCursor;

// ── Queue backpressure ────────────────────────────────────
const QUEUE_MAX      = 800;    // max pending payloads before scraper pauses
const QUEUE_PAUSE_MS = 1_000;

const MAX_PAGES         = 100;   // Kijiji hard limit per region
const EMPTY_PAGE_STOP   = 2;     // consecutive empty pages → region done

// 8 workers: 1× Cerebras, 1× Groq, 1× Gemini, 5× Together AI (if key set)
const WORKER_PROVIDERS = [
  'cerebras',
  'groq',
  'groq2',   // 2nd Groq key (GROQ_API_KEY_2) — separate 14,400 RPD pool
  'gemini',
  // Together AI: 5× Llama-3-8B-Instruct-Lite — serverless, fastest, cheapest
  ...(process.env.TOGETHER_API_KEY ? ['together', 'together', 'together', 'together', 'together'] : []),
] as const;
const PROGRESS_EVERY   = 10;

// 8 scraper workers total.
// Phase 1: all 8 sweep Ontario forward.
// Phase 2: 6 workers → BC forward sweep | 2 workers → Ontario continuous loop.
const SCRAPER_WORKERS  = 8;
const BC_WORKERS       = 6;  // workers 0-5 → BC in Phase 2

// ── Shared state ──────────────────────────────────────────

const queue: RawPayload[] = [];   // raw payloads waiting to be normalised
const seenUrls = new Set<string>(); // dedup within this run

let stopping = false;
const stats = {
  scraped:    0,
  queued:     0,
  normalised: 0,
  published:  0,
  review:     0,
  rejected:   0,
  errors:     0,
  totalMs:    0,
  fastPathed: 0,
};

// ── Phase coordination ─────────────────────────────────────
// Phase 1: all 8 workers sweep Ontario.
// Phase 2: workers 0-5 sweep BC, workers 6-7 loop Ontario continuously.
let currentPhase  = 1;
let p1DoneCount   = 0;
let phase2Resolve!: () => void;
const phase2Signal = new Promise<void>(res => { phase2Resolve = res; });

const startTime  = Date.now();

// ── Graceful shutdown ─────────────────────────────────────

process.on('SIGINT', () => {
  if (!stopping) {
    stopping = true;
    phase2Resolve?.(); // unblock any workers waiting for phase transition
    process.stdout.write('\n');
    log('Ctrl+C — draining queue then stopping…');
  }
});

// ── Global Kijiji rate gate ───────────────────────────────
// All scrapers share a single token bucket so total request rate to Kijiji
// never exceeds KIJIJI_RPM regardless of how many workers are active.
const KIJIJI_RPM        = 25;
const KIJIJI_MIN_GAP_MS = Math.ceil(60_000 / KIJIJI_RPM); // = 2400ms

let   _lastKijijiReq    = 0;
const _kijijiQueue: Array<() => void> = [];
let   _kijijiGateRunning = false;

function _runKijijiGate() {
  if (_kijijiGateRunning) return;
  _kijijiGateRunning = true;
  const tick = () => {
    if (_kijijiQueue.length === 0) { _kijijiGateRunning = false; return; }
    const now  = Date.now();
    const wait = Math.max(0, _lastKijijiReq + KIJIJI_MIN_GAP_MS - now);
    setTimeout(() => {
      _lastKijijiReq = Date.now();
      const resolve = _kijijiQueue.shift()!;
      resolve();
      tick();
    }, wait);
  };
  tick();
}

function kijijiRequest(): Promise<void> {
  return new Promise(resolve => {
    _kijijiQueue.push(resolve);
    _runKijijiGate();
  });
}

// ── Helpers ───────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

function log(msg: string): void {
  process.stdout.write(`\r${' '.repeat(120)}\r`);  // clear progress line
  console.log(`  ${msg}`);
}

function printStatus(): void {
  const elapsedH = ((Date.now() - startTime) / 3_600_000).toFixed(1);
  const avg      = stats.normalised > 0 ? Math.round(stats.totalMs / stats.normalised) : 0;
  const rate     = stats.normalised > 0 ? Math.round(stats.normalised / ((Date.now() - startTime) / 3_600_000)) : 0;
  const phaseStr = currentPhase === 1
    ? `P1:ON ${onCursorP1.currentLabel} pg${onCursorP1.currentPage}`
    : `P2 BC:${bcCursor?.currentLabel ?? '-'} ON:${onCursorP2?.currentLabel ?? '-'}`;
  process.stdout.write(
    `\r  [${elapsedH}h] ${phaseStr}  ` +
    `scraped:${stats.scraped} queue:${queue.length} ` +
    `norm:${stats.normalised}(${rate}/h) pub:${stats.published} rev:${stats.review} rej:${stats.rejected} avg:${avg}ms  `,
  );
}

const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
];

// ── Advanced detail-page scraper ─────────────────────────
// Called for "thin" listings that are missing make/model/year or price
// in the search-results Apollo blob. Fetches the individual listing page
// (which contains a richer __NEXT_DATA__ payload) and merges the extra
// fields back into the existing raw_content JSON.
//
// Also collects up to 10 image URLs (vs 5 from search results), which
// enables the vision colour-detection step in the normaliser worker.
//
// Rate-gated through kijijiRequest() so detail fetches count against the
// same 25 RPM global budget as search-page requests.

function isThinPayload(p: RawPayload): boolean {
  const raw = JSON.parse(p.raw_content) as any;
  const missingCore = !raw.make || !raw.model || !raw.year;
  const missingPrice = raw.priceCents == null || !raw.mileageKm;
  return missingCore || missingPrice;
}

async function fetchDetailPage(payload: RawPayload, ua: string, regionLabel: string): Promise<RawPayload> {
  try {
    await kijijiRequest(); // counts against global rate budget
    const res = await axios.get(payload.listing_url, {
      headers: { 'User-Agent': ua, 'Accept-Language': 'en-CA,en;q=0.9', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' },
      timeout: 20_000,
    });

    const scriptMatch = (res.data as string).match(/<script id="__NEXT_DATA__"[^>]*>(.*?)<\/script>/s);
    if (!scriptMatch) return payload;

    const nextData = JSON.parse(scriptMatch[1]) as any;
    const ad = nextData?.props?.pageProps?.adDetails ?? nextData?.props?.pageProps?.ad;
    if (!ad) return payload;

    const attrs: Record<string, string> = {};
    for (const a of (ad.attributes?.all ?? ad.adAttributes ?? [])) {
      const key = a.canonicalName ?? a.machineKey;
      const val = a.canonicalValues?.[0] ?? a.value;
      if (key && val != null) attrs[key] = String(val);
    }

    const TRANS_MAP: Record<string, string> = { '1': 'Manual', '2': 'Automatic', '3': 'CVT' };
    const BODY_MAP: Record<string, string> = {
      sedan: 'Sedan', suvcrossover: 'SUV/Crossover', hatchback: 'Hatchback',
      pickuptruck: 'Pickup Truck', convertible: 'Convertible', coupe: 'Coupe',
      minivan: 'Minivan', wagon: 'Wagon', van: 'Van',
    };

    const existing = JSON.parse(payload.raw_content) as any;

    const merged = {
      ...existing,
      title:       existing.title       ?? ad.title,
      description: existing.description ?? ad.description,
      priceCents:  existing.priceCents  ?? ad.price?.amount,
      year:        existing.year        ?? (attrs['caryear'] ? parseInt(attrs['caryear'], 10) : ad.vehicleInformation?.years?.[0]),
      make:        existing.make        ?? ((attrs['carmake']  && attrs['carmake']  !== 'othrmake') ? attrs['carmake']  : ad.vehicleInformation?.make),
      model:       existing.model       ?? ((attrs['carmodel'] && attrs['carmodel'] !== 'othrmdl')  ? attrs['carmodel'] : ad.vehicleInformation?.model),
      trim:        existing.trim        ?? attrs['cartrim']         ?? ad.vehicleInformation?.trim,
      mileageKm:   existing.mileageKm   ?? (attrs['carmileageinkms'] ? parseInt(attrs['carmileageinkms'], 10) : ad.vehicleInformation?.mileage),
      colour:      existing.colour      ?? attrs['carcolor']        ?? ad.vehicleInformation?.colour,
      colourInterior: existing.colourInterior ?? attrs['carinteriorcolor'],
      bodyType:    existing.bodyType    ?? BODY_MAP[attrs['carbodytype'] ?? ''] ?? attrs['carbodytype'] ?? ad.vehicleInformation?.bodyType,
      drivetrain:  existing.drivetrain  ?? ((attrs['drivetrain'] && attrs['drivetrain'] !== 'other') ? attrs['drivetrain'] : ad.vehicleInformation?.drivetrain),
      fuelType:    existing.fuelType    ?? attrs['carfueltype']     ?? ad.vehicleInformation?.fuelType,
      transmission:existing.transmission ?? TRANS_MAP[attrs['cartransmission'] ?? ''] ?? ad.vehicleInformation?.transmission,
      doors:       existing.doors       ?? (attrs['noofdoors']  ? parseInt(attrs['noofdoors'], 10)  : undefined),
      seats:       existing.seats       ?? (attrs['noofseats']  ? parseInt(attrs['noofseats'], 10)  : undefined),
      vin:         existing.vin         ?? attrs['vin']             ?? ad.vehicleInformation?.vin,
      condition:   existing.condition   ?? attrs['vehicletype'],
      location:    existing.location    ?? ad.location?.address     ?? ad.location?.name,
      _sellerType: existing._sellerType ?? attrs['forsaleby'],
    };

    const detailImages: string[] = (ad.imageUrls ?? ad.images?.map((i: any) => i.url) ?? []).slice(0, 10);
    const mergedImages = detailImages.length > payload.listing_images.length ? detailImages : payload.listing_images;

    log(`[kj:${regionLabel}] advanced scrape: ${merged.year ?? '?'} ${merged.make ?? '?'} ${merged.model ?? '?'} — filled missing fields`);

    return {
      ...payload,
      raw_content:    JSON.stringify(merged),
      listing_images: mergedImages,
      _advancedScrape: true,
    };
  } catch (err) {
    log(`[kj:${regionLabel}] advanced scrape failed for ${payload.listing_url}: ${(err as Error).message.slice(0, 80)}`);
    return payload;
  }
}

// ── Scraper inner loop ────────────────────────────────────
// Sweeps pages from `cursor` until it's exhausted (or stopping).
// Used for both Phase 1 (Ontario forward) and Phase 2 (BC / Ontario-loop).

async function sweepWithCursor(workerId: number, pool: any, cursor: RegionCursor): Promise<void> {
  while (!stopping) {
    // Backpressure
    while (queue.length >= QUEUE_MAX && !stopping) await sleep(QUEUE_PAUSE_MS);
    if (stopping) break;

    const job = cursor.claim();
    if (!job) break; // region set exhausted (non-continuous cursors only)

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
      if (!scriptMatch) { cursor.report(rIdx, 0); continue; }

      const apollo = (JSON.parse(scriptMatch[1]) as any)?.props?.pageProps?.__APOLLO_STATE__ as Record<string, unknown>;
      if (!apollo) { cursor.report(rIdx, 0); continue; }

      const scrapeRunId = uuidv4();
      payloads = Object.entries(apollo)
        .filter(([key]) => key.startsWith('AutosListing:'))
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
          const BODY_MAP: Record<string, string> = {
            sedan: 'Sedan', suvcrossover: 'SUV/Crossover', hatchback: 'Hatchback',
            pickuptruck: 'Pickup Truck', convertible: 'Convertible', coupe: 'Coupe',
            minivan: 'Minivan', wagon: 'Wagon', van: 'Van',
          };

          const attrYear   = attrs['caryear'] ? parseInt(attrs['caryear'], 10) : undefined;
          const titleMatch = (l.title as string | undefined)?.match(/\b(19[5-9]\d|20[012]\d)\b/);
          const year = attrYear
            ?? l.vehicleInformation?.years?.[0]
            ?? (titleMatch ? parseInt(titleMatch[1], 10) : undefined);

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
            raw_content:       JSON.stringify({
              title:           l.title,
              description:     l.description,
              url:             l.url,
              priceCents:      l.price?.amount,
              priceRating:     attrs['pricerating'] ?? l.price?.priceAnalysis?.label,
              year,
              make,
              model,
              trim:            attrs['cartrim']          ?? l.vehicleInformation?.trim,
              mileageKm:       attrs['carmileageinkms']  ? parseInt(attrs['carmileageinkms'], 10) : l.vehicleInformation?.mileage,
              colour:          attrs['carcolor']         ?? l.vehicleInformation?.colour,
              colourInterior:  attrs['carinteriorcolor'] ?? undefined,
              bodyType:        BODY_MAP[attrs['carbodytype'] ?? ''] ?? attrs['carbodytype'] ?? l.vehicleInformation?.bodyType,
              drivetrain:      (attrs['drivetrain'] && attrs['drivetrain'] !== 'other') ? attrs['drivetrain'] : l.vehicleInformation?.drivetrain,
              fuelType:        attrs['carfueltype']      ?? l.vehicleInformation?.fuelType,
              transmission:    TRANS_MAP[attrs['cartransmission'] ?? ''] ?? l.vehicleInformation?.transmission,
              doors:           attrs['noofdoors']  ? parseInt(attrs['noofdoors'], 10)  : undefined,
              seats:           attrs['noofseats']  ? parseInt(attrs['noofseats'], 10)  : undefined,
              vin:             attrs['vin']               ?? l.vehicleInformation?.vin,
              condition:       attrs['vehicletype']       ?? undefined,
              carproofLink:    attrs['carprooflink']      ?? undefined,
              _sellerType:     sellerType,
              location:        l.location?.address ?? l.location?.name,
            }),
            raw_content_type:  'json' as const,
            listing_images:    (l.imageUrls ?? []).slice(0, 5),
            geo_region:        region.province,   // 'ON' or 'BC' based on region set
            scrape_run_id:     scrapeRunId,
            http_status:       200,
            proxy_used:        false,
            requires_auth:     false,
            is_dealer_listing: sellerType === 'delr',
          } satisfies RawPayload;
        });
    } catch (err) {
      const msg = (err as Error).message ?? '';
      const is429 = msg.includes('429') || msg.includes('Too Many');
      const waitMs = is429
        ? RETRY_429_MIN_MS + Math.floor(Math.random() * RETRY_429_JITTER)
        : 8_000 + Math.floor(Math.random() * 7_000);
      log(`[scraper-${workerId}] ${region.label} p${page} failed (${msg})${is429 ? ` — rate limited, waiting ${Math.round(waitMs/1000)}s` : ' — retrying'}`);
      if (is429) await sleep(waitMs);
      // Treat as non-empty so we don't falsely advance the region on a transient failure
      cursor.report(rIdx, 1);
      continue;
    }

    cursor.report(rIdx, payloads.length);
    if (payloads.length === 0) {
      log(`[scraper] ${region.label} p${page} → empty (Kijiji end)`);
      continue;
    }

    stats.scraped += payloads.length;

    // Dedup: skip listings seen this run or already in DB
    const runFresh = payloads.filter(p => !seenUrls.has(p.listing_url));
    for (const p of payloads) seenUrls.add(p.listing_url);

    if (runFresh.length === 0) { printStatus(); continue; }

    const urls = runFresh.map(p => p.listing_url);
    const { rows } = await pool.query(
      `SELECT source_url FROM listings WHERE source_url = ANY($1)`, [urls],
    );
    const inDb = new Set(rows.map((r: any) => r.source_url as string));
    const fresh = runFresh.filter(p => !inDb.has(p.listing_url));

    if (fresh.length === 0) {
      log(`[scraper] ${region.label} p${page}  ${payloads.length} kj → all in DB`);
      printStatus();
      continue;
    }

    // ── Advanced scrape for thin listings ────────────────
    const enriched: RawPayload[] = [];
    let advancedCount = 0;
    for (const p of fresh) {
      if (isThinPayload(p)) {
        const detailed = await fetchDetailPage(p, ua, region.label);
        enriched.push(detailed);
        advancedCount++;
      } else {
        enriched.push(p);
      }
    }

    queue.push(...enriched);
    stats.queued += enriched.length;
    const advancedNote = advancedCount > 0 ? `  [${advancedCount} detail-fetched]` : '';
    log(`[scraper] ${region.province} ${region.label} p${page}  ${payloads.length} kj → ${enriched.length} new  (queue:${queue.length})${advancedNote}`);
    printStatus();
  }
}

// ── Scraper worker ────────────────────────────────────────
// Phase 1: all 8 workers sweep Ontario forward.
// Phase 2 (auto): workers 0-5 → BC forward sweep | workers 6-7 → Ontario continuous loop.

async function scraperWorker(workerId: number, pool: any): Promise<void> {
  // Stagger startup by 200ms so workers don't all hit claim() simultaneously
  if (workerId > 0) await sleep(workerId * 200);

  // ── Phase 1: Ontario forward sweep ───────────────────────
  await sweepWithCursor(workerId, pool, onCursorP1);

  if (stopping) return;

  // Coordinate the phase transition: wait until ALL Phase-1 workers are done
  p1DoneCount++;
  if (p1DoneCount >= SCRAPER_WORKERS) {
    log(`\n${'═'.repeat(60)}`);
    log(`  ✓ Ontario sweep complete (${ON_REGIONS.length} regions)`);
    log(`  Phase 2: workers 0-${BC_WORKERS - 1} → BC | workers ${BC_WORKERS}-${SCRAPER_WORKERS - 1} → Ontario continuous`);
    log(`${'═'.repeat(60)}\n`);
    bcCursor   = new RegionCursor(BC_REGIONS, 'BC', false);
    onCursorP2 = new RegionCursor(ON_REGIONS, 'ON-loop', true);
    currentPhase = 2;
    phase2Resolve();
  }
  await phase2Signal; // wait until all workers have finished Phase 1

  if (stopping) return;

  // ── Phase 2: BC (workers 0-5) or Ontario continuous (workers 6-7) ────
  const p2Cursor = workerId < BC_WORKERS ? bcCursor : onCursorP2;
  const p2Role   = workerId < BC_WORKERS ? 'BC' : 'ON-loop';
  log(`[scraper-${workerId}] entering Phase 2 as ${p2Role}`);
  await sweepWithCursor(workerId, pool, p2Cursor);

  log(`[scraper-${workerId}] Phase 2 done — exiting`);
}

// ── Facebook Marketplace scraper loop ─────────────────────
// Runs in parallel with scraperWorker(). Restarts automatically after each
// session ends (FB limits scrolls per session), until stopping = true.

async function fbScraperLoop(pool: any): Promise<void> {
  let variantIdx = 0;

  while (!stopping) {
    const currentVariant = FB_URL_VARIANTS[variantIdx % FB_URL_VARIANTS.length];
    variantIdx++;

    const { rows: fbRows } = await pool.query(
      `SELECT source_url FROM listings WHERE source_url LIKE '%facebook.com/marketplace/item/%'`,
    );
    for (const row of fbRows) seenUrls.add(row.source_url as string);

    log(`[fb] Starting session: ${currentVariant.label} (variant ${variantIdx}/${FB_URL_VARIANTS.length})`);
    let sessionYielded = 0;

    try {
      for await (const payload of scrapeFacebook(seenUrls, log, () => stopping, currentVariant)) {
        if (stopping) break;

        while (queue.length >= QUEUE_MAX && !stopping) {
          await sleep(QUEUE_PAUSE_MS);
        }
        if (stopping) break;

        queue.push(payload);
        stats.scraped++;
        stats.queued++;
        sessionYielded++;

        if (sessionYielded % 20 === 0) {
          log(`[fb] session: ${sessionYielded} listings queued so far`);
        }
      }
    } catch (err) {
      log(`[fb] session error: ${(err as Error).message}`);
    }

    if (!stopping) {
      const restartDelay = sessionYielded > 0 ? 5_000 : 2_000;
      log(`[fb] session done (${sessionYielded} listings). Next variant in ${restartDelay / 1000}s...`);
      await sleep(restartDelay);
    }
  }

  log('[fb] scraper stopped');
}

// ── Normaliser worker ─────────────────────────────────────

const PROVIDER_DELAY_MS: Record<string, number> = {
  cerebras:     2_000,
  groq:         2_000,
  groq2:        2_000,
  gemini:       4_000,
  together:       500,  // Llama-3-8B-Lite — very fast serverless
  together_big:   500,  // Llama-3.3-70B-Turbo — same API, higher quality
  anthropic:    1_500,
};

async function normaliserWorker(id: number, pool: any, provider: string): Promise<void> {
  if (id > 0) {
    await sleep(id * 8_000);
    log(`[${provider}] worker started (staggered +${id * 8}s)`);
  }

  const interRequestDelay = PROVIDER_DELAY_MS[provider] ?? 2_000;
  let lastRequestTime = 0;

  while (!stopping || queue.length > 0) {
    const payload = queue.shift();
    if (!payload) {
      await sleep(500);
      continue;
    }

    const sinceLast = Date.now() - lastRequestTime;
    if (sinceLast < interRequestDelay) {
      await sleep(interRequestDelay - sinceLast);
    }
    lastRequestTime = Date.now();

    const raw   = JSON.parse(payload.raw_content) as any;
    const label = `${raw.year ?? '?'} ${raw.make ?? '?'} ${raw.model ?? '?'}`;
    const t0    = Date.now();

    // ── Pre-LLM price filter ───────────────────────────────
    const hasPriceCents = raw.priceCents != null && raw.priceCents > 0;
    const hasPriceInDesc = /\$\s*[\d,]+|\b\d{4,}\s*(?:obo|firm|asking|neg)/i.test(raw.description ?? '');
    const hasPaymentInDesc = /\$\s*\d+\s*\/\s*(?:mo|month|week|bi-?week|bw)/i.test(raw.description ?? '');
    if (!hasPriceCents && !hasPriceInDesc && !hasPaymentInDesc) {
      stats.rejected++;
      if (stats.rejected % 25 === 0) log(`[${provider}] no-price filter: ${stats.rejected} total rejected (latest: "${label}")`);
      printStatus();
      continue;
    }

    try {
      const noImages = payload.listing_images.length === 0;

      // M2a — extraction (fast path or LLM)
      let extraction: ExtractionResult;
      if (isFastPathEligible(payload)) {
        extraction = fastPathExtract(payload);
        stats.fastPathed++;
        if (stats.normalised % 50 === 0) {
          log(`[fast-path] ${label} — structured extract (0ms, no LLM)`);
        }
      } else {
        extraction = await extractFields(payload, provider);
      }
      const validated  = validateAndStandardise(extraction.fields);

      // Province override: if LLM couldn't determine province, use geo_region from scraper
      if (!validated.province && payload.geo_region) {
        validated.province = payload.geo_region; // 'ON' or 'BC'
      }

      // M2g — Vision colour detection (advanced-scraped payloads only)
      if (payload._advancedScrape && !noImages && !validated.colour_exterior) {
        const vision = await detectColour(payload.listing_images);
        if (vision.colour) {
          validated.colour_exterior = vision.colour;
          validated._validationWarnings.push(`colour_exterior filled by vision: ${vision.colour}`);
        }
      }

      // M2f — CARFAX enrichment (VIN known & key fields missing)
      if (validated.vin && (validated.accidents == null || validated.owners == null)) {
        const carfax = await lookupCarfax(validated.vin);
        if (carfax) {
          if (validated.accidents == null && carfax.accidents !== null) {
            validated.accidents = carfax.accidents;
            validated._validationWarnings.push(`accidents from carfax: ${carfax.accidents}`);
          }
          if (validated.owners == null && carfax.owners !== null) {
            validated.owners = carfax.owners;
            validated._validationWarnings.push(`owners from carfax: ${carfax.owners}`);
          }
          if (carfax.hasLien)   validated._validationWarnings.push('CARFAX: active lien registered');
          if (carfax.stolen)    validated._validationWarnings.push('CARFAX: reported stolen');
          if (carfax.hasRecalls) validated._validationWarnings.push('CARFAX: open safety recalls');
        }
      }

      // M2d — PII redaction
      const redaction  = redactPII(validated.description);
      validated.description = redaction.text;

      // M2c — Confidence scoring
      const scored = computeConfidence(validated, noImages);
      await routeAndWrite(pool, payload, scored, extraction, redaction, redaction.failed);

      const ms = Date.now() - t0;
      stats.normalised++;
      stats.totalMs += ms;
      if (scored.outcome === 'published') stats.published++;
      else if (scored.outcome === 'review') stats.review++;
      else stats.rejected++;

      if (stats.normalised % PROGRESS_EVERY === 0) {
        log(`[${provider}] [${stats.normalised}] ${label}  →  ${scored.outcome} (${scored.confidence_score})  ${ms}ms`);
      }
    } catch (err) {
      const msg = (err as Error).message ?? '';

      const retryMatch = msg.match(/try again in (\d+)m([\d.]+)s/i)
                      ?? msg.match(/try again in ([\d.]+)s/i);
      const is429 = msg.includes('429');

      if (retryMatch) {
        const waitMs = retryMatch[2] != null
          ? (parseInt(retryMatch[1], 10) * 60 + parseFloat(retryMatch[2])) * 1000
          : parseFloat(retryMatch[1]) * 1000;
        queue.unshift(payload);
        log(`[${provider}] rate-limited — waiting ${Math.ceil(waitMs / 1000)}s then retrying`);
        await sleep(waitMs + 2_000);
      } else if (is429) {
        queue.unshift(payload);
        log(`[${provider}] rate-limited (no retry hint) — waiting 90s then retrying`);
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


// ── Final summary ─────────────────────────────────────────

async function showSummary(pool: any): Promise<void> {
  const { rows } = await pool.query(`
    SELECT
      COUNT(*)                                        AS total,
      COUNT(*) FILTER (WHERE status = 'active')      AS published,
      COUNT(*) FILTER (WHERE status = 'review')      AS in_review,
      COUNT(*) FILTER (WHERE status = 'rejected')    AS rejected,
      ROUND(AVG(confidence_score))                   AS avg_score,
      COUNT(*) FILTER (WHERE price IS NOT NULL)      AS has_price
    FROM listings
  `);
  const s = rows[0];

  const avgMs = stats.normalised > 0 ? Math.round(stats.totalMs / stats.normalised) : 0;

  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('  Run summary');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log(`  Scraped total  : ${stats.scraped}`);
  console.log(`  Normalised     : ${stats.normalised}  (${stats.errors} errors)`);
  console.log(`  Published      : ${stats.published}   Review: ${stats.review}   Rejected: ${stats.rejected}`);
  console.log(`  Avg LLM time   : ${avgMs}ms per listing`);
  console.log('');
  console.log('  All-time DB totals:');
  console.log(`  Total: ${s.total}   Published: ${s.published}   Review: ${s.in_review}   Rejected: ${s.rejected}`);
  console.log(`  Avg confidence: ${s.avg_score}/100   With price: ${s.has_price}/${s.total}`);
  console.log('');
}

// ── Dedup loop ─────────────────────────────────────────────

const DEDUP_INTERVAL_MS = 30 * 60 * 1000; // every 30 minutes

async function dedupLoop(pool: any): Promise<void> {
  await sleep(5 * 60 * 1000);
  while (!stopping) {
    try {
      const stats = await runDeduplication(pool, (msg: string) => log(msg));
      if (stats.groups > 0) {
        log(`[dedup] pass complete — ${stats.groups} groups | exact:${stats.exact} strong:${stats.strong} weak:${stats.weak} | ${stats.rejected} rejected`);
      }
    } catch (err) {
      log(`[dedup] error: ${(err as Error).message}`);
    }
    await sleep(DEDUP_INTERVAL_MS);
  }
}

// ── Main ──────────────────────────────────────────────────

async function main(): Promise<void> {
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('  Aven — Kijiji + Facebook → M2 Pipeline  (Ctrl+C to stop)');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

  const pool = getPool();
  await pool.query('SELECT 1');
  console.log('✓ Postgres connected');
  console.log(`✓ Normaliser workers: ${WORKER_PROVIDERS.map((p, i) => `${i}→${p}`).join('  ')}   Queue max: ${QUEUE_MAX}`);
  console.log(`✓ Scraper workers: ${SCRAPER_WORKERS}`);
  console.log(`  Phase 1: all ${SCRAPER_WORKERS} workers → Ontario (${ON_REGIONS.length} regions)`);
  console.log(`  Phase 2: workers 0-${BC_WORKERS - 1} → BC (${BC_REGIONS.length} regions) | workers ${BC_WORKERS}-${SCRAPER_WORKERS - 1} → Ontario continuous`);
  console.log(`✓ ON regions: ${ON_REGIONS.map(r => r.label).join(' → ')}`);
  console.log(`✓ BC regions: ${BC_REGIONS.map(r => r.label).join(' → ')}`);
  console.log(`✓ Rate: ${KIJIJI_RPM} RPM global gate, up to ${MAX_PAGES} pages/region, empty-stop after ${EMPTY_PAGE_STOP} empties`);
  console.log('\n  Press Ctrl+C to stop cleanly.\n');

  // Seed seenUrls from DB so we don't re-enqueue existing Kijiji listings
  log('Loading existing Kijiji URLs from DB…');
  const { rows: existingRows } = await pool.query(
    `SELECT source_url FROM listings WHERE source_id = 'kijiji-ca'`,
  );
  for (const r of existingRows) seenUrls.add(r.source_url as string);
  log(`Loaded ${seenUrls.size} existing Kijiji URLs — won't re-queue these`);

  const workers = WORKER_PROVIDERS.map((provider, i) =>
    normaliserWorker(i, getPool(), provider),
  );

  const fbEnabled = process.env.FB_ENABLED === 'true';
  const hasFbSession = fbEnabled && (
    require('fs').existsSync(require('path').join(__dirname, 'fb-session.json'))
    || !!process.env.FB_STORAGE_STATE
  );

  if (hasFbSession) {
    console.log('✓ Facebook Marketplace: enabled');
  } else {
    console.log('  Facebook Marketplace: disabled (set FB_ENABLED=true to re-enable)');
  }
  console.log('');

  const scrapers = Array.from({ length: SCRAPER_WORKERS }, (_, i) => scraperWorker(i, pool));

  // Workers 6-7 run an infinite Ontario continuous loop in Phase 2 — they only
  // stop on Ctrl+C. Workers 0-5 stop naturally when BC is exhausted.
  // When workers 0-5 finish BC, signal stopping so normaliser drains and exits.
  Promise.all(scrapers.slice(0, BC_WORKERS)).then(() => {
    if (!stopping) {
      stopping = true;
      log('BC sweep complete — draining normaliser queue then exiting…');
    }
  });

  await Promise.all([
    ...scrapers,
    ...(hasFbSession ? [fbScraperLoop(pool)] : []),
    dedupLoop(pool),
    ...workers,
  ]);

  await showSummary(pool);
  await closePool();
}

main().catch((err: Error) => {
  console.error('\nFatal:', err.message);
  process.exit(1);
});
