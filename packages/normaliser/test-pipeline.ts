/**
 * Aven — End-to-End Scrape + Normalise Pipeline
 *
 * What this does:
 *   Runs Kijiji and Facebook Marketplace scrapers in parallel with a pool of LLM
 *   normaliser workers. Scraped listings flow through a shared in-memory queue:
 *
 *     scraperLoop()     → queue[] → normaliserWorker(0) → Postgres
 *     fbScraperLoop()   ↗           normaliserWorker(1) ↗
 *                                   normaliserWorker(2) ↗
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
 *
 * Sections in this file:
 *   1. Config & shared state     — constants, queue, stats counter
 *   2. scraperLoop()             — Kijiji pagination scraper
 *   3. fbScraperLoop()           — Facebook Marketplace wrapper (restarts per variant)
 *   4. normaliserWorker()        — LLM pipeline + DB write, one instance per provider
 *   5. dedupLoop()               — periodic deduplication pass
 *   6. main()                    — wires everything together with Promise.all()
 */

import * as dotenv from 'dotenv';
import * as path from 'path';
dotenv.config({ path: path.join(__dirname, '.env'), override: true });

import axios from 'axios';
import { v4 as uuidv4 } from 'uuid';
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

const PAGE_DELAY_MS  = 1_200;  // ms between Kijiji page fetches (polite scraping)

const KIJIJI_REGIONS = [
  { label: 'Toronto',      url: 'https://www.kijiji.ca/b-cars-trucks/city-of-toronto/c174l1700273' },
  { label: 'Peel',         url: 'https://www.kijiji.ca/b-cars-trucks/mississauga-peel-region/c174l1700276' },
  { label: 'York',         url: 'https://www.kijiji.ca/b-cars-trucks/markham-york-region/c174l1700277' },
  { label: 'Durham',       url: 'https://www.kijiji.ca/b-cars-trucks/durham-region/c174l1700275' },
  { label: 'Hamilton',     url: 'https://www.kijiji.ca/b-cars-trucks/hamilton/c174l80014' },
  { label: 'Halton',       url: 'https://www.kijiji.ca/b-cars-trucks/oakville-halton-region/c174l1700278' },
];

// ── Queue backpressure ────────────────────────────────────
// The scraper is faster than the LLM workers, so we cap the in-memory queue.
// When queue.length >= QUEUE_MAX the scraper loops on QUEUE_PAUSE_MS sleeps
// until workers have drained it below the threshold. This prevents unbounded
// memory growth during long overnight runs.
const QUEUE_MAX      = 500;    // max pending payloads before scraper pauses
const QUEUE_PAUSE_MS = 2000;   // how often to re-check queue size while paused

// Each worker has its own dedicated provider so they never contend.
// Worker 0 → Cerebras, Worker 1 → Groq, Worker 2 → Gemini Flash
const WORKER_PROVIDERS = ['cerebras', 'groq', 'gemini'] as const;
const PROGRESS_EVERY   = 10;    // print a stats line every N normalisations

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

// ── Graceful shutdown ─────────────────────────────────────

process.on('SIGINT', () => {
  if (!stopping) {
    stopping = true;
    process.stdout.write('\n');
    log('Ctrl+C — draining queue then stopping…');
  }
});

// ── Helpers ───────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

function log(msg: string): void {
  process.stdout.write(`\r${' '.repeat(100)}\r`);  // clear progress line
  console.log(`  ${msg}`);
}

function printStatus(): void {
  const avg = stats.normalised > 0 ? Math.round(stats.totalMs / stats.normalised) : 0;
  process.stdout.write(
    `\r  scraped:${stats.scraped}  queue:${queue.length}  normalised:${stats.normalised}` +
    `  pub:${stats.published}  review:${stats.review}  rej:${stats.rejected}` +
    `  fast:${stats.fastPathed}  avg:${avg}ms  `,
  );
}

// ── Scraper loop ──────────────────────────────────────────

async function scraperLoop(pool: any, region: { label: string; url: string }, staggerMs = 0): Promise<void> {
  if (staggerMs > 0) await sleep(staggerMs);
  let page = 1;

  while (!stopping) {
    // Backpressure — pause if queue is too large
    while (queue.length >= QUEUE_MAX && !stopping) {
      await sleep(QUEUE_PAUSE_MS);
    }
    if (stopping) break;

    const url = page === 1 ? region.url : `${region.url}?page=${page}`;
    let payloads: RawPayload[];

    try {
      const res = await axios.get(url, {
        headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124 Safari/537.36' },
        timeout: 20_000,
      });

      const scriptMatch = (res.data as string).match(/<script id="__NEXT_DATA__"[^>]*>(.*?)<\/script>/s);
      if (!scriptMatch) { page++; continue; }

      const apollo = (JSON.parse(scriptMatch[1]) as any)?.props?.pageProps?.__APOLLO_STATE__ as Record<string, unknown>;
      if (!apollo)  { page++; continue; }

      const scrapeRunId = uuidv4();
      payloads = Object.entries(apollo)
        .filter(([key]) => key.startsWith('AutosListing:'))
        .map(([, raw]) => {
          const l = raw as any;
          const listingUrl = l.url
            ? (l.url.startsWith('http') ? l.url : `https://www.kijiji.ca${l.url}`)
            : `https://www.kijiji.ca/v-cars/${l.id}`;

          // Parse attributes.all into a flat key→value map (primary data source)
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

          // Year: attributes > vehicleInformation > title regex
          const attrYear   = attrs['caryear'] ? parseInt(attrs['caryear'], 10) : undefined;
          const titleMatch = (l.title as string | undefined)?.match(/\b(19[5-9]\d|20[012]\d)\b/);
          const year = attrYear
            ?? l.vehicleInformation?.years?.[0]
            ?? (titleMatch ? parseInt(titleMatch[1], 10) : undefined);

          // Make/model: skip Kijiji sentinels 'othrmake' / 'othrmdl' (means "not in our list")
          const make  = (attrs['carmake']  && attrs['carmake']  !== 'othrmake') ? attrs['carmake']  : (l.vehicleInformation?.make  ?? undefined);
          const model = (attrs['carmodel'] && attrs['carmodel'] !== 'othrmdl')  ? attrs['carmodel'] : (l.vehicleInformation?.model ?? undefined);

          const sellerType = attrs['forsaleby'] ?? l.forsaleby;

          return {
            payload_id:        uuidv4(),
            source_id:         'kijiji-ca',
            source_category:   'classifieds',
            listing_url:       listingUrl,
            scrape_timestamp:  new Date().toISOString(),
            connector_version: '1.2.0',
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
            geo_region:        'ON-GTA',
            scrape_run_id:     scrapeRunId,
            http_status:       200,
            proxy_used:        false,
            requires_auth:     false,
            is_dealer_listing: sellerType === 'delr',
          } satisfies RawPayload;
        });
    } catch (err) {
      log(`[kj:${region.label}] page ${page} failed (${(err as Error).message}) — retrying page 1`);
      page = 1;
      await sleep(5_000);
      continue;
    }

    if (payloads.length === 0) {
      log(`[kj:${region.label}] page ${page} empty — wrapping to page 1`);
      page = 1;
      await sleep(PAGE_DELAY_MS);
      continue;
    }

    // Dedup: filter URLs seen this run + check DB
    const runFresh = payloads.filter(p => !seenUrls.has(p.listing_url));
    if (runFresh.length === 0) {
      page++;
      await sleep(PAGE_DELAY_MS);
      continue;
    }

    const urls = runFresh.map(p => p.listing_url);
    const { rows } = await pool.query(
      `SELECT source_url FROM listings WHERE source_url = ANY($1)`, [urls],
    );
    const inDb = new Set(rows.map((r: any) => r.source_url as string));
    const fresh = runFresh.filter(p => !inDb.has(p.listing_url));

    // Mark all as seen (even DB dupes) so we don't re-check them
    for (const p of runFresh) seenUrls.add(p.listing_url);

    stats.scraped += payloads.length;
    if (fresh.length > 0) {
      queue.push(...fresh);
      stats.queued += fresh.length;
      log(`[kj:${region.label}] page ${page}  ${payloads.length} scraped  →  ${fresh.length} new queued  (queue: ${queue.length})`);
    } else {
      log(`[kj:${region.label}] page ${page}  ${payloads.length} scraped  →  all already in DB`);
    }

    page++;
    await sleep(PAGE_DELAY_MS);
  }

  log(`[kj:${region.label}] stopped`);
}

// ── Facebook Marketplace scraper loop ─────────────────────
// Runs in parallel with scraperLoop(). Restarts automatically after each
// session ends (FB limits scrolls per session), until stopping = true.

async function fbScraperLoop(pool: any): Promise<void> {
  // Cycle through all city×price-band variants, one per browser session.
  // One session per variant prevents FB from detecting multi-page automation patterns.
  let variantIdx = 0;

  while (!stopping) {
    // Pick the next variant to scan (round-robin)
    const currentVariant = FB_URL_VARIANTS[variantIdx % FB_URL_VARIANTS.length];
    variantIdx++;

    // Refresh seenUrls from DB so already-processed FB listings aren't re-queued
    const { rows: fbRows } = await pool.query(
      `SELECT source_url FROM listings WHERE source_url LIKE '%facebook.com/marketplace/item/%'`,
    );
    for (const row of fbRows) seenUrls.add(row.source_url as string);

    log(`[fb] Starting session: ${currentVariant.label} (variant ${variantIdx}/${FB_URL_VARIANTS.length})`);
    let sessionYielded = 0;

    try {
      for await (const payload of scrapeFacebook(seenUrls, log, () => stopping, currentVariant)) {
        if (stopping) break;

        // Backpressure
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
      // Short break between variants — enough for FB to not see rapid repeated requests
      // Reduced from 20s/10s to 8s/4s — delays add up significantly across 40 variants
      const restartDelay = sessionYielded > 0 ? 5_000 : 2_000;
      log(`[fb] session done (${sessionYielded} listings). Next variant in ${restartDelay / 1000}s...`);
      await sleep(restartDelay);
    }
  }

  log('[fb] scraper stopped');
}

// ── Normaliser worker ─────────────────────────────────────

// Proactive inter-request delay per provider (prevents burst-then-429 cycle).
// Each worker sleeps this long between requests — much cheaper than hitting the
// rate limit, waiting for a retry-after header, and re-queuing the payload.
//
// How the numbers are derived:
//   Cerebras  free tier: 30 RPM cap  → 1 req/2s = 30 RPM theoretical max.
//                        Pulled back to 3.5s (~17 RPM) because daily token limits
//                        are the binding constraint, not per-minute rate.
//   Groq      free tier: 30 RPM + 14400 RPD for 8b-instant.
//                        3.5s keeps us at ~17 RPM, sustainable for overnight runs.
//   Gemini    free tier: 15 RPM hard cap on Flash models.
//                        5s = ~12 RPM — gives headroom for startup bursts.
//   Anthropic paid key:  ~1000 RPM (varies by tier) — 1.5s is conservative.
const PROVIDER_DELAY_MS: Record<string, number> = {
  cerebras:  3_500,   // ~17 RPM — below 30 RPM cap; daily limit is the real bottleneck
  groq:      3_500,   // ~17 RPM — sustainable all-day without hitting 14400 RPD ceiling
  gemini:    5_000,   // ~12 RPM — comfortably under the 15 RPM free-tier hard limit
  anthropic: 1_500,   // ~40 RPM — paid key; generous limits so we can run faster
};

async function normaliserWorker(id: number, pool: any, provider: string): Promise<void> {
  // Stagger worker startup to avoid all workers hitting rate limits simultaneously.
  // Worker 0 starts immediately; workers 1, 2, ... each wait an extra 8 seconds.
  // This spreads the initial burst across providers and avoids simultaneous 429s.
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

    // Proactive rate limiting: ensure minimum gap between requests
    const sinceLast = Date.now() - lastRequestTime;
    if (sinceLast < interRequestDelay) {
      await sleep(interRequestDelay - sinceLast);
    }
    lastRequestTime = Date.now();

    const raw   = JSON.parse(payload.raw_content) as any;
    const label = `${raw.year ?? '?'} ${raw.make ?? '?'} ${raw.model ?? '?'}`;
    const t0    = Date.now();

    try {
      const noImages = payload.listing_images.length === 0;

      // M2a — extraction (fast path or LLM)
      let extraction: ExtractionResult;
      if (isFastPathEligible(payload)) {
        extraction = fastPathExtract(payload);
        stats.fastPathed++;
        // log fast-path usage every 50 listings for visibility
        if (stats.normalised % 50 === 0) {
          log(`[fast-path] ${label} — structured extract (0ms, no LLM)`);
        }
      } else {
        extraction = await extractFields(payload, provider);
      }
      const validated  = validateAndStandardise(extraction.fields);

      // M2g — Vision colour detection (only if colour missing and images exist)
      if (!noImages && !validated.colour_exterior) {
        const vision = await detectColour(payload.listing_images);
        if (vision.colour) {
          validated.colour_exterior = vision.colour;
          validated._validationWarnings.push(`colour_exterior filled by vision: ${vision.colour}`);
        }
      }

      // M2f — CARFAX enrichment (only if VIN known and key fields missing)
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
          // Flag lien/stolen regardless
          if (carfax.hasLien)   validated._validationWarnings.push('CARFAX: active lien registered');
          if (carfax.stolen)    validated._validationWarnings.push('CARFAX: reported stolen');
          if (carfax.hasRecalls) validated._validationWarnings.push('CARFAX: open safety recalls');
        }
      }

      // M2d — PII redaction
      const redaction  = redactPII(validated.description);
      validated.description = redaction.text;

      // M2c — Confidence scoring (hard reject rules also applied here)
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

      // ── Rate-limit backoff ────────────────────────────────
      // Some providers embed the exact retry delay in the error message.
      // Parse both formats before falling back to a fixed 90s pause.
      //   Groq/OpenAI format: "Please try again in 14m52.5s"  → groups: [min, sec]
      //   Short format:       "Please try again in 3.5s"      → groups: [sec]
      const retryMatch = msg.match(/try again in (\d+)m([\d.]+)s/i)
                      ?? msg.match(/try again in ([\d.]+)s/i);
      const is429 = msg.includes('429');

      if (retryMatch) {
        // retryMatch[2] present → "Xm Y.Zs" format; absent → "Y.Zs" only
        const waitMs = retryMatch[2] != null
          ? (parseInt(retryMatch[1], 10) * 60 + parseFloat(retryMatch[2])) * 1000
          : parseFloat(retryMatch[1]) * 1000;
        // Return payload to the FRONT of the queue so it's next when we resume
        queue.unshift(payload);
        log(`[${provider}] rate-limited — waiting ${Math.ceil(waitMs / 1000)}s then retrying`);
        await sleep(waitMs + 2_000);  // +2s buffer so the window has fully reset
      } else if (is429) {
        // Provider returned 429 with no retry hint (e.g. Cerebras) — fixed 90s back-off.
        // Why 90s: Cerebras resets its per-minute window every ~60s; 90s gives headroom.
        queue.unshift(payload);
        log(`[${provider}] rate-limited (no retry hint) — waiting 90s then retrying`);
        await sleep(90_000);
      } else {
        // Non-rate-limit error (network failure, bad response, etc.) — log and move on
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
// Runs the deduplication engine every DEDUP_INTERVAL_MS in the background.
// Starts after an initial delay to let the first scrape batch arrive.

const DEDUP_INTERVAL_MS = 30 * 60 * 1000; // every 30 minutes

async function dedupLoop(pool: any): Promise<void> {
  // Wait 5 minutes before first pass so the pipeline has data to work with
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
  console.log(`✓ Workers: ${WORKER_PROVIDERS.map((p, i) => `${i}→${p}`).join('  ')}   Queue max: ${QUEUE_MAX}`);
  console.log(`✓ Kijiji regions: ${KIJIJI_REGIONS.map(r => r.label).join(', ')}`);
  console.log('\n  Scraper runs at full speed. Each worker uses its own LLM provider in parallel.');
  console.log('  Press Ctrl+C to stop cleanly.\n');

  // Start both scrapers + one worker per provider, all concurrently
  const workers = WORKER_PROVIDERS.map((provider, i) =>
    normaliserWorker(i, getPool(), provider),
  );

  // FB_ENABLED=true must be explicitly set to turn on Facebook scraping.
  // Disabled by default — set FB_ENABLED=true in .env to re-enable.
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

  await Promise.all([
    ...KIJIJI_REGIONS.map((region, i) => scraperLoop(pool, region, i * 3_000)),
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
