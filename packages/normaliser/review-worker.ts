/**
 * Aven — Review Queue Worker
 *
 * Two parallel agents that continuously drain the review queue by re-running
 * the LLM extraction + confidence scorer on each pending listing.
 *
 * Decision logic per listing:
 *   score ≥ 70                           → APPROVE  (status: active, needs_review: false)
 *   score ≥ 60 AND rerun_count ≥ 2       → APPROVE  (good enough after retries)
 *   score < 50                           → REJECT   (status: rejected)
 *   rerun_count ≥ 3 AND score < 60       → REJECT   (stuck — not improving)
 *   otherwise                            → RETRY    (increment rerun_count, leave in queue)
 *
 * Workers use Groq (worker 0) and Groq2 (worker 1) — free-tier, fast,
 * sufficient for re-extraction of mostly-structured Kijiji data.
 * Falls back to Cerebras if Groq2 key is not set.
 *
 * How to run:
 *   npx ts-node review-worker.ts
 *   Ctrl+C to stop after current batch finishes.
 */

import * as dotenv from 'dotenv';
import * as path   from 'path';
dotenv.config({ path: path.join(__dirname, '.env'), override: true });

import { getPool, closePool }    from './src/lib/db';
import { extractFields }         from './src/m2a-extractor';
import { validateAndStandardise }from './src/m2b-validator';
import { redactPII }             from './src/m2d-redactor';
import { computeConfidence }     from './src/m2c-scorer';
import { RawPayload }            from './src/types';
import { randomUUID as uuidv4 }  from 'crypto';

// ── Config ────────────────────────────────────────────────

const WORKER_PROVIDERS = [
  'groq',
  process.env.GROQ_API_KEY_2 ? 'groq2' : 'cerebras',
] as const;

// Delay between requests per provider (ms)
const PROVIDER_DELAY_MS: Record<string, number> = {
  groq:     2_200,
  groq2:    2_200,
  cerebras: 2_200,
};

// How long to sleep when queue is empty before rechecking
const EMPTY_POLL_MS = 15_000;

// ── State ─────────────────────────────────────────────────

let stopping = false;
const stats = { approved: 0, rejected: 0, retried: 0, errors: 0 };

process.on('SIGINT', () => {
  if (!stopping) {
    stopping = true;
    process.stdout.write('\n');
    log('Ctrl+C — finishing current items then stopping…');
  }
});

// ── Helpers ───────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

function log(msg: string): void {
  console.log(`  [review-worker] ${msg}`);
}

function printStats(): void {
  process.stdout.write(
    `\r  approved:${stats.approved}  rejected:${stats.rejected}  retried:${stats.retried}  errors:${stats.errors}  `
  );
}

// ── Reconstruct a RawPayload from a stored listing row ────
// The review_queue row has a FK to listings, which has all the
// extracted (and human-readable) data. We rebuild a JSON blob
// that looks like what the Kijiji scraper would have produced,
// so the LLM prompt sees the same format it was trained on.

function buildPayloadFromListing(listing: any): RawPayload {
  const rawContent = JSON.stringify({
    title:       `${listing.year ?? ''} ${listing.make ?? ''} ${listing.model ?? ''} ${listing.trim ?? ''}`.trim(),
    description: listing.description,
    priceCents:  listing.price ? listing.price * 100 : undefined,
    year:        listing.year,
    make:        listing.make,
    model:       listing.model,
    trim:        listing.trim,
    mileageKm:   listing.mileage_km,
    colour:      listing.colour_exterior,
    colourInterior: listing.colour_interior,
    bodyType:    listing.body_type,
    drivetrain:  listing.drivetrain,
    fuelType:    listing.fuel_type,
    transmission:listing.transmission,
    doors:       listing.doors,
    seats:       listing.seats,
    vin:         listing.vin,
    condition:   listing.condition,
    location:    listing.city ? `${listing.city}${listing.province ? ', ' + listing.province : ''}` : undefined,
    _sellerType: listing.seller_type === 'Dealer' ? 'delr' : listing.seller_type === 'Private' ? 'ownr' : undefined,
  });

  return {
    payload_id:        uuidv4(),
    source_id:         listing.source_id,
    source_category:   'classifieds',
    listing_url:       listing.source_url,
    scrape_timestamp:  new Date().toISOString(),
    connector_version: '1.2.0',
    raw_content:       rawContent,
    raw_content_type:  'json',
    listing_images:    listing.photo_urls ?? [],
    geo_region:        'ON',
    scrape_run_id:     listing.scrape_run_id ?? uuidv4(),
    http_status:       200,
    proxy_used:        false,
    requires_auth:     false,
    is_dealer_listing: listing.seller_type === 'Dealer',
    _advancedScrape:   (listing.photo_urls?.length ?? 0) > 0,
  };
}

// ── Worker ────────────────────────────────────────────────

async function reviewWorker(workerId: number, provider: string): Promise<void> {
  const pool = getPool();
  const delay = PROVIDER_DELAY_MS[provider] ?? 2_200;

  // Stagger startup so workers don't contend on the same queue item
  if (workerId > 0) await sleep(workerId * 5_000);
  log(`Worker ${workerId} (${provider}) started`);

  let lastRequestTime = 0;

  while (!stopping) {
    // Rate-limit gap between requests
    const sinceLast = Date.now() - lastRequestTime;
    if (sinceLast < delay) await sleep(delay - sinceLast);

    // Grab next pending item — use SKIP LOCKED so the two workers don't race
    let queueId: string | null = null;
    let listing: any = null;
    let rerunCount = 0;

    {
      let client: import('pg').PoolClient | null = await pool.connect();
      try {
        const { rows } = await client.query(`
          SELECT rq.id AS queue_id, rq.rerun_count,
            l.id AS listing_id, l.source_id, l.source_url, l.scrape_run_id,
            l.make, l.model, l.year, l.trim, l.body_type, l.drivetrain,
            l.fuel_type, l.transmission, l.colour_exterior, l.colour_interior,
            l.engine, l.doors, l.seats, l.vin, l.condition, l.mileage_km,
            l.safetied, l.accidents, l.owners,
            l.price, l.price_type, l.price_raw, l.payment_amount, l.payment_frequency,
            l.city, l.province, l.seller_type, l.description,
            l.photo_urls
          FROM public.review_queue rq
          JOIN public.listings l ON l.id = rq.listing_id
          WHERE rq.decision IS NULL
          ORDER BY rq.rerun_count ASC, rq.created_at ASC
          LIMIT 1
          FOR UPDATE OF rq SKIP LOCKED
        `);

        if (!rows[0]) {
          // Queue empty — release client, null it out so finally skips, then sleep
          client.release(); client = null;
          await sleep(EMPTY_POLL_MS);
          continue;
        }

        queueId    = rows[0].queue_id as string;
        listing    = rows[0];
        rerunCount = Number(rows[0].rerun_count ?? 0);

        // Immediately claim this item by incrementing rerun_count,
        // so the other worker skips it during our processing window
        await client.query(
          `UPDATE public.review_queue SET rerun_count = rerun_count + 1 WHERE id = $1`,
          [queueId]
        );
      } finally {
        if (client) client.release();
      }
    }

    lastRequestTime = Date.now();
    const label = `${listing.year ?? '?'} ${listing.make ?? '?'} ${listing.model ?? '?'}`;
    log(`Worker ${workerId}: processing [${label}] (attempt ${rerunCount + 1})`);

    try {
      const payload    = buildPayloadFromListing(listing);
      const extraction = await extractFields(payload, provider);
      const validated  = validateAndStandardise(extraction.fields);
      const redaction  = redactPII(validated.description);
      validated.description = redaction.text;
      const scored     = computeConfidence(validated, payload.listing_images.length === 0);

      const score     = scored.confidence_score;
      const attempts  = rerunCount + 1; // includes this one

      // Decide outcome
      let decision: 'approved' | 'rejected' | 'retry';
      if (score >= 70)                          decision = 'approved';
      else if (score >= 60 && attempts >= 2)    decision = 'approved';  // good enough after retries
      else if (score < 50)                      decision = 'rejected';
      else if (attempts >= 3 && score < 60)     decision = 'rejected';  // stuck
      else                                      decision = 'retry';

      const db = await pool.connect();
      try {
        await db.query('BEGIN');

        if (decision === 'approved') {
          // Merge any newly-extracted fields back into listings, publish it
          await db.query(`
            UPDATE public.listings SET
              status           = 'active',
              needs_review     = false,
              confidence_score = $1,
              confidence_details = $2,
              extraction_model = $3,
              colour_exterior  = COALESCE($4, colour_exterior),
              colour_interior  = COALESCE($5, colour_interior),
              mileage_km       = COALESCE($6, mileage_km),
              body_type        = COALESCE($7, body_type),
              drivetrain       = COALESCE($8, drivetrain),
              fuel_type        = COALESCE($9, fuel_type),
              transmission     = COALESCE($10, transmission),
              vin              = COALESCE($11, vin),
              updated_at       = NOW()
            WHERE id = $12
          `, [
            score,
            JSON.stringify(scored.confidence_details),
            extraction.model,
            validated.colour_exterior,
            validated.colour_interior,
            validated.mileage_km,
            validated.body_type,
            validated.drivetrain,
            validated.fuel_type,
            validated.transmission,
            validated.vin,
            listing.listing_id,
          ]);
          await db.query(
            `UPDATE public.review_queue SET decision = 'approved', reviewed_at = NOW() WHERE id = $1`,
            [queueId]
          );
          await db.query('COMMIT');
          stats.approved++;
          log(`Worker ${workerId}: ✓ APPROVED [${label}] score=${score}`);

        } else if (decision === 'rejected') {
          await db.query(
            `UPDATE public.listings SET status = 'rejected', needs_review = false, updated_at = NOW() WHERE id = $1`,
            [listing.listing_id]
          );
          await db.query(
            `UPDATE public.review_queue SET decision = 'rejected', reviewed_at = NOW() WHERE id = $1`,
            [queueId]
          );
          await db.query('COMMIT');
          stats.rejected++;
          log(`Worker ${workerId}: ✗ REJECTED [${label}] score=${score} attempts=${attempts}`);

        } else {
          // retry — rerun_count already incremented above; just commit
          await db.query('COMMIT');
          stats.retried++;
          log(`Worker ${workerId}: ↺ RETRY [${label}] score=${score} attempt=${attempts}`);
        }
      } catch (dbErr) {
        await db.query('ROLLBACK').catch(() => {});
        throw dbErr;
      } finally {
        db.release();
      }
    } catch (err) {
      const msg = (err as Error).message ?? '';
      const is429 = msg.includes('429') || msg.includes('rate') || msg.toLowerCase().includes('too many');
      stats.errors++;
      log(`Worker ${workerId}: error on [${label}] — ${msg.slice(0, 100)}`);
      if (is429) {
        log(`Worker ${workerId}: rate-limited — backing off 90s`);
        await sleep(90_000);
      }
    }

    printStats();
  }

  log(`Worker ${workerId} stopped — approved:${stats.approved} rejected:${stats.rejected} retried:${stats.retried}`);
}

// ── Main ──────────────────────────────────────────────────

async function main(): Promise<void> {
  const pool = getPool();

  // Verify DB connection
  await pool.query('SELECT 1');
  log(`Connected to DB. Starting ${WORKER_PROVIDERS.length} review workers…`);

  // Check queue depth
  const { rows } = await pool.query(
    `SELECT COUNT(*) AS n FROM public.review_queue WHERE decision IS NULL`
  );
  log(`Pending review items: ${rows[0].n}`);

  if (Number(rows[0].n) === 0) {
    log('Queue is already clear — nothing to do.');
    await closePool();
    return;
  }

  await Promise.all(
    WORKER_PROVIDERS.map((provider, i) => reviewWorker(i, provider))
  );

  process.stdout.write('\n');
  log(`All workers done. Final: approved=${stats.approved} rejected=${stats.rejected} retried=${stats.retried} errors=${stats.errors}`);
  await closePool();
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
