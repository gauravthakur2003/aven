/**
 * M2e — Review Queue Router
 *
 * Writes a scored, validated listing to PostgreSQL in a single transaction:
 *
 *   1. listings (upsert) — the canonical record. ON CONFLICT on source_url
 *      updates only mutable fields (price, mileage, status, score) so that
 *      editorial corrections made via the dashboard are not overwritten by
 *      a re-scrape of the same URL.
 *
 *   2. review_queue (insert if needed) — queues the listing for human review
 *      when confidence is 50–69 or PII redaction failed.
 *
 *   3. extraction_log (always insert) — one row per pipeline run for audit,
 *      cost tracking, and model comparison. Written for ALL outcomes including
 *      rejected listings — see note below.
 *
 * UPSERT STRATEGY (ON CONFLICT source_url):
 *   We upsert on source_url rather than insert-and-ignore for two reasons:
 *   a) Prices change — if a seller drops their price we want to reflect that.
 *   b) Re-runs after LLM prompt changes should update the extraction fields.
 *   Fields NOT updated on conflict: make, model, year, trim, VIN, description,
 *   photo_urls — these are expensive to re-extract and stable after first pass.
 *
 * WHY WE LOG REJECTED LISTINGS:
 *   extraction_log records every pipeline attempt regardless of outcome.
 *   This lets us:
 *   - Track what fraction of scraped listings are rejected and why
 *   - Measure LLM cost per listing (prompt + completion tokens)
 *   - Detect if a prompt change degrades rejection rates
 *   - Identify systematic scraper quality issues (e.g. FB returning junk listings)
 */

import { Pool } from 'pg';
import {
  RawPayload,
  ScoredRecord,
  NormalisedListing,
  ExtractionLogEntry,
} from './types';
import { ExtractionResult }    from './m2a-extractor';
import { RedactionResult }     from './m2d-redactor';
import { checkAndSendAlerts }  from './m2h-alerts';
import { logger }              from './lib/logger';

const NORMALISATION_VERSION = '1.0.0';

// ── Main router ───────────────────────────────────────────

export async function routeAndWrite(
  pool:         Pool,
  payload:      RawPayload,
  scored:       ScoredRecord,
  extraction:   ExtractionResult,
  redaction:    RedactionResult,
  piiForceReview: boolean,
): Promise<string | null> {   // returns listing_id or null on rejection

  const f = scored.fields;

  // If PII validation failed, override routing to review regardless of score.
  const forcedReview = piiForceReview;
  let outcome = forcedReview ? 'review' : scored.outcome;

  // Financing-only listings → always hold for review.
  // A listing with only a monthly/biweekly payment and no purchase price cannot be
  // compared or sorted by consumers — keep it in the review queue until a human
  // confirms or derives the actual purchase price.
  const isFinancingOnly = f.price == null && f.payment_amount != null;
  if (isFinancingOnly && outcome !== 'rejected') outcome = 'review';

  const status  = outcomeToStatus(outcome, scored.needs_review);

  // Build NormalisedListing
  const listing: NormalisedListing = {
    source_id:             payload.source_id,
    source_category:       payload.source_category,
    source_url:            payload.listing_url,
    payload_id:            payload.payload_id,
    scrape_run_id:         payload.scrape_run_id,
    make:                  f.make  ?? 'Unknown',
    model:                 f.model ?? 'Unknown',
    year:                  f.year  ?? 1900,
    trim:                  f.trim,
    body_type:             f.body_type,
    drivetrain:            f.drivetrain,
    fuel_type:             f.fuel_type,
    transmission:          f.transmission,
    colour_exterior:       f.colour_exterior,
    colour_interior:       f.colour_interior,
    engine:                f.engine,
    doors:                 f.doors,
    seats:                 f.seats,
    vin:                   f.vin,
    condition:             f.condition ?? 'Unknown',
    mileage_km:            f.mileage_km,
    safetied:              f.safetied,
    accidents:             f.accidents,
    owners:                f.owners,
    price:                 f.price,
    price_type:            f.price_type,
    price_qualifier:       f.price_qualifier,
    price_raw:             f.price_raw || '',
    price_currency_orig:   f.price_currency_orig,
    price_exchange_rate:   null,
    payment_amount:        f.payment_amount,
    payment_frequency:     f.payment_frequency,
    city:                  f.city ?? 'Unknown',
    province:              f.province,
    seller_type:           f.seller_type ?? 'Unknown',
    dealer_name:           f.dealer_name,
    description:           redaction.text || null,
    photo_urls:            payload.listing_images,
    status,
    listed_date:           f.listed_date,
    confidence_score:      scored.confidence_score,
    confidence_details:    scored.confidence_details,
    extraction_method:     'LLM',
    extraction_model:      extraction.model,
    normalisation_version: NORMALISATION_VERSION,
    needs_review:          scored.needs_review || forcedReview,
  };

  const client = await pool.connect();
  let listingId: string | null = null;

  try {
    await client.query('BEGIN');

    // Upsert into listings.
    // ON CONFLICT (source_url): only update mutable fields — price, mileage, status, score.
    // Immutable fields (make, model, year, VIN, description, photo_urls) are NOT updated
    // so that human editorial corrections made via the dashboard survive re-scrapes.
    // IMPORTANT: last_seen_at is always updated so we can detect stale/removed listings.
    const { rows } = await client.query<{ id: string }>(`
      INSERT INTO listings (
        source_id, source_category, source_url, payload_id, scrape_run_id,
        make, model, year, trim, body_type, drivetrain, fuel_type, transmission,
        colour_exterior, colour_interior, engine, doors, seats, vin,
        condition, mileage_km, safetied, accidents, owners,
        price, price_type, price_qualifier, price_raw, price_currency_orig,
        price_exchange_rate, payment_amount, payment_frequency,
        city, province, seller_type, dealer_name,
        description, photo_urls, status, listed_date,
        confidence_score, confidence_details, extraction_method, extraction_model,
        normalisation_version, needs_review, last_seen_at
      ) VALUES (
        $1,$2,$3,$4,$5,
        $6,$7,$8,$9,$10,$11,$12,$13,
        $14,$15,$16,$17,$18,$19,
        $20,$21,$22,$23,$24,
        $25,$26,$27,$28,$29,
        $30,$31,$32,
        $33,$34,$35,$36,
        $37,$38,$39,$40,
        $41,$42,$43,$44,
        $45,$46, NOW()
      )
      ON CONFLICT (source_url) DO UPDATE SET
        last_seen_at        = NOW(),
        price               = EXCLUDED.price,
        price_raw           = EXCLUDED.price_raw,
        mileage_km          = EXCLUDED.mileage_km,
        status              = EXCLUDED.status,
        confidence_score    = EXCLUDED.confidence_score,
        confidence_details  = EXCLUDED.confidence_details,
        needs_review        = EXCLUDED.needs_review,
        updated_at          = NOW()
      RETURNING id
    `, [
      listing.source_id, listing.source_category, listing.source_url,
      listing.payload_id, listing.scrape_run_id,
      listing.make, listing.model, listing.year, listing.trim, listing.body_type,
      listing.drivetrain, listing.fuel_type, listing.transmission,
      listing.colour_exterior, listing.colour_interior, listing.engine,
      listing.doors, listing.seats, listing.vin,
      listing.condition, listing.mileage_km, listing.safetied,
      listing.accidents, listing.owners,
      listing.price, listing.price_type, listing.price_qualifier,
      listing.price_raw, listing.price_currency_orig,
      listing.price_exchange_rate, listing.payment_amount, listing.payment_frequency,
      listing.city, listing.province, listing.seller_type, listing.dealer_name,
      listing.description, listing.photo_urls, listing.status, listing.listed_date,
      listing.confidence_score, JSON.stringify(listing.confidence_details),
      listing.extraction_method, listing.extraction_model,
      listing.normalisation_version, listing.needs_review,
    ]);

    listingId = rows[0]?.id ?? null;

    // Insert into review_queue if the listing needs human review.
    // ON CONFLICT DO NOTHING: if the listing was already in the queue from a previous
    // run (e.g. re-analysed), we leave the existing queue row untouched so the
    // reviewer's work-in-progress isn't clobbered.
    if ((outcome === 'review' || forcedReview) && listingId) {
      const reason = forcedReview
        ? 'PII_REDACTION_FAILED'
        : isFinancingOnly
          ? `FINANCING_ONLY: $${f.payment_amount}/${f.payment_frequency ?? 'mo'} — no purchase price`
          : `Low confidence score: ${scored.confidence_score}`;
      await client.query(`
        INSERT INTO review_queue (listing_id, confidence_score, reason)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
      `, [listingId, scored.confidence_score, reason]);
    }

    // Extraction log
    const logEntry: ExtractionLogEntry = {
      payload_id:            payload.payload_id,
      listing_id:            listingId,
      source_id:             payload.source_id,
      extraction_method:     'LLM',
      extraction_model:      extraction.model,
      normalisation_version: NORMALISATION_VERSION,
      llm_prompt_tokens:     extraction.promptTokens,
      llm_completion_tokens: extraction.completionTokens,
      llm_latency_ms:        extraction.latencyMs,
      confidence_score:      scored.confidence_score,
      confidence_details:    scored.confidence_details,
      fields_extracted:      nonNullFields(scored.fields as unknown as Record<string, unknown>),
      fields_null:           nullFields(scored.fields as unknown as Record<string, unknown>),
      pii_items_redacted:    redaction.itemsRemoved,
      pii_redaction_failed:  redaction.failed,
      outcome,
      error_code:            null,
      error_message:         null,
    };

    await client.query(`
      INSERT INTO extraction_log (
        payload_id, listing_id, source_id, extraction_method, extraction_model,
        normalisation_version, llm_prompt_tokens, llm_completion_tokens, llm_latency_ms,
        confidence_score, confidence_details, fields_extracted, fields_null,
        pii_items_redacted, pii_redaction_failed, outcome, error_code, error_message
      ) VALUES (
        $1,$2,$3,$4,$5, $6,$7,$8,$9, $10,$11,$12,$13, $14,$15,$16,$17,$18
      )
    `, [
      logEntry.payload_id, logEntry.listing_id, logEntry.source_id,
      logEntry.extraction_method, logEntry.extraction_model,
      logEntry.normalisation_version, logEntry.llm_prompt_tokens,
      logEntry.llm_completion_tokens, logEntry.llm_latency_ms,
      logEntry.confidence_score, JSON.stringify(logEntry.confidence_details),
      logEntry.fields_extracted, logEntry.fields_null,
      logEntry.pii_items_redacted, logEntry.pii_redaction_failed,
      logEntry.outcome, logEntry.error_code, logEntry.error_message,
    ]);

    await client.query('COMMIT');

    logger.info({
      message:          'Listing written',
      listing_id:       listingId,
      source_url:       listing.source_url,
      outcome,
      confidence_score: scored.confidence_score,
    });

    // Fire-and-forget: check saved searches and send alert emails for new active listings
    if (status === 'active' && listingId) {
      checkAndSendAlerts(pool, {
        id:         listingId,
        make:       listing.make,
        model:      listing.model,
        year:       listing.year,
        trim:       listing.trim ?? null,
        price:      listing.price ?? null,
        price_type: listing.price_type,
        mileage_km: listing.mileage_km ?? null,
        city:       listing.city,
        province:   listing.province ?? null,
        source_url: listing.source_url,
        photo_urls: listing.photo_urls ?? null,
      }).catch(err => logger.warn({ message: 'Alert check error', error: (err as Error).message }));
    }

  } catch (err) {
    await client.query('ROLLBACK');
    logger.error({ message: 'DB write failed', error: (err as Error).message, source_url: listing.source_url });
    throw err;
  } finally {
    client.release();
  }

  return listingId;
}

// ── Helpers ───────────────────────────────────────────────

function outcomeToStatus(outcome: string, needs_review: boolean): string {
  if (outcome === 'published') return 'active';
  if (outcome === 'review')    return 'review';
  return 'rejected';
}

function nonNullFields(f: Record<string, unknown>): string[] {
  const skip = new Set(['confidence', '_validationWarnings', 'price_raw', 'price_type', 'price_currency_orig']);
  return Object.entries(f)
    .filter(([k, v]) => !skip.has(k) && v != null && v !== '')
    .map(([k]) => k);
}

function nullFields(f: Record<string, unknown>): string[] {
  const skip = new Set(['confidence', '_validationWarnings', 'price_raw', 'price_type', 'price_currency_orig']);
  return Object.entries(f)
    .filter(([k, v]) => !skip.has(k) && (v == null || v === ''))
    .map(([k]) => k);
}
