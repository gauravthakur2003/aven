/**
 * Enrichment + quality-gate pass for listings that fall short of the publish bar.
 *
 * PRODUCT RULES (from founder):
 *   - Try to fill missing data points from the DESCRIPTION (price, mileage) and IMAGES (colour)
 *     before giving up on a listing.
 *   - PRICE is mandatory: if no purchase price can be recovered → delete the listing.
 *   - MILEAGE is mandatory: "no mileage, no listing" → delete the listing.
 *   - All other fields are negotiable (a listing can publish without them).
 *
 * This runs over EXISTING DB rows (review + active) — it does not re-scrape. It uses what we
 * already stored: description text, photo_urls, payment_amount/frequency. Listings that still
 * lack price or mileage after enrichment are retired (archived to deleted_listings, status set
 * to 'rejected' with a removal_reason). Going-forward, the same bar is enforced live by the
 * m2c-scorer hard-reject rules.
 */

import { Pool } from 'pg';
import { detectColour } from './m2g-vision';
import { logger }       from './lib/logger';

const MILES_TO_KM = 1.60934;

export interface EnrichmentStats {
  examined:        number;
  price_recovered: number;
  km_recovered:    number;
  colour_added:    number;
  retired_no_price:   number;
  retired_no_mileage: number;
  kept:            number;
}

interface Row {
  id:           string;
  description:  string | null;
  price:        number | null;
  mileage_km:   number | null;
  colour_exterior: string | null;
  photo_urls:   string[] | null;
  status:       string;
}

/**
 * Recover a vehicle odometer reading (in km) from free text.
 * Handles "150,000 km", "150000kms", "150k km", and miles → km conversion.
 * Returns null if nothing in a plausible range (1–600,000 km) is found.
 */
export function recoverMileage(text: string): number | null {
  if (!text) return null;
  const re = /(\d{1,3}(?:[,\s]?\d{3})+|\d{2,3}\s?k|\d{4,6})\s*(km|kms|kilometre?s?|kilometers?|miles?|mi)\b/gi;
  let best: number | null = null;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    let numStr = m[1].toLowerCase().replace(/[,\s]/g, '');
    let val: number;
    if (numStr.endsWith('k')) val = parseFloat(numStr.slice(0, -1)) * 1000;
    else                      val = parseInt(numStr, 10);
    const unit = m[2].toLowerCase();
    if (unit.startsWith('mi')) val = Math.round(val * MILES_TO_KM);
    if (val >= 1000 && val <= 600_000) {
      // Prefer the largest plausible reading (avoids matching "8,534 free kms/year" snippets
      // when the real odometer "150,000 km" is also present).
      if (best === null || val > best) best = val;
    }
  }
  return best;
}

/**
 * Recover a purchase price from free text, ignoring monthly/biweekly payment figures.
 * Returns null if nothing in a plausible range ($1,000–$300,000) is found.
 */
export function recoverPrice(text: string): number | null {
  if (!text) return null;
  const re = /\$\s?(\d{1,3}(?:[,\s]?\d{3})+|\d{4,6})(?:\.\d{2})?/g;
  let best: number | null = null;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    const val = parseInt(m[1].replace(/[,\s]/g, ''), 10);
    if (val < 1000 || val > 300_000) continue;
    // Skip if this dollar amount is part of a payment phrase (e.g. "$795/mo", "$1,090 monthly",
    // "$199 bi-weekly", "$120/week"). Look at the 18 chars following the match.
    const tail = text.slice(m.index, m.index + (m[0].length) + 18).toLowerCase();
    if (/\/\s?(mo|month|wk|week|bw|b\/w)|\bmonthly\b|\bbi[-\s]?weekly\b|\bweekly\b|per\s?(mo|month|week)/.test(tail)) continue;
    // Prefer the largest plausible figure — the asking price is typically the biggest dollar
    // amount in a listing (vs deposit, savings, fees).
    if (best === null || val > best) best = val;
  }
  return best;
}

/** Archive a snapshot and retire the listing as a quality rejection. */
async function retire(pool: Pool, id: string, reason: string): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`
      INSERT INTO deleted_listings (id, original_data, deleted_at)
      SELECT id, to_jsonb(listings.*), NOW() FROM listings WHERE id = $1
      ON CONFLICT (id) DO UPDATE SET original_data = EXCLUDED.original_data, deleted_at = NOW()
    `, [id]);
    await client.query(`
      UPDATE listings
      SET status = 'rejected', removal_reason = $2, updated_at = NOW()
      WHERE id = $1
    `, [id, reason]);
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Run the enrichment + quality-gate pass.
 *
 * @param opts.useVision  if true, call the vision model to recover exterior colour for
 *                        listings that otherwise qualify (have price + mileage). Costs a
 *                        Groq vision call each, so it is opt-in. Default false.
 */
export async function runEnrichmentPass(
  pool: Pool,
  opts: { batch?: number; useVision?: boolean; onProgress?: (msg: string) => void } = {},
): Promise<EnrichmentStats> {
  const batch = opts.batch ?? 500;
  const s: EnrichmentStats = {
    examined: 0, price_recovered: 0, km_recovered: 0, colour_added: 0,
    retired_no_price: 0, retired_no_mileage: 0, kept: 0,
  };

  // Candidates: not-yet-retired listings missing a mandatory field.
  const { rows } = await pool.query<Row>(`
    SELECT id, description, price, mileage_km, colour_exterior, photo_urls, status
    FROM listings
    WHERE status IN ('active', 'review')
      AND (price IS NULL OR mileage_km IS NULL)
    ORDER BY (status = 'active') DESC, created_at DESC
    LIMIT $1
  `, [batch]);

  for (const r of rows) {
    s.examined++;
    const desc = r.description ?? '';
    let price   = r.price;
    let mileage = r.mileage_km;

    if (price == null) {
      const p = recoverPrice(desc);
      if (p != null) { price = p; s.price_recovered++; }
    }
    if (mileage == null) {
      const km = recoverMileage(desc);
      if (km != null) { mileage = km; s.km_recovered++; }
    }

    // Enforce mandatory-field gate.
    if (price == null)   { await retire(pool, r.id, 'no_price');   s.retired_no_price++;   continue; }
    if (mileage == null) { await retire(pool, r.id, 'no_mileage'); s.retired_no_mileage++; continue; }

    // Survived the gate — persist any recovered values.
    let colour = r.colour_exterior;
    if (opts.useVision && !colour && r.photo_urls && r.photo_urls.length) {
      try {
        const v = await detectColour(r.photo_urls);
        if (v.colour) { colour = v.colour; s.colour_added++; }
      } catch (err) {
        logger.warn({ message: 'Enrichment vision error', id: r.id, error: (err as Error).message });
      }
    }

    await pool.query(`
      UPDATE listings
      SET price = $2, mileage_km = $3, colour_exterior = COALESCE($4, colour_exterior), updated_at = NOW()
      WHERE id = $1
    `, [r.id, price, mileage, colour]);
    s.kept++;

    if (opts.onProgress && s.examined % 25 === 0) {
      opts.onProgress(`[enrich] ${s.examined}/${rows.length} — kept:${s.kept} price+:${s.price_recovered} km+:${s.km_recovered} retired:${s.retired_no_price + s.retired_no_mileage}`);
    }
  }
  return s;
}

// ── Standalone CLI: `ts-node src/enrichment.ts [batch] [--vision]` ──
if (require.main === module) {
  (async () => {
    await import('dotenv/config');
    const { getPool, closePool } = await import('./lib/db');
    const pool    = getPool();
    const batch   = process.argv[2] && !process.argv[2].startsWith('--') ? parseInt(process.argv[2], 10) : 500;
    const vision  = process.argv.includes('--vision');
    console.log(`Enrichment pass starting (batch=${batch}, vision=${vision})…`);
    const s = await runEnrichmentPass(pool, { batch, useVision: vision, onProgress: (m) => console.log('  ' + m) });
    console.log(`\nDone — examined:${s.examined} kept:${s.kept} | recovered price:${s.price_recovered} km:${s.km_recovered} colour:${s.colour_added} | retired no_price:${s.retired_no_price} no_mileage:${s.retired_no_mileage}`);
    await closePool();
  })().catch(err => { console.error('Fatal:', err.message); process.exit(1); });
}
