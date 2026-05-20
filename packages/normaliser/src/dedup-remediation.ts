/**
 * One-off remediation for the tier-3 deduplicator false positives.
 *
 * The old isTier3Match grouped two listings of the same year/make/model/city with a
 * similar price as "duplicates" WITHOUT checking VIN or mileage, then parked them in
 * status='review'. This buried thousands of genuinely distinct cars.
 *
 * This script re-evaluates every current tier-3 hold (status='review' with a
 * "Duplicate of …" note) against its canonical using the CORRECTED isTier3Match
 * (VIN → mileage → colour → trim → price cascade). If the pair is no longer a match
 * (i.e. a different car), the listing is restored to 'active' and its duplicate flags
 * are cleared. Genuine possible-dupes stay in review.
 *
 * Idempotent and reversible (only flips status / clears canonical_id + review_notes).
 * Run: ts-node src/dedup-remediation.ts [--apply]   (default is a dry run)
 */

import { Pool } from 'pg';
import { isTier3Match, ListingRow } from './deduplicator';

interface PairRow {
  // duplicate
  did: string; dvin: string | null; dmake: string; dmodel: string; dyear: number;
  dkm: number | null; dprice: number | null; dcity: string; dcolour: string | null; dtrim: string | null;
  // canonical
  cid: string; cvin: string | null; cmake: string; cmodel: string; cyear: number;
  ckm: number | null; cprice: number | null; ccity: string; ccolour: string | null; ctrim: string | null;
}

function toRow(p: PairRow, side: 'd' | 'c'): ListingRow {
  const g = (k: string) => (p as any)[side + k];
  return {
    id: side === 'd' ? p.did : p.cid,
    source_id: '', source_url: '', confidence_score: 0, canonical_id: null,
    vin: g('vin'), make: g('make'), model: g('model'), year: g('year'),
    mileage_km: g('km'), price: g('price'), city: g('city'),
    colour_exterior: g('colour'), trim: g('trim'),
  };
}

export async function runRemediation(pool: Pool, apply: boolean): Promise<{ examined: number; restore: number; keep: number; skippedIncomplete: number; }> {
  const { rows } = await pool.query<PairRow>(`
    SELECT
      dup.id did, trim(dup.vin) dvin, dup.make dmake, dup.model dmodel, dup.year dyear,
      dup.mileage_km dkm, dup.price dprice, dup.city dcity, dup.colour_exterior dcolour, dup.trim dtrim,
      c.id cid, trim(c.vin) cvin, c.make cmake, c.model cmodel, c.year cyear,
      c.mileage_km ckm, c.price cprice, c.city ccity, c.colour_exterior ccolour, c.trim ctrim
    FROM listings dup
    JOIN listings c ON c.id = dup.canonical_id
    WHERE dup.status = 'review' AND dup.review_notes LIKE 'Duplicate of%'
  `);

  const toRestore: string[] = [];
  let keep = 0, skippedIncomplete = 0;

  for (const p of rows) {
    const stillDup = isTier3Match(toRow(p, 'd'), toRow(p, 'c'));
    if (stillDup) { keep++; continue; }
    // Not a duplicate. Only return to the live surface if it meets the mandatory-field bar.
    if (p.dprice == null || p.dkm == null) { skippedIncomplete++; continue; }
    toRestore.push(p.did);
  }

  if (apply && toRestore.length) {
    // Restore in chunks to keep parameter lists sane.
    for (let i = 0; i < toRestore.length; i += 500) {
      const chunk = toRestore.slice(i, i + 500);
      await pool.query(`
        UPDATE listings
        SET status = 'active', needs_review = false, canonical_id = NULL,
            review_notes = NULL, updated_at = NOW()
        WHERE id = ANY($1::uuid[])
      `, [chunk]);
    }
  }

  return { examined: rows.length, restore: toRestore.length, keep, skippedIncomplete };
}

if (require.main === module) {
  (async () => {
    await import('dotenv/config');
    const { getPool, closePool } = await import('./lib/db');
    const pool  = getPool();
    const apply = process.argv.includes('--apply');
    console.log(`Dedup remediation — ${apply ? 'APPLY' : 'DRY RUN'}…`);
    const s = await runRemediation(pool, apply);
    console.log(`\nexamined:${s.examined}  restore->active:${s.restore}  keep-in-review(real dup):${s.keep}  skip(incomplete):${s.skippedIncomplete}`);
    if (!apply) console.log('\n(dry run — re-run with --apply to write changes)');
    await closePool();
  })().catch(err => { console.error('Fatal:', err.message); process.exit(1); });
}
