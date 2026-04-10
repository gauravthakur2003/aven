// Module 3 — Deduplication Engine
// Finds duplicate listings across and within sources, groups them, and marks
// non-canonical copies as rejected.
//
// Tier 1 (exact):  same VIN                                          → definite duplicate
// Tier 2 (strong): year + make + model + mileage ±3% + price ±10% + city → very likely
// Tier 3 (weak):   year + make + model + price ±15% + city           → possible (review)

import { Pool, PoolClient } from 'pg';

// ─────────────────────────── Types ────────────────────────────────────────────

export interface DeduplicationStats {
  groups:   number;   // total duplicate groups found
  exact:    number;   // groups resolved via Tier 1 (VIN match)
  strong:   number;   // groups resolved via Tier 2
  weak:     number;   // groups resolved via Tier 3
  rejected: number;   // individual listings marked rejected
}

export interface DuplicateStats {
  total_listings: number; active_listings: number; review_listings: number;
  rejected_listings: number; deduplicated_groups: number;
  tier1_groups: number; tier2_groups: number; tier3_groups: number;
}

interface ListingRow {
  id: string; source_id: string; source_url: string; vin: string | null;
  make: string; model: string; year: number; mileage_km: number | null;
  price: number | null; city: string; confidence_score: number; canonical_id: string | null;
}

type Tier = 1 | 2 | 3;
interface DupGroup { tier: Tier; listingIds: string[]; }

// ─────────────────────────── Source priority ──────────────────────────────────
// Lower number = higher priority (preferred as canonical)
const SOURCE_PRIORITY: Record<string, number> = { 'kijiji-ca': 1, 'facebook-mp': 2 };
function sourcePriority(sourceId: string): number { return SOURCE_PRIORITY[sourceId] ?? 99; }

// ─────────────────────────── Helpers ──────────────────────────────────────────

function withinPct(a: number, b: number, pct: number): boolean {
  if (a === 0 && b === 0) return true;
  const avg = (Math.abs(a) + Math.abs(b)) / 2;
  if (avg === 0) return false;
  return Math.abs(a - b) / avg <= pct / 100;
}

function normaliseCity(city: string): string { return city.toLowerCase().trim().replace(/[^a-z0-9]/g, ''); }
function normaliseVin(vin: string | null): string | null {
  if (!vin) return null;
  const v = vin.trim().toUpperCase();
  return v.length === 17 ? v : null;
}
function normaliseMakeModel(s: string): string { return s.toLowerCase().trim().replace(/\s+/g, ' '); }

// ─────────────────────────── Matching logic ───────────────────────────────────

function isTier1Match(a: ListingRow, b: ListingRow): boolean {
  const va = normaliseVin(a.vin), vb = normaliseVin(b.vin);
  if (!va || !vb) return false;
  return va === vb;
}

function isTier2Match(a: ListingRow, b: ListingRow): boolean {
  if (a.year !== b.year) return false;
  if (normaliseMakeModel(a.make) !== normaliseMakeModel(b.make)) return false;
  if (normaliseMakeModel(a.model) !== normaliseMakeModel(b.model)) return false;
  if (normaliseCity(a.city) !== normaliseCity(b.city)) return false;
  // Both must have mileage and price
  if (a.mileage_km == null || b.mileage_km == null) return false;
  if (a.price == null || b.price == null) return false;
  if (!withinPct(a.mileage_km, b.mileage_km, 3)) return false;
  if (!withinPct(a.price, b.price, 10)) return false;
  return true;
}

function isTier3Match(a: ListingRow, b: ListingRow): boolean {
  if (a.year !== b.year) return false;
  if (normaliseMakeModel(a.make) !== normaliseMakeModel(b.make)) return false;
  if (normaliseMakeModel(a.model) !== normaliseMakeModel(b.model)) return false;
  if (normaliseCity(a.city) !== normaliseCity(b.city)) return false;
  if (a.price == null || b.price == null) return false;
  if (!withinPct(a.price, b.price, 15)) return false;
  return true;
}

// ─────────────────────────── Union-Find ───────────────────────────────────────
// Used to merge overlapping duplicate pairs into groups.

class UnionFind {
  private parent: Map<string, string> = new Map();
  private rank:   Map<string, number> = new Map();

  find(x: string): string {
    if (this.parent.get(x) === undefined) { this.parent.set(x, x); this.rank.set(x, 0); }
    if (this.parent.get(x) !== x) this.parent.set(x, this.find(this.parent.get(x)!));
    return this.parent.get(x)!;
  }

  union(x: string, y: string): void {
    const px = this.find(x), py = this.find(y);
    if (px === py) return;
    const rx = this.rank.get(px) ?? 0, ry = this.rank.get(py) ?? 0;
    if (rx < ry)      this.parent.set(px, py);
    else if (rx > ry) this.parent.set(py, px);
    else { this.parent.set(py, px); this.rank.set(px, rx + 1); }
  }

  /** Returns all groups (arrays of ids) with more than one member. */
  groups(allIds: string[]): Map<string, string[]> {
    for (const id of allIds) this.find(id);
    const map = new Map<string, string[]>();
    for (const id of allIds) {
      const root = this.find(id);
      const arr = map.get(root) ?? [];
      arr.push(id);
      map.set(root, arr);
    }
    for (const [root, members] of map) { if (members.length < 2) map.delete(root); }
    return map;
  }
}

// ─────────────────────────── Core deduplication ───────────────────────────────

function findDuplicateGroups(listings: ListingRow[]): { groups: DupGroup[]; exact: number; strong: number; weak: number; } {
  const uf       = new UnionFind();
  const pairTier = new Map<string, Tier>();  // "id1|id2" → tier (to track per-pair)

  // We record the highest (numerically lowest) tier between each merged pair
  const recordPair = (aId: string, bId: string, tier: Tier): void => {
    const key = aId < bId ? `${aId}|${bId}` : `${bId}|${aId}`;
    const existing = pairTier.get(key);
    if (!existing || tier < existing) pairTier.set(key, tier);
    uf.union(aId, bId);
  };

  // O(n²) comparison — acceptable for typical volumes (<50 k listings per run)
  for (let i = 0; i < listings.length; i++) {
    for (let j = i + 1; j < listings.length; j++) {
      const a = listings[i], b = listings[j];
      if (isTier1Match(a, b))      recordPair(a.id, b.id, 1);
      else if (isTier2Match(a, b)) recordPair(a.id, b.id, 2);
      else if (isTier3Match(a, b)) recordPair(a.id, b.id, 3);
    }
  }

  const rawGroups = uf.groups(listings.map(l => l.id));

  // Determine dominant tier for each group: the best (lowest) tier among all
  // pairs that fall within this group.
  const groups: DupGroup[] = [];
  let exact = 0, strong = 0, weak = 0;

  for (const [, members] of rawGroups) {
    let bestTier: Tier = 3;
    for (const [pairKey, t] of pairTier) {
      const [idA, idB] = pairKey.split('|');
      if (members.includes(idA) && members.includes(idB) && t < bestTier) bestTier = t;
    }
    groups.push({ tier: bestTier, listingIds: members });
    if (bestTier === 1) exact++;
    else if (bestTier === 2) strong++;
    else weak++;
  }

  return { groups, exact, strong, weak };
}

// ─────────────────────────── Canonical selection ──────────────────────────────

function pickCanonical(group: DupGroup, listingMap: Map<string, ListingRow>): string {
  const members = group.listingIds.map(id => listingMap.get(id)!).filter(Boolean);
  // Sort: source priority ASC, then confidence_score DESC
  members.sort((a, b) => {
    const pa = sourcePriority(a.source_id), pb = sourcePriority(b.source_id);
    return pa !== pb ? pa - pb : b.confidence_score - a.confidence_score;
  });
  return members[0].id;
}

// ─────────────────────────── DB writes ────────────────────────────────────────

async function applyGroup(
  client: PoolClient, group: DupGroup, listingMap: Map<string, ListingRow>, log: (msg: string) => void,
): Promise<number> {
  const canonicalId = pickCanonical(group, listingMap);
  const canonical   = listingMap.get(canonicalId)!;
  const duplicates  = group.listingIds.filter(id => id !== canonicalId);
  const dupUrls     = duplicates.map(id => listingMap.get(id)!.source_url);

  log(`[dedup] Tier ${group.tier} group: canonical=${canonicalId} (${canonical.source_id}) duplicates=[${duplicates.join(', ')}]`);

  // 1. Set canonical_id = self on the canonical listing (idempotent marker)
  await client.query(
    `UPDATE listings
        SET canonical_id          = $1,
            duplicate_source_urls = (
              SELECT array_agg(DISTINCT u)
              FROM unnest(
                COALESCE(duplicate_source_urls, ARRAY[]::text[]) || $2::text[]
              ) AS u
            ),
            updated_at = NOW()
      WHERE id = $1`,
    [canonicalId, dupUrls],
  );

  // 2. Mark each duplicate as rejected, point its canonical_id at the winner
  for (const dupId of duplicates) {
    const tier3 = group.tier === 3;
    // Tier 3 is "weak" — flag for review rather than outright reject
    await client.query(
      `UPDATE listings
          SET canonical_id  = $2,
              status        = $3::listing_status_enum,
              review_notes  = $4,
              needs_review  = $5,
              updated_at    = NOW()
        WHERE id = $1
          AND status NOT IN ('rejected')`,   // don't downgrade already-rejected
      [dupId, canonicalId, tier3 ? 'review' : 'rejected', `Duplicate of ${canonicalId}`, tier3],
    );
  }

  return duplicates.length;
}

// ─────────────────────────── Public API ───────────────────────────────────────

/**
 * Run the full deduplication pass.
 *
 * @param pool  pg Pool connected to aven_dev
 * @param log   logging callback (e.g. console.log)
 * @returns     summary statistics
 */
export async function runDeduplication(pool: Pool, log: (msg: string) => void): Promise<DeduplicationStats> {
  log('[dedup] Starting deduplication pass…');

  const client = await pool.connect();
  try {
    // ── 1. Load all active listings ────────────────────────────────────────
    const { rows } = await client.query<ListingRow>(`
      SELECT
        id, source_id, source_url,
        trim(vin)   AS vin,
        make, model, year,
        mileage_km, price, city,
        confidence_score,
        canonical_id
      FROM listings
      WHERE status = 'active'
      ORDER BY created_at ASC
    `);

    log(`[dedup] Loaded ${rows.length} active listings`);

    if (rows.length < 2) {
      log('[dedup] Nothing to deduplicate.');
      return { groups: 0, exact: 0, strong: 0, weak: 0, rejected: 0 };
    }

    // ── 2. Find duplicate groups ───────────────────────────────────────────
    const { groups, exact, strong, weak } = findDuplicateGroups(rows);
    log(`[dedup] Found ${groups.length} duplicate groups (T1=${exact} T2=${strong} T3=${weak})`);

    if (groups.length === 0) return { groups: 0, exact: 0, strong: 0, weak: 0, rejected: 0 };

    const listingMap = new Map<string, ListingRow>(rows.map(r => [r.id, r]));

    // ── 3. Apply groups inside a single transaction ────────────────────────
    let totalRejected = 0;
    await client.query('BEGIN');
    try {
      for (const group of groups) totalRejected += await applyGroup(client, group, listingMap, log);
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK');
      log(`[dedup] ERROR — rolled back: ${(err as Error).message}`);
      throw err;
    }

    const stats: DeduplicationStats = { groups: groups.length, exact, strong, weak, rejected: totalRejected };
    log(`[dedup] Done. Stats: ${JSON.stringify(stats)}`);
    return stats;

  } finally {
    client.release();
  }
}

/**
 * Returns current deduplication stats for the dashboard.
 */
export async function getDuplicateStats(pool: Pool): Promise<DuplicateStats> {
  const client = await pool.connect();
  try {
    const [totals, groups] = await Promise.all([
      client.query<{
        total_listings: string; active_listings: string;
        review_listings: string; rejected_listings: string;
      }>(`
        SELECT
          COUNT(*)                                            AS total_listings,
          COUNT(*) FILTER (WHERE status = 'active')          AS active_listings,
          COUNT(*) FILTER (WHERE status = 'review')          AS review_listings,
          COUNT(*) FILTER (WHERE status = 'rejected')        AS rejected_listings
        FROM listings
      `),
      client.query<{
        deduplicated_groups: string; tier1_groups: string; tier2_groups: string; tier3_groups: string;
      }>(`
        WITH canonical_groups AS (
          SELECT
            canonical_id,
            COUNT(*) AS group_size,
            -- infer tier from review_notes on the non-canonical members
            BOOL_OR(review_notes LIKE 'Duplicate of%' AND status = 'rejected') AS has_rejected,
            BOOL_OR(review_notes LIKE 'Duplicate of%' AND status = 'review')   AS has_review
          FROM listings
          WHERE canonical_id IS NOT NULL
            AND id != canonical_id    -- exclude the canonical listing itself
          GROUP BY canonical_id
        )
        SELECT
          COUNT(*)                                    AS deduplicated_groups,
          -- Tier 1/2 groups produce "rejected" duplicates; tier 3 produces "review"
          COUNT(*) FILTER (WHERE has_rejected AND NOT has_review) AS tier1_groups,
          COUNT(*) FILTER (WHERE has_rejected AND NOT has_review) AS tier2_groups,
          COUNT(*) FILTER (WHERE has_review  AND NOT has_rejected) AS tier3_groups
        FROM canonical_groups
      `),
    ]);

    const t = totals.rows[0], g = groups.rows[0];
    return {
      total_listings:      parseInt(t.total_listings,    10),
      active_listings:     parseInt(t.active_listings,   10),
      review_listings:     parseInt(t.review_listings,   10),
      rejected_listings:   parseInt(t.rejected_listings, 10),
      deduplicated_groups: parseInt(g.deduplicated_groups, 10),
      tier1_groups:        parseInt(g.tier1_groups,       10),
      tier2_groups:        parseInt(g.tier2_groups,       10),
      tier3_groups:        parseInt(g.tier3_groups,       10),
    };
  } finally {
    client.release();
  }
}
