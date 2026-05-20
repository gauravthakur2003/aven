/**
 * Liveness Checker — re-verifies that active listings are still live at their source,
 * and retires the ones that have sold / been removed.
 *
 * AGE-BUCKETED CADENCE (per product spec):
 *   - Listing < 48h old           → not checked yet (brand new, certainly live)
 *   - Listing 48h–7d old          → checked every 2 days
 *   - Listing > 7d old            → checked every 24h
 * Rationale: sale probability rises with age, so older listings are verified more often.
 * "Age" is measured from first_seen_at (when we first scraped it — best proxy for post age).
 *
 * DETECTION (Kijiji):
 *   - HTTP 404 / 410, or redirect away from the listing id → DEAD (sold/removed)
 *   - HTTP 200 with a valid __NEXT_DATA__ payload          → ALIVE
 *   - network error / timeout / other                      → INCONCLUSIVE (retry next cycle)
 * We only retire on a *positive* dead signal (404/410). A false negative (keeping a sold
 * listing one extra cycle) is far cheaper than a false positive (deleting a live listing).
 *
 * RETIREMENT:
 *   A dead listing is set to status='sold', sold_at=NOW(), removal_reason set, and a full
 *   JSONB snapshot is archived to deleted_listings. The row stays in `listings` (so audit /
 *   extraction_log FKs are intact) but disappears from the browse/active surface immediately.
 *
 * Facebook listings are NOT checked here (Railway IP is blocked by FB). FB liveness is
 * derived from the Apify re-scrape, whose payload carries isLive / isSold / isPending.
 */

import { Pool } from 'pg';
import axios    from 'axios';
import { logger } from './lib/logger';

// ── Tunables ──────────────────────────────────────────────
const CHECK_RPM        = 20;                              // polite global rate to Kijiji
const MIN_GAP_MS       = Math.ceil(60_000 / CHECK_RPM);  // 3000ms between checks
const REQUEST_TIMEOUT  = 20_000;
const DEFAULT_BATCH    = 300;                             // listings re-checked per pass

const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15',
];

// ── Internal rate gate (token bucket, shared by all callers in-process) ──
let _lastReq = 0;
const _queue: Array<() => void> = [];
let _gateRunning = false;
function _runGate() {
  if (_gateRunning) return;
  _gateRunning = true;
  const tick = () => {
    if (_queue.length === 0) { _gateRunning = false; return; }
    const wait = Math.max(0, _lastReq + MIN_GAP_MS - Date.now());
    setTimeout(() => { _lastReq = Date.now(); _queue.shift()!(); tick(); }, wait);
  };
  tick();
}
function rateGate(): Promise<void> {
  return new Promise(resolve => { _queue.push(resolve); _runGate(); });
}

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

export interface LivenessStats {
  checked:       number;
  alive:         number;
  retired:       number;
  inconclusive:  number;
}

type Liveness = 'alive' | 'dead' | 'inconclusive';

interface DueListing {
  id:         string;
  source_url: string;
}

/**
 * Find active Kijiji listings due for a liveness check, most-overdue first.
 */
async function fetchDueListings(pool: Pool, batch: number): Promise<DueListing[]> {
  const { rows } = await pool.query<DueListing>(`
    SELECT id, source_url
    FROM listings
    WHERE status = 'active'
      AND source_id = 'kijiji-ca'
      AND first_seen_at <= NOW() - INTERVAL '48 hours'
      AND (
        ( first_seen_at <= NOW() - INTERVAL '7 days'
          AND (last_checked_at IS NULL OR last_checked_at <= NOW() - INTERVAL '24 hours') )
        OR
        ( first_seen_at > NOW() - INTERVAL '7 days'
          AND (last_checked_at IS NULL OR last_checked_at <= NOW() - INTERVAL '2 days') )
      )
    ORDER BY last_checked_at ASC NULLS FIRST
    LIMIT $1
  `, [batch]);
  return rows;
}

/**
 * Probe a Kijiji listing URL and classify it.
 */
async function probeKijiji(url: string): Promise<Liveness> {
  await rateGate();
  const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
  try {
    const res = await axios.get<string>(url, {
      headers: {
        'User-Agent':      ua,
        'Accept-Language': 'en-CA,en;q=0.9',
        'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      },
      timeout:          REQUEST_TIMEOUT,
      maxRedirects:     5,
      validateStatus:   () => true,   // we inspect the status ourselves
    });

    if (res.status === 404 || res.status === 410) return 'dead';
    if (res.status !== 200) return 'inconclusive';

    const body = res.data || '';
    // A live listing page always embeds __NEXT_DATA__ with the ad payload.
    if (!body.includes('__NEXT_DATA__')) return 'inconclusive';

    // Soft-removal: Kijiji sometimes serves a 200 page whose __NEXT_DATA__ marks the ad gone.
    const m = body.match(/<script id="__NEXT_DATA__"[^>]*>(.*?)<\/script>/s);
    if (m) {
      try {
        const data = JSON.parse(m[1]) as any;
        const pp   = data?.props?.pageProps ?? {};
        const ad   = pp.adDetails ?? pp.ad ?? pp.listing;
        const status = (ad?.status ?? ad?.adStatus ?? '').toString().toUpperCase();
        if (status && /SOLD|DELET|REMOV|EXPIR|INACTIVE|CLOSED/.test(status)) return 'dead';
        if (pp.statusCode === 404 || pp.notFound === true)                    return 'dead';
      } catch { /* fall through — body had __NEXT_DATA__, treat as alive */ }
    }
    return 'alive';
  } catch (err) {
    logger.warn({ message: 'Liveness probe error', url, error: (err as Error).message });
    return 'inconclusive';
  }
}

/** Mark a listing alive: bump last_seen_at / last_checked_at / check_count. */
async function markAlive(pool: Pool, id: string): Promise<void> {
  await pool.query(`
    UPDATE listings
    SET last_seen_at = NOW(), last_checked_at = NOW(), check_count = check_count + 1
    WHERE id = $1
  `, [id]);
}

/** Mark a listing inconclusive: only bump last_checked_at / check_count. */
async function markChecked(pool: Pool, id: string): Promise<void> {
  await pool.query(`
    UPDATE listings
    SET last_checked_at = NOW(), check_count = check_count + 1
    WHERE id = $1
  `, [id]);
}

/** Retire a dead listing: archive snapshot + set status='sold'. */
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
      SET status = 'sold', sold_at = NOW(), last_checked_at = NOW(),
          check_count = check_count + 1, removal_reason = $2, updated_at = NOW()
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
 * Run a single liveness pass over the listings currently due for a check.
 * Returns counts. Safe to call repeatedly (idempotent per-listing).
 */
export async function runLivenessPass(
  pool: Pool,
  opts: { batch?: number; onProgress?: (msg: string) => void } = {},
): Promise<LivenessStats> {
  const batch = opts.batch ?? DEFAULT_BATCH;
  const due   = await fetchDueListings(pool, batch);
  const stats: LivenessStats = { checked: 0, alive: 0, retired: 0, inconclusive: 0 };

  for (const l of due) {
    const verdict = await probeKijiji(l.source_url);
    stats.checked++;
    if (verdict === 'alive') {
      await markAlive(pool, l.id);
      stats.alive++;
    } else if (verdict === 'dead') {
      await retire(pool, l.id, 'not_found');
      stats.retired++;
    } else {
      await markChecked(pool, l.id);
      stats.inconclusive++;
    }
    if (opts.onProgress && stats.checked % 25 === 0) {
      opts.onProgress(`[liveness] ${stats.checked}/${due.length} — alive:${stats.alive} retired:${stats.retired} inconcl:${stats.inconclusive}`);
    }
  }
  return stats;
}

/**
 * Continuous loop for the 24/7 pipeline. Runs a pass, sleeps, repeats.
 * Integrated into test-pipeline.ts's Promise.all alongside the scraper/dedup loops.
 */
export async function livenessLoop(
  pool: Pool,
  log: (msg: string) => void,
  isStopping: () => boolean,
  intervalMs = 30 * 60 * 1000,
): Promise<void> {
  await sleep(2 * 60 * 1000); // let the pipeline warm up first
  while (!isStopping()) {
    try {
      const s = await runLivenessPass(pool, { onProgress: log });
      if (s.checked > 0) {
        log(`[liveness] pass complete — checked:${s.checked} alive:${s.alive} retired:${s.retired} inconclusive:${s.inconclusive}`);
      }
    } catch (err) {
      log(`[liveness] error: ${(err as Error).message}`);
    }
    await sleep(intervalMs);
  }
}

// ── Standalone CLI: `ts-node src/liveness-checker.ts [batch]` ──
if (require.main === module) {
  (async () => {
    await import('dotenv/config');
    const { getPool, closePool } = await import('./lib/db');
    const pool  = getPool();
    const batch = process.argv[2] ? parseInt(process.argv[2], 10) : DEFAULT_BATCH;
    console.log(`Liveness pass starting (batch=${batch})…`);
    const s = await runLivenessPass(pool, { batch, onProgress: (m) => console.log('  ' + m) });
    console.log(`\nDone — checked:${s.checked} alive:${s.alive} retired:${s.retired} inconclusive:${s.inconclusive}`);
    await closePool();
  })().catch(err => { console.error('Fatal:', err.message); process.exit(1); });
}
