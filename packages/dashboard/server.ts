/**
 * Aven Dashboard Server — v0.5
 *
 * Single Express server serving both internal (admin) and consumer-facing pages.
 * All routes render server-side HTML — no client-side framework.
 *
 * ROUTE MAP:
 *
 *   Internal / admin:
 *     GET  /                                → Command Center dashboard (live stats, review queue)
 *     GET  /listings                        → All listings, paginated + filterable by source/status
 *     GET  /review                          → Full review queue, paginated (50/page), filterable
 *     POST /api/review/:queueId/remove      → Reject a listing and close its review queue item
 *     POST /api/review/:queueId/reanalyse   → Delete listing so the pipeline re-processes it
 *     POST /api/review/bulk-reanalyse       → Bulk delete + re-queue (SSE stream, max 50 per batch)
 *     GET  /api/stats                       → Raw JSON stats blob (used by external dashboards)
 *     GET  /health                          → Health check (Railway/Render uptime probe)
 *
 *   Consumer-facing:
 *     GET  /browse                          → Public car search page (filtered grid of active listings)
 *     GET  /api/browse-options              → Filter dropdown options (makes, body types, cities) as JSON
 *     GET  /alerts                          → Alert sign-up form (email + make + max price + min year)
 *     POST /api/alerts                      → Save alert to saved_searches table
 *     GET  /unsubscribe?id=UUID             → One-click unsubscribe (sets saved_search.is_active = false)
 *
 * ARCHITECTURE NOTE:
 *   This is intentionally a single flat file (no route modules). The dashboard
 *   is an internal tool — colocation of HTML builders, query functions, and routes
 *   makes it faster to iterate than splitting across many files.
 */

import * as dotenv from 'dotenv';
import * as path   from 'path';
// Load local .env when running locally; on Railway/Render DATABASE_URL is injected by the platform
dotenv.config({ path: path.join(__dirname, '../normaliser/.env') });
dotenv.config({ path: path.join(__dirname, '.env') }); // dashboard-level .env fallback

import express      from 'express';
import { Pool }     from 'pg';

const PORT       = parseInt(process.env.PORT ?? '3030', 10);
const REFRESH_MS = 60_000;
const PAGE_SIZE  = 50;

if (!process.env.DATABASE_URL) {
  console.error('❌  DATABASE_URL is not set. Set it in .env or as a platform environment variable.');
  process.exit(1);
}

// Neon (and most cloud Postgres) requires SSL — detect by URL prefix
const dbUrl = process.env.DATABASE_URL;
const needsSsl = dbUrl.includes('neon.tech') || dbUrl.includes('supabase.co') ||
                 dbUrl.includes('railway.app') || process.env.NODE_ENV === 'production';

const pool = new Pool({
  connectionString: dbUrl,
  ssl: needsSsl ? { rejectUnauthorized: false } : false,
});
const app  = express();
app.use(express.json());
app.use((_req, res, next) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.setHeader('Pragma', 'no-cache');
  next();
});

// ── Ontario Region Classifier ─────────────────────────────
// Maps a city name to a broad Ontario region for customer-facing location filtering.

const ONTARIO_REGIONS: Record<string, string> = {
  // GTA
  'toronto': 'GTA', 'scarborough': 'GTA', 'etobicoke': 'GTA', 'north york': 'GTA',
  'mississauga': 'GTA', 'brampton': 'GTA', 'caledon': 'GTA',
  'markham': 'GTA', 'vaughan': 'GTA', 'richmond hill': 'GTA', 'aurora': 'GTA',
  'newmarket': 'GTA', 'king': 'GTA', 'whitchurch-stouffville': 'GTA',
  'oshawa': 'GTA', 'ajax': 'GTA', 'pickering': 'GTA', 'whitby': 'GTA',
  'durham': 'GTA', 'clarington': 'GTA', 'uxbridge': 'GTA',
  'oakville': 'GTA', 'burlington': 'GTA', 'halton hills': 'GTA', 'milton': 'GTA',
  // Central Ontario
  'hamilton': 'Central Ontario', 'stoney creek': 'Central Ontario',
  'guelph': 'Central Ontario',
  'kitchener': 'Central Ontario', 'waterloo': 'Central Ontario',
  'cambridge': 'Central Ontario',
  'brantford': 'Central Ontario', 'brant': 'Central Ontario',
  'st. catharines': 'Central Ontario', 'niagara': 'Central Ontario',
  'niagara falls': 'Central Ontario', 'welland': 'Central Ontario',
  'barrie': 'Central Ontario', 'innisfil': 'Central Ontario', 'collingwood': 'Central Ontario',
  'orillia': 'Central Ontario', 'midland': 'Central Ontario',
  // Eastern Ontario
  'ottawa': 'Eastern Ontario', 'gatineau': 'Eastern Ontario',
  'kingston': 'Eastern Ontario', 'belleville': 'Eastern Ontario',
  'peterborough': 'Eastern Ontario', 'cobourg': 'Eastern Ontario',
  'trenton': 'Eastern Ontario', 'brockville': 'Eastern Ontario',
  'cornwall': 'Eastern Ontario', 'pembroke': 'Eastern Ontario',
  // Southwest Ontario
  'london': 'Southwest Ontario', 'st. thomas': 'Southwest Ontario',
  'windsor': 'Southwest Ontario', 'chatham': 'Southwest Ontario',
  'sarnia': 'Southwest Ontario', 'leamington': 'Southwest Ontario',
  'strathroy': 'Southwest Ontario', 'woodstock': 'Southwest Ontario',
  'tillsonburg': 'Southwest Ontario', 'ingersoll': 'Southwest Ontario',
  // Northern Ontario
  'sudbury': 'Northern Ontario', 'greater sudbury': 'Northern Ontario',
  'thunder bay': 'Northern Ontario',
  'sault ste. marie': 'Northern Ontario', 'sault ste marie': 'Northern Ontario',
  'north bay': 'Northern Ontario', 'timmins': 'Northern Ontario',
  'kapuskasing': 'Northern Ontario', 'kenora': 'Northern Ontario',
  'elliot lake': 'Northern Ontario',
};

export function getOntarioRegion(city: string | null): string {
  if (!city) return 'Ontario';
  const key = city.toLowerCase().trim();
  return ONTARIO_REGIONS[key] ?? 'Ontario';
}

// ── Types ─────────────────────────────────────────────────

interface RecentRow {
  id: string;
  make: string; model: string; year: number; trim: string | null;
  price: number | null; price_type: string; mileage_km: number | null;
  colour_exterior: string | null; colour_interior: string | null;
  city: string; province: string | null;
  seller_type: string; dealer_name: string | null;
  confidence_score: number; status: string;
  needs_review: boolean; source_url: string; source_id: string;
  created_at: string;
  photo_urls: string[] | null;
  vin: string | null;
  body_type: string | null; drivetrain: string | null;
  fuel_type: string | null; transmission: string | null;
  accidents: number | null; owners: number | null; safetied: boolean | null;
  engine: string | null;
  is_duplicate: boolean;
}

interface ReviewRow {
  queue_id: string; listing_id: string;
  make: string; model: string; year: number;
  confidence_score: number; reason: string; created_at: string;
  source_url: string; source_id: string;
  photo_urls: string[] | null; vin: string | null;
}

interface Stats {
  overview: { total: number; active: number; review: number; rejected: number; needs_review: number; avg_confidence: number; with_price: number; with_mileage: number; kijiji_count: number; facebook_count: number; };
  pipeline: { total_processed: number; published: number; in_review: number; rejected: number; avg_latency_ms: number; avg_prompt_tokens: number; pii_redacted: number; pii_failed: number; };
  sources: Array<{ source_id: string; count: number; pct: number }>;
  recent: RecentRow[];
  review_queue: ReviewRow[];
  confidence_dist: Array<{ bucket: string; count: number }>;
  dedup: { duplicates_removed: number; canonical_groups: number };
  last_sync: string;
}

// ── Data queries ──────────────────────────────────────────

const LISTING_COLS = `
  l.id, l.make, l.model, l.year, l.trim,
  l.price, l.price_type, l.mileage_km,
  l.colour_exterior, l.colour_interior,
  l.city, l.province, l.seller_type, l.dealer_name,
  l.confidence_score, l.status, l.needs_review,
  l.source_url, l.source_id,
  (l.canonical_id IS NOT NULL AND l.canonical_id != l.id) AS is_duplicate,
  l.photo_urls, l.vin,
  l.body_type, l.drivetrain, l.fuel_type, l.transmission,
  l.accidents, l.owners, l.safetied, l.engine
`;

async function fetchStats(): Promise<Stats> {
  const client = await pool.connect();
  try {
    const [overviewRes, pipelineRes, sourcesRes, recentRes, reviewRes, confRes, dedupRes] = await Promise.all([
      client.query(`
        SELECT
          COUNT(*)                                               AS total,
          COUNT(*) FILTER (WHERE status = 'active')             AS active,
          COUNT(*) FILTER (WHERE status = 'review')             AS review,
          COUNT(*) FILTER (WHERE status = 'rejected')           AS rejected,
          (SELECT COUNT(*) FROM review_queue WHERE decision IS NULL) AS needs_review,
          COALESCE(ROUND(AVG(confidence_score))::int, 0)        AS avg_confidence,
          COUNT(*) FILTER (WHERE price IS NOT NULL)             AS with_price,
          COUNT(*) FILTER (WHERE mileage_km IS NOT NULL)        AS with_mileage,
          COUNT(*) FILTER (WHERE source_id = 'kijiji-ca')       AS kijiji_count,
          COUNT(*) FILTER (WHERE source_id = 'facebook-mp-ca')  AS facebook_count
        FROM listings
      `),
      client.query(`
        SELECT
          COUNT(*)                                               AS total_processed,
          COUNT(*) FILTER (WHERE outcome = 'published')         AS published,
          COUNT(*) FILTER (WHERE outcome = 'review')            AS in_review,
          COUNT(*) FILTER (WHERE outcome = 'rejected')          AS rejected,
          COALESCE(ROUND(AVG(llm_latency_ms))::int, 0)         AS avg_latency_ms,
          COALESCE(ROUND(AVG(llm_prompt_tokens))::int, 0)       AS avg_prompt_tokens,
          COALESCE(SUM(pii_items_redacted), 0)                  AS pii_redacted,
          COUNT(*) FILTER (WHERE pii_redaction_failed = true)   AS pii_failed
        FROM extraction_log
      `),
      client.query(`
        SELECT source_id, COUNT(*) AS count,
          ROUND(100.0 * COUNT(*) / NULLIF((SELECT COUNT(*) FROM listings), 0))::int AS pct
        FROM listings GROUP BY source_id ORDER BY count DESC
      `),
      client.query(`
        SELECT ${LISTING_COLS},
          TO_CHAR(l.created_at, 'HH24:MI  DD Mon') AS created_at
        FROM listings l
        ORDER BY l.created_at DESC LIMIT 15
      `),
      client.query(`
        SELECT rq.id AS queue_id, rq.listing_id,
          l.make, l.model, l.year, l.source_id,
          rq.confidence_score, rq.reason,
          TO_CHAR(rq.created_at, 'HH24:MI  DD Mon') AS created_at,
          l.source_url, l.photo_urls, l.vin
        FROM review_queue rq
        JOIN listings l ON l.id = rq.listing_id
        WHERE rq.decision IS NULL
        ORDER BY rq.created_at DESC NULLS LAST
        LIMIT 25
      `),
      client.query(`
        SELECT CASE
          WHEN confidence_score >= 90 THEN '90-100'
          WHEN confidence_score >= 70 THEN '70-89'
          WHEN confidence_score >= 50 THEN '50-69'
          ELSE '0-49' END AS bucket,
          COUNT(*) AS count
        FROM listings GROUP BY 1 ORDER BY 1 DESC
      `),
      client.query(`
        SELECT
          COUNT(*) FILTER (WHERE canonical_id IS NOT NULL AND canonical_id != id) AS duplicates_removed,
          COUNT(DISTINCT canonical_id) FILTER (WHERE canonical_id IS NOT NULL)    AS canonical_groups
        FROM listings
      `),
    ]);

    const o = overviewRes.rows[0];
    const p = pipelineRes.rows[0];
    return {
      overview: {
        total: Number(o.total), active: Number(o.active), review: Number(o.review),
        rejected: Number(o.rejected), needs_review: Number(o.needs_review),
        avg_confidence: Number(o.avg_confidence), with_price: Number(o.with_price),
        with_mileage: Number(o.with_mileage),
        kijiji_count: Number(o.kijiji_count), facebook_count: Number(o.facebook_count),
      },
      pipeline: {
        total_processed: Number(p.total_processed), published: Number(p.published),
        in_review: Number(p.in_review), rejected: Number(p.rejected),
        avg_latency_ms: Number(p.avg_latency_ms), avg_prompt_tokens: Number(p.avg_prompt_tokens),
        pii_redacted: Number(p.pii_redacted), pii_failed: Number(p.pii_failed),
      },
      sources: sourcesRes.rows.map(r => ({ source_id: r.source_id, count: Number(r.count), pct: Number(r.pct) })),
      recent: recentRes.rows,
      review_queue: reviewRes.rows,
      confidence_dist: confRes.rows.map(r => ({ bucket: r.bucket, count: Number(r.count) })),
      dedup: { duplicates_removed: Number(dedupRes.rows[0].duplicates_removed), canonical_groups: Number(dedupRes.rows[0].canonical_groups) },
      last_sync: new Date().toLocaleTimeString('en-CA', { hour: '2-digit', minute: '2-digit', hour12: false }),
    };
  } finally {
    client.release();
  }
}

// ── Browse types ──────────────────────────────────────────

interface BrowseOptions {
  makes: string[];
  body_types: string[];
  cities: string[];
  regions: string[];
}

interface BrowseListing {
  id: string;
  make: string; model: string; year: number; trim: string | null;
  price: number | null; price_type: string; mileage_km: number | null;
  city: string; province: string | null; region: string;
  source_id: string; source_url: string;
  photo_urls: string[] | null;
  body_type: string | null; colour_exterior: string | null;
  transmission: string | null; fuel_type: string | null;
  drivetrain: string | null;
  confidence_score: number;
  created_at: string;
}

interface BrowseFilters {
  make: string; model: string;
  min_year: number; max_year: number;
  min_price: number; max_price: number;
  body_type: string; city: string; region: string; source: string;
  page: number;
  sort: string;
}

const BROWSE_PAGE_SIZE = 24;

async function fetchBrowseOptions(): Promise<BrowseOptions> {
  const client = await pool.connect();
  try {
    const [makesRes, bodyRes, cityRes] = await Promise.all([
      client.query(`SELECT DISTINCT make FROM listings WHERE status='active' AND make IS NOT NULL AND make != 'Unknown' ORDER BY make`),
      client.query(`SELECT DISTINCT body_type FROM listings WHERE status='active' AND body_type IS NOT NULL ORDER BY body_type`),
      client.query(`SELECT DISTINCT city FROM listings WHERE status='active' AND city IS NOT NULL AND city != 'Unknown' ORDER BY city`),
    ]);
    const cities = cityRes.rows.map(r => r.city as string);
    const regions = [...new Set(cities.map(c => getOntarioRegion(c)))].sort();
    return {
      makes:      makesRes.rows.map(r => r.make),
      body_types: bodyRes.rows.map(r => r.body_type),
      cities,
      regions,
    };
  } finally {
    client.release();
  }
}

// Whitelist map for sort — never interpolate user input directly
const BROWSE_SORT_MAP: Record<string, string> = {
  newest:      'created_at DESC',
  price_asc:   'price ASC NULLS LAST',
  price_desc:  'price DESC NULLS FIRST',
  mileage_asc: 'mileage_km ASC NULLS LAST',
};

async function fetchBrowseListings(f: BrowseFilters): Promise<{ rows: BrowseListing[]; total: number }> {
  const offset  = (f.page - 1) * BROWSE_PAGE_SIZE;
  const orderBy = BROWSE_SORT_MAP[f.sort] ?? BROWSE_SORT_MAP['newest'];
  const client  = await pool.connect();
  try {
    const [dataRes, countRes] = await Promise.all([
      client.query(`
        SELECT id, make, model, year, trim, price, price_type, mileage_km,
               city, province, source_id, source_url, photo_urls, body_type,
               colour_exterior, transmission, fuel_type, drivetrain, confidence_score,
               created_at
        FROM listings
        WHERE status = 'active'
          AND ($1 = '' OR LOWER(make) = LOWER($1))
          AND ($2 = '' OR LOWER(model) ILIKE '%' || LOWER($2) || '%')
          AND ($3 = 0 OR year >= $3)
          AND ($4 = 0 OR year <= $4)
          AND ($5 = 0 OR price >= $5)
          AND ($6 = 0 OR price <= $6)
          AND ($7 = '' OR LOWER(body_type) = LOWER($7))
          AND ($8 = '' OR LOWER(city) ILIKE '%' || LOWER($8) || '%')
          AND ($9 = '' OR source_id = $9)
        ORDER BY ${orderBy}
        LIMIT ${BROWSE_PAGE_SIZE} OFFSET ${offset}
      `, [f.make, f.model, f.min_year, f.max_year, f.min_price, f.max_price, f.body_type, f.city, f.source]),
      client.query(`
        SELECT COUNT(*) AS total
        FROM listings
        WHERE status = 'active'
          AND ($1 = '' OR LOWER(make) = LOWER($1))
          AND ($2 = '' OR LOWER(model) ILIKE '%' || LOWER($2) || '%')
          AND ($3 = 0 OR year >= $3)
          AND ($4 = 0 OR year <= $4)
          AND ($5 = 0 OR price >= $5)
          AND ($6 = 0 OR price <= $6)
          AND ($7 = '' OR LOWER(body_type) = LOWER($7))
          AND ($8 = '' OR LOWER(city) ILIKE '%' || LOWER($8) || '%')
          AND ($9 = '' OR source_id = $9)
      `, [f.make, f.model, f.min_year, f.max_year, f.min_price, f.max_price, f.body_type, f.city, f.source]),
    ]);
    // Attach computed region to each row
    const rows = dataRes.rows.map(r => ({ ...r, region: getOntarioRegion(r.city) }));
    return { rows, total: Number(countRes.rows[0].total) };
  } finally {
    client.release();
  }
}

// ── Homepage types + query ────────────────────────────────

interface HomepageListing {
  id: string;
  make: string; model: string; year: number; trim: string | null;
  price: number | null; mileage_km: number | null;
  city: string; province: string | null;
  source_id: string; source_url: string;
  photo_urls: string[] | null;
  body_type: string | null;
  colour_exterior: string | null;
  drivetrain: string | null;
  confidence_score: number;
  created_at: string;
}

interface HomepageStats {
  total_active: number;
  body_counts: Record<string, number>;
  recent: HomepageListing[];
  makes: string[];
  body_types: string[];
}

async function fetchHomepageStats(): Promise<HomepageStats> {
  const client = await pool.connect();
  try {
    const [totalRes, bodyRes, recentRes, makesRes, bodyTypesRes] = await Promise.all([
      client.query(`SELECT COUNT(*) AS total FROM listings WHERE status = 'active'`),
      client.query(`
        SELECT body_type, COUNT(*) AS cnt
        FROM listings WHERE status = 'active' AND body_type IS NOT NULL
        GROUP BY body_type
      `),
      client.query(`
        SELECT id, make, model, year, trim, price, mileage_km,
               city, province, source_id, source_url, photo_urls,
               body_type, colour_exterior, drivetrain, confidence_score,
               created_at
        FROM listings WHERE status = 'active'
        ORDER BY created_at DESC LIMIT 3
      `),
      client.query(`SELECT DISTINCT make FROM listings WHERE status='active' AND make IS NOT NULL AND make != 'Unknown' ORDER BY make`),
      client.query(`SELECT DISTINCT body_type FROM listings WHERE status='active' AND body_type IS NOT NULL ORDER BY body_type`),
    ]);

    const body_counts: Record<string, number> = {};
    for (const row of bodyRes.rows) {
      body_counts[row.body_type] = Number(row.cnt);
    }

    return {
      total_active:  Number(totalRes.rows[0].total),
      body_counts,
      recent:        recentRes.rows,
      makes:         makesRes.rows.map((r: { make: string }) => r.make),
      body_types:    bodyTypesRes.rows.map((r: { body_type: string }) => r.body_type),
    };
  } finally {
    client.release();
  }
}

// ── Shared HTML helpers ───────────────────────────────────

/** Escape HTML special chars so user-supplied strings are safe to embed in HTML content/attributes. */
function esc(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function sourceBadge(sourceId: string): string {
  const label = sourceId === 'kijiji-ca' ? 'KJ' : sourceId === 'facebook-mp-ca' ? 'FB' : sourceId.slice(0,3).toUpperCase();
  const cls   = sourceId === 'kijiji-ca' ? 'src-kijiji' : sourceId === 'facebook-mp-ca' ? 'src-fb' : 'src-other';
  return `<span class="src-badge ${cls}" title="${sourceId}">${label}</span>`;
}

function statusTag(status: string, needsReview: boolean, isDuplicate = false): string {
  if (needsReview || status === 'review') return `<span class="tag tag-amber">REVIEW</span>`;
  if (status === 'active')   return `<span class="tag tag-green">LIVE</span>`;
  if (status === 'rejected' && isDuplicate) return `<span class="tag" style="background:#1a1a2b;color:#7a7aff;border:1px solid #3a3a6a;">DUPLICATE</span>`;
  if (status === 'rejected') return `<span class="tag tag-red">REJECTED</span>`;
  return `<span class="tag">${status}</span>`;
}

function fmt(n: number | null | undefined): string {
  if (n == null) return 'N/A';
  return n.toLocaleString('en-CA');
}

function carfaxUrl(vin: string | null): string | null {
  return vin ? `https://www.carfax.ca/en/vehicle-history-report/${vin.trim()}` : null;
}

// Encode tooltip data as base64 — handles all special chars (quotes, &, URLs, unicode)
function tooltipData(r: RecentRow | ReviewRow): string {
  const isRecent = 'price' in r;
  const row = r as RecentRow;
  const d = {
    img:      r.photo_urls?.[0] ?? null,
    vin:      r.vin,
    carfax:   carfaxUrl(r.vin),
    src:      r.source_url,
    year:     r.year, make: r.make, model: r.model,
    trim:     row.trim ?? null,
    price:    isRecent && row.price ? `$${Number(row.price).toLocaleString('en-CA')}` : null,
    mileage:  isRecent && row.mileage_km ? `${Number(row.mileage_km).toLocaleString()} km` : null,
    colour:   row.colour_exterior ?? null,
    colourIn: row.colour_interior ?? null,
    body:     row.body_type ?? null,
    drive:    row.drivetrain ?? null,
    fuel:     row.fuel_type ?? null,
    trans:    row.transmission ?? null,
    accidents: row.accidents ?? null,
    owners:   row.owners ?? null,
    safetied: row.safetied ?? null,
    engine:   row.engine ?? null,
    seller:   isRecent ? (row.seller_type ?? null) : null,
    dealer:   isRecent ? (row.dealer_name ?? null) : null,
    city:     isRecent ? row.city : null,
    score:    r.confidence_score,
  };
  // Base64 avoids ALL HTML-encoding edge cases (quotes, &, unicode in URLs, etc.)
  return Buffer.from(JSON.stringify(d)).toString('base64');
}

// ── Shared CSS ────────────────────────────────────────────

const SHARED_CSS = `
  @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Libre+Baskerville:wght@400;700&display=swap');
  * { box-sizing:border-box; margin:0; padding:0; }
  body { font-family:'DM Mono',monospace; background:#0d0d0d; padding:24px; color:#e8e0d0; min-height:100vh; }
  a { color:inherit; text-decoration:none; }
  .topbar { display:flex; justify-content:space-between; align-items:center; margin-bottom:28px; border-bottom:1px solid #2a1a1a; padding-bottom:16px; }
  .logo { font-family:'Libre Baskerville',serif; font-size:22px; color:#c0392b; letter-spacing:2px; }
  .logo span { color:#e8e0d0; font-size:11px; font-family:'DM Mono',monospace; display:block; letter-spacing:3px; margin-top:2px; }
  .live-pill { background:#1a0a0a; border:1px solid #c0392b; border-radius:2px; padding:4px 12px; font-size:11px; color:#e74c3c; letter-spacing:2px; display:flex; align-items:center; gap:6px; }
  .live-dot { width:6px; height:6px; border-radius:50%; background:#e74c3c; animation:blink 1.4s infinite; }
  @keyframes blink { 0%,100%{opacity:1} 50%{opacity:.2} }
  .nav-link { font-size:11px; color:#888; letter-spacing:1px; padding:4px 10px; border:1px solid #2a1a1a; border-radius:2px; cursor:pointer; }
  .nav-link:hover { color:#e8e0d0; border-color:#555; }
  .nav-link.active { color:#e8e0d0; border-color:#c0392b; }
  .grid4 { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-bottom:20px; }
  .grid3 { display:grid; grid-template-columns:repeat(3,1fr); gap:12px; margin-bottom:20px; }
  .grid2 { display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-bottom:20px; }
  .card { background:#141414; border:1px solid #2a1a1a; border-radius:4px; padding:18px; position:relative; overflow:hidden; }
  .card::before { content:''; position:absolute; top:0; left:0; width:3px; height:100%; background:#c0392b; }
  .card.blue::before { background:#2980b9; } .card.green::before { background:#27ae60; }
  .card.amber::before { background:#e67e22; } .card.purple::before { background:#8e44ad; }
  .card.gray::before { background:#555; }
  .card-label { font-size:10px; letter-spacing:2px; color:#888; text-transform:uppercase; margin-bottom:8px; }
  .card-value { font-size:28px; font-weight:500; color:#e8e0d0; line-height:1; margin-bottom:4px; }
  .card-sub { font-size:11px; color:#666; margin-top:4px; }
  .card-delta { font-size:11px; margin-top:6px; }
  .up{color:#27ae60} .down{color:#e74c3c} .neutral{color:#888}
  .section-title { font-size:10px; letter-spacing:3px; color:#c0392b; text-transform:uppercase; margin-bottom:14px; }
  .source-table { width:100%; border-collapse:collapse; font-size:12px; }
  .source-table th { text-align:left; color:#666; font-size:10px; letter-spacing:2px; text-transform:uppercase; padding:0 0 10px; font-weight:400; border-bottom:1px solid #1f1f1f; }
  .source-table td { padding:10px 0; border-bottom:1px solid #1a1a1a; color:#c0b8a8; vertical-align:middle; }
  .source-table td:last-child { text-align:right; }
  .source-dot { display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:8px; vertical-align:middle; }
  .bar-wrap { background:#1a1a1a; border-radius:2px; height:4px; width:100%; margin-top:4px; }
  .bar-fill { height:4px; border-radius:2px; }
  .pipeline { display:flex; align-items:center; gap:0; }
  .pipe-stage { flex:1; text-align:center; }
  .pipe-box { background:#1a1a1a; border:1px solid #2a1a1a; border-radius:3px; padding:12px 8px; }
  .pipe-arrow { color:#c0392b; font-size:16px; flex-shrink:0; padding:0 4px; }
  .pipe-num { font-size:18px; font-weight:500; color:#e8e0d0; }
  .pipe-label { font-size:10px; color:#666; margin-top:4px; letter-spacing:1px; }
  .dedup-row { display:flex; justify-content:space-between; align-items:center; padding:8px 0; border-bottom:1px solid #1a1a1a; font-size:12px; }
  .dedup-row:last-child { border-bottom:none; }
  .tag { display:inline-block; padding:2px 8px; border-radius:2px; font-size:10px; letter-spacing:1px; }
  .tag-green { background:#0d2b18; color:#27ae60; border:1px solid #1a4a2a; }
  .tag-red { background:#2b0d0d; color:#e74c3c; border:1px solid #4a1a1a; }
  .tag-amber { background:#2b1f0d; color:#e67e22; border:1px solid #4a3a1a; }
  .tag-blue { background:#0d1f2b; color:#2980b9; border:1px solid #1a3a4a; }
  .src-badge { display:inline-block; padding:2px 5px; border-radius:2px; font-size:9px; letter-spacing:1px; margin-right:5px; font-weight:500; }
  .src-kijiji { background:#0d1f2b; color:#2980b9; border:1px solid #1a3a4a; }
  .src-fb { background:#0d1a2b; color:#4a9eff; border:1px solid #1a2f4a; }
  .src-other { background:#1a1a1a; color:#888; border:1px solid #333; }
  .unsure-alert { background:#1a1200; border:1px solid #c0392b; border-radius:3px; padding:14px; display:flex; justify-content:space-between; align-items:center; margin-bottom:20px; }
  .alert-text { font-size:12px; color:#e8d0a0; }
  .alert-badge { background:#c0392b; color:#fff; padding:4px 14px; border-radius:2px; font-size:11px; cursor:pointer; letter-spacing:1px; text-decoration:none; }
  .data-table { width:100%; border-collapse:collapse; font-size:11px; }
  .data-table th { text-align:left; color:#555; font-size:10px; letter-spacing:2px; text-transform:uppercase; padding:0 8px 10px 0; font-weight:400; border-bottom:1px solid #1f1f1f; }
  .data-table td { padding:7px 8px 7px 0; border-bottom:1px solid #161616; color:#c0b8a8; vertical-align:middle; white-space:nowrap; }
  .data-table tbody tr { cursor:pointer; }
  .data-table tbody tr:hover td { background:#161616; }
  .thumb { width:52px; height:38px; object-fit:cover; border-radius:2px; background:#111; display:block; }
  .thumb-cell { width:60px; padding-right:6px !important; }
  .data-table td:first-child { color:#e8e0d0; }
  .listing-link { color:#e8e0d0; font-weight:500; }
  .listing-link:hover { color:#2980b9; text-decoration:underline; }
  .stat-row { display:flex; justify-content:space-between; align-items:baseline; padding:8px 0; border-bottom:1px solid #1a1a1a; font-size:12px; color:#c0b8a8; }
  .stat-row:last-child { border-bottom:none; }
  .stat-val { font-size:14px; color:#e8e0d0; }
  .footer-bar { border-top:1px solid #2a1a1a; padding-top:14px; display:flex; justify-content:space-between; font-size:10px; color:#444; letter-spacing:1px; margin-top:20px; }
  .btn { display:inline-block; padding:3px 10px; border-radius:2px; font-size:10px; letter-spacing:1px; cursor:pointer; border:none; font-family:'DM Mono',monospace; }
  .btn-remove { background:#2b0d0d; color:#e74c3c; border:1px solid #4a1a1a; }
  .btn-remove:hover { background:#3b1010; }
  .btn-approve { background:#0b2010; color:#27ae60; border:1px solid #1a4a2a; }
  .btn-approve:hover { background:#0e2a16; }
  .btn-reanalyse { background:#0d1f2b; color:#2980b9; border:1px solid #1a3a4a; }
  .btn-reanalyse:hover { background:#112a3b; }
  /* Row animations */
  @keyframes spin { to { transform:rotate(360deg); } }
  @keyframes row-fade-green { 0%{background:#051a0a;opacity:1} 80%{background:#051a0a;opacity:1} 100%{opacity:0} }
  @keyframes row-fade-red   { 0%{background:#1a0505;opacity:1} 80%{background:#1a0505;opacity:1} 100%{opacity:0} }
  .row-success { animation: row-fade-green 1.2s ease-out forwards !important; }
  .row-deleting { animation: row-fade-red   0.7s ease-out forwards !important; }
  .btn-spinning::before { content:'⟳'; display:inline-block; animation:spin 0.8s linear infinite; margin-right:4px; }
  /* Tooltip */
  #tt { position:fixed; z-index:9999; background:#181818; border:1px solid #333; border-radius:4px; padding:14px; width:320px; pointer-events:none; display:none; font-size:11px; box-shadow:0 8px 32px #000; }
  #tt img { width:100%; height:140px; object-fit:cover; border-radius:3px; margin-bottom:10px; display:block; background:#111; }
  #tt .tt-title { font-size:13px; font-weight:500; color:#e8e0d0; margin-bottom:6px; }
  #tt .tt-row { display:flex; justify-content:space-between; padding:3px 0; border-bottom:1px solid #222; color:#888; }
  #tt .tt-row:last-child { border-bottom:none; }
  #tt .tt-val { color:#c0b8a8; text-align:right; max-width:180px; overflow:hidden; text-overflow:ellipsis; }
  #tt .tt-links { display:flex; gap:8px; margin-top:8px; padding-top:8px; border-top:1px solid #222; }
  #tt .tt-link { font-size:10px; color:#2980b9; letter-spacing:1px; }
  #tt .tt-score { display:inline-block; padding:2px 8px; border-radius:2px; font-size:11px; margin-bottom:8px; }
  /* Processing row animation */
  @keyframes pulse-blue { 0%,100%{opacity:1} 50%{opacity:0.55} }
  .processing { animation:pulse-blue 0.9s ease-in-out infinite; }
  /* Pagination */
  .pager { display:flex; gap:8px; align-items:center; margin-top:16px; font-size:11px; }
  .pager a { color:#888; padding:4px 10px; border:1px solid #2a1a1a; border-radius:2px; }
  .pager a:hover { color:#e8e0d0; border-color:#555; }
  .pager .cur { color:#e8e0d0; border-color:#c0392b; padding:4px 10px; border:1px solid #c0392b; border-radius:2px; }
  /* Filter bar */
  .filter-bar { display:flex; gap:8px; margin-bottom:16px; flex-wrap:wrap; }
  .filter-bar a { font-size:10px; letter-spacing:1px; padding:4px 12px; border:1px solid #2a1a1a; border-radius:2px; color:#888; }
  .filter-bar a:hover { color:#e8e0d0; border-color:#555; }
  .filter-bar a.on { color:#e8e0d0; border-color:#c0392b; background:#1a0a0a; }
`;

// ── Homepage CSS ──────────────────────────────────────────

const HOME_CSS = `
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
  * { box-sizing:border-box; margin:0; padding:0; }
  body { font-family:'Inter',sans-serif; background:#fff; color:#1a1a1a; min-height:100vh; }
  a { color:inherit; text-decoration:none; }

  /* ── Topbar ── */
  .h-topbar { background:#fff; border-bottom:1px solid #e5e7eb; padding:0 32px; height:64px; display:flex; align-items:center; justify-content:space-between; position:sticky; top:0; z-index:200; box-shadow:0 1px 6px rgba(0,0,0,.06); }
  .h-logo { font-size:21px; font-weight:700; letter-spacing:2px; color:#111827; }
  .h-logo span { font-size:12px; font-weight:400; color:#9ca3af; letter-spacing:1px; margin-left:10px; }
  .h-nav { display:flex; align-items:center; gap:12px; }
  .h-nav a { font-size:13px; color:#6b7280; font-weight:500; padding:7px 16px; border-radius:8px; transition:background .15s, color .15s; }
  .h-nav a:hover { background:#f3f4f6; color:#111827; }
  .h-nav a.h-btn { background:#16a34a; color:#fff; font-weight:600; box-shadow:0 1px 3px rgba(22,163,74,.3); }
  .h-nav a.h-btn:hover { background:#15803d; }

  /* ── Hero split ── */
  .h-hero { display:grid; grid-template-columns:1fr 1fr; min-height:480px; }
  .h-hero-left { background:#fff; padding:56px 48px 56px 64px; display:flex; flex-direction:column; justify-content:center; border-right:1px solid #e5e7eb; position:relative; }
  .h-hero-right { background:#f0fdf4; padding:56px 64px 56px 48px; display:flex; flex-direction:column; justify-content:center; }
  .h-hero-divider {
    position:absolute; right:-1px; top:50%; transform:translateY(-50%);
    display:flex; flex-direction:column; align-items:center; gap:8px; z-index:10;
  }
  .h-hero-divider::before, .h-hero-divider::after {
    content:''; display:block; width:1px; height:80px; background:#e5e7eb;
  }
  .h-hero-or {
    background:#fff; border:1px solid #e5e7eb; border-radius:50%;
    width:32px; height:32px; display:flex; align-items:center; justify-content:center;
    font-size:11px; font-weight:700; color:#9ca3af; letter-spacing:.5px; flex-shrink:0;
  }
  .h-headline { font-size:32px; font-weight:700; color:#111827; margin-bottom:8px; line-height:1.2; }
  .h-sub { font-size:15px; color:#6b7280; margin-bottom:28px; line-height:1.5; }
  .h-field { margin-bottom:12px; }
  .h-field label { display:block; font-size:12px; font-weight:600; color:#4b5563; margin-bottom:4px; }
  .h-field select, .h-field input[type=text], .h-field input[type=number] {
    width:100%; border:1.5px solid #e5e7eb; border-radius:8px; padding:9px 11px;
    font-size:13px; font-family:inherit; background:#f9fafb; color:#111827; outline:none;
    transition:border-color .15s;
  }
  .h-field select:focus, .h-field input:focus { border-color:#16a34a; background:#fff; box-shadow:0 0 0 3px rgba(22,163,74,.1); }
  .h-field-row { display:grid; grid-template-columns:1fr 1fr; gap:8px; }
  .h-search-btn {
    width:100%; background:#16a34a; color:#fff; border:none; border-radius:9px;
    padding:12px; font-size:14px; font-weight:700; cursor:pointer; font-family:inherit;
    margin-top:4px; transition:background .15s; box-shadow:0 1px 4px rgba(22,163,74,.3);
  }
  .h-search-btn:hover { background:#15803d; }
  .h-body-pills { display:flex; flex-wrap:wrap; gap:6px; margin-top:16px; }
  .h-body-pill {
    font-size:12px; font-weight:500; padding:5px 12px; border:1.5px solid #e5e7eb;
    border-radius:20px; color:#6b7280; background:#fff; transition:all .15s;
    cursor:pointer;
  }
  .h-body-pill:hover { border-color:#16a34a; color:#16a34a; background:#f0fdf4; }

  /* ── AI search half ── */
  .h-ai-headline { font-size:28px; font-weight:700; color:#166534; margin-bottom:8px; line-height:1.2; }
  .h-ai-sub { font-size:14px; color:#4b7a5c; margin-bottom:24px; line-height:1.6; }
  .h-ai-textarea {
    width:100%; border:1.5px solid #bbf7d0; border-radius:10px; padding:14px;
    font-size:13px; font-family:inherit; background:#fff; color:#111827;
    resize:vertical; min-height:110px; outline:none; transition:border-color .15s;
    line-height:1.5;
  }
  .h-ai-textarea:focus { border-color:#16a34a; box-shadow:0 0 0 3px rgba(22,163,74,.12); }
  .h-ai-btn {
    margin-top:10px; background:#166534; color:#fff; border:none; border-radius:9px;
    padding:12px 24px; font-size:14px; font-weight:700; cursor:pointer; font-family:inherit;
    transition:background .15s; width:100%;
  }
  .h-ai-btn:hover { background:#15803d; }
  .h-ai-note { font-size:11px; color:#6b9e7a; margin-top:8px; text-align:center; }

  /* ── Section: Browse by category ── */
  .h-section { max-width:1200px; margin:0 auto; padding:56px 32px; }
  .h-section-title { font-size:22px; font-weight:700; color:#111827; margin-bottom:28px; }
  .h-cats { display:grid; grid-template-columns:repeat(6,1fr); gap:14px; }
  .h-cat-card {
    background:#fff; border:1.5px solid #e5e7eb; border-radius:14px; padding:20px 14px;
    text-align:center; transition:box-shadow .2s, transform .2s, border-color .2s;
    display:block;
  }
  .h-cat-card:hover { box-shadow:0 8px 28px rgba(0,0,0,.1); transform:translateY(-3px); border-color:#16a34a; }
  .h-cat-emoji { font-size:28px; margin-bottom:10px; display:block; }
  .h-cat-label { font-size:13px; font-weight:700; color:#111827; display:block; }
  .h-cat-sub { font-size:11px; color:#9ca3af; display:block; margin-top:3px; }

  /* ── Section: Explore by make ── */
  .h-makes-row { display:flex; gap:8px; flex-wrap:wrap; }
  .h-make-pill {
    font-size:13px; font-weight:500; padding:8px 16px; border:1.5px solid #e5e7eb;
    border-radius:20px; background:#f9fafb; color:#374151; transition:all .15s;
    white-space:nowrap; display:inline-block;
  }
  .h-make-pill:hover { border-color:#16a34a; color:#16a34a; background:#f0fdf4; }

  /* ── Stats bar ── */
  .h-stats-bar {
    background:#111827; padding:18px 32px; text-align:center;
    font-size:15px; font-weight:600; color:#d1fae5; letter-spacing:.3px;
  }
  .h-stats-bar span { color:#4ade80; }

  /* ── Recent listings ── */
  .h-recent-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:18px; }
  .h-see-all { display:inline-block; margin-top:24px; font-size:14px; font-weight:600; color:#16a34a; }
  .h-see-all:hover { text-decoration:underline; }

  /* ── Browse cards (reused from browse for recent listings preview) ── */
  .b-card {
    background:#fff; border:1px solid #e5e7eb; border-radius:14px; overflow:hidden;
    transition:box-shadow .2s, transform .2s; cursor:pointer; display:block; color:inherit;
  }
  .b-card:hover { box-shadow:0 8px 32px rgba(0,0,0,.12); transform:translateY(-4px); border-color:#d1d5db; }
  .b-card-img { aspect-ratio:4/3; background:#f3f4f6; overflow:hidden; position:relative; }
  .b-card-img img { width:100%; height:100%; object-fit:cover; display:block; }
  .b-card-img .b-no-img { width:100%; height:100%; display:flex; align-items:center; justify-content:center; color:#d1d5db; font-size:36px; }
  .b-src-pill { display:inline-block; font-size:11px; font-weight:600; padding:3px 10px; border-radius:20px; margin-top:8px; }
  .b-src-pill-kj { background:#dbeafe; color:#1d4ed8; }
  .b-src-pill-fb { background:#ede9fe; color:#6d28d9; }
  .b-src-pill-other { background:#f3f4f6; color:#6b7280; }
  .b-card-body { padding:15px; }
  .b-card-title { font-size:14px; font-weight:700; color:#111827; margin-bottom:5px; line-height:1.35; }
  .b-card-trim { font-size:12px; color:#6b7280; margin-bottom:7px; }
  .b-card-price { font-size:22px; font-weight:700; color:#16a34a; margin-bottom:8px; line-height:1; }
  .b-card-price.no-price { color:#9ca3af; font-size:14px; font-weight:500; }
  .b-card-meta { font-size:12px; color:#9ca3af; display:flex; align-items:center; gap:6px; }
  .b-card-meta-sep { color:#d1d5db; }
  .b-verified-badge { display:inline-flex; align-items:center; gap:3px; font-size:11px; font-weight:600; color:#15803d; background:#f0fdf4; border:1px solid #bbf7d0; border-radius:4px; padding:2px 7px; margin-top:6px; }
  .b-limited-info { display:inline-block; font-size:11px; color:#9ca3af; background:#f9fafb; border:1px solid #e5e7eb; border-radius:4px; padding:2px 7px; margin-top:6px; }
  .b-card-view-link { font-size:12px; color:#16a34a; font-weight:600; margin-top:8px; display:block; }
  .b-card-view-link:hover { text-decoration:underline; }

  /* ── Footer ── */
  .h-footer { background:#f9fafb; border-top:1px solid #e5e7eb; padding:24px 32px; text-align:center; font-size:13px; color:#9ca3af; }
  .h-footer span { color:#6b7280; font-weight:500; }

  /* ── Dividers between sections ── */
  .h-divider { border:none; border-top:1px solid #f3f4f6; margin:0; }

  /* ── Mobile ── */
  @media (max-width:1024px) {
    .h-cats { grid-template-columns:repeat(3,1fr); }
  }
  @media (max-width:768px) {
    .h-hero { grid-template-columns:1fr; min-height:auto; }
    .h-hero-left { padding:36px 24px; border-right:none; border-bottom:1px solid #e5e7eb; }
    .h-hero-right { padding:36px 24px; }
    .h-hero-divider { display:none; }
    .h-section { padding:36px 20px; }
    .h-cats { grid-template-columns:repeat(2,1fr); }
    .h-recent-grid { grid-template-columns:1fr; }
    .h-topbar { padding:0 16px; }
    .h-stats-bar { font-size:13px; }
  }
  @media (max-width:480px) {
    .h-cats { grid-template-columns:repeat(2,1fr); }
    .h-makes-row { gap:6px; }
  }
`;

// ── Browse CSS (consumer-facing light theme) ──────────────

const BROWSE_CSS = `
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
  * { box-sizing:border-box; margin:0; padding:0; }
  body { font-family:'Inter',sans-serif; background:#f8f9fa; color:#1a1a1a; min-height:100vh; }
  a { color:inherit; text-decoration:none; }

  /* ── Image shimmer skeleton ── */
  @keyframes shimmer {
    0%   { background-position: -600px 0; }
    100% { background-position:  600px 0; }
  }
  .b-img-skeleton {
    background: linear-gradient(90deg, #e8e8e8 25%, #f2f2f2 50%, #e8e8e8 75%);
    background-size: 600px 100%;
    animation: shimmer 1.4s infinite linear;
  }

  /* ── Topbar ── */
  .b-topbar { background:#fff; border-bottom:1px solid #e5e7eb; padding:0 32px; height:64px; display:flex; align-items:center; justify-content:space-between; position:sticky; top:0; z-index:200; box-shadow:0 1px 6px rgba(0,0,0,.06); }
  .b-logo { font-size:21px; font-weight:700; letter-spacing:2px; color:#111827; }
  .b-logo span { font-size:12px; font-weight:400; color:#9ca3af; letter-spacing:1px; margin-left:10px; }
  .b-nav { display:flex; align-items:center; gap:12px; }
  .b-nav a { font-size:13px; color:#6b7280; font-weight:500; padding:7px 16px; border-radius:8px; transition:background .15s, color .15s; }
  .b-nav a:hover { background:#f3f4f6; color:#111827; }
  .b-nav a.b-btn { background:#16a34a; color:#fff; font-weight:600; box-shadow:0 1px 3px rgba(22,163,74,.3); }
  .b-nav a.b-btn:hover { background:#15803d; }

  /* ── Hero banner ── */
  .b-hero {
    background: linear-gradient(135deg, #0f172a 0%, #134e2a 100%);
    padding: 56px 32px 52px;
    text-align: center;
    position: relative;
    overflow: hidden;
  }
  .b-hero::before {
    content:'';
    position:absolute; inset:0;
    background: radial-gradient(ellipse at 70% 50%, rgba(22,163,74,.18) 0%, transparent 60%);
    pointer-events:none;
  }
  .b-hero-eyebrow { font-size:12px; font-weight:600; letter-spacing:2px; color:#4ade80; text-transform:uppercase; margin-bottom:14px; }
  .b-hero-title { font-size:clamp(28px,4vw,46px); font-weight:700; color:#fff; line-height:1.15; margin-bottom:12px; }
  .b-hero-sub { font-size:16px; color:#94a3b8; margin-bottom:32px; font-weight:400; }
  .b-hero-search { display:flex; gap:10px; max-width:520px; margin:0 auto; }
  .b-hero-search input {
    flex:1; border:none; border-radius:10px; padding:13px 18px; font-size:15px; font-family:inherit;
    background:rgba(255,255,255,.12); color:#fff; outline:none; transition:background .2s;
    backdrop-filter:blur(4px);
    border:1px solid rgba(255,255,255,.15);
  }
  .b-hero-search input::placeholder { color:rgba(255,255,255,.45); }
  .b-hero-search input:focus { background:rgba(255,255,255,.18); border-color:rgba(255,255,255,.35); }
  .b-hero-search button {
    background:#16a34a; color:#fff; border:none; border-radius:10px; padding:13px 24px;
    font-size:14px; font-weight:700; cursor:pointer; font-family:inherit; transition:background .15s;
    white-space:nowrap; box-shadow:0 2px 8px rgba(22,163,74,.4);
  }
  .b-hero-search button:hover { background:#15803d; }

  /* ── Layout ── */
  .b-wrap { max-width:1320px; margin:0 auto; padding:28px 24px; }
  .b-layout { display:grid; grid-template-columns:280px 1fr; gap:28px; align-items:start; }

  /* ── Sidebar filters ── */
  .b-sidebar {
    background:#fff; border:1px solid #e5e7eb; border-radius:14px; padding:22px;
    position:sticky; top:88px;
  }
  .b-sidebar-title { font-size:11px; font-weight:700; letter-spacing:1.5px; color:#9ca3af; text-transform:uppercase; margin-bottom:18px; display:flex; align-items:center; justify-content:space-between; }
  .b-field { margin-bottom:16px; }
  .b-field label { display:block; font-size:12px; font-weight:600; color:#4b5563; margin-bottom:5px; letter-spacing:.3px; }
  .b-field select, .b-field input[type=text], .b-field input[type=number] {
    width:100%; border:1.5px solid #e5e7eb; border-radius:8px; padding:9px 11px;
    font-size:13px; font-family:inherit; background:#f9fafb; color:#111827; outline:none; transition:border-color .15s, background .15s;
  }
  .b-field select:focus, .b-field input:focus { border-color:#16a34a; background:#fff; box-shadow:0 0 0 3px rgba(22,163,74,.1); }
  .b-field-row { display:grid; grid-template-columns:1fr 1fr; gap:8px; }
  .b-source-btns { display:flex; gap:6px; flex-wrap:wrap; }
  .b-source-btn {
    font-size:12px; font-weight:600; padding:7px 14px; border:1.5px solid #e5e7eb;
    border-radius:20px; background:#f9fafb; cursor:pointer; font-family:inherit;
    color:#6b7280; transition:all .15s; letter-spacing:.3px;
  }
  .b-source-btn.active { background:#16a34a; color:#fff; border-color:#16a34a; box-shadow:0 1px 4px rgba(22,163,74,.3); }
  .b-source-btn:hover:not(.active) { border-color:#16a34a; color:#16a34a; background:#f0fdf4; }
  .b-apply {
    width:100%; background:#16a34a; color:#fff; border:none; border-radius:9px; padding:11px;
    font-size:14px; font-weight:700; cursor:pointer; font-family:inherit; margin-top:8px;
    transition:background .15s; box-shadow:0 1px 4px rgba(22,163,74,.3); letter-spacing:.2px;
  }
  .b-apply:hover { background:#15803d; }
  .b-clear { display:block; text-align:center; font-size:12px; color:#9ca3af; margin-top:10px; cursor:pointer; transition:color .15s; }
  .b-clear:hover { color:#ef4444; }

  /* ── Mobile filter toggle ── */
  .b-filter-toggle { display:none; }

  /* ── Results header bar ── */
  .b-results-bar {
    display:flex; justify-content:space-between; align-items:center;
    margin-bottom:18px; padding:12px 16px;
    background:#fff; border:1px solid #e5e7eb; border-radius:10px;
  }
  .b-results-count { font-size:14px; font-weight:600; color:#374151; }
  .b-results-count span { color:#9ca3af; font-weight:400; }
  .b-sort-select {
    border:1.5px solid #e5e7eb; border-radius:8px; padding:7px 32px 7px 11px; font-size:13px;
    font-family:inherit; background:#f9fafb; color:#374151; outline:none; cursor:pointer;
    appearance:none; background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' fill='%236b7280' viewBox='0 0 16 16'%3E%3Cpath d='M7.247 11.14L2.451 5.658C1.885 5.013 2.345 4 3.204 4h9.592a1 1 0 0 1 .753 1.659l-4.796 5.48a1 1 0 0 1-1.506 0z'/%3E%3C/svg%3E");
    background-repeat:no-repeat; background-position:right 10px center;
    transition:border-color .15s;
  }
  .b-sort-select:focus { border-color:#16a34a; background-color:#fff; box-shadow:0 0 0 3px rgba(22,163,74,.1); }

  /* ── Active filter chips ── */
  .b-chips { display:flex; flex-wrap:wrap; gap:7px; margin-bottom:14px; }
  .b-chip {
    display:inline-flex; align-items:center; gap:5px; background:#f0fdf4; border:1px solid #bbf7d0;
    color:#166534; font-size:12px; font-weight:600; padding:4px 10px; border-radius:20px;
  }
  .b-chip a { color:#16a34a; font-size:13px; font-weight:700; line-height:1; text-decoration:none; margin-left:2px; }
  .b-chip a:hover { color:#ef4444; }

  /* ── Cards grid ── */
  .b-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:18px; }
  .b-card {
    background:#fff; border:1px solid #e5e7eb; border-radius:14px; overflow:hidden;
    transition:box-shadow .2s, transform .2s; cursor:pointer; display:block; color:inherit;
  }
  .b-card:hover { box-shadow:0 8px 32px rgba(0,0,0,.12); transform:translateY(-4px); border-color:#d1d5db; }
  .b-card-img {
    aspect-ratio:4/3; background:#f3f4f6; overflow:hidden; position:relative;
  }
  .b-card-img img { width:100%; height:100%; object-fit:cover; display:block; }
  .b-card-img .b-no-img {
    width:100%; height:100%; display:flex; align-items:center; justify-content:center;
    color:#d1d5db; font-size:36px;
  }
  /* Source badge overlaid bottom-left of image */
  .b-img-badge {
    position:absolute; bottom:10px; left:10px;
    background:rgba(0,0,0,.55); backdrop-filter:blur(4px);
    color:#fff; font-size:10px; font-weight:700; letter-spacing:.8px;
    padding:3px 8px; border-radius:5px;
  }
  .b-img-badge.b-img-badge-kj { background:rgba(29,78,216,.7); }
  .b-img-badge.b-img-badge-fb { background:rgba(109,40,217,.7); }
  /* NEW badge */
  .b-new-badge {
    position:absolute; top:10px; right:10px;
    background:#16a34a; color:#fff; font-size:10px; font-weight:700;
    padding:3px 8px; border-radius:5px; letter-spacing:.5px;
    display:flex; align-items:center; gap:4px;
  }
  .b-new-dot { width:6px; height:6px; border-radius:50%; background:#4ade80; flex-shrink:0; }
  .b-card-body { padding:15px; }
  .b-card-title { font-size:14px; font-weight:700; color:#111827; margin-bottom:7px; line-height:1.35; }
  .b-card-price { font-size:22px; font-weight:700; color:#16a34a; margin-bottom:8px; line-height:1; }
  .b-card-price.no-price { color:#9ca3af; font-size:14px; font-weight:500; }
  .b-card-meta { font-size:12px; color:#9ca3af; display:flex; align-items:center; gap:6px; }
  .b-card-meta-sep { color:#d1d5db; }
  .b-card-trim { font-size:12px; color:#6b7280; margin-bottom:6px; }
  .b-src-pill { display:inline-block; font-size:11px; font-weight:600; padding:3px 10px; border-radius:20px; margin-top:8px; }
  .b-src-pill-kj { background:#dbeafe; color:#1d4ed8; }
  .b-src-pill-fb { background:#ede9fe; color:#6d28d9; }
  .b-src-pill-other { background:#f3f4f6; color:#6b7280; }
  .b-verified-badge { display:inline-flex; align-items:center; gap:3px; font-size:11px; font-weight:600; color:#15803d; background:#f0fdf4; border:1px solid #bbf7d0; border-radius:4px; padding:2px 7px; margin-top:6px; }
  .b-limited-info { display:inline-block; font-size:11px; color:#9ca3af; background:#f9fafb; border:1px solid #e5e7eb; border-radius:4px; padding:2px 7px; margin-top:6px; }
  .b-card-view-link { font-size:12px; color:#16a34a; font-weight:600; margin-top:8px; display:block; }
  .b-card-view-link:hover { text-decoration:underline; }

  /* ── Pagination ── */
  .b-pager-wrap { margin-top:28px; }
  .b-pager-info { text-align:center; font-size:13px; color:#9ca3af; margin-bottom:12px; }
  .b-pager { display:flex; align-items:center; gap:6px; justify-content:center; flex-wrap:wrap; }
  .b-pager a, .b-pager span {
    padding:9px 16px; border-radius:9px; font-size:13px; font-weight:600;
    border:1.5px solid #e5e7eb; background:#fff; color:#374151; transition:all .15s;
  }
  .b-pager a:hover { background:#f0fdf4; border-color:#16a34a; color:#16a34a; }
  .b-pager .b-cur { background:#16a34a; color:#fff; border-color:#16a34a; }
  .b-pager .b-pager-nav { border-color:#d1d5db; color:#6b7280; }
  .b-pager .b-pager-nav:hover { border-color:#16a34a; color:#16a34a; background:#f0fdf4; }
  .b-pager .b-info { background:none; border:none; color:#9ca3af; font-size:13px; padding:9px 6px; }

  /* ── Empty state ── */
  .b-empty {
    text-align:center; padding:72px 24px; color:#9ca3af; grid-column:1/-1;
    background:#fff; border:1px solid #e5e7eb; border-radius:14px;
  }
  .b-empty-title { font-size:18px; font-weight:700; color:#374151; margin-bottom:8px; }
  .b-empty-sub { font-size:14px; color:#9ca3af; margin-bottom:24px; }
  .b-empty-clear { display:inline-block; background:#16a34a; color:#fff; border-radius:9px; padding:10px 24px; font-size:14px; font-weight:700; transition:background .15s; }
  .b-empty-clear:hover { background:#15803d; color:#fff; }

  /* ── Alerts & Unsubscribe forms ── */
  .b-center-wrap { max-width:500px; margin:60px auto; padding:0 20px; }
  .b-form-card { background:#fff; border:1px solid #e5e7eb; border-radius:16px; padding:36px; }
  .b-form-card h1 { font-size:22px; font-weight:700; margin-bottom:6px; }
  .b-form-card .b-subtitle { font-size:14px; color:#9ca3af; margin-bottom:28px; }
  .b-form-card .b-field label { font-size:13px; font-weight:600; color:#4b5563; margin-bottom:5px; }
  .b-form-card .b-field { margin-bottom:16px; }
  .b-form-card .b-field input, .b-form-card .b-field select {
    width:100%; border:1.5px solid #e5e7eb; border-radius:9px; padding:10px 13px;
    font-size:14px; font-family:inherit; background:#f9fafb; color:#111827; outline:none;
  }
  .b-form-card .b-field input:focus, .b-form-card .b-field select:focus {
    border-color:#16a34a; background:#fff; box-shadow:0 0 0 3px rgba(22,163,74,.1);
  }
  .b-submit { width:100%; background:#16a34a; color:#fff; border:none; border-radius:10px; padding:13px; font-size:14px; font-weight:700; cursor:pointer; font-family:inherit; margin-top:8px; transition:background .15s; }
  .b-submit:hover { background:#15803d; }
  .b-success { background:#f0fdf4; border:1px solid #bbf7d0; border-radius:10px; padding:18px; color:#166534; font-size:14px; text-align:center; margin-top:16px; }
  .b-error { background:#fef2f2; border:1px solid #fecaca; border-radius:10px; padding:18px; color:#991b1b; font-size:14px; text-align:center; margin-top:16px; }

  /* ── Unsubscribe ── */
  .b-unsub-card { background:#fff; border:1px solid #e5e7eb; border-radius:16px; padding:48px 36px; text-align:center; }
  .b-unsub-card .b-icon { font-size:48px; margin-bottom:16px; }
  .b-unsub-card h1 { font-size:22px; font-weight:700; margin-bottom:10px; }
  .b-unsub-card p { font-size:14px; color:#9ca3af; }
  .b-unsub-card a { color:#16a34a; font-weight:600; }

  /* ── Mobile responsive ── */
  @media (max-width:768px) {
    .b-hero { padding:40px 20px 36px; }
    .b-hero-search { flex-direction:column; }
    .b-hero-search button { width:100%; }
    .b-wrap { padding:16px; }
    .b-layout { grid-template-columns:1fr; }
    .b-sidebar { position:static; display:none; }
    .b-sidebar.open { display:block; }
    .b-filter-toggle {
      display:flex; align-items:center; gap:8px; background:#fff; border:1.5px solid #e5e7eb;
      border-radius:9px; padding:9px 16px; font-size:13px; font-weight:600; color:#374151;
      cursor:pointer; font-family:inherit; margin-bottom:12px; width:100%; justify-content:center;
    }
    .b-filter-toggle:hover { border-color:#16a34a; color:#16a34a; }
    .b-grid { grid-template-columns:repeat(2,1fr); gap:12px; }
    .b-topbar { padding:0 16px; }
  }
  @media (max-width:480px) {
    .b-grid { grid-template-columns:1fr; }
    .b-form-card { padding:24px 18px; }
    .b-pager a, .b-pager span { padding:8px 12px; font-size:12px; }
  }
`;

// ── Tooltip + action JS ───────────────────────────────────

const SHARED_JS = `
  // ── Tooltip ───────────────────────────────────────────
  const tt = document.createElement('div');
  tt.id = 'tt';
  document.body.appendChild(tt);
  let ttVisible = false;

  // data-tip attribute is base64-encoded JSON — decode safely
  function parseTip(el) {
    try { return JSON.parse(atob(el.getAttribute('data-tip') || '')); }
    catch(e) { console.warn('tooltip parse error:', e); return null; }
  }

  function showTip(e, el) {
    const d = parseTip(el);
    if (!d) return;
    const scoreColor = d.score >= 70 ? '#27ae60' : d.score >= 50 ? '#e67e22' : '#e74c3c';
    let html = '';
    if (d.img) html += '<img src="' + d.img + '" onerror="this.style.display=\'none\'" loading="lazy"/>';
    html += '<div class="tt-title">' + [d.year, d.make, d.model, d.trim].filter(Boolean).join(' ') + '</div>';
    html += '<span class="tt-score" style="background:' + scoreColor + '22;color:' + scoreColor + ';border:1px solid ' + scoreColor + '44">Score ' + d.score + ' / 100</span>';
    const rows = [
      ['Price',         d.price],
      ['Mileage',       d.mileage],
      ['Colour (ext)',  d.colour],
      ['Colour (int)',  d.colourIn],
      ['Body type',     d.body],
      ['Drivetrain',    d.drive],
      ['Fuel type',     d.fuel],
      ['Transmission',  d.trans],
      ['Engine',        d.engine],
      ['Accidents',     d.accidents != null ? String(d.accidents) : null],
      ['Prev owners',   d.owners   != null ? String(d.owners)    : null],
      ['Safetied',      d.safetied != null ? (d.safetied ? 'Yes ✓' : 'No') : null],
      ['VIN',           d.vin],
      ['Seller',        d.seller],
      ['Dealer',        d.dealer],
      ['Location',      d.city],
    ];
    for (const [k, v] of rows) {
      if (v != null && v !== '') {
        html += '<div class="tt-row"><span>' + k + '</span><span class="tt-val">' + v + '</span></div>';
      }
    }
    html += '<div class="tt-links">';
    if (d.src)    html += '<a class="tt-link" href="' + d.src    + '" target="_blank">VIEW LISTING →</a>';
    if (d.carfax) html += '<a class="tt-link" href="' + d.carfax + '" target="_blank" style="color:#27ae60">CARFAX →</a>';
    html += '</div>';
    tt.innerHTML = html;
    tt.style.display = 'block';
    ttVisible = true;
    moveTip(e);
  }

  function moveTip(e) {
    const x = e.clientX + 20, y = e.clientY + 20;
    const W = window.innerWidth, H = window.innerHeight;
    tt.style.left = (x + 330 > W ? x - 346 : x) + 'px';
    tt.style.top  = (y + 380 > H ? y - 380 : y) + 'px';
  }

  function hideTip() { tt.style.display = 'none'; ttVisible = false; }

  document.addEventListener('mousemove', e => { if (ttVisible) moveTip(e); });

  // Attach to all rows with data-tip
  document.querySelectorAll('[data-tip]').forEach(el => {
    el.addEventListener('mouseenter', e => showTip(e, el));
    el.addEventListener('mouseleave', hideTip);
  });

  // ── Review actions ────────────────────────────────────
  async function reviewAction(queueId, action) {
    const row  = document.querySelector('tr[data-queue-row="' + queueId + '"]');
    const btns = row ? row.querySelectorAll('button') : [];
    btns.forEach(b => { b.disabled = true; });

    const rerunBtn   = row ? row.querySelector('[data-action="reanalyse"]') : null;
    const removeBtn  = row ? row.querySelector('[data-action="remove"]') : null;
    const approveBtn = row ? row.querySelector('[data-action="approve"]') : null;

    if (action === 'reanalyse') {
      if (rerunBtn) {
        rerunBtn.classList.add('btn-spinning');
        rerunBtn.textContent = 'RUNNING AI...';
      }
      if (row) row.querySelectorAll('td').forEach(td => { td.style.color = '#4a9eff'; td.style.background = '#08141f'; });
    } else if (action === 'approve') {
      if (approveBtn) approveBtn.textContent = '✓ APPROVING...';
      if (row) row.querySelectorAll('td').forEach(td => { td.style.color = '#27ae60'; td.style.background = '#051a0a'; });
    } else {
      if (removeBtn) removeBtn.textContent = '✕ REMOVING...';
      if (row) row.querySelectorAll('td').forEach(td => { td.style.color = '#e74c3c'; td.style.background = '#1a0505'; });
    }

    try {
      const r = await fetch('/api/review/' + queueId + '/' + action, { method: 'POST' });
      const j = await r.json();

      if (j.ok) {
        if (row) {
          // Success animation — green for reanalyse/approve, red fade-out for remove
          if (action === 'reanalyse') {
            if (rerunBtn) { rerunBtn.classList.remove('btn-spinning'); rerunBtn.textContent = '✓ SENT TO AI'; }
            row.querySelectorAll('td').forEach(td => { td.style.color = '#27ae60'; td.style.background = ''; });
            row.classList.add('row-success');
          } else if (action === 'approve') {
            row.classList.add('row-success');
          } else {
            row.classList.add('row-deleting');
          }
          // Remove row from DOM after animation completes
          setTimeout(() => { if (row.parentNode) row.remove(); }, action === 'remove' ? 700 : 1200);
        }
      } else {
        // Error — restore buttons, show inline message
        if (row) {
          row.querySelectorAll('td').forEach(td => { td.style.color = ''; td.style.background = ''; });
          btns.forEach(b => { b.disabled = false; });
          if (rerunBtn)   { rerunBtn.classList.remove('btn-spinning');  rerunBtn.textContent  = 'RE-RUN AI'; }
          if (removeBtn)  { removeBtn.textContent  = 'REMOVE'; }
          if (approveBtn) { approveBtn.textContent = 'APPROVE'; }
          // Show error inline — visible even if browser blocks alerts
          const lastTd = row.querySelector('td:last-child');
          if (lastTd) {
            const errSpan = document.createElement('div');
            errSpan.style.cssText = 'color:#e74c3c;font-size:9px;margin-top:4px;letter-spacing:0;word-break:break-all;';
            errSpan.textContent = 'ERR: ' + (j.error || 'unknown');
            lastTd.appendChild(errSpan);
            setTimeout(() => errSpan.remove(), 10000);
          }
        }
        console.error('Review action error:', j.error);
      }
    } catch(err) {
      if (row) {
        row.querySelectorAll('td').forEach(td => { td.style.color = ''; td.style.background = ''; });
        btns.forEach(b => { b.disabled = false; });
        if (rerunBtn)   { rerunBtn.classList.remove('btn-spinning');  rerunBtn.textContent  = 'RE-RUN AI'; }
        if (removeBtn)  { removeBtn.textContent  = 'REMOVE'; }
        if (approveBtn) { approveBtn.textContent = 'APPROVE'; }
        const lastTd = row.querySelector('td:last-child');
        if (lastTd) {
          const errSpan = document.createElement('div');
          errSpan.style.cssText = 'color:#e74c3c;font-size:9px;margin-top:4px;letter-spacing:0;';
          errSpan.textContent = 'NETWORK ERROR — check Railway logs';
          lastTd.appendChild(errSpan);
          setTimeout(() => errSpan.remove(), 10000);
        }
      }
      console.error('Review action network error:', err);
    }
  }

  // ── Re-run ALL pending ────────────────────────────────
  async function rerunAllPending() {
    const btn  = document.getElementById('rerun-all-btn');
    const prog = document.getElementById('rerun-all-progress');
    if (btn)  btn.disabled = true;
    if (prog) prog.textContent = 'Fetching queue...';

    try {
      const resp = await fetch('/api/review/bulk-reanalyse-all', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      });
      if (!resp.ok || !resp.body) throw new Error('Server error ' + resp.status);

      const reader  = resp.body.getReader();
      const decoder = new TextDecoder();
      let   buf     = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        const parts = buf.split('\\n\\n');
        buf = parts.pop() ?? '';
        for (const part of parts) {
          if (!part.startsWith('data: ')) continue;
          try {
            const ev = JSON.parse(part.slice(6));
            if (prog) {
              if (ev.finished) {
                prog.textContent = '✓ ' + ev.done + ' listings re-queued for AI — reloading...';
                setTimeout(() => location.reload(), 2000);
              } else {
                prog.textContent = ev.done + ' / ' + ev.total + ' processed...';
                // Fade out the row if it's visible
                const row = document.querySelector('tr[data-queue-row="' + ev.queueId + '"]');
                if (row) { row.classList.add('row-success'); setTimeout(() => { if (row.parentNode) row.remove(); }, 1200); }
              }
            }
          } catch {}
        }
      }
    } catch(err) {
      if (prog) prog.textContent = 'Error: ' + err.message;
      if (btn)  btn.disabled = false;
    }
  }

  // ── Bulk review checkbox logic ─────────────────────────
  function getCheckedIds() {
    return Array.from(document.querySelectorAll('.rq-cb:checked')).map(el => el.value);
  }

  function updateBulkBar() {
    const ids = getCheckedIds();
    const bar = document.getElementById('bulk-bar');
    const countEl = document.getElementById('bulk-count');
    if (!bar || !countEl) return;
    if (ids.length > 0) {
      bar.style.display = 'flex';
      countEl.textContent = ids.length + ' SELECTED';
    } else {
      bar.style.display = 'none';
    }
  }

  function toggleAllCheckboxes(checked) {
    document.querySelectorAll('.rq-cb').forEach(cb => { cb.checked = checked; });
    updateBulkBar();
  }

  function clearSelection() {
    document.querySelectorAll('.rq-cb').forEach(cb => { cb.checked = false; });
    const sa = document.getElementById('rq-select-all');
    if (sa) sa.checked = false;
    updateBulkBar();
  }

  async function bulkReanalyse() {
    const ids = getCheckedIds();
    if (ids.length === 0) return;
    const btn = document.getElementById('bulk-rerun-btn');
    const prog = document.getElementById('bulk-progress');
    if (btn) btn.disabled = true;
    if (prog) prog.textContent = 'Starting...';

    // Highlight selected rows
    ids.forEach(id => {
      const row = document.querySelector('tr[data-queue-row="' + id + '"]');
      if (row) {
        row.classList.add('processing');
        row.querySelectorAll('td').forEach(td => { td.style.color = '#4a9eff'; td.style.background = '#08141f'; });
      }
    });

    try {
      const resp = await fetch('/api/review/bulk-reanalyse', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ queueIds: ids }),
      });

      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buf = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        const parts = buf.split('\n\n');
        buf = parts.pop() || '';
        for (const part of parts) {
          if (!part.startsWith('data:')) continue;
          try {
            const ev = JSON.parse(part.slice(5).trim());
            if (prog) prog.textContent = ev.done + ' / ' + ev.total + ' queued for AI re-run';
            // Remove processed row
            if (ev.queueId) {
              const row = document.querySelector('tr[data-queue-row="' + ev.queueId + '"]');
              if (row) {
                row.classList.remove('processing');
                row.querySelectorAll('td').forEach(td => { td.style.color = '#27ae60'; td.style.background = '#051a0a'; });
                setTimeout(() => {
                  row.style.transition = 'opacity 0.4s';
                  row.style.opacity = '0';
                  setTimeout(() => row.remove(), 400);
                }, 600 + ev.done * 80);
              }
            }
            if (ev.finished) {
              if (prog) prog.textContent = '✓ ' + ev.done + ' listings queued — AI will re-process shortly';
              setTimeout(() => location.reload(), 2500);
            }
          } catch {}
        }
      }
    } catch(err) {
      if (prog) prog.textContent = 'Error: ' + err.message;
      if (btn) btn.disabled = false;
    }
  }
`;

// ── Dashboard HTML ─────────────────────────────────────────

function buildDashboardHtml(s: Stats): string {
  const reviewCount = s.overview.needs_review;
  const activeRate  = s.overview.total > 0 ? Math.round(100 * s.overview.active / s.overview.total) : 0;

  const sourcesRows = s.sources.map(src => {
    const col = src.source_id === 'kijiji-ca' ? '#2980b9' : src.source_id === 'facebook-mp-ca' ? '#4a9eff' : '#888';
    return `
    <tr>
      <td><span class="source-dot" style="background:${col}"></span>${src.source_id}</td>
      <td><div>${fmt(src.count)}</div><div class="bar-wrap"><div class="bar-fill" style="width:${src.pct}%;background:${col}"></div></div></td>
      <td>${src.pct}%</td>
    </tr>`;
  }).join('');

  const confRows = s.confidence_dist.map(r => {
    const cls = r.bucket === '90-100' ? 'tag-green' : r.bucket === '70-89' ? 'tag-blue' : r.bucket === '50-69' ? 'tag-amber' : 'tag-red';
    return `<div class="dedup-row"><span>Score ${r.bucket}</span><span><span class="tag ${cls}">${fmt(r.count)}</span></span></div>`;
  }).join('');

  const recentRows = s.recent.map(r => {
    const price   = r.price ? `$${Number(r.price).toLocaleString('en-CA')}` : `[${r.price_type}]`;
    const mileage = r.mileage_km ? `${Number(r.mileage_km).toLocaleString()} km` : '—';
    const sc      = r.confidence_score;
    const scColor = sc >= 70 ? '#27ae60' : sc >= 50 ? '#e67e22' : '#e74c3c';
    const thumb = r.photo_urls?.[0]
      ? `<img class="thumb" src="${r.photo_urls[0]}" loading="lazy" onerror="this.style.display='none'">`
      : `<div class="thumb" style="background:#111;"></div>`;
    return `
    <tr data-tip="${tooltipData(r)}">
      <td class="thumb-cell">${thumb}</td>
      <td>${sourceBadge(r.source_id)}<a class="listing-link" href="${r.source_url}" target="_blank">${r.year} ${r.make} ${r.model}${r.trim ? ' ' + r.trim : ''}</a></td>
      <td>${price}</td>
      <td>${mileage}</td>
      <td>${r.colour_exterior ?? '—'}</td>
      <td>${r.city}${r.province ? ', ' + r.province : ''}</td>
      <td>${r.seller_type}</td>
      <td><span style="color:${scColor}">${sc}</span></td>
      <td>${statusTag(r.status, r.needs_review, r.is_duplicate)}</td>
      <td style="color:#555;font-size:10px;">${r.created_at}</td>
    </tr>`;
  }).join('');

  const reviewRows = s.review_queue.map(r => `
    <tr data-queue-row="${r.queue_id}" data-tip="${tooltipData(r as unknown as RecentRow)}">
      <td style="width:28px;padding-right:4px;"><input type="checkbox" class="rq-cb" value="${r.queue_id}" onchange="updateBulkBar()"></td>
      <td>${sourceBadge(r.source_id)}<a class="listing-link" href="${r.source_url}" target="_blank">${r.year} ${r.make} ${r.model}</a></td>
      <td><span style="color:#e67e22">${r.confidence_score}</span></td>
      <td style="color:#888;font-size:11px;">${r.reason}</td>
      <td style="color:#555;font-size:10px;">${r.created_at}</td>
      <td>
        <button class="btn btn-approve"   data-qid="${r.queue_id}" data-action="approve"   onclick="reviewAction('${r.queue_id}','approve')" style="margin-left:4px">APPROVE</button>
        <button class="btn btn-reanalyse" data-qid="${r.queue_id}" data-action="reanalyse" onclick="reviewAction('${r.queue_id}','reanalyse')" style="margin-left:4px">RE-RUN AI</button>
        <button class="btn btn-remove"    data-qid="${r.queue_id}" data-action="remove"    onclick="reviewAction('${r.queue_id}','remove')" style="margin-left:4px">REMOVE</button>
      </td>
    </tr>`).join('');

  const pipelinePublishRate = s.pipeline.total_processed > 0 ? Math.round(100 * s.pipeline.published / s.pipeline.total_processed) : 0;

  return `<!DOCTYPE html>
<html lang="en"><head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Aven — Command Center</title>
  <style>${SHARED_CSS}</style>
</head><body>

  <div class="topbar">
    <div class="logo">AVEN <span>COMMAND CENTER  ·  v0.4</span></div>
    <div style="display:flex;gap:10px;align-items:center;">
      <a href="/" class="nav-link active">DASHBOARD</a>
      <a href="/listings" class="nav-link">ALL LISTINGS</a>
      <a href="/home" class="nav-link">BROWSE</a>
      <div class="live-pill"><div class="live-dot"></div> LIVE</div>
      <div style="font-size:11px;color:#555;">Last sync: ${s.last_sync}</div>
      <div style="font-size:11px;color:#555;cursor:pointer;" onclick="location.reload()">↺ REFRESH</div>
    </div>
  </div>

  ${reviewCount > 0 ? `
  <div class="unsure-alert">
    <div class="alert-text">⚠  <strong style="color:#e8d0a0;">${reviewCount} listing${reviewCount !== 1 ? 's' : ''}</strong> in the <strong style="color:#e8d0a0;">REVIEW QUEUE</strong> — require manual validation before publishing.</div>
    <a href="#review" class="alert-badge">REVIEW NOW ↓</a>
  </div>` : ''}

  <div class="section-title">Overview</div>
  <div class="grid4">
    <div class="card">
      <div class="card-label">Total Listings</div>
      <div class="card-value">${fmt(s.overview.total)}</div>
      <div class="card-delta neutral">${s.overview.active} live  ·  ${s.overview.review} review  ·  ${s.overview.rejected} rejected</div>
    </div>
    <div class="card green">
      <div class="card-label">Live (Published)</div>
      <div class="card-value" style="color:#27ae60">${fmt(s.overview.active)}</div>
      <div class="card-delta neutral">${activeRate}% publish rate</div>
    </div>
    <div class="card amber">
      <div class="card-label">Review Queue</div>
      <div class="card-value" style="color:${reviewCount > 0 ? '#e67e22' : '#27ae60'}">${fmt(reviewCount)}</div>
      <div class="card-delta ${reviewCount > 0 ? 'down' : 'up'}">${reviewCount > 0 ? 'requires attention' : 'queue clear'}</div>
    </div>
    <div class="card blue">
      <div class="card-label">Avg Confidence</div>
      <div class="card-value" style="color:#2980b9">${s.overview.avg_confidence}<span style="font-size:14px;color:#555">/100</span></div>
      <div class="card-delta neutral">${fmt(s.overview.with_price)} with price  ·  ${fmt(s.overview.with_mileage)} with mileage</div>
    </div>
  </div>

  <!-- Source split + dedup bar -->
  <div style="display:flex;gap:12px;margin-bottom:20px;">
    <div class="card" style="flex:1;padding:14px 18px;display:flex;align-items:center;justify-content:space-between;">
      <div>
        <div class="card-label">Kijiji Canada</div>
        <div style="font-size:20px;font-weight:500;color:#2980b9">${fmt(s.overview.kijiji_count)}</div>
      </div>
      <span class="src-badge src-kijiji" style="font-size:12px;padding:4px 10px;">KJ</span>
    </div>
    <div class="card" style="flex:1;padding:14px 18px;display:flex;align-items:center;justify-content:space-between;">
      <div>
        <div class="card-label">Facebook Marketplace</div>
        <div style="font-size:20px;font-weight:500;color:#4a9eff">${fmt(s.overview.facebook_count)}</div>
        ${s.overview.facebook_count === 0 ? '<div style="font-size:10px;color:#555;margin-top:2px;">→ run fb-auth-setup.ts to activate</div>' : ''}
      </div>
      <span class="src-badge src-fb" style="font-size:12px;padding:4px 10px;">FB</span>
    </div>
    <div class="card" style="flex:1;padding:14px 18px;display:flex;align-items:center;justify-content:space-between;">
      <div>
        <div class="card-label">Duplicates Removed</div>
        <div style="font-size:20px;font-weight:500;color:#888">${fmt(s.dedup.duplicates_removed)}</div>
        <div style="font-size:10px;color:#555;margin-top:2px;">${fmt(s.dedup.canonical_groups)} cross-source groups found</div>
      </div>
      <span style="font-size:18px;color:#444;">⊘</span>
    </div>
  </div>

  <div class="grid2">
    <div class="card gray" style="padding:18px;">
      <div class="section-title">Listings per source</div>
      ${s.sources.length === 0 ? '<div style="color:#555;font-size:12px;">No listings yet</div>' : `
      <table class="source-table">
        <thead><tr><th>Platform</th><th>Listings</th><th style="text-align:right">Share</th></tr></thead>
        <tbody>${sourcesRows}</tbody>
      </table>`}
    </div>
    <div class="card purple" style="padding:18px;">
      <div class="section-title">Confidence distribution</div>
      ${confRows || '<div style="color:#555;font-size:12px;">No data yet</div>'}
      <div style="margin-top:16px;padding-top:12px;border-top:1px solid #1f1f1f;">
        <div class="card-label">Overall avg confidence</div>
        <div style="font-size:22px;font-weight:500;color:#8e44ad">${s.overview.avg_confidence}<span style="font-size:14px;color:#555"> / 100</span></div>
        <div class="card-sub">publish ≥70  ·  review 50–69  ·  reject &lt;50</div>
      </div>
    </div>
  </div>

  <div class="card gray" style="margin-bottom:20px;">
    <div class="section-title">M2 Normalisation pipeline (all-time)</div>
    <div class="pipeline">
      <div class="pipe-stage"><div class="pipe-box"><div class="pipe-num">${fmt(s.pipeline.total_processed)}</div><div class="pipe-label">PROCESSED</div></div></div>
      <div class="pipe-arrow">→</div>
      <div class="pipe-stage"><div class="pipe-box"><div class="pipe-num" style="color:#27ae60">${fmt(s.pipeline.published)}</div><div class="pipe-label">PUBLISHED</div></div></div>
      <div class="pipe-arrow">→</div>
      <div class="pipe-stage"><div class="pipe-box"><div class="pipe-num" style="color:#e67e22">${fmt(s.pipeline.in_review)}</div><div class="pipe-label">IN REVIEW</div></div></div>
      <div class="pipe-arrow">→</div>
      <div class="pipe-stage"><div class="pipe-box"><div class="pipe-num" style="color:#e74c3c">${fmt(s.pipeline.rejected)}</div><div class="pipe-label">REJECTED</div></div></div>
      <div class="pipe-arrow">→</div>
      <div class="pipe-stage"><div class="pipe-box"><div class="pipe-num" style="color:#2980b9">${pipelinePublishRate}%</div><div class="pipe-label">PUBLISH RATE</div></div></div>
    </div>
  </div>

  <div class="grid3" style="margin-bottom:20px;">
    <div class="card">
      <div class="card-label">Avg LLM Latency</div>
      <div class="card-value" style="font-size:22px">${fmt(s.pipeline.avg_latency_ms)}<span style="font-size:12px;color:#555"> ms</span></div>
      <div class="card-sub">per listing extraction</div>
    </div>
    <div class="card">
      <div class="card-label">Avg Prompt Tokens</div>
      <div class="card-value" style="font-size:22px">${fmt(s.pipeline.avg_prompt_tokens)}</div>
      <div class="card-sub">per extraction call</div>
    </div>
    <div class="card ${s.pipeline.pii_failed > 0 ? 'amber' : 'green'}">
      <div class="card-label">PII Redaction</div>
      <div class="card-value" style="font-size:22px">${fmt(s.pipeline.pii_redacted)}</div>
      <div class="card-sub">phone/email items stripped from descriptions  ·  ${s.pipeline.pii_failed} failed</div>
    </div>
  </div>

  <div class="card gray" style="padding:18px;margin-bottom:20px;">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;">
      <div class="section-title" style="margin-bottom:0">Recent listings</div>
      <a href="/listings" style="font-size:10px;color:#555;letter-spacing:1px;">VIEW ALL ${fmt(s.overview.total)} →</a>
    </div>
    ${s.recent.length === 0 ? '<div style="color:#555;font-size:12px;">No listings yet — run the pipeline to populate.</div>' : `
    <div style="overflow-x:auto;">
      <table class="data-table">
        <thead><tr>
          <th class="thumb-cell"></th>
          <th>Vehicle</th><th>Price</th><th>Mileage</th><th>Colour</th>
          <th>Location</th><th>Seller</th><th>Score</th><th>Status</th><th>Added</th>
        </tr></thead>
        <tbody>${recentRows}</tbody>
      </table>
    </div>`}
  </div>

  <div id="review" class="card ${s.review_queue.length > 0 ? 'amber' : 'gray'}" style="padding:18px;margin-bottom:20px;">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
      <div class="section-title" style="color:${s.review_queue.length > 0 ? '#e67e22' : '#555'};margin-bottom:0;">
        Review queue
        <span style="font-size:11px;font-weight:normal;margin-left:8px;color:#666;">${s.review_queue.length > 0 ? `${s.overview.needs_review} pending` : '— queue clear'}</span>
      </div>
      <a href="/review" style="font-size:10px;color:#e67e22;letter-spacing:1px;border:1px solid #4a2a00;padding:3px 10px;border-radius:2px;background:#1a0e00;text-decoration:none;">VIEW FULL QUEUE →</a>
    </div>

    ${s.review_queue.length > 0 ? `
    <div style="font-size:11px;color:#666;margin-bottom:12px;">
      Showing latest 25 of ${s.overview.needs_review}  ·  Hover any row for full data  ·  <strong style="color:#2980b9">RE-RUN AI</strong> re-processes through LLM from scratch  ·  <strong style="color:#e74c3c">REMOVE</strong> permanently rejects
    </div>
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;">
      <button id="rerun-all-btn" class="btn btn-reanalyse" onclick="rerunAllPending()" style="padding:5px 14px;font-size:11px;">⟳ RE-RUN ALL ${s.overview.needs_review} PENDING</button>
      <span id="rerun-all-progress" style="font-size:11px;color:#555;"></span>
    </div>
    <div id="bulk-bar" style="display:none;background:#0d1f2b;border:1px solid #1a3a4a;border-radius:3px;padding:10px 14px;margin-bottom:12px;align-items:center;gap:12px;">
      <span id="bulk-count" style="font-size:12px;color:#4a9eff;letter-spacing:1px;">0 SELECTED</span>
      <button id="bulk-rerun-btn" class="btn btn-reanalyse" onclick="bulkReanalyse()" style="padding:5px 14px;font-size:11px;">⟳ RE-RUN AI ON SELECTED</button>
      <button class="btn" onclick="clearSelection()" style="background:#1a1a1a;color:#888;border:1px solid #333;padding:5px 10px;font-size:11px;">✕ DESELECT ALL</button>
      <div id="bulk-progress" style="flex:1;font-size:11px;color:#888;"></div>
    </div>
    <div style="overflow-x:auto;">
      <table class="data-table">
        <thead><tr>
          <th style="width:28px;"><input type="checkbox" id="rq-select-all" onchange="toggleAllCheckboxes(this.checked)" title="Select all"></th>
          <th>Vehicle</th><th>Score</th><th>Reason</th><th>Added</th><th>Actions</th>
        </tr></thead>
        <tbody>${reviewRows}</tbody>
      </table>
    </div>` : `
    <div style="color:#555;font-size:12px;padding:16px 0;">All clear — no listings waiting for review.</div>`}
  </div>

  <div class="footer-bar">
    <span>AVEN  ·  INTERNAL DASHBOARD</span>
    <span>DB: /opt/homebrew/var/postgresql@16/aven_dev (local)</span>
    <span>NEXT REFRESH: <span id="countdown">10</span>s</span>
    <span>BUILD v0.4.0</span>
  </div>

  <script>
    ${SHARED_JS}
    let secs = ${Math.floor(REFRESH_MS / 1000)};
    const el = document.getElementById('countdown');
    setInterval(() => { secs--; if (el) el.textContent = String(secs); if (secs <= 0) location.reload(); }, 1000);
  </script>
</body></html>`;
}

// ── All Listings page ─────────────────────────────────────

const ALLOWED_STATUSES = new Set(['active', 'review', 'rejected']);

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
function isValidUuid(s: string): boolean { return UUID_RE.test(s); }

async function fetchAllListings(page: number, source: string, status: string): Promise<{ rows: RecentRow[]; total: number }> {
  const offset = (page - 1) * PAGE_SIZE;
  const conditions: string[] = [];
  const params: unknown[] = [];
  let pi = 1;

  // Only apply status filter when it is a known enum value — prevents Postgres enum cast errors
  if (source) { conditions.push(`l.source_id = $${pi++}`); params.push(source); }
  if (status && ALLOWED_STATUSES.has(status)) { conditions.push(`l.status = $${pi++}`); params.push(status); }

  const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

  const client = await pool.connect();
  try {
    const [dataRes, countRes] = await Promise.all([
      client.query(`
        SELECT ${LISTING_COLS},
          TO_CHAR(l.created_at, 'HH24:MI  DD Mon') AS created_at
        FROM listings l ${where}
        ORDER BY l.created_at DESC
        LIMIT ${PAGE_SIZE} OFFSET ${offset}
      `, params),
      client.query(`SELECT COUNT(*) AS total FROM listings l ${where}`, params),
    ]);
    return { rows: dataRes.rows, total: Number(countRes.rows[0].total) };
  } finally {
    client.release();
  }
}

function buildAllListingsHtml(rows: RecentRow[], total: number, page: number, source: string, status: string): string {
  const totalPages = Math.ceil(total / PAGE_SIZE);

  const buildLink = (p: number, src = source, st = status) => {
    const params = new URLSearchParams();
    if (p > 1) params.set('page', String(p));
    if (src) params.set('source', src);
    if (st) params.set('status', st);
    const q = params.toString();
    return `/listings${q ? '?' + q : ''}`;
  };

  const allRows = rows.map(r => {
    const price   = r.price ? `$${Number(r.price).toLocaleString('en-CA')}` : `[${r.price_type}]`;
    const mileage = r.mileage_km ? `${Number(r.mileage_km).toLocaleString()} km` : '—';
    const sc      = r.confidence_score;
    const scColor = sc >= 70 ? '#27ae60' : sc >= 50 ? '#e67e22' : '#e74c3c';
    const thumb = r.photo_urls?.[0]
      ? `<img class="thumb" src="${r.photo_urls[0]}" loading="lazy" onerror="this.style.display='none'">`
      : `<div class="thumb" style="background:#111;"></div>`;
    return `
    <tr data-tip="${tooltipData(r)}">
      <td class="thumb-cell">${thumb}</td>
      <td>${sourceBadge(r.source_id)}<a class="listing-link" href="${r.source_url}" target="_blank">${r.year} ${r.make} ${r.model}${r.trim ? ' ' + r.trim : ''}</a></td>
      <td>${price}</td>
      <td>${mileage}</td>
      <td>${r.colour_exterior ?? '—'}</td>
      <td>${r.city}${r.province ? ', ' + r.province : ''}</td>
      <td>${r.seller_type}</td>
      <td><span style="color:${scColor}">${sc}</span></td>
      <td>${statusTag(r.status, r.needs_review, r.is_duplicate)}</td>
      <td style="color:#555;font-size:10px;">${r.created_at}</td>
    </tr>`;
  }).join('');

  // Pagination
  let pager = `<div class="pager">`;
  if (page > 1) pager += `<a href="${buildLink(page - 1)}">← PREV</a>`;
  const start = Math.max(1, page - 2), end = Math.min(totalPages, page + 2);
  for (let i = start; i <= end; i++) {
    pager += i === page
      ? `<span class="cur">${i}</span>`
      : `<a href="${buildLink(i)}">${i}</a>`;
  }
  if (page < totalPages) pager += `<a href="${buildLink(page + 1)}">NEXT →</a>`;
  pager += `<span style="color:#555;margin-left:8px;">${fmt(total)} total  ·  page ${page}/${totalPages}</span></div>`;

  // Filters
  const filterSrc = (id: string, label: string) =>
    `<a href="${buildLink(1, id === source ? '' : id, status)}" class="${source === id ? 'on' : ''}">${label}</a>`;
  const filterSt = (st: string, label: string) =>
    `<a href="${buildLink(1, source, st === status ? '' : st)}" class="${status === st ? 'on' : ''}">${label}</a>`;

  return `<!DOCTYPE html>
<html lang="en"><head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Aven — All Listings</title>
  <style>${SHARED_CSS}</style>
</head><body>

  <div class="topbar">
    <div class="logo">AVEN <span>ALL LISTINGS  ·  ${fmt(total)} entries</span></div>
    <div style="display:flex;gap:10px;align-items:center;">
      <a href="/" class="nav-link">DASHBOARD</a>
      <a href="/listings" class="nav-link active">ALL LISTINGS</a>
      <a href="/home" class="nav-link">BROWSE</a>
    </div>
  </div>

  <div class="filter-bar">
    <span style="font-size:10px;color:#555;letter-spacing:1px;padding:4px 0;">SOURCE:</span>
    ${filterSrc('kijiji-ca', 'KIJIJI')}
    ${filterSrc('facebook-mp-ca', 'FACEBOOK')}
    <span style="font-size:10px;color:#333;padding:4px 8px;">|</span>
    <span style="font-size:10px;color:#555;letter-spacing:1px;padding:4px 0;">STATUS:</span>
    ${filterSt('active', 'LIVE')}
    ${filterSt('review', 'REVIEW')}
    ${filterSt('rejected', 'REJECTED')}
    ${source || status ? `<a href="/listings" style="color:#e74c3c;">✕ CLEAR</a>` : ''}
  </div>

  <div class="card gray" style="padding:18px;">
    <div style="overflow-x:auto;">
      <table class="data-table">
        <thead><tr>
          <th class="thumb-cell"></th>
          <th>Vehicle</th><th>Price</th><th>Mileage</th><th>Colour</th>
          <th>Location</th><th>Seller</th><th>Score</th><th>Status</th><th>Added</th>
        </tr></thead>
        <tbody>${allRows}</tbody>
      </table>
    </div>
    ${pager}
  </div>

  <div class="footer-bar">
    <span>AVEN  ·  ALL LISTINGS</span>
    <span>Hover any row for full parsed data + CARFAX link</span>
    <span>DB: /opt/homebrew/var/postgresql@16/aven_dev</span>
  </div>

  <script>${SHARED_JS}</script>
</body></html>`;
}

// ── Browse HTML ────────────────────────────────────────────

function browseSourcePill(sourceId: string): string {
  if (sourceId === 'kijiji-ca')      return `<span class="b-src-pill b-src-pill-kj">Kijiji</span>`;
  if (sourceId === 'facebook-mp-ca') return `<span class="b-src-pill b-src-pill-fb">Facebook Marketplace</span>`;
  return `<span class="b-src-pill b-src-pill-other">${sourceId}</span>`;
}

function browseViewLink(sourceId: string): string {
  if (sourceId === 'kijiji-ca')      return 'View on Kijiji &rarr;';
  if (sourceId === 'facebook-mp-ca') return 'View on Facebook &rarr;';
  return 'View Listing &rarr;';
}

function buildBrowseHtml(opts: BrowseOptions, rows: BrowseListing[], total: number, f: BrowseFilters): string {
  const totalPages = Math.ceil(total / BROWSE_PAGE_SIZE);
  const now = Date.now();

  const buildLink = (overrides: Partial<BrowseFilters> = {}): string => {
    const merged = { ...f, ...overrides };
    const p = new URLSearchParams();
    if (merged.make)       p.set('make', merged.make);
    if (merged.model)      p.set('model', merged.model);
    if (merged.min_year)   p.set('min_year', String(merged.min_year));
    if (merged.max_year)   p.set('max_year', String(merged.max_year));
    if (merged.min_price)  p.set('min_price', String(merged.min_price));
    if (merged.max_price)  p.set('max_price', String(merged.max_price));
    if (merged.body_type)  p.set('body_type', merged.body_type);
    if (merged.city)       p.set('city', merged.city);
    if (merged.source)     p.set('source', merged.source);
    if (merged.sort && merged.sort !== 'newest') p.set('sort', merged.sort);
    if (merged.page > 1)   p.set('page', String(merged.page));
    const qs = p.toString();
    return `/browse${qs ? '?' + qs : ''}`;
  };

  const makeOptions = opts.makes.map(m =>
    `<option value="${m.replace(/"/g, '&quot;')}"${f.make.toLowerCase() === m.toLowerCase() ? ' selected' : ''}>${m}</option>`
  ).join('');
  const bodyOptions = opts.body_types.map(b =>
    `<option value="${b.replace(/"/g, '&quot;')}"${f.body_type.toLowerCase() === b.toLowerCase() ? ' selected' : ''}>${b}</option>`
  ).join('');
  const cityOptions = opts.cities.map(c =>
    `<option value="${c.replace(/"/g, '&quot;')}"${f.city.toLowerCase() === c.toLowerCase() ? ' selected' : ''}>${c}</option>`
  ).join('');

  const regionOptions = opts.regions.map(r =>
    `<option value="${r.replace(/"/g, '&quot;')}"${f.region === r ? ' selected' : ''}>${r}</option>`
  ).join('');

  const srcActive = (val: string) => f.source === val ? ' active' : '';

  // ── Active filter chips ──
  const chipLink = (overrides: Partial<BrowseFilters>) => buildLink({ ...overrides, page: 1 });
  const chips: string[] = [];
  if (f.make)      chips.push(`<span class="b-chip">${esc(f.make)} <a href="${chipLink({ make: '' })}" title="Remove">&#x2715;</a></span>`);
  if (f.model)     chips.push(`<span class="b-chip">Model: ${esc(f.model)} <a href="${chipLink({ model: '' })}" title="Remove">&#x2715;</a></span>`);
  if (f.min_year)  chips.push(`<span class="b-chip">From ${f.min_year} <a href="${chipLink({ min_year: 0 })}" title="Remove">&#x2715;</a></span>`);
  if (f.max_year)  chips.push(`<span class="b-chip">To ${f.max_year} <a href="${chipLink({ max_year: 0 })}" title="Remove">&#x2715;</a></span>`);
  if (f.min_price) chips.push(`<span class="b-chip">Min $${f.min_price.toLocaleString('en-CA')} <a href="${chipLink({ min_price: 0 })}" title="Remove">&#x2715;</a></span>`);
  if (f.max_price) chips.push(`<span class="b-chip">Max $${f.max_price.toLocaleString('en-CA')} <a href="${chipLink({ max_price: 0 })}" title="Remove">&#x2715;</a></span>`);
  if (f.body_type) chips.push(`<span class="b-chip">${esc(f.body_type)} <a href="${chipLink({ body_type: '' })}" title="Remove">&#x2715;</a></span>`);
  if (f.region)    chips.push(`<span class="b-chip">📍 ${esc(f.region)} <a href="${chipLink({ region: '' })}" title="Remove">&#x2715;</a></span>`);
  if (f.city)      chips.push(`<span class="b-chip">${esc(f.city)} <a href="${chipLink({ city: '' })}" title="Remove">&#x2715;</a></span>`);
  if (f.source)    chips.push(`<span class="b-chip">${f.source === 'kijiji-ca' ? 'Kijiji' : 'Facebook'} <a href="${chipLink({ source: '' })}" title="Remove">&#x2715;</a></span>`);

  const hasFilters = chips.length > 0;
  const activeFilterCount = chips.length;

  // ── Cards ──
  const cards = rows.length === 0
    ? `<div class="b-empty">
        <div class="b-empty-title">No cars match your filters</div>
        <div class="b-empty-sub">Try broadening your search — adjust the price range, year, or location.</div>
        ${hasFilters ? `<a class="b-empty-clear" href="/browse">Clear all filters</a>` : ''}
       </div>`
    : rows.map(r => {
        const priceHtml = r.price != null
          ? `<div class="b-card-price">$${Number(r.price).toLocaleString('en-CA')}</div>`
          : `<div class="b-card-price no-price">Price not listed</div>`;
        const mileage  = r.mileage_km != null ? `${Number(r.mileage_km).toLocaleString('en-CA')} km` : null;
        const location = [r.city, r.province].filter(Boolean).join(', ');
        const region   = r.region && r.region !== 'Ontario' ? r.region : null;
        // Determine if listing is new (within last 24 hours)
        const isNew = r.created_at && (now - new Date(r.created_at).getTime()) < 86_400_000;
        const newBadge = isNew ? `<div class="b-new-badge"><span class="b-new-dot"></span>NEW</div>` : '';
        const imgContent = r.photo_urls?.[0]
          ? `<img src="${r.photo_urls[0]}" loading="lazy" alt="${r.year} ${r.make} ${r.model}" onerror="this.parentElement.innerHTML='<div class=\\'b-no-img\\'>&#x1F697;</div>'">`
          : `<div class="b-no-img b-img-skeleton"></div>`;

        // ── Change 3: data richness + verified badge logic ──
        const hasAllKey = r.price != null && r.mileage_km != null && r.colour_exterior != null && r.body_type != null;
        const showVerified = r.confidence_score >= 90 && r.price != null && hasAllKey;
        const showLimited  = r.price == null || r.mileage_km == null;
        const verifiedBadge = showVerified ? `<div class="b-verified-badge">&#10003; Verified</div>` : '';
        const limitedTag    = showLimited  ? `<div class="b-limited-info">Limited info</div>` : '';
        const trimLine = [r.trim, r.drivetrain].filter(Boolean).join(' · ');

        return `
        <a class="b-card" href="${r.source_url}" target="_blank" rel="noopener noreferrer">
          <div class="b-card-img">
            ${imgContent}
            ${newBadge}
          </div>
          <div class="b-card-body">
            <div class="b-card-title">${r.year} ${r.make} ${r.model}</div>
            ${trimLine ? `<div class="b-card-trim">${trimLine}</div>` : ''}
            ${priceHtml}
            <div class="b-card-meta">
              ${mileage ? `<span>${mileage}</span><span class="b-card-meta-sep">·</span>` : ''}
              <span>📍 ${location}</span>
              ${region ? `<span class="b-card-meta-sep">·</span><span style="font-size:11px;color:#6B7280;">${region}</span>` : ''}
            </div>
            ${verifiedBadge}${limitedTag}
            ${browseSourcePill(r.source_id)}
            <div class="b-card-view-link">${browseViewLink(r.source_id)}</div>
          </div>
        </a>`;
      }).join('');

  // ── Pagination ──
  let pager = '';
  if (totalPages > 1) {
    const showStart = (f.page - 1) * BROWSE_PAGE_SIZE + 1;
    const showEnd   = Math.min(f.page * BROWSE_PAGE_SIZE, total);
    pager = `<div class="b-pager-wrap">
      <div class="b-pager-info">Showing ${showStart.toLocaleString('en-CA')}–${showEnd.toLocaleString('en-CA')} of ${total.toLocaleString('en-CA')}</div>
      <div class="b-pager">`;
    if (f.page > 1) pager += `<a class="b-pager-nav" href="${buildLink({ page: f.page - 1 })}">&#8592; Previous</a>`;
    const start = Math.max(1, f.page - 2), end = Math.min(totalPages, f.page + 2);
    if (start > 1) {
      pager += `<a href="${buildLink({ page: 1 })}">1</a>`;
      if (start > 2) pager += `<span class="b-info">…</span>`;
    }
    for (let i = start; i <= end; i++) {
      pager += i === f.page
        ? `<span class="b-cur">${i}</span>`
        : `<a href="${buildLink({ page: i })}">${i}</a>`;
    }
    if (end < totalPages) {
      if (end < totalPages - 1) pager += `<span class="b-info">…</span>`;
      pager += `<a href="${buildLink({ page: totalPages })}">${totalPages}</a>`;
    }
    if (f.page < totalPages) pager += `<a class="b-pager-nav" href="${buildLink({ page: f.page + 1 })}">Next &#8594;</a>`;
    pager += `</div></div>`;
  }

  // ── Sort dropdown options ──
  const sortOptions = [
    { value: 'newest',      label: 'Newest first' },
    { value: 'price_asc',   label: 'Price: Low to High' },
    { value: 'price_desc',  label: 'Price: High to Low' },
    { value: 'mileage_asc', label: 'Lowest mileage' },
  ];
  const sortSelectOptions = sortOptions.map(o =>
    `<option value="${o.value}"${f.sort === o.value ? ' selected' : ''}>${o.label}</option>`
  ).join('');

  return `<!DOCTYPE html>
<html lang="en"><head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Browse Cars in the GTA — Aven</title>
  <style>${BROWSE_CSS}</style>
</head><body>

  <!-- Topbar -->
  <div class="b-topbar">
    <div>
      <span class="b-logo">AVEN</span><span class="b-logo"><span>GTA Car Search</span></span>
    </div>
    <div class="b-nav">
      <a href="/browse">Browse</a>
      <a href="/alerts" class="b-btn">Set Alert</a>
    </div>
  </div>

  <!-- Hero banner -->
  <section class="b-hero">
    <div class="b-hero-eyebrow">Canada's used car search</div>
    <h1 class="b-hero-title">Find your next car in the GTA</h1>
    <p class="b-hero-sub">${total > 0 ? `${total.toLocaleString('en-CA')}+ listings` : 'Thousands of listings'} from Kijiji &amp; Facebook Marketplace</p>
    <form class="b-hero-search" method="GET" action="/browse">
      <input type="text" name="make" value="${esc(f.make)}" placeholder="Search by make, e.g. Toyota, Ford…" autocomplete="off">
      <button type="submit">Search</button>
    </form>
  </section>

  <div class="b-wrap">
    <div class="b-layout">

      <!-- Sidebar filters -->
      <aside class="b-sidebar" id="filterSidebar">
        <div class="b-sidebar-title">
          <span>Filters</span>
          ${hasFilters ? `<a href="/browse" style="font-size:11px;color:#9ca3af;font-weight:600;letter-spacing:.5px;">Clear all</a>` : ''}
        </div>
        <form method="GET" action="/browse" id="filterForm">

          <div class="b-field">
            <label>Make (${opts.makes.length})</label>
            <select name="make">
              <option value="">All Makes</option>
              ${makeOptions}
            </select>
          </div>

          <div class="b-field">
            <label>Model</label>
            <input type="text" name="model" value="${esc(f.model)}" placeholder="e.g. Civic, F-150&#8230;">
          </div>

          <div class="b-field">
            <label>Year</label>
            <div class="b-field-row">
              <input type="number" name="min_year" value="${f.min_year || ''}" placeholder="Min" min="1980" max="2030">
              <input type="number" name="max_year" value="${f.max_year || ''}" placeholder="Max" min="1980" max="2030">
            </div>
          </div>

          <div class="b-field">
            <label>Price ($)</label>
            <div class="b-field-row">
              <input type="number" name="min_price" value="${f.min_price || ''}" placeholder="Min" min="0">
              <input type="number" name="max_price" value="${f.max_price || ''}" placeholder="Max" min="0">
            </div>
          </div>

          <div class="b-field">
            <label>Body Type</label>
            <select name="body_type">
              <option value="">All Types</option>
              ${bodyOptions}
            </select>
          </div>

          <div class="b-field">
            <label>Region</label>
            <select name="region">
              <option value="">All Ontario</option>
              ${regionOptions}
            </select>
          </div>

          <div class="b-field">
            <label>City</label>
            <select name="city">
              <option value="">All Cities</option>
              ${cityOptions}
            </select>
          </div>

          <div class="b-field">
            <label>Source</label>
            <div class="b-source-btns">
              <button type="button" class="b-source-btn${srcActive('')}"             onclick="setSource(this,'')">All</button>
              <button type="button" class="b-source-btn${srcActive('kijiji-ca')}"    onclick="setSource(this,'kijiji-ca')">Kijiji</button>
              <button type="button" class="b-source-btn${srcActive('facebook-mp-ca')}" onclick="setSource(this,'facebook-mp-ca')">Facebook</button>
            </div>
            <input type="hidden" name="source" id="sourceInput" value="${esc(f.source)}">
          </div>

          <input type="hidden" name="sort" value="${esc(f.sort)}">

          <button type="submit" class="b-apply">Apply Filters</button>
          ${hasFilters ? `<a class="b-clear" href="/browse">&#x2715; Clear all filters</a>` : ''}
        </form>
      </aside>

      <!-- Results -->
      <main>
        <!-- Mobile filter toggle -->
        <button class="b-filter-toggle" onclick="toggleFilters()">
          &#9776; Filters${activeFilterCount > 0 ? ` (${activeFilterCount})` : ''}
        </button>

        <!-- Active filter chips -->
        ${chips.length > 0 ? `<div class="b-chips">${chips.join('')}</div>` : ''}

        <!-- Results count bar + sort -->
        <div class="b-results-bar">
          <div class="b-results-count">
            ${total > 0 ? `<strong>${total.toLocaleString('en-CA')}</strong> used cars for sale near Toronto, ON` : 'No results'}
          </div>
          <form method="GET" action="/browse" id="sortForm" style="display:flex;align-items:center;gap:8px;">
            ${f.make       ? `<input type="hidden" name="make"       value="${esc(f.make)}">` : ''}
            ${f.model      ? `<input type="hidden" name="model"      value="${esc(f.model)}">` : ''}
            ${f.min_year   ? `<input type="hidden" name="min_year"   value="${f.min_year}">` : ''}
            ${f.max_year   ? `<input type="hidden" name="max_year"   value="${f.max_year}">` : ''}
            ${f.min_price  ? `<input type="hidden" name="min_price"  value="${f.min_price}">` : ''}
            ${f.max_price  ? `<input type="hidden" name="max_price"  value="${f.max_price}">` : ''}
            ${f.body_type  ? `<input type="hidden" name="body_type"  value="${esc(f.body_type)}">` : ''}
            ${f.city       ? `<input type="hidden" name="city"       value="${esc(f.city)}">` : ''}
            ${f.source     ? `<input type="hidden" name="source"     value="${esc(f.source)}">` : ''}
            <label for="sortSelect" style="font-size:12px;color:#9ca3af;white-space:nowrap;">Sort by</label>
            <select id="sortSelect" name="sort" class="b-sort-select" onchange="document.getElementById('sortForm').submit()">
              ${sortSelectOptions}
            </select>
          </form>
        </div>

        <!-- Cards grid -->
        <div class="b-grid">
          ${cards}
        </div>

        <!-- Pagination -->
        ${pager}
      </main>

    </div>
  </div>

  <script>
    function setSource(btn, val) {
      document.getElementById('sourceInput').value = val;
      document.querySelectorAll('.b-source-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
    }
    function toggleFilters() {
      var sidebar = document.getElementById('filterSidebar');
      sidebar.classList.toggle('open');
    }
  </script>
</body></html>`;
}

// ── Alerts HTML ────────────────────────────────────────────

function buildAlertsHtml(opts: { success?: boolean; error?: string; makes: string[] }): string {
  const makeOptions = opts.makes.map(m =>
    `<option value="${m.replace(/"/g, '&quot;')}">${m}</option>`
  ).join('');

  const successHtml = opts.success
    ? `<div class="b-success">✅ You're signed up! We'll email you when a match is found.</div>`
    : '';
  const errorHtml = opts.error
    ? `<div class="b-error">⚠ ${opts.error}</div>`
    : '';

  return `<!DOCTYPE html>
<html lang="en"><head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Set a Car Alert — Aven</title>
  <style>${BROWSE_CSS}</style>
</head><body>

  <div class="b-topbar">
    <div>
      <span class="b-logo">AVEN</span><span style="font-size:13px;color:#888;margin-left:8px;font-weight:400;">Browse Cars</span>
    </div>
    <div class="b-nav">
      <a href="/browse">Browse</a>
      <a href="/alerts" class="b-btn">🔔 Set Alert</a>
    </div>
  </div>

  <div class="b-center-wrap">
    <div class="b-form-card">
      <h1>🔔 Set a Car Alert</h1>
      <p class="b-subtitle">We'll email you when a matching listing goes live.</p>

      ${successHtml}${errorHtml}

      ${!opts.success ? `
      <form method="POST" action="/api/alerts">
        <div class="b-field">
          <label>Email address <span style="color:#e74c3c">*</span></label>
          <input type="email" name="email" required placeholder="you@example.com">
        </div>
        <div class="b-field">
          <label>Make (optional)</label>
          <select name="make">
            <option value="">Any make</option>
            ${makeOptions}
          </select>
        </div>
        <div class="b-field">
          <label>Max Price (optional)</label>
          <input type="number" name="max_price" placeholder="e.g. 25000" min="0">
        </div>
        <div class="b-field">
          <label>Minimum Year (optional)</label>
          <input type="number" name="min_year" placeholder="e.g. 2015" min="1980" max="2030" value="2010">
        </div>
        <button type="submit" class="b-submit">Subscribe to Alerts</button>
      </form>` : `
      <div style="text-align:center;margin-top:20px;">
        <a href="/browse" style="color:#16a34a;font-weight:600;font-size:14px;">← Browse listings</a>
      </div>`}
    </div>
  </div>
</body></html>`;
}

// ── Unsubscribe HTML ───────────────────────────────────────

function buildUnsubscribeHtml(success: boolean, error?: string): string {
  return `<!DOCTYPE html>
<html lang="en"><head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Unsubscribe — Aven</title>
  <style>${BROWSE_CSS}</style>
</head><body>

  <div class="b-topbar">
    <div>
      <span class="b-logo">AVEN</span><span style="font-size:13px;color:#888;margin-left:8px;font-weight:400;">Browse Cars</span>
    </div>
    <div class="b-nav">
      <a href="/browse">Browse</a>
      <a href="/alerts" class="b-btn">🔔 Set Alert</a>
    </div>
  </div>

  <div class="b-center-wrap">
    <div class="b-form-card b-unsub-card">
      ${success
        ? `<div class="b-icon">✅</div>
           <h1>You've been unsubscribed.</h1>
           <p>You won't receive any more alerts from Aven.</p>
           <p style="margin-top:16px;"><a href="/browse">← Browse listings</a></p>`
        : `<div class="b-icon">⚠</div>
           <h1>Something went wrong</h1>
           <p>${error ?? 'Invalid or missing unsubscribe link.'}</p>
           <p style="margin-top:16px;"><a href="/browse">← Browse listings</a></p>`}
    </div>
  </div>
</body></html>`;
}

// ── Homepage HTML ──────────────────────────────────────────

function buildHomeHtml(hs: HomepageStats): string {
  const totalStr = hs.total_active.toLocaleString('en-CA');

  // Make dropdown for hero filter
  const makeOptions = hs.makes.map(m =>
    `<option value="${m.replace(/"/g, '&quot;')}">${m}</option>`
  ).join('');

  // Body type dropdown for hero filter
  const bodyTypeOptions = hs.body_types.map(b =>
    `<option value="${b.replace(/"/g, '&quot;')}">${b}</option>`
  ).join('');

  // Category cards
  const catCount = (bodyKey: string): string => {
    const count = hs.body_counts[bodyKey];
    return count != null ? `${count.toLocaleString('en-CA')} available` : 'Browse all';
  };
  const categories = [
    { emoji: '🔋', label: 'Fuel Efficient', sub: 'Save on gas', href: '/browse?sort=price_asc' },
    { emoji: '🛡️', label: 'Low Mileage', sub: catCount(''), href: '/browse?sort=mileage_asc' },
    { emoji: '💰', label: 'Best Deals', sub: 'Lowest prices first', href: '/browse?sort=price_asc' },
    { emoji: '🚗', label: 'SUVs & Crossovers', sub: catCount('SUV/Crossover'), href: '/browse?body_type=SUV%2FCrossover' },
    { emoji: '🛻', label: 'Trucks', sub: catCount('Pickup Truck'), href: '/browse?body_type=Pickup+Truck' },
    { emoji: '🏎️', label: 'Sports Cars', sub: catCount('Coupe'), href: '/browse?body_type=Coupe' },
  ];
  const catCards = categories.map(c => `
    <a class="h-cat-card" href="${c.href}">
      <span class="h-cat-emoji">${c.emoji}</span>
      <span class="h-cat-label">${c.label}</span>
      <span class="h-cat-sub">${c.sub}</span>
    </a>`).join('');

  // Make pills
  const makePills = [
    'Toyota','Honda','Ford','Chevrolet','BMW','Mercedes-Benz',
    'Hyundai','Kia','Nissan','Volkswagen','Audi','Mazda','Jeep','Ram','Subaru',
  ].map(m => `<a class="h-make-pill" href="/browse?make=${encodeURIComponent(m)}">${m}</a>`).join('');

  // Body type quick pills for hero
  const bodyPills = ['SUV','Sedan','Truck','Hatchback','Van','Coupe'].map(b =>
    `<a class="h-body-pill" href="/browse?body_type=${encodeURIComponent(b)}">${b}</a>`
  ).join('');

  // Recent listing cards
  const recentCards = hs.recent.map(r => {
    const priceHtml = r.price != null
      ? `<div class="b-card-price">$${Number(r.price).toLocaleString('en-CA')}</div>`
      : `<div class="b-card-price no-price">Price not listed</div>`;
    const mileage  = r.mileage_km != null ? `${Number(r.mileage_km).toLocaleString('en-CA')} km` : null;
    const location = [r.city, r.province].filter(Boolean).join(', ');

    const imgContent = r.photo_urls?.[0]
      ? `<img src="${r.photo_urls[0]}" loading="lazy" alt="${r.year} ${r.make} ${r.model}" onerror="this.parentElement.innerHTML='<div class=\\'b-no-img\\'>&#x1F697;</div>'">`
      : `<div class="b-no-img" style="background:#f3f4f6;font-size:36px;display:flex;align-items:center;justify-content:center;width:100%;height:100%;">&#x1F697;</div>`;

    const srcPillCls = r.source_id === 'kijiji-ca' ? 'b-src-pill-kj' : r.source_id === 'facebook-mp-ca' ? 'b-src-pill-fb' : 'b-src-pill-other';
    const srcLabel   = r.source_id === 'kijiji-ca' ? 'Kijiji' : r.source_id === 'facebook-mp-ca' ? 'Facebook Marketplace' : r.source_id;
    const viewLabel  = r.source_id === 'kijiji-ca' ? 'View on Kijiji →' : r.source_id === 'facebook-mp-ca' ? 'View on Facebook →' : 'View Listing →';

    // Data richness check
    const hasAllKey = r.price != null && r.mileage_km != null && r.colour_exterior != null && r.body_type != null;
    const showVerified = r.confidence_score >= 90 && r.price != null && hasAllKey;
    const showLimited  = r.price == null || r.mileage_km == null;

    const verifiedBadge = showVerified ? `<div class="b-verified-badge">&#10003; Verified</div>` : '';
    const limitedTag    = showLimited  ? `<div class="b-limited-info">Limited info</div>` : '';

    return `
    <a class="b-card" href="${r.source_url}" target="_blank" rel="noopener noreferrer">
      <div class="b-card-img">${imgContent}</div>
      <div class="b-card-body">
        <div class="b-card-title">${r.year} ${r.make} ${r.model}${r.trim ? ' ' + r.trim : ''}</div>
        ${r.trim || r.drivetrain ? `<div class="b-card-trim">${[r.trim, r.drivetrain].filter(Boolean).join(' · ')}</div>` : ''}
        ${priceHtml}
        <div class="b-card-meta">
          ${mileage ? `<span>${mileage}</span><span class="b-card-meta-sep">·</span>` : ''}
          <span>${location}</span>
        </div>
        ${verifiedBadge}${limitedTag}
        <span class="b-src-pill ${srcPillCls}">${srcLabel}</span>
        <div class="b-card-view-link">${viewLabel}</div>
      </div>
    </a>`;
  }).join('');

  return `<!DOCTYPE html>
<html lang="en"><head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Aven — Find Your Next Used Car in the GTA</title>
  <style>${HOME_CSS}</style>
</head><body>

  <!-- Topbar -->
  <div class="h-topbar">
    <div>
      <span class="h-logo">AVEN <span>GTA Car Search</span></span>
    </div>
    <div class="h-nav">
      <a href="/home">Home</a>
      <a href="/browse">Browse</a>
      <a href="/alerts" class="h-btn">Set Alert</a>
    </div>
  </div>

  <!-- Hero: split screen -->
  <section class="h-hero">

    <!-- LEFT: Traditional filter panel -->
    <div class="h-hero-left">
      <h1 class="h-headline">Find your next car</h1>
      <p class="h-sub">Search ${totalStr}+ used car listings across the GTA and Ontario.</p>

      <form method="GET" action="/browse">
        <div class="h-field">
          <label>Make</label>
          <select name="make">
            <option value="">All Makes</option>
            ${makeOptions}
          </select>
        </div>
        <div class="h-field">
          <label>Model</label>
          <input type="text" name="model" placeholder="e.g. Civic, F-150&#8230;" autocomplete="off">
        </div>
        <div class="h-field">
          <label>Year</label>
          <div class="h-field-row">
            <input type="number" name="min_year" placeholder="From" min="1980" max="2030">
            <input type="number" name="max_year" placeholder="To"   min="1980" max="2030">
          </div>
        </div>
        <div class="h-field">
          <label>Price ($)</label>
          <div class="h-field-row">
            <input type="number" name="min_price" placeholder="Min" min="0">
            <input type="number" name="max_price" placeholder="Max" min="0">
          </div>
        </div>
        <div class="h-field">
          <label>Body Type</label>
          <select name="body_type">
            <option value="">All Types</option>
            ${bodyTypeOptions}
          </select>
        </div>
        <button type="submit" class="h-search-btn">Search</button>
      </form>

      <div class="h-body-pills">
        ${bodyPills}
      </div>

      <!-- OR divider -->
      <div class="h-hero-divider">
        <div class="h-hero-or">OR</div>
      </div>
    </div>

    <!-- RIGHT: AI natural language search -->
    <div class="h-hero-right">
      <h2 class="h-ai-headline">Or just tell us what you need</h2>
      <p class="h-ai-sub">As casual as you want — "fuel efficient car under $20k" or "reliable SUV for family"</p>
      <form method="GET" action="/browse" id="aiForm">
        <textarea
          class="h-ai-textarea"
          name="make"
          id="aiInput"
          placeholder="e.g. I need a reliable SUV for my family, under $30,000, low mileage&#8230;"
          rows="4"
        ></textarea>
        <button type="submit" class="h-ai-btn">Find my car &#8594;</button>
        <p class="h-ai-note">AI-powered search coming soon &mdash; currently searches by keyword</p>
      </form>
    </div>

  </section>

  <hr class="h-divider">

  <!-- Stats bar -->
  <div class="h-stats-bar">
    <span>${totalStr}</span> listings &nbsp;&middot;&nbsp; Updated live &nbsp;&middot;&nbsp; Kijiji + Facebook Marketplace &nbsp;&middot;&nbsp; GTA &amp; Ontario
  </div>

  <hr class="h-divider">

  <!-- Section B: Browse by category -->
  <div class="h-section">
    <h2 class="h-section-title">Browse by what matters to you</h2>
    <div class="h-cats">
      ${catCards}
    </div>
  </div>

  <hr class="h-divider">

  <!-- Section C: Explore by make -->
  <div class="h-section" style="padding-top:40px;padding-bottom:40px;">
    <h2 class="h-section-title">Explore by make</h2>
    <div class="h-makes-row">
      ${makePills}
    </div>
  </div>

  <hr class="h-divider">

  <!-- Section E: Recent listings preview -->
  <div class="h-section">
    <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:24px;">
      <h2 class="h-section-title" style="margin-bottom:0;">Just listed</h2>
      <a class="h-see-all" href="/browse">See all &#8594;</a>
    </div>
    <div class="h-recent-grid">
      ${recentCards || '<p style="color:#9ca3af;font-size:14px;">No listings yet — check back soon.</p>'}
    </div>
  </div>

  <!-- Footer -->
  <footer class="h-footer">
    <span>Aven</span> &nbsp;&middot;&nbsp; GTA Used Car Search &nbsp;&middot;&nbsp; Data from Kijiji Canada &amp; Facebook Marketplace
  </footer>

</body></html>`;
}

// ── Full Review Queue page ────────────────────────────────

const REVIEW_PAGE_SIZE = 50;

interface ReviewQueueRow {
  queue_id: string;
  listing_id: string;
  make: string;
  model: string;
  year: number;
  source_id: string;
  confidence_score: number;
  reason: string;
  created_at: string;
  source_url: string;
  photo_urls: string[] | null;
  vin: string | null;
  price: number | null;
  mileage_km: number | null;
  city: string;
  province: string | null;
  rerun_count: number;
}

async function fetchReviewQueue(page: number, source: string): Promise<{ rows: ReviewQueueRow[]; total: number }> {
  const offset = (page - 1) * REVIEW_PAGE_SIZE;
  const conditions = [`rq.decision IS NULL`];
  const params: unknown[] = [];

  if (source) {
    params.push(source);
    conditions.push(`l.source_id = $${params.length}`);
  }

  const where = conditions.join(' AND ');

  params.push(REVIEW_PAGE_SIZE, offset);
  const limitIdx  = params.length - 1;
  const offsetIdx = params.length;

  const client = await pool.connect();
  try {
    const [dataRes, countRes] = await Promise.all([
      client.query(`
        SELECT rq.id AS queue_id, rq.listing_id,
          l.make, l.model, l.year, l.source_id,
          rq.confidence_score, rq.reason,
          TO_CHAR(rq.created_at, 'HH24:MI  DD Mon') AS created_at,
          l.source_url, l.photo_urls, l.vin,
          l.price, l.mileage_km, l.city, l.province,
          COALESCE(rq.rerun_count, 0) AS rerun_count
        FROM public.review_queue rq
        JOIN public.listings l ON l.id = rq.listing_id
        WHERE ${where}
        ORDER BY rq.created_at ASC
        LIMIT $${limitIdx} OFFSET $${offsetIdx}
      `, params),
      client.query(`
        SELECT COUNT(*) AS total
        FROM public.review_queue rq
        JOIN public.listings l ON l.id = rq.listing_id
        WHERE ${where}
      `, params.slice(0, params.length - 2)),
    ]);
    return { rows: dataRes.rows as ReviewQueueRow[], total: Number(countRes.rows[0].total) };
  } finally {
    client.release();
  }
}

function buildReviewHtml(rows: ReviewQueueRow[], total: number, page: number, source: string): string {
  const totalPages = Math.max(1, Math.ceil(total / REVIEW_PAGE_SIZE));
  const from = total === 0 ? 0 : (page - 1) * REVIEW_PAGE_SIZE + 1;
  const to   = Math.min(page * REVIEW_PAGE_SIZE, total);

  const sources = ['', 'kijiji-ca', 'facebook-mp-ca'];

  const sourceFilter = sources.map(s => {
    const label = s === '' ? 'ALL' : s === 'kijiji-ca' ? 'KIJIJI' : 'FACEBOOK';
    const active = s === source;
    return `<a href="/review?source=${s}&page=1" style="padding:4px 10px;border-radius:2px;font-size:10px;letter-spacing:1px;background:${active ? '#c0392b' : '#1a1a1a'};color:${active ? '#fff' : '#888'};border:1px solid ${active ? '#c0392b' : '#333'};text-decoration:none;">${label}</a>`;
  }).join('');

  const rows_html = rows.map(r => {
    const price   = r.price ? `$${Number(r.price).toLocaleString('en-CA')}` : '—';
    const mileage = r.mileage_km ? `${Number(r.mileage_km).toLocaleString()} km` : '—';
    const sc      = r.confidence_score;
    const scColor = sc >= 70 ? '#27ae60' : sc >= 50 ? '#e67e22' : '#e74c3c';
    const thumb   = r.photo_urls?.[0]
      ? `<img style="width:64px;height:48px;object-fit:cover;border-radius:2px;background:#111;" src="${r.photo_urls[0]}" loading="lazy" onerror="this.style.display='none'">`
      : `<div style="width:64px;height:48px;background:#111;border-radius:2px;"></div>`;
    const srcColor = r.source_id === 'kijiji-ca' ? '#2980b9' : '#4a9eff';
    const srcLabel = r.source_id === 'kijiji-ca' ? 'KJ' : 'FB';
    const reruns   = r.rerun_count > 0 ? `<span style="color:#888;font-size:9px;margin-left:4px">(×${r.rerun_count})</span>` : '';
    return `
    <tr data-queue-row="${r.queue_id}" style="border-bottom:1px solid #1a1a1a;">
      <td style="padding:8px 4px;width:28px;"><input type="checkbox" class="rq-cb" value="${r.queue_id}" onchange="updateBulkBar()"></td>
      <td style="padding:8px 6px;width:72px;">${thumb}</td>
      <td style="padding:8px 6px;">
        <span style="background:${srcColor}22;color:${srcColor};border:1px solid ${srcColor}44;font-size:9px;padding:1px 5px;border-radius:2px;margin-right:6px;">${srcLabel}</span>
        <a href="${r.source_url}" target="_blank" style="color:#e8e0d0;font-size:12px;">${r.year} ${r.make} ${r.model}</a>
        ${reruns}
      </td>
      <td style="padding:8px 6px;white-space:nowrap;"><span style="color:${scColor}">${sc}</span></td>
      <td style="padding:8px 6px;color:#888;font-size:11px;max-width:240px;">${r.reason ?? '—'}</td>
      <td style="padding:8px 6px;color:#aaa;font-size:11px;white-space:nowrap;">${price}</td>
      <td style="padding:8px 6px;color:#666;font-size:11px;white-space:nowrap;">${mileage}</td>
      <td style="padding:8px 6px;color:#555;font-size:10px;white-space:nowrap;">${r.city}${r.province ? ', ' + r.province : ''}</td>
      <td style="padding:8px 6px;color:#444;font-size:10px;white-space:nowrap;">${r.created_at}</td>
      <td style="padding:8px 6px;white-space:nowrap;">
        <button class="btn btn-approve"   data-qid="${r.queue_id}" data-action="approve"   onclick="reviewAction('${r.queue_id}','approve')">APPROVE</button>
        <button class="btn btn-reanalyse" data-qid="${r.queue_id}" data-action="reanalyse" onclick="reviewAction('${r.queue_id}','reanalyse')" style="margin-left:4px">RE-RUN AI</button>
        <button class="btn btn-remove"    data-qid="${r.queue_id}" data-action="remove"    onclick="reviewAction('${r.queue_id}','remove')" style="margin-left:4px">REMOVE</button>
      </td>
    </tr>`;
  }).join('');

  const prevHref = page > 1          ? `/review?source=${source}&page=${page - 1}` : '#';
  const nextHref = page < totalPages  ? `/review?source=${source}&page=${page + 1}` : '#';
  const prevStyle = page <= 1         ? 'opacity:.3;pointer-events:none;' : '';
  const nextStyle = page >= totalPages ? 'opacity:.3;pointer-events:none;' : '';

  return `<!DOCTYPE html>
<html lang="en"><head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Aven — Review Queue (${total})</title>
  <style>${SHARED_CSS}
    .btn { display:inline-block;padding:3px 10px;border-radius:2px;font-size:10px;letter-spacing:1px;cursor:pointer;border:none;font-family:'DM Mono',monospace; }
    .btn-remove { background:#2b0d0d;color:#e74c3c;border:1px solid #4a1a1a; }
    .btn-remove:hover { background:#3b1010; }
    .btn-approve { background:#0b2010;color:#27ae60;border:1px solid #1a4a2a; }
    .btn-approve:hover { background:#0e2a16; }
    .btn-reanalyse { background:#0d1f2b;color:#2980b9;border:1px solid #1a3a4a; }
    .btn-reanalyse:hover { background:#112a3b; }
    table { border-collapse:collapse;width:100%; }
    th { font-size:10px;letter-spacing:1px;color:#555;padding:6px;text-align:left;border-bottom:1px solid #2a1a1a;white-space:nowrap; }
    tr.processing td { color:#4a9eff !important;background:#08141f !important; }
    .pager a { color:#888;font-size:11px;letter-spacing:1px;padding:4px 10px;border:1px solid #2a1a1a;border-radius:2px;text-decoration:none; }
    .pager a:hover { border-color:#555; }
  </style>
</head><body>
  <div class="topbar">
    <div class="logo">AVEN<span>REVIEW QUEUE</span></div>
    <div style="display:flex;align-items:center;gap:12px;">
      <a href="/" style="font-size:11px;color:#555;letter-spacing:1px;border:1px solid #2a1a1a;padding:4px 10px;border-radius:2px;">← DASHBOARD</a>
      <span style="font-size:11px;color:#555;letter-spacing:1px;">${total} PENDING</span>
    </div>
  </div>

  <!-- Filters -->
  <div style="display:flex;gap:8px;align-items:center;margin-bottom:20px;flex-wrap:wrap;">
    <span style="font-size:10px;color:#555;letter-spacing:1px;">SOURCE</span>
    ${sourceFilter}
    <span style="font-size:10px;color:#555;margin-left:12px;">SHOWING ${from}–${to} OF ${total}</span>
  </div>

  <!-- Re-run all + bulk controls -->
  <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap;">
    <button id="rerun-all-btn" class="btn btn-reanalyse" onclick="rerunAllPending()" style="padding:5px 14px;font-size:11px;">⟳ RE-RUN ALL PENDING</button>
    <span id="rerun-all-progress" style="font-size:11px;color:#555;"></span>
  </div>
  <div id="bulk-bar" style="display:none;background:#0d1f2b;border:1px solid #1a3a4a;border-radius:3px;padding:10px 14px;margin-bottom:12px;align-items:center;gap:12px;">
    <span id="bulk-count" style="font-size:12px;color:#4a9eff;letter-spacing:1px;">0 SELECTED</span>
    <button id="bulk-rerun-btn" class="btn btn-reanalyse" onclick="bulkReanalyse()" style="padding:5px 14px;font-size:11px;">⟳ RE-RUN AI ON SELECTED</button>
    <button class="btn" onclick="clearSelection()" style="background:#1a1a1a;color:#888;border:1px solid #333;padding:5px 10px;font-size:11px;">✕ DESELECT ALL</button>
    <div id="bulk-progress" style="flex:1;font-size:11px;color:#888;"></div>
  </div>

  <!-- Table -->
  <table>
    <thead>
      <tr>
        <th style="width:28px;"><input type="checkbox" id="select-all" onchange="document.querySelectorAll('.rq-cb').forEach(c=>{c.checked=this.checked});updateBulkBar()"></th>
        <th style="width:72px;">PHOTO</th>
        <th>LISTING</th>
        <th>SCORE</th>
        <th>REASON</th>
        <th>PRICE</th>
        <th>MILEAGE</th>
        <th>LOCATION</th>
        <th>QUEUED</th>
        <th>ACTIONS</th>
      </tr>
    </thead>
    <tbody>
      ${rows_html || '<tr><td colspan="10" style="padding:40px;text-align:center;color:#555;font-size:12px;letter-spacing:1px;">NO PENDING ITEMS</td></tr>'}
    </tbody>
  </table>

  <!-- Pagination -->
  <div class="pager" style="display:flex;justify-content:space-between;align-items:center;margin-top:20px;padding-top:16px;border-top:1px solid #1a1a1a;">
    <a href="${prevHref}" style="${prevStyle}">← PREV</a>
    <span style="font-size:10px;color:#555;letter-spacing:1px;">PAGE ${page} / ${totalPages}</span>
    <a href="${nextHref}" style="${nextStyle}">NEXT →</a>
  </div>

  <script>
    ${SHARED_JS}
  </script>
</body></html>`;
}

// ── Express routes ────────────────────────────────────────

// Dashboard
app.get('/', async (_req, res) => {
  try {
    const stats = await fetchStats();
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(buildDashboardHtml(stats));
  } catch (err) {
    res.status(500).send(`<pre style="background:#0d0d0d;color:#e74c3c;padding:24px;font-family:monospace;">Error: ${(err as Error).message}</pre>`);
  }
});

// Public homepage
app.get('/home', async (_req, res) => {
  try {
    const hs = await fetchHomepageStats();
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(buildHomeHtml(hs));
  } catch (err) {
    res.status(500).send(`<pre style="padding:24px;font-family:monospace;color:red;">Error: ${(err as Error).message}</pre>`);
  }
});

// All listings
app.get('/listings', async (req, res) => {
  try {
    const page   = Math.max(1, parseInt(String(req.query.page ?? '1'), 10));
    const source = String(req.query.source ?? '');
    const status = String(req.query.status ?? '');
    const { rows, total } = await fetchAllListings(page, source, status);
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(buildAllListingsHtml(rows, total, page, source, status));
  } catch (err) {
    res.status(500).send(`<pre style="background:#0d0d0d;color:#e74c3c;padding:24px;font-family:monospace;">Error: ${(err as Error).message}</pre>`);
  }
});

// Full review queue page
app.get('/review', async (req, res) => {
  try {
    const page   = Math.max(1, parseInt(String(req.query.page   ?? '1'), 10));
    const source = String(req.query.source ?? '');
    const { rows, total } = await fetchReviewQueue(page, source);
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(buildReviewHtml(rows, total, page, source));
  } catch (err) {
    res.status(500).send(`<pre style="background:#0d0d0d;color:#e74c3c;padding:24px;font-family:monospace;">Error: ${(err as Error).message}</pre>`);
  }
});

// Review action: remove (reject the listing, close the queue item)
app.post('/api/review/:queueId/remove', async (req, res) => {
  const { queueId } = req.params;
  if (!isValidUuid(queueId)) return res.status(400).json({ ok: false, error: 'Invalid queue ID format' });
  let client: import('pg').PoolClient | null = null;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    // Grab listing_id first, then do both updates in one transaction
    const { rows } = await client.query(
      `SELECT listing_id FROM public.review_queue WHERE id = $1`, [queueId]
    );
    if (!rows[0]) { await client.query('ROLLBACK'); return res.status(404).json({ ok: false, error: 'Queue item not found' }); }
    const listingId = rows[0].listing_id;
    // Archive full listing row to deleted_listings before removing (48h auto-purge)
    // Wrapped in try/catch — if table doesn't exist the delete still proceeds
    try {
      await client.query(`
        INSERT INTO public.deleted_listings (id, original_data)
        SELECT id, row_to_json(listings.*)::jsonb FROM public.listings WHERE id = $1
        ON CONFLICT DO NOTHING
      `, [listingId]);
    } catch (archErr) {
      console.warn('[review/remove] deleted_listings archive skipped:', (archErr as Error).message);
    }
    // Clear FK dependents
    try { await client.query(`DELETE FROM public.extraction_log WHERE listing_id = $1`, [listingId]); } catch {}
    await client.query(`DELETE FROM public.review_queue WHERE listing_id = $1`, [listingId]);
    await client.query(`DELETE FROM public.listings WHERE id = $1`, [listingId]);
    await client.query('COMMIT');
    res.json({ ok: true });
  } catch (err) {
    console.error('[review/remove] error for queue=%s:', queueId, err);
    if (client) await client.query('ROLLBACK').catch(() => {});
    res.status(500).json({ ok: false, error: (err as Error).message });
  } finally {
    if (client) client.release();
  }
});

// Review action: reanalyse (delete the listing so the pipeline re-processes it on next run)
app.post('/api/review/:queueId/reanalyse', async (req, res) => {
  const { queueId } = req.params;
  if (!isValidUuid(queueId)) return res.status(400).json({ ok: false, error: 'Invalid queue ID format' });
  let client: import('pg').PoolClient | null = null;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    const { rows } = await client.query(
      `SELECT listing_id FROM public.review_queue WHERE id = $1`, [queueId]
    );
    if (!rows[0]) { await client.query('ROLLBACK'); return res.status(404).json({ ok: false, error: 'Queue item not found' }); }
    const listingId = rows[0].listing_id;
    // Clear FK dependents before deleting the listing
    // extraction_log may not exist in all environments — tolerate gracefully
    try {
      await client.query(`DELETE FROM public.extraction_log WHERE listing_id = $1`, [listingId]);
    } catch (elErr) {
      console.warn('[review/reanalyse] extraction_log delete skipped:', (elErr as Error).message);
    }
    await client.query(`DELETE FROM public.review_queue WHERE listing_id = $1`, [listingId]);
    await client.query(`DELETE FROM public.listings WHERE id = $1`, [listingId]);
    await client.query('COMMIT');
    res.json({ ok: true, message: 'Listing deleted — pipeline will re-process on next run' });
  } catch (err) {
    console.error('[review/reanalyse] error for queue=%s:', queueId, err);
    if (client) await client.query('ROLLBACK').catch(() => {});
    res.status(500).json({ ok: false, error: (err as Error).message });
  } finally {
    if (client) client.release();
  }
});

// Review action: approve (publish the listing, mark queue item done)
app.post('/api/review/:queueId/approve', async (req, res) => {
  const { queueId } = req.params;
  if (!isValidUuid(queueId)) return res.status(400).json({ ok: false, error: 'Invalid queue ID format' });
  let client: import('pg').PoolClient | null = null;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    const { rows } = await client.query(
      `SELECT listing_id FROM public.review_queue WHERE id = $1`, [queueId]
    );
    if (!rows[0]) { await client.query('ROLLBACK'); return res.status(404).json({ ok: false, error: 'Queue item not found' }); }
    const listingId = rows[0].listing_id;
    await client.query(
      `UPDATE public.listings SET status = 'active', needs_review = false, updated_at = NOW() WHERE id = $1`,
      [listingId]
    );
    await client.query(
      `UPDATE public.review_queue SET decision = 'approved', reviewed_at = NOW() WHERE id = $1`,
      [queueId]
    );
    await client.query('COMMIT');
    res.json({ ok: true });
  } catch (err) {
    console.error('[review/approve] error for queue=%s:', queueId, err);
    if (client) await client.query('ROLLBACK').catch(() => {});
    res.status(500).json({ ok: false, error: (err as Error).message });
  } finally {
    if (client) client.release();
  }
});

// Bulk reanalyse — SSE stream, one event per listing
app.post('/api/review/bulk-reanalyse', async (req, res) => {
  const { queueIds } = req.body as { queueIds?: string[] };
  if (!Array.isArray(queueIds) || queueIds.length === 0) {
    return res.status(400).json({ ok: false, error: 'No queue IDs provided' });
  }
  const ids = queueIds.slice(0, 50); // cap at 50 per batch

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (data: object) => res.write(`data: ${JSON.stringify(data)}\n\n`);
  send({ total: ids.length, done: 0 });

  let done = 0;
  for (const queueId of ids) {
    if (!isValidUuid(queueId)) {
      done++;
      send({ total: ids.length, done, queueId, ok: false, error: 'Invalid queue ID format' });
      continue;
    }
    let client: import('pg').PoolClient | null = null;
    try {
      client = await pool.connect();
      await client.query('BEGIN');
      const { rows } = await client.query(
        `SELECT listing_id FROM public.review_queue WHERE id = $1`, [queueId]
      );
      if (rows[0]) {
        const listingId = rows[0].listing_id;
        try {
          await client.query(`DELETE FROM public.extraction_log WHERE listing_id = $1`, [listingId]);
        } catch (elErr) {
          console.warn('[bulk-reanalyse] extraction_log delete skipped:', (elErr as Error).message);
        }
        await client.query(`DELETE FROM public.review_queue WHERE listing_id = $1`, [listingId]);
        await client.query(`DELETE FROM public.listings WHERE id = $1`, [listingId]);
        await client.query('COMMIT');
      } else {
        await client.query('ROLLBACK');
      }
      done++;
      send({ total: ids.length, done, queueId, ok: true });
    } catch (err) {
      console.error('[bulk-reanalyse] error for queue=%s:', queueId, err);
      if (client) await client.query('ROLLBACK').catch(() => {});
      done++;
      send({ total: ids.length, done, queueId, ok: false, error: (err as Error).message });
    } finally {
      if (client) client.release();
    }
  }

  send({ total: ids.length, done, finished: true });
  res.end();
});

// Re-run ALL pending items in review queue (SSE stream, no cap)
app.post('/api/review/bulk-reanalyse-all', async (req, res) => {
  // Fetch all undecided queue IDs
  let allIds: string[];
  try {
    const { rows } = await pool.query(
      `SELECT id FROM public.review_queue WHERE decision IS NULL ORDER BY created_at ASC`
    );
    allIds = rows.map((r: any) => r.id as string);
  } catch (err) {
    return res.status(500).json({ ok: false, error: (err as Error).message });
  }

  if (allIds.length === 0) {
    return res.json({ ok: true, message: 'Queue is empty' });
  }

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (data: object) => res.write(`data: ${JSON.stringify(data)}\n\n`);
  send({ total: allIds.length, done: 0 });

  let done = 0;
  for (const queueId of allIds) {
    let client: import('pg').PoolClient | null = null;
    try {
      client = await pool.connect();
      await client.query('BEGIN');
      const { rows } = await client.query(
        `SELECT listing_id FROM public.review_queue WHERE id = $1`, [queueId]
      );
      if (rows[0]) {
        const listingId = rows[0].listing_id;
        try { await client.query(`DELETE FROM public.extraction_log WHERE listing_id = $1`, [listingId]); } catch {}
        await client.query(`DELETE FROM public.review_queue WHERE listing_id = $1`, [listingId]);
        await client.query(`DELETE FROM public.listings WHERE id = $1`, [listingId]);
        await client.query('COMMIT');
      } else {
        await client.query('ROLLBACK');
      }
      done++;
      send({ total: allIds.length, done, queueId, ok: true });
    } catch (err) {
      console.error('[bulk-reanalyse-all] error for queue=%s:', queueId, err);
      if (client) await client.query('ROLLBACK').catch(() => {});
      done++;
      send({ total: allIds.length, done, queueId, ok: false, error: (err as Error).message });
    } finally {
      if (client) client.release();
    }
  }

  send({ total: allIds.length, done, finished: true });
  res.end();
});

// JSON stats
app.get('/api/stats', async (_req, res) => {
  try { res.json(await fetchStats()); }
  catch (err) { res.status(500).json({ error: (err as Error).message }); }
});

app.get('/health', (_req, res) => res.json({ ok: true }));

app.get('/flowchart', (_req, res) => res.sendFile(path.join(__dirname, 'flowchart.html')));

// ── Consumer browse routes ────────────────────────────────

// GET /browse — consumer car search page
app.get('/browse', async (req, res) => {
  try {
    const rawSort = String(req.query.sort ?? 'newest');
    const f: BrowseFilters = {
      make:      String(req.query.make      ?? ''),
      model:     String(req.query.model     ?? ''),
      min_year:  parseInt(String(req.query.min_year  ?? '0'), 10)  || 0,
      max_year:  parseInt(String(req.query.max_year  ?? '0'), 10)  || 0,
      min_price: parseInt(String(req.query.min_price ?? '0'), 10)  || 0,
      max_price: parseInt(String(req.query.max_price ?? '0'), 10)  || 0,
      body_type: String(req.query.body_type ?? ''),
      city:      String(req.query.city      ?? ''),
      region:    String(req.query.region    ?? ''),
      source:    String(req.query.source    ?? ''),
      page:      Math.max(1, parseInt(String(req.query.page ?? '1'), 10)),
      sort:      Object.prototype.hasOwnProperty.call(BROWSE_SORT_MAP, rawSort) ? rawSort : 'newest',
    };
    const [opts, { rows, total }] = await Promise.all([
      fetchBrowseOptions(),
      fetchBrowseListings(f),
    ]);
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(buildBrowseHtml(opts, rows, total, f));
  } catch (err) {
    res.status(500).send(`<pre style="padding:24px;font-family:monospace;color:red;">Error: ${(err as Error).message}</pre>`);
  }
});

// GET /api/browse-options — filter dropdown data as JSON
app.get('/api/browse-options', async (_req, res) => {
  try {
    res.json(await fetchBrowseOptions());
  } catch (err) {
    res.status(500).json({ error: (err as Error).message });
  }
});

// GET /alerts — alert sign-up page
app.get('/alerts', async (req, res) => {
  try {
    const opts = await fetchBrowseOptions();
    const success = req.query.success === '1';
    const error   = req.query.error ? String(req.query.error) : undefined;
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(buildAlertsHtml({ success, error, makes: opts.makes }));
  } catch (err) {
    res.status(500).send(`<pre style="padding:24px;font-family:monospace;color:red;">Error: ${(err as Error).message}</pre>`);
  }
});

// POST /api/alerts — save alert to DB
app.post('/api/alerts', express.urlencoded({ extended: false }), async (req, res) => {
  const email     = String(req.body.email     ?? '').trim();
  const make      = String(req.body.make      ?? '').trim() || null;
  const max_price = parseInt(String(req.body.max_price ?? ''), 10) || null;
  const min_year  = parseInt(String(req.body.min_year  ?? ''), 10) || null;

  // Validate email
  const emailRe = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!email || !emailRe.test(email)) {
    // If form submission, redirect back with error
    if (req.headers['content-type']?.includes('application/x-www-form-urlencoded')) {
      return res.redirect('/alerts?error=' + encodeURIComponent('Please enter a valid email address.'));
    }
    return res.status(400).json({ ok: false, error: 'Invalid email address' });
  }

  const client = await pool.connect();
  try {
    await client.query(
      `INSERT INTO public.saved_searches (email, make, max_price, min_year) VALUES ($1, $2, $3, $4)`,
      [email, make, max_price, min_year]
    );
    if (req.headers['content-type']?.includes('application/x-www-form-urlencoded')) {
      return res.redirect('/alerts?success=1');
    }
    return res.json({ ok: true });
  } catch (err) {
    const msg = (err as Error).message;
    const isTableMissing = msg.includes('relation') && msg.includes('does not exist');
    const friendly = isTableMissing ? 'Alerts not yet configured' : msg;
    if (req.headers['content-type']?.includes('application/x-www-form-urlencoded')) {
      return res.redirect('/alerts?error=' + encodeURIComponent(friendly));
    }
    return res.status(500).json({ ok: false, error: friendly });
  } finally {
    client.release();
  }
});

// GET /unsubscribe?id=UUID — deactivate a saved search
app.get('/unsubscribe', async (req, res) => {
  const id = String(req.query.id ?? '').trim();
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  if (!id) {
    return res.status(400).send(buildUnsubscribeHtml(false, 'Missing unsubscribe ID.'));
  }
  if (!isValidUuid(id)) {
    return res.status(400).send(buildUnsubscribeHtml(false, 'Invalid unsubscribe link.'));
  }
  const client = await pool.connect();
  try {
    const result = await client.query(
      `UPDATE public.saved_searches SET is_active = false WHERE id = $1`,
      [id]
    );
    if (result.rowCount === 0) {
      return res.status(404).send(buildUnsubscribeHtml(false, 'Unsubscribe link not found or already inactive.'));
    }
    return res.send(buildUnsubscribeHtml(true));
  } catch (err) {
    const msg = (err as Error).message;
    const isTableMissing = msg.includes('relation') && msg.includes('does not exist');
    return res.status(500).send(buildUnsubscribeHtml(false, isTableMissing ? 'Alerts not yet configured.' : msg));
  } finally {
    client.release();
  }
});

// ── Start ─────────────────────────────────────────────────

pool.query('SELECT 1')
  .then(() => {
    app.listen(PORT, () => {
      console.log(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
      console.log(`  Aven Dashboard  →  http://localhost:${PORT}`);
      console.log(`  All Listings    →  http://localhost:${PORT}/listings`);
      console.log(`  Browse Cars     →  http://localhost:${PORT}/browse`);
      console.log(`  Set Alert       →  http://localhost:${PORT}/alerts`);
      console.log(`  Auto-refresh: every ${REFRESH_MS / 1000}s`);
      console.log(`━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`);

      // Purge deleted_listings older than 48 hours, every hour.
      // The table may not exist yet on first deploy — tolerate gracefully.
      const purgeDeletedListings = () => {
        pool.query(`DELETE FROM public.deleted_listings WHERE deleted_at < NOW() - INTERVAL '48 hours'`)
          .then(r => { if (r.rowCount && r.rowCount > 0) console.log(`[purge] deleted_listings: ${r.rowCount} row(s) purged`); })
          .catch(err => { /* table may not exist yet — silent until migrated */ });
      };
      purgeDeletedListings(); // run once on startup
      setInterval(purgeDeletedListings, 60 * 60 * 1000); // then every hour
    });
  })
  .catch(err => { console.error('Postgres not reachable:', (err as Error).message); process.exit(1); });
