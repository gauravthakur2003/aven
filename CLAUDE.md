# Aven — CLAUDE.md
> Complete project context. Read this in full before any Aven work. Last updated: April 27, 2026.

---

## 1. What Is Aven

**Aven** is a Canadian used car aggregator — not a competitor to AutoTrader, but a complement/aggregator that sits *above* it. It scrapes listings from every publicly accessible source (Kijiji, Facebook Marketplace, Craigslist, dealer sites) and surfaces them through a single buyer-facing interface.

**Business model:** Free to list (aggregated), paid per click/lead sent to seller. $0.10–$0.30 per lead. Target: 100k listings × 50 avg visits × $0.20 = $1M+ annual revenue. Also: listing boosts, affiliate marketing (insurance, financing).

**Founder:** Gaurav Thakur — PM/founder, product-level (not deep-technical). Treat as a product owner. Don't over-explain code.

**Key constraint:** AutoTrader is the competition — never scrape AutoTrader. Data partnership required before any AutoTrader work.

---

## 2. Codebase Location & Structure

```
/Users/gauravthakur/Documents/aven/
├── packages/
│   ├── scraper/          ← Module 1 (original scraper, mostly superseded)
│   ├── normaliser/       ← Module 2 (LLM pipeline) + main pipeline runner
│   │   ├── test-pipeline.ts        ← THE MAIN RUNNER — everything runs from here
│   │   ├── src/
│   │   │   ├── fb-noauth-scraper.ts ← FB Marketplace no-auth scraper (NEW)
│   │   │   ├── fb-scraper.ts        ← Old Playwright-based FB scraper (DEPRECATED)
│   │   │   ├── m2a-extractor.ts     ← LLM extraction + fast-path
│   │   │   ├── m2b-validator.ts     ← Validation rules
│   │   │   ├── m2c-scorer.ts        ← Confidence scoring
│   │   │   ├── m2d-redactor.ts      ← PII redaction
│   │   │   ├── m2e-router.ts        ← DB write router
│   │   │   ├── m2f-carfax.ts        ← CARFAX enrichment
│   │   │   ├── m2g-vision.ts        ← Vision colour detection
│   │   │   ├── m2h-alerts.ts        ← Email alerts
│   │   │   ├── deduplicator.ts      ← Union-Find dedup engine
│   │   │   ├── types.ts             ← RawPayload and all shared types
│   │   │   └── lib/
│   │   │       ├── db.ts            ← Postgres pool
│   │   │       └── ...
│   │   ├── railway.json             ← Railway deploy config
│   │   ├── nixpacks.toml            ← Railway build config
│   │   └── .env                     ← Local env vars
│   └── dashboard/
│       ├── server.ts                ← Express dashboard server
│       ├── railway.json             ← Railway deploy config
│       └── .env                     ← Local env vars (same DB creds)
├── db/migrations/        ← 10 SQL migration files (0001–0010)
├── prompts/              ← LLM extraction prompt
├── Procfile              ← web: cd packages/dashboard && npm run build && npm start
├── CLAUDE.md             ← THIS FILE
└── .github/workflows/    ← daily-deploy.yml, release-log.yml
```

---

## 3. Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | TypeScript + Node.js 20 |
| HTTP scraping | Axios |
| DB | Postgres (Supabase, Canada Central) via `pg` pool |
| LLM providers | Cerebras, Groq (×2), Gemini, Together AI |
| Pipeline queue | In-memory array (no Redis in production path) |
| Dashboard | Express + raw HTML (no React yet) |
| Cloud hosting | Railway (two services: dashboard + pipeline) |
| Version control | GitHub (`gauravthakur2003/aven`, private) |
| Build system | Nixpacks (Railway), ts-node for local dev |

---

## 4. API Keys & Environment Variables

### packages/normaliser/.env (local dev)
> ⚠️ Actual key values are stored in `packages/normaliser/.env` (gitignored) and in Railway env vars. Never commit real keys to git.

```env
DATABASE_URL=postgresql://postgres.<project-ref>:<password>@aws-1-ca-central-1.pooler.supabase.com:5432/postgres
CEREBRAS_API_KEY=<see Railway env vars>
CEREBRAS_MODEL=llama3.1-8b
GROQ_API_KEY=<see Railway env vars>
GROQ_API_KEY_2=<see Railway env vars>
GROQ_MODEL=llama-3.1-8b-instant
GEMINI_API_KEY=<see Railway env vars>
GEMINI_MODEL=gemini-2.0-flash
TOGETHER_API_KEY=<see Railway env vars>
TOGETHER_MODEL=meta-llama/Llama-3.2-3B-Instruct-Turbo
ANTHROPIC_API_KEY=<see Railway env vars>
NODE_ENV=development
```

### Railway Environment Variables (pipeline service)
Same as above but `NODE_ENV=production`. Also:
```env
FB_PROXY_URL=http://<webshare-user>:<webshare-pass>@p.webshare.io:80
```
> **Note:** Webshare rotating residential proxy ($9/3GB plan). Currently NOT working for Facebook (Webshare residential IPs are blocked by Facebook). Still useful for Kijiji if Railway IP gets blocked.

### Key Service Credentials
> Actual credentials are in `packages/normaliser/.env` and `packages/dashboard/.env` (both gitignored). For Railway, check the environment variable panel in the Railway dashboard.

| Service | Notes |
|---------|-------|
| Supabase DB | Session pooler URL — required because Railway is IPv4 only. Password in local .env. |
| Supabase project | `yaufnnguklsmrkxpxair` — Canada Central (`aws-1-ca-central-1`) |
| Webshare proxy | Rotating residential, format: `http://user:pass@p.webshare.io:80` — in Railway env as `FB_PROXY_URL` |
| Neon DB (old) | NOT IN USE — ignore |

---

## 5. Infrastructure

### Railway Services
| Service | URL | Deploys from | Start command |
|---------|-----|-------------|---------------|
| `aven-dashboard` | `https://aven-dashboard-production.up.railway.app` | `packages/dashboard` | `npm run build && npm start` |
| `aven-pipeline` (normaliser) | Internal | `packages/normaliser` | `npm run pipeline` |

**railway.json (normaliser) — CURRENT:**
```json
{
  "build": { "builder": "NIXPACKS" },
  "deploy": {
    "startCommand": "npm run pipeline",
    "restartPolicyType": "ALWAYS"
  }
}
```
> Changed from `ON_FAILURE / maxRetries: 5` to `ALWAYS` on April 20 — prevents stalling on clean exits.

**Railway restart policy:** `ALWAYS` — restarts on ANY exit (clean or crash). This is intentional because the pipeline is meant to run forever.

### GitHub
- Repo: `https://github.com/gauravthakur2003/aven`
- Branch strategy: `dev → staging → main` (Railway auto-deploys from main)
- Workflows: `daily-deploy.yml` (nightly), `release-log.yml` (auto release notes on every push)

---

## 6. Pipeline Architecture (Current State — April 27, 2026)

```
Kijiji Canada (21 BC regions + 23 ON regions)
  └──▶ 3 scraperWorkers (SKIP_PHASE1=true, goes straight to Phase 2)
         ├── Workers 0-1: BC regions (continuous loop)
         └── Worker 2: ON regions (continuous loop)
              └──▶ shared in-memory queue (max 800)
                       └──▶ 9 LLM normaliser workers drain the queue:
                              cerebras, groq, groq2, gemini, 5×together

Facebook Marketplace (14 cities × 4 price bands)
  └──▶ 5 parallel fbScraperLoop workers
         ├── fb-0: Toronto, Ottawa, Hamilton
         ├── fb-1: London ON, Kitchener, Windsor
         ├── fb-2: Barrie, Oshawa, Niagara
         ├── fb-3: Vancouver, Calgary, Edmonton
         └── fb-4: Montreal, Winnipeg
              └──▶ same shared queue → same normaliser workers

dedupLoop() runs every 30 minutes
```

### Key Pipeline Constants
```typescript
SCRAPER_WORKERS = 3    // Kijiji workers
BC_WORKERS = 2         // workers 0-1 → BC, worker 2 → ON
SKIP_PHASE1 = true     // skip initial Ontario forward sweep, go straight to Phase 2
KIJIJI_RPM = 25        // global rate gate (all workers share)
QUEUE_MAX = 800        // backpressure limit
EMPTY_PAGE_STOP = 4    // consecutive empty pages before moving region (raised from 2)
MAX_PAGES = 100        // max pages per region
```

### Phase 1 / Phase 2 Logic
- **SKIP_PHASE1 = true**: Skip Ontario forward sweep entirely, jump to Phase 2
- **Phase 2**: Workers 0-1 → BC (continuous=true), Worker 2 → ON (continuous=true)
- **Both cursors are continuous** — neither exits naturally. Only stops on SIGINT.
- The old "BC done → stop" kill switch was removed April 20.

---

## 7. Kijiji Scraper Details

### Ontario Regions (23)
Northern: Thunder Bay, Sault Ste Marie, Sudbury, North Bay
Southwest: Sarnia, Brantford, Niagara, Windsor, London
Eastern: Belleville, Kingston, Peterborough, Ottawa
Central: Barrie, Cambridge, Kitchener, Guelph
GTA: Hamilton, Halton, Durham, York, Peel, Toronto

### BC Regions (21) — ordered small→large
Williams Lake, Dawson Creek, North Shore, Victoria, Nanaimo, Comox/Courtenay, Cowichan Valley, Terrace, Chilliwack, Kelowna, Kamloops, Cranbrook, Abbotsford, Fraser Valley, Prince George, Burnaby/New West, North Shore GVA, Vancouver, Richmond, Tricities/Pitt/Maple, Delta/Surrey/Langley

**BC total expected: ~7,300 listings** (confirmed from provincial page c174l9007 scan)

### Kijiji Rate Limiting
- Global gate: 25 RPM shared across ALL workers
- Gap: 2,400ms minimum between requests
- 429 handling: 90–180s random backoff
- Block detection: if response <15KB with no `__NEXT_DATA__` → likely captcha, back off 3–4min, retry same page (do NOT advance cursor)
- User-agent rotation: 4 browser strings, random

### Kijiji Data Extraction
- Source: `__NEXT_DATA__` script tag in HTML → Apollo state → `AutosListing:*` entries
- Key fields from `attributes.all` array (NOT `vehicleInformation` which is always null):
  `caryear`, `carmake`, `carmodel`, `carmileageinkms`, `vin`, `carcolor`,
  `carinteriorcolor`, `cartransmission`, `cardrivetraintype`, `carfueltype`,
  `carbodytype`, `carprooflink`
- Advanced scrape: thin listings (missing make/model/year/price) get detail page fetched
- `_advancedScrape: true` set when detail page fetched → enables M2g vision colour detection
- `geo_region` field set to region's province ('ON' or 'BC') → used as province override in normaliser

---

## 8. Facebook Marketplace Scraper

### Current Scraper: fb-noauth-scraper.ts
**No auth required. No Playwright. Pure HTTP/axios.**

How it works:
1. **Discovery**: Fetch grid page `facebook.com/marketplace/{city}/vehicles` → parse `data-sjs` scripts → extract 12-16 digit listing IDs (~1,000 per city/band)
2. **Enrichment**: Fetch each detail page `facebook.com/marketplace/item/{id}/` → parse large (>20KB) `data-sjs` script containing full vehicle data

Key fields available:
- `vehicle_make_display_name`, `vehicle_model_display_name`
- `vehicle_odometer_data` (mileage, unit)
- `vehicle_exterior_color`, `vehicle_interior_color`
- `vehicle_transmission_type`, `vehicle_fuel_type`, `vehicle_condition`
- `vehicle_seller_type`, `vehicle_identification_number`, `dealership_name`
- Year extracted from title regex `\b(19[5-9]\d|20[0-3]\d)\b`
- Images from `listing_photos` array (up to 10)
- Description from `redacted_description.text`

Timing: `DETAIL_DELAY_MS=1200`, `GRID_DELAY_MS=3000`, `SWEEP_PAUSE_MS=5min`

`_advancedScrape: true` set on ALL FB listings (always full detail page) → vision colour detection runs.

### FB IP Blocking Problem
- Railway's AWS IP → Facebook returns 400 immediately
- Webshare residential proxies ($9/3GB) → also blocked by Facebook
- Local Mac IP → works fine (1,002 IDs returned from Toronto)
- Mobile proxies (4G/LTE) → would work but $30-50/month (over budget)
- Hetzner VPS ($4/month) → 50/50 chance, worth trying
- Apify FB actor → $90+/month ongoing at scale (not viable)

**Current status:** FB scraper runs on Railway but gets 0 IDs. All 5 workers log `→ 0 new (0 already seen)` because Railway IP is blocked.

**Proxy config in code:** `FB_PROXY_URL` env var → `hpagent` HttpsProxyAgent → threaded into both grid and detail axios calls.

### Old Playwright FB Scraper (DEPRECATED)
`fb-scraper.ts` — still in codebase but no longer used. The 552 FB listings in DB came from this running locally.

---

## 9. Normalisation Pipeline (M2)

### Stage Flow
```
RawPayload → M2a (extract) → M2b (validate) → M2g (vision) → M2f (CARFAX) → M2d (PII) → M2c (score) → M2e (DB write)
```

### M2a — Extraction
- **Fast path** (74% of Kijiji): if payload has structured JSON with make+model+year+price → skip LLM, extract directly. 0ms, $0.
- `isFastPathEligible()`: checks for `carmake`, `carmodel`, `caryear`, `priceCents` in raw_content
- LLM path: send to provider with `extraction-v1.0.txt` prompt
- Province override: if `validated.province` empty AND `payload.geo_region` set → use geo_region

### LLM Workers (9 total)
| Worker | Provider | Model | Rate limit | Speed |
|--------|---------|-------|-----------|-------|
| cerebras | Cerebras | llama3.1-8b | ~30 RPM | ~750ms |
| groq | Groq | llama-3.1-8b-instant | 14,400 RPD | ~900ms |
| groq2 | Groq (key 2) | llama-3.1-8b-instant | 14,400 RPD | ~900ms |
| gemini | Gemini | gemini-2.0-flash | ~1,500/day | ~600ms |
| together (×5) | Together AI | Llama-3.2-3B-Instruct-Turbo | Paid | ~500ms |

### M2b — Validation Rules
- Year < 1950 → rejected
- No price AND no biweekly payment → `NO_PRICE_OR_PAYMENT`
- No images → `NO_IMAGES`
- Non-car keywords (motorcycle, boat, ATV, etc.) → rejected
- Payment-as-price: price <$3k on luxury brand → rejected
- US location → rejected

### M2g — Vision Colour Detection
- Runs when: `payload._advancedScrape && !noImages && !validated.colour_exterior`
- Both Kijiji advanced-scraped AND all FB listings qualify

### M2f — CARFAX
- Runs when: `validated.vin && (accidents == null || owners == null)`
- Uses `carprooflink` from Kijiji attributes

---

## 10. Database Schema

**Connection:** `postgresql://postgres.yaufnnguklsmrkxpxair:Usedcars%23147369@aws-1-ca-central-1.pooler.supabase.com:5432/postgres`
> Must use session pooler URL (not direct URL) — Railway only supports IPv4, direct Supabase URL resolves to IPv6.

### migrations/
- `0001` — enums + extensions
- `0002` — dealer_accounts stub
- `0003` — listings table (full DDL, pgvector HNSW index)
- `0004` — extraction_log
- `0005` — review_queue
- `0006` — listings indexes
- `0007` — review_queue rerun_count
- `0008` — saved_searches (email alerts)
- `0009` — deleted_listings
- `0010` — backfill province='ON' for pre-tagging Kijiji listings

### Key listings columns
`id, source_id, source_url, make, model, year, trim, price, mileage_km, colour, interior_colour, drivetrain, transmission, fuel_type, body_type, vin, accidents, owners, condition, safetied, seller_type, seller_name, city, province, region, photo_urls, description, confidence_score, status (published/review/rejected), carfax_url, created_at, updated_at`

**Province values:** `'ON'`, `'BC'`, `'AB'`, `'QC'`, `'MB'`, `'SK'`
**Source IDs:** `'kijiji-ca'`, `'facebook-mp-ca'`
**Status enum:** `published`, `review`, `rejected` (+ `duplicate` badge shown in UI but stored as `rejected`)

### Backfill (run once on Railway DB)
```sql
UPDATE listings SET province = 'ON' WHERE source_id = 'kijiji-ca' AND province IS NULL;
```

---

## 11. Dashboard

**URL:** `https://aven-dashboard-production.up.railway.app`

### Routes
| Route | What it does |
|-------|-------------|
| `/` | Main dashboard — stats, review queue, recent listings, pipeline health |
| `/listings` | All listings, paginated 50/page, filterable by source + status + province |
| `/browse` | Consumer-facing browse (CarGurus-style) |
| `/flowchart` | Technical architecture diagram |
| `/alerts` | Email alerts signup |
| `/api/stats` | JSON health check (Railway healthcheck endpoint) |

### Province Filter
`/listings?province=ON` (or BC, AB, QC, etc.) — wired through route handler → SQL `WHERE province = $1`. Filter buttons in UI for each province. Works once province backfill has been run on Railway DB.

### Mobile Responsive
Full mobile breakpoints added (480px/768px) — `topbar-nav`, `topbar-meta` classes, grid reflow 4→2→1 columns. Present in current server.ts.

---

## 12. Data Counts (as of April 27, 2026)

| Source | Count |
|--------|-------|
| Kijiji | ~22,660 |
| Facebook Marketplace | 556 (stale — last updated April 10) |
| **Total** | **~23,216** |

| Province | Count |
|----------|-------|
| ON | ~21,265 |
| BC | ~1,350 (17% of ~7,300 expected) |

**BC sweep status:** ~17% complete. Pipeline stalled multiple times due to Kijiji captcha pages being treated as empty (now fixed with block detection).

---

## 13. Known Bugs Fixed (Critical History)

| Bug | Root cause | Fix |
|-----|-----------|-----|
| BC sweep stalls → clean exit | Captcha page (no `__NEXT_DATA__`) counted as empty → cursor exhausts | Block detection: if HTML <15KB or contains "captcha" → back off 3-4min, retry same page |
| Railway gives up after 5 retries | `ON_FAILURE` + `maxRetries: 5` | Changed to `ALWAYS` restart |
| BC cursor dies → `stopping=true` | `continuous=false` + kill switch | BC cursor now `continuous=true`, kill switch removed |
| Province filter shows 0 results | Historical listings had NULL province | Backfill SQL in migration 0010 |
| `ERR_REQUIRE_ESM` for https-proxy-agent | v7 is ESM-only, can't require() in CJS | Switched to `hpagent` (CJS-compatible) |
| FB scraper 0 IDs on Railway | Railway AWS IP blocked by Facebook | Known issue — Webshare residential also blocked |
| Railway IPv6 | Direct Supabase URL → IPv6 | Use session pooler URL |
| `vehicleInformation` always null | Kijiji data in `attributes.all` not `vehicleInformation` | Read from `attributes.all` |
| Year = 1900 | Mistral sentinel value | Reject year < 1950 |
| FB `window.scrollBy()` no-op | Headless IntersectionObserver not triggered | `page.mouse.wheel()` |
| 429 storm on Railway | All workers retry simultaneously (sync) | 45-120s random backoff, 25 RPM global gate |
| Delete FK constraint | extraction_log + review_queue FK to listings | Delete in order: extraction_log → review_queue → listings |
| JSON truncation by LLM | Cerebras/Groq hit token limit | JSON repair step |

---

## 14. Facebook Marketplace — Full Strategy Context

**The problem:** Facebook blocks all datacenter IPs (AWS/Railway/GCP). Even residential proxy providers (Webshare) have their IPs blacklisted.

**What works:** Local Mac IP (residential), mobile 4G/LTE IPs.

**What doesn't work at our budget ($10-15/mo):**
- Webshare residential → blocked ❌
- Apify FB actor → $90+/month ongoing ❌
- Mobile proxies → $30-50/month (over budget) ❌

**What might work:**
- Hetzner VPS €3.29/month → 50/50 odds, auth session improves chances
- Browser extension (crowdsourced) → free, sustainable, scales with users

**Long-term recommendation:** Build a Chrome/Safari extension. Users browse FB Marketplace, extension passively reads listing data and sends to DB. Their residential IPs, no blocking, zero proxy cost. This is how Honey/Karma built their data moats.

**The session-based scraper (fb-scraper.ts) ran locally and produced 552 listings.** It used Playwright with a saved `fb-session.json`. This approach works from a residential IP.

---

## 15. Product Decisions (Do Not Revisit Without Gaurav)

| Decision | Status |
|----------|--------|
| AutoTrader | Never scrape — it's the competition. Partnership only. |
| Kijiji | Active, working |
| Facebook Marketplace | Blocked on Railway. Browser extension is long-term play. |
| Craigslist Canada | Not yet added. Should work from Railway (no aggressive blocking). Would add ~15-20k listings. |
| Dealer websites | Not yet added. Medium effort. High value (50k+ listings). |
| React/Next.js frontend | Not built yet. Current dashboard is Express + raw HTML. |
| User accounts | Not implemented. Supabase auth ready when needed. |
| Email alerts | `m2h-alerts.ts` built, not confirmed end-to-end. |
| Deduplication | Running every 30min. Union-Find 3-tier (VIN → fields → fuzzy price). |

---

## 16. Local Development

```bash
# Dashboard
cd /Users/gauravthakur/Documents/aven/packages/dashboard
npm run start:local

# Pipeline
cd /Users/gauravthakur/Documents/aven/packages/normaliser
npx ts-node test-pipeline.ts

# Check DB counts
psql "$DATABASE_URL" -c "SELECT source_id, province, COUNT(*) FROM listings GROUP BY 1,2 ORDER BY 3 DESC;"

# Check last listing added
psql "$DATABASE_URL" -c "SELECT source_id, MAX(created_at) as last, COUNT(*) FROM listings GROUP BY source_id;"

# Compile check
cd packages/normaliser && npx tsc --noEmit
cd packages/dashboard && npx tsc --noEmit
```

---

## 17. Pending Work (as of April 27, 2026)

1. **BC sweep** — 17% done (~1,350/7,300). Pipeline restarts continuously due to `ALWAYS` restart policy. Block detection now prevents false exhaustion.
2. **Province filter on Railway** — code is correct; run the backfill SQL on Railway DB.
3. **FB listings** — 556 stale. FB scraper running but blocked on Railway. Needs Hetzner VPS or browser extension solution.
4. **Craigslist Canada** — not yet added. Easy win, no proxy needed.
5. **Mobile responsive** — code is deployed, may need hard refresh to see on mobile.
6. **Deduplication review** — 2,000+ items in review queue. Investigate.
7. **New dedup features planned** — pre-bucketing by (year,make,model), tighter Tier 3, `removed`/`sold` status.
8. **Liveness checker** — background worker to check `last_seen_at`, mark sold/removed.
9. **Price history table** — `price_history(id, listing_id, old_price, new_price, changed_at)`.
10. **React frontend** — current Express+HTML not suitable for consumer launch.
11. **Gemini quota** — may need higher limits from Google Cloud.
12. **AB, MB, QC Kijiji regions** — currently 0 listings. Need to find and verify region IDs.

---

## 18. Revenue Model (for context)

- **Target:** 100k+ listings, 50 avg visits/listing, $0.10-$0.30 per lead sent to seller/dealer
- **Annual revenue at target:** $500k–$1.5M
- **Secondary:** Listing boosts (flat/time-based), affiliate marketing (TD Insurance, financing brokers)
- **Differentiator vs AutoTrader:** Aggregates private sellers (Kijiji, FB) not just dealers. Free to host, pay per performance.

---

## 19. How Claude Should Work on This Project

- Always check DB counts before making assumptions about data state
- Always run `npx tsc --noEmit` before committing TypeScript changes
- Commit with `git pull --rebase origin main` before `git push` (remote often has release log commits)
- Railway auto-deploys on every push to main — no manual deploy needed
- The `test-pipeline.ts` file is the single source of truth for pipeline behavior — not `src/index.ts`
- When pipeline stalls: check DB last_added timestamp first, then Railway logs
- Province must be 2-char uppercase ('ON', 'BC', etc.)
- `seenUrls` Set deduplicates within a run; DB query deduplicates across runs
- Fast path handles 74% of Kijiji — don't break `isFastPathEligible()` checks
- `_advancedScrape: true` on a payload enables M2g vision colour detection
- `geo_region` on payload → used as province override in normaliser worker
