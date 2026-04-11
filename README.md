# Aven

Scrapes Kijiji & Facebook Marketplace across GTA, normalises listings with LLM pipelines (Cerebras/Groq/Gemini), and serves a live dashboard with search, review queue & email alerts.

**Stack:** Node.js · TypeScript · PostgreSQL (Neon) · Redis · Playwright · Railway

---

## How It Works

```
Kijiji (6 regions)  ──┐
                       ├──▶  Pipeline (normaliser)  ──▶  Postgres  ──▶  Dashboard
Facebook Marketplace ──┘         LLM extraction                        aven-dashboard-production.up.railway.app
```

1. **Scraper** — Polls Kijiji (6 GTA regions in parallel) and Facebook Marketplace continuously. New listings are queued for processing.
2. **Normaliser** — Each raw listing goes through a multi-step LLM pipeline: extract → validate → score → redact PII → route. Fast-path for structured Kijiji data (no LLM needed). LLM workers use Cerebras, Groq, and Gemini with rate pacing.
3. **Dashboard** — Express.js server on port 3030. Serves the admin dashboard, browse page, review queue, and email alert management.

---

## Project Structure

```
aven/
├── packages/
│   ├── dashboard/       # Express server + frontend (Railway deploy)
│   ├── normaliser/      # Scraper + LLM pipeline
│   └── scraper/         # M1 scraper service (Redis-based)
├── db/
│   └── migrations/      # PostgreSQL schema (run in order)
├── prompts/             # LLM extraction prompts
├── docker-compose.yml   # Local Redis + Postgres + services
└── .env.example         # Environment variable template
```

---

## Prerequisites

- Node.js 18+
- PostgreSQL (or a [Neon](https://neon.tech) cloud database)
- Redis (local via Docker, or [Upstash](https://upstash.com))

---

## Setup

### 1. Clone the repo

```bash
git clone git@github.com:gauravthakur2003/aven.git
cd aven
```

### 2. Install dependencies

```bash
cd packages/dashboard && npm install && cd ../..
cd packages/normaliser && npm install && cd ../..
cd packages/scraper    && npm install && cd ../..
```

### 3. Configure environment variables

```bash
cp .env.example packages/normaliser/.env
```

Open `packages/normaliser/.env` and fill in:

| Variable | Description |
|---|---|
| `DATABASE_URL` | Postgres connection string (Neon or local) |
| `REDIS_URL` | Redis connection string |
| `CEREBRAS_API_KEY` | [Cerebras Cloud](https://cloud.cerebras.ai) |
| `GROQ_API_KEY` | [Groq Cloud](https://console.groq.com) |
| `GEMINI_API_KEY` | [Google AI Studio](https://aistudio.google.com) |
| `BRIGHTDATA_*` | Brightdata proxy credentials (optional) |
| `OXYLABS_*` | Oxylabs proxy credentials (optional) |

Also create `packages/dashboard/.env` with:

```env
DATABASE_URL=your_postgres_connection_string
```

### 4. Run database migrations

```bash
cd packages/normaliser
PATH=/usr/local/bin:$PATH npm run db:migrate
```

Migrations live in `db/migrations/` and run in order (0001 → 0008).

---

## Running Locally

Open **3 terminal tabs:**

### Tab 1 — Dashboard (http://localhost:3030)

```bash
cd packages/dashboard
PATH=/usr/local/bin:$PATH npm run start:local
```

### Tab 2 — Pipeline (scraper + normaliser, runs continuously)

```bash
cd packages/normaliser
PATH=/usr/local/bin:$PATH nohup npm run pipeline > /tmp/aven-pipeline.log 2>&1 &
echo $! > /tmp/aven-pipeline.pid

# Watch live logs
tail -f /tmp/aven-pipeline.log
```

### Tab 3 — Optional: Docker services (Redis + Postgres local)

```bash
docker-compose up -d redis postgres
```

> Skip Tab 3 if you're using Neon (cloud Postgres) and Upstash (cloud Redis).

---

## Stopping the Pipeline

```bash
kill $(cat /tmp/aven-pipeline.pid)
```

---

## Dashboard Routes

| Route | Description |
|---|---|
| `/` | Admin dashboard — pipeline stats, listing counts |
| `/listings` | All listings table |
| `/browse` | Consumer browse & search (CarGurus-style) |
| `/alerts` | Set email alerts for saved searches |
| `/flowchart` | System architecture diagram |
| `/api/stats` | JSON health check (used by Railway) |

---

## Deploying to Railway

The dashboard (`packages/dashboard`) is deployed on Railway. Config is in `packages/dashboard/railway.json`.

```bash
cd packages/dashboard
PATH=/Users/gauravthakur/.npm-global/node_modules/.bin:/usr/local/bin:$PATH railway up --detach
```

Live URL: **https://aven-dashboard-production.up.railway.app**

---

## Branch Strategy

| Branch | Purpose |
|---|---|
| `main` | Stable — Railway deploys from here |
| `staging` | Reviewed & tested, ready to merge to main |
| `dev` | Active development — all changes go here first |

**Workflow:**
```
dev  →  staging  →  main  →  Railway (auto-deploy)
```

---

## Database Schema

Key tables (full schema in `db/migrations/`):

- **`listings`** — Normalised vehicle listings (make, model, year, price, mileage, VIN, location, photos, confidence score, etc.)
- **`review_queue`** — Listings flagged for human review (low confidence, missing fields)
- **`extraction_log`** — LLM extraction audit trail (model used, tokens, latency, confidence)
- **`dealer_accounts`** — Dealer profiles linked to listings
- **`saved_searches`** — User alert subscriptions

---

## LLM Pipeline (M2)

Each raw listing passes through these stages:

```
M2a  Extract      — LLM pulls structured fields from raw text/HTML
M2b  Validate     — Check required fields, flag anomalies
M2c  Score        — Confidence score 0–100
M2d  Redact       — Strip PII (phone numbers, emails)
M2e  Route        — publish / review queue / reject
M2f  Carfax       — VIN enrichment (if available)
M2g  Vision       — Photo analysis (if available)
M2h  Alerts       — Trigger email alerts for matching saved searches
```

LLM workers: **Cerebras** (primary) → **Groq** (fallback) → **Gemini** (fallback). Rate-limited with automatic backoff.
