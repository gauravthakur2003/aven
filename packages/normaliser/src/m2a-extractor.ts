/**
 * M2a — LLM Extraction Worker
 *
 * Converts a raw listing payload (Kijiji JSON or Facebook JSON) into a structured
 * ExtractedFields object by sending the listing text to an LLM with a fixed
 * extraction prompt (prompts/extraction-v1.0.txt).
 *
 * PROVIDER ROUTING:
 *   The LLM_PROVIDER env var selects the backend. extractFields() also accepts
 *   a providerOverride so test-pipeline.ts can assign a specific provider to each
 *   parallel worker — this prevents workers from contending on the same rate-limit bucket.
 *
 *   'anthropic' → Claude claude-sonnet-4-5 (production default, best extraction quality)
 *   'openai'    → GPT-4o-mini (cloud fallback when Anthropic fails)
 *   'gemini'    → Gemini 2.0 Flash via OpenAI-compatible endpoint (free tier, 15 RPM)
 *   'groq'      → Llama 3.1 8B-Instant via Groq (free tier, ~500 tok/s, 30 RPM)
 *   'cerebras'  → Llama 3.1 8B via Cerebras (free tier, ~2000 tok/s, 30 RPM)
 *   'ollama'    → local model via Ollama (free, zero latency, for dev/testing only)
 *
 * ALL non-Anthropic providers expose an OpenAI-compatible chat completions API,
 * so we reuse the OpenAI SDK for all of them with a different baseURL.
 * Anthropic uses its own SDK because its message format differs.
 */

import Anthropic from '@anthropic-ai/sdk';
import OpenAI    from 'openai';
import * as fs   from 'fs';
import * as path from 'path';
import { RawPayload, ExtractedFields } from './types';
import { logger } from './lib/logger';

const NORMALISATION_VERSION = '1.0.0';
const PRIMARY_MODEL         = 'claude-sonnet-4-5-20251001';
const OPENAI_MODEL          = 'gpt-4o-mini';
const OLLAMA_MODEL          = process.env.OLLAMA_MODEL ?? 'mistral';
const OLLAMA_BASE_URL       = process.env.OLLAMA_BASE_URL ?? 'http://localhost:11434/v1';
const GEMINI_MODEL          = process.env.GEMINI_MODEL ?? 'gemini-2.0-flash';
const GEMINI_BASE_URL       = 'https://generativelanguage.googleapis.com/v1beta/openai/';
// llama-3.1-8b-instant: 30 RPM, 14400 RPD (vs 1000 RPD for 70b) — much better for overnight runs
const GROQ_MODEL            = process.env.GROQ_MODEL ?? 'llama-3.1-8b-instant';
const GROQ_BASE_URL         = 'https://api.groq.com/openai/v1';
const CEREBRAS_MODEL        = process.env.CEREBRAS_MODEL ?? 'llama3.1-8b';
const CEREBRAS_BASE_URL     = 'https://api.cerebras.ai/v1';
// Together AI — OpenAI-compatible, paid, no meaningful rate limit
// Llama-3.1-8B-Instruct-Turbo: $0.18/1M tokens, very fast inference
const TOGETHER_MODEL        = process.env.TOGETHER_MODEL ?? 'meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo';
const TOGETHER_BASE_URL     = 'https://api.together.xyz/v1';
const MAX_TOKENS            = 1024;
// Ollama serializes requests — queue wait + processing can exceed 2 min per request.
// Cloud APIs (Anthropic/OpenAI/Gemini) are fast so 60s is plenty for them.
const TIMEOUT_MS_OLLAMA = 300_000;
const TIMEOUT_MS_CLOUD  = 60_000;
const RETRY_DELAY_MS    = 2_000;

const SYSTEM_PROMPT = fs.readFileSync(
  path.join(__dirname, '../prompts/extraction-v1.0.txt'),
  'utf-8',
);

// Lazy clients — created on first use so env vars are loaded by the time they're needed.
let _anthropic: Anthropic | null = null;
let _openai:    OpenAI    | null = null;
let _ollama:    OpenAI    | null = null;
let _gemini:    OpenAI    | null = null;
let _groq:      OpenAI    | null = null;
let _groq2:     OpenAI    | null = null;  // 2nd Groq account (GROQ_API_KEY_2)
let _cerebras:  OpenAI    | null = null;
let _together:  OpenAI    | null = null;

function getAnthropic(): Anthropic {
  if (!_anthropic) _anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY, timeout: TIMEOUT_MS_CLOUD });
  return _anthropic;
}

function getOpenAI(): OpenAI {
  if (!_openai) _openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY, timeout: TIMEOUT_MS_CLOUD });
  return _openai;
}

function getOllama(): OpenAI {
  // Ollama exposes an OpenAI-compatible API — reuse the OpenAI SDK with a different baseURL.
  if (!_ollama) _ollama = new OpenAI({ apiKey: 'ollama', baseURL: OLLAMA_BASE_URL, timeout: TIMEOUT_MS_OLLAMA });
  return _ollama;
}

function getGemini(): OpenAI {
  // Gemini exposes an OpenAI-compatible endpoint.
  if (!_gemini) _gemini = new OpenAI({ apiKey: process.env.GEMINI_API_KEY ?? '', baseURL: GEMINI_BASE_URL, timeout: TIMEOUT_MS_CLOUD });
  return _gemini;
}

function getGroq(): OpenAI {
  // Groq exposes an OpenAI-compatible API — free tier, ~500 tok/s.
  if (!_groq) _groq = new OpenAI({ apiKey: process.env.GROQ_API_KEY ?? '', baseURL: GROQ_BASE_URL, timeout: TIMEOUT_MS_CLOUD });
  return _groq;
}

function getGroq2(): OpenAI {
  // Second Groq account key — separate 14,400 RPD pool. Set GROQ_API_KEY_2 in Railway env.
  if (!_groq2) _groq2 = new OpenAI({ apiKey: process.env.GROQ_API_KEY_2 ?? '', baseURL: GROQ_BASE_URL, timeout: TIMEOUT_MS_CLOUD });
  return _groq2;
}

function getCerebras(): OpenAI {
  // Cerebras exposes an OpenAI-compatible API — free tier, ~2000 tok/s.
  if (!_cerebras) _cerebras = new OpenAI({ apiKey: process.env.CEREBRAS_API_KEY ?? '', baseURL: CEREBRAS_BASE_URL, timeout: TIMEOUT_MS_CLOUD });
  return _cerebras;
}

function getTogether(): OpenAI {
  // Together AI — OpenAI-compatible, paid, no rate limit. $0.18/1M tokens.
  if (!_together) _together = new OpenAI({ apiKey: process.env.TOGETHER_API_KEY ?? '', baseURL: TOGETHER_BASE_URL, timeout: TIMEOUT_MS_CLOUD });
  return _together;
}

export interface ExtractionResult {
  fields:           ExtractedFields;
  model:            string;
  promptTokens:     number;
  completionTokens: number;
  latencyMs:        number;
  normalisationVersion: string;
}

// ── Main entry point ──────────────────────────────────────

export async function extractFields(
  payload: RawPayload,
  providerOverride?: string,
): Promise<ExtractionResult> {
  const provider = (providerOverride ?? process.env.LLM_PROVIDER ?? 'anthropic').toLowerCase();
  const rawText  = prepareInput(payload);

  if (provider === 'ollama')    return await callOllama(rawText);
  if (provider === 'openai')    return await callOpenAI(rawText);
  if (provider === 'gemini')    return await callGemini(rawText);
  if (provider === 'groq')      return await callGroq(rawText);
  if (provider === 'groq2')     return await callGroq2(rawText);
  if (provider === 'cerebras')  return await callCerebras(rawText);
  if (provider === 'together')  return await callTogether(rawText);

  // Default: Anthropic with OpenAI fallback
  try {
    return await callAnthropic(rawText);
  } catch (err) {
    const e = err as { status?: number; message: string };
    logger.warn({
      message:    'Primary LLM failed, trying OpenAI fallback',
      error:      e.message,
      httpStatus: e.status,
      payload_id: payload.payload_id,
    });
    return await callOpenAI(rawText);
  }
}

// ── Input preparation ─────────────────────────────────────

// Converts a RawPayload into a plain text string for the LLM prompt.
// Why text instead of raw JSON: LLMs extract more reliably from labelled key:value
// lines than from deeply nested JSON — shorter prompt, less hallucination.
// The two sources (Kijiji JSON, Facebook JSON) use different field names for the
// same concepts, so we normalise them here before the LLM ever sees the data.
function prepareInput(payload: RawPayload): string {
  if (payload.raw_content_type === 'json') {
    try {
      const parsed = JSON.parse(payload.raw_content) as Record<string, unknown>;
      return formatJsonAsText(parsed);
    } catch {
      // JSON parse failed (malformed payload) — pass raw string and let the LLM try
      return payload.raw_content;
    }
  }
  // HTML payloads (future scrapers) — strip tags before sending
  return stripHtml(payload.raw_content);
}

// Formats a parsed listing object as "Label: value\n" lines for the LLM.
// We detect source by field names (Facebook has 'priceAmount', Kijiji has 'priceCents')
// and map each source's schema to a consistent set of human-readable labels.
// The LLM prompt is source-agnostic — it only sees the normalised labels.
function formatJsonAsText(obj: Record<string, unknown>): string {
  const lines: string[] = [];
  const add = (k: string, v: unknown) => {
    if (v != null && v !== '') lines.push(`${k}: ${v}`);
  };

  // Detect source: Facebook listings have 'priceAmount', Kijiji have 'priceCents'
  const isFacebook = 'priceAmount' in obj;

  add('Title', obj['title']);

  if (isFacebook) {
    // Facebook Marketplace field names
    const price = obj['priceAmount'];
    if (price != null) {
      add('Price', `$${(price as number).toLocaleString('en-CA')} ${obj['priceCurrency'] ?? 'CAD'}`);
    } else if (obj['paymentAmount'] != null) {
      // Dealer financing: estimate price range from payment
      const pmt   = obj['paymentAmount'] as number;
      const freq  = String(obj['paymentFreq'] ?? 'monthly');
      const label = freq === 'biweekly' ? 'bi-weekly' : 'monthly';
      const periods = freq === 'biweekly' ? 26 * 6 : 12 * 6;
      const estLow  = Math.round(pmt * periods * 0.6 / 1000) * 1000;
      const estHigh = Math.round(pmt * periods * 0.8 / 1000) * 1000;
      add('Price', `Not listed (financing: $${pmt}/${label} — est. $${estLow.toLocaleString('en-CA')}–$${estHigh.toLocaleString('en-CA')} CAD)`);
    } else {
      add('Price', 'Not listed');
    }
    const st = obj['sellerType'];
    add('Seller type',     st === 'dealer' ? 'Dealer' : st === 'private' ? 'Private' : st);
    add('Seller name',     obj['sellerName']);
    add('Interior colour', obj['colourInterior']);
    add('Doors',           obj['doors']);
    add('Seats',           obj['seats']);
    add('Accidents',       obj['accidents'] != null ? `${obj['accidents']} reported` : null);
    add('Previous owners', obj['owners']);
  } else {
    // Kijiji field names
    add('Price',        obj['priceCents'] != null
      ? `$${((obj['priceCents'] as number) / 100).toLocaleString('en-CA')} CAD`
      : 'Not listed');
    add('Price rating', obj['priceRating']);
    add('Seller type',  obj['_sellerType'] === 'delr' ? 'Dealer'
                      : obj['_sellerType'] === 'ownr'  ? 'Private'
                      : obj['_sellerType']);
    add('Interior colour', obj['colourInterior']);
    add('Doors',           obj['doors']);
    add('Seats',           obj['seats']);
    add('CARFAX link',     obj['carproofLink']);
    add('URL',             obj['url']);
  }

  // Shared fields (same names across both sources)
  add('Year',             obj['year']);
  add('Make',             obj['make']);
  add('Model',            obj['model']);
  add('Trim',             obj['trim']);
  add('Mileage',          obj['mileageKm'] != null ? `${obj['mileageKm']} km` : null);
  add('Exterior colour',  obj['colour']);
  add('Body type',        obj['bodyType']);
  add('Drivetrain',       obj['drivetrain']);
  add('Fuel type',        obj['fuelType']);
  add('Transmission',     obj['transmission']);
  add('VIN',              obj['vin']);
  add('Condition',        obj['condition']);
  add('Location',         obj['location']);
  add('Description',      obj['description']);
  return lines.join('\n');
}

function stripHtml(html: string): string {
  return html
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/\s{2,}/g, ' ')
    .trim()
    .slice(0, 8000);
}

// ── Ollama (local, free) ──────────────────────────────────

async function callOllama(text: string): Promise<ExtractionResult> {
  const t0 = Date.now();

  const response = await getOllama().chat.completions.create({
    model:       OLLAMA_MODEL,
    max_tokens:  MAX_TOKENS,
    temperature: 0,
    messages: [
      { role: 'system', content: SYSTEM_PROMPT },
      { role: 'user',   content: text },
    ],
  });

  const latencyMs        = Date.now() - t0;
  const rawJson          = response.choices[0]?.message?.content ?? '';
  const fields           = parseJson(rawJson);
  const promptTokens     = response.usage?.prompt_tokens     ?? 0;
  const completionTokens = response.usage?.completion_tokens ?? 0;
  const modelName        = `ollama/${OLLAMA_MODEL}`;

  return { fields, model: modelName, promptTokens, completionTokens, latencyMs, normalisationVersion: NORMALISATION_VERSION };
}

// ── Anthropic (production primary) ───────────────────────

async function callAnthropic(text: string): Promise<ExtractionResult> {
  const t0 = Date.now();

  let response: Awaited<ReturnType<Anthropic['messages']['create']>>;
  try {
    response = await getAnthropic().messages.create({
      model:      PRIMARY_MODEL,
      max_tokens: MAX_TOKENS,
      temperature: 0,
      system:     SYSTEM_PROMPT,
      messages:   [{ role: 'user', content: text }],
    });
  } catch (err) {
    const e = err as { status?: number; message: string };
    if (e.status === 429 || (e.status && e.status >= 500)) {
      await sleep(RETRY_DELAY_MS);
      response = await getAnthropic().messages.create({
        model:      PRIMARY_MODEL,
        max_tokens: MAX_TOKENS,
        temperature: 0,
        system:     SYSTEM_PROMPT,
        messages:   [{ role: 'user', content: text }],
      });
    } else {
      throw err;
    }
  }

  const latencyMs        = Date.now() - t0;
  const content          = response.content[0];
  const rawJson          = content.type === 'text' ? content.text : '';
  const fields           = parseJson(rawJson);
  const promptTokens     = response.usage.input_tokens;
  const completionTokens = response.usage.output_tokens;

  return { fields, model: PRIMARY_MODEL, promptTokens, completionTokens, latencyMs, normalisationVersion: NORMALISATION_VERSION };
}

// ── OpenAI (cloud fallback) ───────────────────────────────

async function callOpenAI(text: string): Promise<ExtractionResult> {
  const t0 = Date.now();

  const response = await getOpenAI().chat.completions.create({
    model:       OPENAI_MODEL,
    max_tokens:  MAX_TOKENS,
    temperature: 0,
    messages: [
      { role: 'system', content: SYSTEM_PROMPT },
      { role: 'user',   content: text },
    ],
  });

  const latencyMs        = Date.now() - t0;
  const rawJson          = response.choices[0]?.message?.content ?? '';
  const fields           = parseJson(rawJson);
  const promptTokens     = response.usage?.prompt_tokens     ?? 0;
  const completionTokens = response.usage?.completion_tokens ?? 0;

  return { fields, model: OPENAI_MODEL, promptTokens, completionTokens, latencyMs, normalisationVersion: NORMALISATION_VERSION };
}

// ── Gemini (cloud, free tier via OpenAI-compatible endpoint) ─────────────────

async function callGemini(text: string): Promise<ExtractionResult> {
  const t0 = Date.now();

  const response = await getGemini().chat.completions.create({
    model:       GEMINI_MODEL,
    max_tokens:  MAX_TOKENS,
    temperature: 0,
    messages: [
      { role: 'system', content: SYSTEM_PROMPT },
      { role: 'user',   content: text },
    ],
  });

  const latencyMs        = Date.now() - t0;
  const rawJson          = response.choices[0]?.message?.content ?? '';
  const fields           = parseJson(rawJson);
  const promptTokens     = response.usage?.prompt_tokens     ?? 0;
  const completionTokens = response.usage?.completion_tokens ?? 0;

  return { fields, model: `gemini/${GEMINI_MODEL}`, promptTokens, completionTokens, latencyMs, normalisationVersion: NORMALISATION_VERSION };
}

// ── Groq (cloud, free tier, ~500 tok/s via Llama 3.3 70B) ────────────────────

async function callGroq(text: string): Promise<ExtractionResult> {
  const t0 = Date.now();

  const response = await getGroq().chat.completions.create({
    model:       GROQ_MODEL,
    max_tokens:  MAX_TOKENS,
    temperature: 0,
    messages: [
      { role: 'system', content: SYSTEM_PROMPT },
      { role: 'user',   content: text },
    ],
  });

  const latencyMs        = Date.now() - t0;
  const rawJson          = response.choices[0]?.message?.content ?? '';
  const fields           = parseJson(rawJson);
  const promptTokens     = response.usage?.prompt_tokens     ?? 0;
  const completionTokens = response.usage?.completion_tokens ?? 0;

  return { fields, model: `groq/${GROQ_MODEL}`, promptTokens, completionTokens, latencyMs, normalisationVersion: NORMALISATION_VERSION };
}

// ── Groq (2nd account key — separate 14,400 RPD pool) ────────────────────────

async function callGroq2(text: string): Promise<ExtractionResult> {
  const t0 = Date.now();

  const response = await getGroq2().chat.completions.create({
    model:       GROQ_MODEL,
    max_tokens:  MAX_TOKENS,
    temperature: 0,
    messages: [
      { role: 'system', content: SYSTEM_PROMPT },
      { role: 'user',   content: text },
    ],
  });

  const latencyMs        = Date.now() - t0;
  const rawJson          = response.choices[0]?.message?.content ?? '';
  const fields           = parseJson(rawJson);
  const promptTokens     = response.usage?.prompt_tokens     ?? 0;
  const completionTokens = response.usage?.completion_tokens ?? 0;

  return { fields, model: `groq2/${GROQ_MODEL}`, promptTokens, completionTokens, latencyMs, normalisationVersion: NORMALISATION_VERSION };
}

// ── Cerebras (cloud, free tier, ~2000 tok/s via Llama 3.3 70B) ───────────────

async function callCerebras(text: string): Promise<ExtractionResult> {
  const t0 = Date.now();

  const response = await getCerebras().chat.completions.create({
    model:       CEREBRAS_MODEL,
    max_tokens:  MAX_TOKENS,
    temperature: 0,
    messages: [
      { role: 'system', content: SYSTEM_PROMPT },
      { role: 'user',   content: text },
    ],
  });

  const latencyMs        = Date.now() - t0;
  const rawJson          = response.choices[0]?.message?.content ?? '';
  const fields           = parseJson(rawJson);
  const promptTokens     = response.usage?.prompt_tokens     ?? 0;
  const completionTokens = response.usage?.completion_tokens ?? 0;

  return { fields, model: `cerebras/${CEREBRAS_MODEL}`, promptTokens, completionTokens, latencyMs, normalisationVersion: NORMALISATION_VERSION };
}

async function callTogether(text: string): Promise<ExtractionResult> {
  const t0 = Date.now();

  const response = await getTogether().chat.completions.create({
    model:       TOGETHER_MODEL,
    max_tokens:  MAX_TOKENS,
    temperature: 0,
    messages: [
      { role: 'system', content: SYSTEM_PROMPT },
      { role: 'user',   content: text },
    ],
  });

  const latencyMs        = Date.now() - t0;
  const rawJson          = response.choices[0]?.message?.content ?? '';
  const fields           = parseJson(rawJson);
  const promptTokens     = response.usage?.prompt_tokens     ?? 0;
  const completionTokens = response.usage?.completion_tokens ?? 0;

  return { fields, model: `together/${TOGETHER_MODEL}`, promptTokens, completionTokens, latencyMs, normalisationVersion: NORMALISATION_VERSION };
}

// ── JSON parsing ──────────────────────────────────────────

// Attempts to fix truncated JSON by closing unclosed strings, arrays, objects.
//
// Why this exists: we cap LLM output at MAX_TOKENS (1024) to control cost and latency.
// When a listing description is long, the LLM may hit the token limit mid-JSON output,
// producing something like: {"make":"Toyota","model":"Camry","description":"This car has
// That is invalid JSON and JSON.parse() throws. Rather than discarding the entire
// extraction (wasting the LLM call), we close any open strings/arrays/objects and
// parse what we have — recovering all fields that were already written before the cut.
//
// Observed failure rate: ~2-3% of listings when description > 400 tokens.
function repairTruncatedJson(s: string): string {
  // Remove trailing incomplete key-value pair (e.g. `,"key":` or `,"key":"...`)
  let t = s.trimEnd();

  // If ends inside a string, close it
  let inStr = false, escaped = false;
  for (let i = 0; i < t.length; i++) {
    const ch = t[i];
    if (escaped) { escaped = false; continue; }
    if (ch === '\\' && inStr) { escaped = true; continue; }
    if (ch === '"') inStr = !inStr;
  }
  if (inStr) t += '"';

  // Remove trailing comma or incomplete key
  t = t.replace(/,\s*$/, '').replace(/,\s*"[^"]*"\s*:\s*$/, '').replace(/,\s*"[^"]*"$/, '');

  // Count and close unclosed braces/brackets
  let depth = 0;
  const stack: string[] = [];
  inStr = false; escaped = false;
  for (let i = 0; i < t.length; i++) {
    const ch = t[i];
    if (escaped) { escaped = false; continue; }
    if (ch === '\\' && inStr) { escaped = true; continue; }
    if (ch === '"') { inStr = !inStr; continue; }
    if (inStr) continue;
    if (ch === '{') { stack.push('}'); depth++; }
    else if (ch === '[') { stack.push(']'); depth++; }
    else if ((ch === '}' || ch === ']') && depth > 0) { stack.pop(); depth--; }
  }
  // Close in reverse order
  while (stack.length > 0) t += stack.pop();
  return t;
}

function sanitiseJsonString(s: string): string {
  // Remove markdown code fences
  s = s.replace(/^```json\s*/i, '').replace(/\s*```$/i, '').trim();
  // Replace raw control characters (0x00–0x1F except \t \n \r) inside JSON strings
  // They cause "Bad control character" parse errors from some LLM providers (Groq)
  // We replace them with a space — they're almost always from stray bytes in description text
  s = s.replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, ' ');
  return s;
}

function parseJson(raw: string): ExtractedFields {
  const cleaned = sanitiseJsonString(raw);
  let parsed: ExtractedFields;
  try {
    parsed = JSON.parse(cleaned) as ExtractedFields;
  } catch {
    // Try repair — handles truncated output from token-limited responses
    parsed = JSON.parse(repairTruncatedJson(cleaned)) as ExtractedFields;
  }

  if (!parsed.price_type)          parsed.price_type          = 'UNKNOWN';
  if (!parsed.price_currency_orig) parsed.price_currency_orig = 'CAD';
  if (typeof parsed.price_raw !== 'string') parsed.price_raw  = '';
  if (!parsed.confidence) {
    parsed.confidence = {
      make: 'low', model: 'low', year: 'low',
      price: 'none', mileage_km: 'none', safetied: 'none', city: 'low',
    };
  }

  return parsed;
}

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}
