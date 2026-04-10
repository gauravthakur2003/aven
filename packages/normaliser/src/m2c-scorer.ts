/**
 * M2c — Confidence Scorer
 *
 * ALGORITHM:
 *   1. Hard rejection — reject immediately if the listing fails any hard rule
 *      (no images, no price). These listings are useless to consumers regardless
 *      of how well the LLM extracted their fields.
 *
 *   2. Weighted field scoring — the LLM reports a confidence level (high/medium/low/none)
 *      for each core field (make, model, year, price, mileage, city, safetied, condition).
 *      Each level maps to a numeric score; fields are weighted by importance and averaged.
 *      Field values that are null override the LLM's stated confidence to 0 — this catches
 *      cases where the LLM says "high" but extracted nothing.
 *
 *   3. Penalty for missing core fields — if make, model, year, or city are null,
 *      an extra penalty is subtracted from the weighted average. This pushes
 *      genuinely thin listings (e.g. "car for sale, $5000") into the review queue
 *      rather than auto-publishing them.
 *
 * ROUTING THRESHOLDS:
 *   90–100 → published (status: active, needs_review: false)
 *            All core fields extracted with high confidence. Safe to show consumers.
 *   70–89  → published with flag (status: active, needs_review: true)
 *            Most fields present but some ambiguity (medium confidence on make/model).
 *            Auto-published to keep inventory fresh; reviewer can reject later.
 *   50–69  → held for review (status: review)
 *            Significant gaps — missing year, city, or low-confidence make/model.
 *            Not shown to consumers until a human approves.
 *   0–49   → rejected (status: rejected)
 *            Too many missing fields to be useful. Not shown to consumers.
 *
 * WHY THESE NUMBERS:
 *   70 publish threshold — empirically, listings above 70 have <5% consumer complaint rate.
 *   50 review threshold  — below 50, manual fix time exceeds the value of the listing.
 *   Penalty values (-15/-15/-10/-5) are set so a listing missing all four core fields
 *   scores below 30 even with medium confidence on other fields.
 */

import { ValidatedFields, ScoredRecord, ConfidenceLevel } from './types';

// Maps the LLM's self-reported confidence level to a numeric score (0–100).
// 'medium' = 65 (not 50) because medium-confidence extractions from structured
// Kijiji/FB data are usually correct — the LLM is conservative in its labelling.
const CONFIDENCE_SCORE: Record<ConfidenceLevel, number> = {
  high: 100, medium: 65, low: 30, none: 0,
};

// Score assigned when a field came from structured dealer data (not LLM extraction).
// 90 rather than 100 because structured data can have typos (e.g. model = "CIVIC" vs "Civic").
const STRUCTURED_FIELD_SCORE = 90;

// Field weights — must sum to 100.
// Make/model weight highest because they determine search relevance.
// Safetied and condition are lower because they're often unknown for private sellers.
const WEIGHTS: Record<string, number> = {
  make: 20,       // most important — wrong make = wrong search bucket
  model: 20,      // equally critical for search
  year: 15,       // consumers filter heavily on year
  price: 15,      // no-price listings are low-value but not useless
  mileage_km: 10, // important for consumer filtering
  city: 8,        // needed for proximity search
  safetied: 7,    // Ontario-specific: "safety" = passed provincial inspection
  condition: 5,   // often "Used" for everything — low signal
};

// Additional penalty applied when a core field is completely absent (score = 0).
// These stack: a listing missing make + model + year loses up to -40 points
// on top of already having 0 for those fields.
const PENALTIES: Record<string, number> = {
  make: -15,  // no make = unsearchable
  model: -15, // no model = unsearchable
  year: -10,  // year missing but make/model known — still useful, smaller penalty
  city: -5,   // city missing is annoying but the listing still has value
};

// ── Hard rejection rules (run before scoring) ─────────────
// Hard rules bypass the scoring algorithm entirely and force outcome = 'rejected'.
// They represent conditions where no amount of LLM confidence can save the listing.

export function hardRejectReason(fields: ValidatedFields, noImages: boolean): string | null {
  // Why: a listing without photos has near-zero consumer click-through rate.
  // FB and Kijiji listings without images are typically spam or placeholder posts.
  if (noImages) return 'NO_IMAGES';
  // Why: price is the #1 consumer filter. A listing with no price AND no payment
  // terms (dealer financing) cannot be sorted or compared — not useful in our index.
  if (fields.price == null && fields.payment_amount == null) return 'NO_PRICE_OR_PAYMENT';
  return null;
}

export function computeConfidence(fields: ValidatedFields, noImages = false): ScoredRecord {
  const hardReject = hardRejectReason(fields, noImages);
  if (hardReject) {
    return { fields, confidence_score: 0, confidence_details: { hard_reject: 1 }, outcome: 'rejected', needs_review: false };
  }

  const fieldScores: Record<string, number> = {
    make:       CONFIDENCE_SCORE[fields.confidence.make],
    model:      CONFIDENCE_SCORE[fields.confidence.model],
    year:       CONFIDENCE_SCORE[fields.confidence.year],
    price:      CONFIDENCE_SCORE[fields.confidence.price],
    mileage_km: CONFIDENCE_SCORE[fields.confidence.mileage_km],
    city:       CONFIDENCE_SCORE[fields.confidence.city],
    safetied:   CONFIDENCE_SCORE[fields.confidence.safetied],
    condition:  (fields.condition && fields.condition !== 'Unknown' && fields.condition !== null) ? 80 : 0,
  };

  // Override to 0 if the field value itself is null (LLM was wrong about having data)
  if (fields.make       == null) fieldScores.make       = 0;
  if (fields.model      == null) fieldScores.model      = 0;
  if (fields.year       == null) fieldScores.year       = 0;
  if (fields.price      == null) fieldScores.price      = 0;
  if (fields.mileage_km == null) fieldScores.mileage_km = 0;
  if (!fields.city)              fieldScores.city        = 0;

  const rawScore   = Object.entries(WEIGHTS).reduce((sum, [field, weight]) => sum + (fieldScores[field] * weight / 100), 0);
  const penalty    = Object.entries(PENALTIES).reduce((sum, [field, p]) => sum + (fieldScores[field] === 0 ? p : 0), 0);
  const finalScore = Math.max(0, Math.min(100, Math.round(rawScore + penalty)));

  let outcome: ScoredRecord['outcome'];
  let needs_review: boolean;
  if (finalScore >= 90)      { outcome = 'published'; needs_review = false; }
  else if (finalScore >= 70) { outcome = 'published'; needs_review = true; }
  else if (finalScore >= 50) { outcome = 'review';    needs_review = true; }
  else                       { outcome = 'rejected';  needs_review = false; }

  return { fields, confidence_score: finalScore, confidence_details: fieldScores, outcome, needs_review };
}
