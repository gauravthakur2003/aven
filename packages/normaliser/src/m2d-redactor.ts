// M2d — PII Redactor
// Two-pass redaction on listing description text.
// Pass 1: Deterministic regex patterns (fast, always runs).
// Pass 2: Validation scan — hard-fails if any phone or email remains.
// PIPEDA compliance requirement: zero-tolerance on published PII.

export interface RedactionResult {
  text:         string;
  itemsRemoved: number;
  failed:       boolean;  // true if validation scan still finds PII after redaction
}

// ── Redaction patterns (from PRD §9.1) ───────────────────

const PATTERNS: Array<{ re: RegExp; replacement: string }> = [
  // Canadian phone numbers (various formats)
  { re: /(?:\+?1[\s.\-]?)?\(?[2-9]\d{2}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}/g, replacement: '[phone removed]' },
  // Email addresses
  { re: /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g, replacement: '[email removed]' },
  // WhatsApp / text references with numbers
  { re: /(whatsapp|text|txt|wa\.me)[:\s]?[\+\d\s.\-]{10,15}/gi, replacement: '[contact removed]' },
  // Facebook profile URLs
  { re: /(facebook\.com\/|fb\.me\/)[a-zA-Z0-9.]+/gi, replacement: '[contact removed]' },
  // Street addresses
  { re: /\d+\s[A-Z][a-zA-Z]+(\s[A-Z][a-zA-Z]+)?\s(St|Ave|Blvd|Rd|Dr|Crt|Cres|Way|Lane|Pl|Pkwy)\.?/gi, replacement: '[address removed]' },
];

// Validation patterns — used after redaction to verify nothing slipped through
const VALIDATION_PATTERNS: RegExp[] = [
  /(?:\+?1[\s.\-]?)?\(?[2-9]\d{2}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}/,
  /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/,
];

// ── Main entry point ──────────────────────────────────────

export function redactPII(text: string | null): RedactionResult {
  if (!text) return { text: text ?? '', itemsRemoved: 0, failed: false };

  let result  = text;
  let removed = 0;

  for (const { re, replacement } of PATTERNS) {
    const before = result;
    result = result.replace(re, replacement);
    const escapedReplacement = replacement.replace(/[[\]]/g, '\\$&');
    const beforeCount = (before.match(new RegExp(escapedReplacement, 'g')) ?? []).length;
    const afterCount  = (result.match(new RegExp(escapedReplacement, 'g')) ?? []).length;
    removed += Math.max(0, afterCount - beforeCount);
  }

  // Validation pass — zero-tolerance
  const failed = VALIDATION_PATTERNS.some(re => re.test(result));
  return { text: result, itemsRemoved: removed, failed };
}
