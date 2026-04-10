import { randomBytes } from 'crypto';

// Real Chrome UA strings from the last 6 months (curated list).
// Rotated per session — never hardcode a single UA.
const USER_AGENTS: string[] = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
];

// Common desktop resolutions (curated from real user data).
const RESOLUTIONS = [
  { width: 1920, height: 1080 },
  { width: 1366, height: 768 },
  { width: 1440, height: 900 },
  { width: 1280, height: 800 },
  { width: 1536, height: 864 },
];

/**
 * Returns a random integer in [0, max) using cryptographically random bytes.
 * Never use Math.random() in connectors — deterministic patterns are a bot fingerprint.
 */
function cryptoRandInt(max: number): number {
  const value = randomBytes(4).readUInt32LE(0);
  return Math.floor((value / 0xffffffff) * max);
}

export function buildUserAgent(): string {
  return USER_AGENTS[cryptoRandInt(USER_AGENTS.length)];
}

export function randomResolution(): { width: number; height: number } {
  return RESOLUTIONS[cryptoRandInt(RESOLUTIONS.length)];
}

/**
 * Returns a Promise that resolves after a random delay in [minMs, maxMs].
 * Uses crypto RNG — required per PRD-M1-TECH-001 §7.3.
 */
export function randomDelay(minMs: number, maxMs: number): Promise<void> {
  const range = maxMs - minMs;
  const delay = minMs + Math.floor((randomBytes(4).readUInt32LE(0) / 0xffffffff) * range);
  return new Promise((resolve) => setTimeout(resolve, delay));
}

/**
 * A simple in-memory bloom filter substitute using a Set.
 * Used per-run to prevent the same URL being pushed twice within one scrape run.
 * Not a real bloom filter — acceptable for MVP at this listing volume.
 */
export class PerRunDeduplicator {
  private seen = new Set<string>();

  has(url: string): boolean {
    return this.seen.has(url);
  }

  add(url: string): void {
    this.seen.add(url);
  }

  clear(): void {
    this.seen.clear();
  }

  get size(): number {
    return this.seen.size;
  }
}
