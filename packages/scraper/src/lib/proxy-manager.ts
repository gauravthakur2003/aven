// M1b — Proxy & Rotation Manager
//
// Single shared service used by all connectors.
// No connector may manage its own proxy selection — all proxy interactions go through here.
// Tracks health scores per (IP, source) pair and rotates bans automatically.

import { ProxyConfig, RotationStrategy } from '../types';
import { logger } from './logger';
import { proxyBanRate } from './metrics';

interface ProxyPoolConfig {
  provider: 'brightdata' | 'oxylabs' | 'none';
  host: string;
  port: number;
  username: string;
  password: string;
}

interface IPHealth {
  score: number;                          // 0–100. Below 30 = quarantined.
  bannedForSources: Set<string>;
  cooldownUntil: Map<string, number>;     // sourceId → epoch ms
  bannedAtMs: Map<string, number>;        // sourceId → when ban was applied
}

type BanReason = 'rate_limited' | 'forbidden' | 'captcha' | 'timeout';

const SCORE_PENALTIES: Record<BanReason, number> = {
  rate_limited: -20,
  forbidden:    -40,
  captcha:      -30,
  timeout:      -10,
};

const COOLDOWN_MS: Record<BanReason, number> = {
  rate_limited: 30 * 60 * 1000,   // 30 min
  forbidden:    Infinity,
  captcha:       5 * 60 * 1000,   //  5 min
  timeout:      10 * 60 * 1000,   // 10 min
};

// Re-evaluate quarantined IPs after 6 hours.
const QUARANTINE_RECHECK_MS = 6 * 60 * 60 * 1000;
const QUARANTINE_THRESHOLD   = 30;

export class ProxyManager {
  private pools = new Map<string, ProxyPoolConfig>();
  private health = new Map<string, IPHealth>();

  constructor() {
    this.initPools();
  }

  // ── Public API ────────────────────────────────────────────

  /**
   * Allocates a proxy for a given pool and source.
   * Returns null for pool_id 'none' or if the pool is not configured.
   */
  async allocate(
    poolId: string,
    sourceId: string,
    strategy: RotationStrategy,
  ): Promise<ProxyConfig | null> {
    if (poolId === 'none') return null;

    // Fall back to Oxylabs if Brightdata pool is degraded.
    let pool = this.pools.get(poolId);
    if (!pool && poolId === 'brightdata-residential-ca') {
      pool = this.pools.get('oxylabs-residential-ca');
      if (pool) {
        logger.warn({
          message: 'ProxyManager: falling back to Oxylabs (Brightdata not configured)',
          sourceId,
        });
      }
    }

    if (!pool) return null;

    const sessionId = this.sessionId(sourceId, strategy);

    return {
      host:     pool.host,
      port:     pool.port,
      // Brightdata / Oxylabs sticky-session encoding: append session ID to username.
      username: `${pool.username}-session-${sessionId}`,
      password: pool.password,
      provider: pool.provider,
    };
  }

  reportSuccess(proxyKey: string): void {
    const h = this.ensureHealth(proxyKey);
    h.score = Math.min(100, h.score + 1);
  }

  reportFailure(proxyKey: string, sourceId: string, reason: BanReason): void {
    const h = this.ensureHealth(proxyKey);
    h.score = Math.max(0, h.score + SCORE_PENALTIES[reason]);

    if (reason === 'forbidden') {
      h.bannedForSources.add(sourceId);
      h.bannedAtMs.set(sourceId, Date.now());
      logger.warn({
        message:  'ProxyManager: proxy permanently banned for source',
        proxyKey, sourceId,
        score:    h.score,
      });
    } else {
      const cooldown = COOLDOWN_MS[reason];
      if (isFinite(cooldown)) {
        h.cooldownUntil.set(sourceId, Date.now() + cooldown);
      }
    }

    if (h.score < QUARANTINE_THRESHOLD) {
      logger.warn({
        message:  'ProxyManager: proxy quarantined (score below threshold)',
        proxyKey, score: h.score,
      });
    }
  }

  isHealthy(proxyKey: string, sourceId: string): boolean {
    const h = this.health.get(proxyKey);
    if (!h) return true;

    if (h.bannedForSources.has(sourceId)) {
      // Re-evaluate after 6 hours.
      const bannedAt = h.bannedAtMs.get(sourceId) ?? 0;
      if (Date.now() - bannedAt > QUARANTINE_RECHECK_MS) {
        h.bannedForSources.delete(sourceId);
        h.score = 50; // restore partial score for re-evaluation
      } else {
        return false;
      }
    }

    const cooldown = h.cooldownUntil.get(sourceId) ?? 0;
    if (Date.now() < cooldown) return false;

    return h.score >= QUARANTINE_THRESHOLD;
  }

  /** Emit ban-rate metric for M1d. Called by health monitor. */
  getBanRate(sourceId: string): number {
    let total = 0;
    let banned = 0;
    for (const h of this.health.values()) {
      total++;
      if (h.bannedForSources.has(sourceId) || h.score < QUARANTINE_THRESHOLD) banned++;
    }
    return total === 0 ? 0 : banned / total;
  }

  // ── Private helpers ───────────────────────────────────────

  private initPools(): void {
    if (process.env.BRIGHTDATA_HOST) {
      this.pools.set('brightdata-residential-ca', {
        provider: 'brightdata',
        host:     process.env.BRIGHTDATA_HOST,
        port:     Number(process.env.BRIGHTDATA_PORT ?? 22225),
        username: process.env.BRIGHTDATA_USERNAME ?? '',
        password: process.env.BRIGHTDATA_PASSWORD ?? '',
      });
    }

    if (process.env.OXYLABS_HOST) {
      this.pools.set('oxylabs-residential-ca', {
        provider: 'oxylabs',
        host:     process.env.OXYLABS_HOST,
        port:     Number(process.env.OXYLABS_PORT ?? 7777),
        username: process.env.OXYLABS_USERNAME ?? '',
        password: process.env.OXYLABS_PASSWORD ?? '',
      });
    }
  }

  /**
   * Builds a session identifier whose uniqueness depends on rotation strategy:
   *   PER_REQUEST → unique every call (new proxy each request)
   *   PER_SESSION → changes every ~minute (one proxy per listing page session)
   *   PER_RUN     → changes every 6 hours (one proxy for the full run)
   */
  private sessionId(sourceId: string, strategy: RotationStrategy): string {
    switch (strategy) {
      case 'PER_REQUEST':
        return `${sourceId}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      case 'PER_SESSION':
        return `${sourceId}-${Math.floor(Date.now() / 60_000)}`;
      case 'PER_RUN':
        return `${sourceId}-${Math.floor(Date.now() / (6 * 3_600_000))}`;
    }
  }

  private ensureHealth(proxyKey: string): IPHealth {
    if (!this.health.has(proxyKey)) {
      this.health.set(proxyKey, {
        score:            100,
        bannedForSources: new Set(),
        cooldownUntil:    new Map(),
        bannedAtMs:       new Map(),
      });
    }
    return this.health.get(proxyKey)!;
  }
}
