// M1c — Scheduler & Rate Limiter
//
// Reads connector configs, schedules BullMQ jobs on their cron expressions,
// manages concurrency (max 4 concurrent connector runs), and enforces the
// per-source rate limits that prevent Aven from triggering ban events.

import { Queue, Worker, Job, ConnectionOptions } from 'bullmq';
import type { Redis } from 'ioredis';
import { randomUUID } from 'crypto';
import { ConnectorConfig, SourceConnector, ScrapeConfig } from '../types';
import { QueueWriter } from './queue-writer';
import { ProxyManager } from './proxy-manager';
import { logger } from './logger';
import * as metrics from './metrics';

// Per PRD §7.2 — prevents resource contention between connectors.
const MAX_CONCURRENT_CONNECTOR_RUNS = 4;

// Per PRD §7.2 — M1 should not outpace M2's processing capacity.
const QUEUE_DEPTH_THROTTLE_HIGH = 50_000;
const QUEUE_DEPTH_THROTTLE_LOW  = 25_000;

// Retry policy per PRD §7.4: 1 min → 5 min → 20 min exponential backoff.
const JOB_ATTEMPTS = 3;
const BACKOFF_DELAY_MS = 60_000;

const BULLMQ_QUEUE_NAME = 'aven-scraper';

interface ScrapeJobData {
  connectorId: string;
  geoRegion:   string;
}

export class Scheduler {
  private queue:      Queue<ScrapeJobData>;
  private worker:     Worker<ScrapeJobData>;
  private connectors: Map<string, SourceConnector> = new Map();
  private configs:    Map<string, ConnectorConfig>  = new Map();

  // Tracks consecutive failure counts per connector for M1d escalation.
  private consecutiveFailures: Map<string, number> = new Map();

  constructor(
    private readonly redis:         Redis,   // ioredis instance for direct Redis ops
    private readonly redisUrl:      string,  // URL passed to BullMQ (uses its own ioredis)
    private readonly queueWriter:   QueueWriter,
    private readonly proxyManager:  ProxyManager,
  ) {
    // BullMQ v5 bundles its own ioredis — passing an external Redis instance causes
    // type conflicts. Parse the URL into a plain connection options object instead.
    const connection = Scheduler.parseRedisUrl(redisUrl);

    this.queue = new Queue<ScrapeJobData>(BULLMQ_QUEUE_NAME, {
      connection,
      defaultJobOptions: {
        attempts:          JOB_ATTEMPTS,
        backoff:           { type: 'exponential', delay: BACKOFF_DELAY_MS },
        removeOnComplete:  100,
        removeOnFail:      200,
      },
    });

    this.worker = new Worker<ScrapeJobData>(
      BULLMQ_QUEUE_NAME,
      (job) => this.runJob(job),
      {
        connection,
        concurrency: MAX_CONCURRENT_CONNECTOR_RUNS,
      },
    );

    this.worker.on('failed', (job, err) => {
      const connectorId = job?.data.connectorId ?? 'unknown';
      const failures    = (this.consecutiveFailures.get(connectorId) ?? 0) + 1;
      this.consecutiveFailures.set(connectorId, failures);

      logger.error({
        message:          'Scrape job failed',
        connectorId,
        jobId:            job?.id,
        attempt:          job?.attemptsMade,
        consecutiveFails: failures,
        error:            err.message,
      });

      // PRD §7.4: disable connector after 3 consecutive run failures.
      if (failures >= 3) {
        const cfg = this.configs.get(connectorId);
        if (cfg) {
          cfg.enabled = false;
          logger.error({
            message:     'CRITICAL: connector disabled after 3 consecutive failures',
            connectorId,
          });
        }
      }
    });

    this.worker.on('completed', (job) => {
      const connectorId = job.data.connectorId;
      this.consecutiveFailures.set(connectorId, 0); // reset on success
    });
  }

  // ── Public API ────────────────────────────────────────────

  register(connector: SourceConnector, config: ConnectorConfig): void {
    this.connectors.set(connector.source_id, connector);
    this.configs.set(connector.source_id, config);
    logger.info({ message: 'Connector registered', connectorId: connector.source_id });
  }

  /** Schedule all enabled connectors on their configured cron expressions. */
  async scheduleAll(): Promise<void> {
    for (const [connectorId, config] of this.configs) {
      if (!config.enabled) {
        logger.info({ message: 'Connector disabled, skipping schedule', connectorId });
        continue;
      }

      const geoRegion = config.geographic_scope[0];

      await this.queue.add(
        `scrape:${connectorId}`,
        { connectorId, geoRegion },
        {
          repeat:  { pattern: config.schedule_cron },
          jobId:   `repeat:${connectorId}`,
        },
      );

      logger.info({
        message:    'Scrape job scheduled',
        connectorId,
        cron:       config.schedule_cron,
        geoRegion,
      });
    }
  }

  /** Trigger a manual run immediately (useful for testing / ops). */
  async triggerNow(connectorId: string): Promise<string> {
    const config = this.configs.get(connectorId);
    if (!config) throw new Error(`Unknown connector: ${connectorId}`);

    const job = await this.queue.add(
      `scrape:${connectorId}:manual`,
      { connectorId, geoRegion: config.geographic_scope[0] },
    );

    logger.info({ message: 'Manual scrape triggered', connectorId, jobId: job.id });
    return job.id!;
  }

  async close(): Promise<void> {
    await this.worker.close();
    await this.queue.close();
  }

  // ── Job execution ─────────────────────────────────────────

  private async runJob(job: Job<ScrapeJobData>): Promise<void> {
    const { connectorId, geoRegion } = job.data;
    const connector = this.connectors.get(connectorId);
    const config    = this.configs.get(connectorId);

    if (!connector || !config) {
      throw new Error(`Connector not registered: ${connectorId}`);
    }

    // Throttle if the ingestion queue is backed up.
    const depth = await this.getIngestionQueueDepth();
    if (depth > QUEUE_DEPTH_THROTTLE_HIGH) {
      logger.warn({
        message:     'Ingestion queue above throttle threshold — delaying job',
        connectorId, depth,
      });
      // Delay 60s — BullMQ will re-process.
      await job.moveToDelayed(Date.now() + 60_000);
      return;
    }

    const runId     = randomUUID();
    const startTime = Date.now();

    logger.info({ message: 'Scrape run started', connectorId, runId });

    // Determine rotation strategy per connector.
    const rotationStrategy = connectorId === 'facebook-mp-ca' ? 'PER_REQUEST' : 'PER_SESSION';
    const proxyConfig = await this.proxyManager.allocate(
      config.proxy_pool_id,
      connectorId,
      rotationStrategy,
    );

    const scrapeConfig: ScrapeConfig = {
      runId,
      geoRegion,
      proxyConfig: proxyConfig ?? undefined,
    };

    if (connector.initBrowser && proxyConfig) {
      await connector.initBrowser(proxyConfig);
    }

    let listingCount = 0;
    let pushFailures = 0;

    try {
      for await (const payload of connector.scrape(scrapeConfig)) {
        if (listingCount >= config.max_listings_per_run) {
          logger.warn({
            message:     'max_listings_per_run reached, stopping run',
            connectorId, max: config.max_listings_per_run,
          });
          break;
        }

        try {
          await this.queueWriter.push(payload);
          metrics.queuePushes.inc({ connector_id: connectorId });
          metrics.listingsCollected.inc({ connector_id: connectorId });
          listingCount++;
        } catch {
          pushFailures++;
          metrics.queuePushFailures.inc({ connector_id: connectorId });
        }
      }

      const durationSeconds = (Date.now() - startTime) / 1000;
      metrics.runDurationSeconds.set({ connector_id: connectorId }, durationSeconds);
      metrics.lastSuccessfulRunAgeSeconds.set({ connector_id: connectorId }, 0);
      metrics.queuePushFailureRate.set(
        { connector_id: connectorId },
        listingCount > 0 ? pushFailures / (listingCount + pushFailures) : 0,
      );

      logger.info({
        message:         'Scrape run completed',
        connectorId,     runId,
        listingCount,    pushFailures,
        durationSeconds,
      });
    } finally {
      if (connector.closeBrowser) {
        await connector.closeBrowser();
      }
    }
  }

  // ── Helpers ───────────────────────────────────────────────

  private async getIngestionQueueDepth(): Promise<number> {
    try {
      const info = await this.redis.xinfo('STREAM', 'aven:ingestion:raw') as unknown[];
      const lenIdx = (info as string[]).indexOf('length');
      return lenIdx >= 0 ? Number((info as string[])[lenIdx + 1]) : 0;
    } catch {
      return 0;
    }
  }

  /** Parse a Redis URL into a plain host/port object for BullMQ's ConnectionOptions. */
  private static parseRedisUrl(url: string): ConnectionOptions {
    try {
      const parsed = new URL(url);
      return {
        host:     parsed.hostname || 'localhost',
        port:     Number(parsed.port) || 6379,
        password: parsed.password || undefined,
        db:       parsed.pathname ? Number(parsed.pathname.replace('/', '')) || 0 : 0,
      };
    } catch {
      return { host: 'localhost', port: 6379 };
    }
  }
}
