// Aven — Module 1: Data Scraping Engine
// Entry point. Reads CONNECTOR_ID from environment and starts
// the configured connector + scheduler for that container.
//
// Each connector runs in its own container (per architectural requirement BS-01).
// The CONNECTOR_ID env var selects which connector this instance runs.

import 'dotenv/config';
import Redis from 'ioredis';
import { QueueWriter }      from './lib/queue-writer';
import { ProxyManager }     from './lib/proxy-manager';
import { Scheduler }        from './lib/scheduler';
import { HealthMonitor }    from './lib/health-monitor';
import { logger }           from './lib/logger';

import { KijijiConnector }             from './connectors/kijiji-ca';
import { FacebookMarketplaceConnector } from './connectors/facebook-mp-ca';
import { AutoTraderConnector }         from './connectors/autotrader-ca';

import {
  KIJIJI_CONFIG,
  FACEBOOK_CONFIG,
  AUTOTRADER_CONFIG,
} from './config/connector-configs';
import { ConnectorConfig, SourceConnector } from './types';

// ── Bootstrap ─────────────────────────────────────────────

async function main(): Promise<void> {
  const connectorId = process.env.CONNECTOR_ID;
  if (!connectorId) {
    logger.error({ message: 'CONNECTOR_ID env var is required' });
    process.exit(1);
  }

  const redisUrl = process.env.REDIS_URL ?? 'redis://localhost:6379';

  // maxRetriesPerRequest: null is required by BullMQ.
  // retryStrategy: give up after 3 attempts instead of retrying forever,
  // so a missing Redis doesn't spam the terminal on startup.
  const redis = new Redis(redisUrl, {
    maxRetriesPerRequest: null,
    retryStrategy: (times: number) => times > 3 ? null : Math.min(times * 500, 2000),
    enableOfflineQueue: false,
  });

  let redisReady = false;
  redis.on('ready', () => { redisReady = true; });
  redis.on('error', (err: Error & { code?: string }) => {
    if (err.code === 'ECONNREFUSED' && !redisReady) {
      // Only log once — suppress the repeated reconnect noise.
    } else {
      logger.error({ message: 'Redis connection error', error: err.message });
    }
  });

  // Wait up to 3s for Redis; abort cleanly if unavailable.
  await new Promise<void>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('Redis not reachable at ' + redisUrl)), 3000);
    redis.once('ready', () => { clearTimeout(t); resolve(); });
    redis.once('close', () => { clearTimeout(t); reject(new Error('Redis connection closed')); });
  }).catch((err: Error) => {
    logger.error({ message: err.message + ' — start Redis first: docker-compose up redis' });
    process.exit(1);
  });

  const queueWriter    = new QueueWriter(redis);
  const proxyManager   = new ProxyManager();
  const scheduler      = new Scheduler(redis, redisUrl, queueWriter, proxyManager);

  const healthPort     = Number(process.env.HEALTH_MONITOR_PORT ?? 9090);
  const healthMonitor  = new HealthMonitor();
  healthMonitor.start(healthPort);

  // ── Connector registry ───────────────────────────────────

  const registry: Record<string, { connector: SourceConnector; config: ConnectorConfig }> = {
    'kijiji-ca': {
      connector: new KijijiConnector(KIJIJI_CONFIG),
      config:    KIJIJI_CONFIG,
    },
    'facebook-mp-ca': {
      connector: new FacebookMarketplaceConnector(FACEBOOK_CONFIG),
      config:    FACEBOOK_CONFIG,
    },
    'autotrader-ca': {
      connector: new AutoTraderConnector(),
      config:    AUTOTRADER_CONFIG,
    },
  };

  const entry = registry[connectorId];
  if (!entry) {
    logger.error({ message: `Unknown CONNECTOR_ID: ${connectorId}` });
    process.exit(1);
  }

  if (!entry.config.enabled) {
    logger.error({ message: `Connector is disabled: ${connectorId}` });
    process.exit(1);
  }

  // ── Health check before starting ────────────────────────

  const health = await entry.connector.healthCheck();
  logger.info({ message: 'Connector health check', connectorId, result: health });

  if (health.status === 'FAILED') {
    logger.error({ message: 'Connector failed health check — aborting', connectorId });
    process.exit(1);
  }

  // ── Register and schedule ────────────────────────────────

  scheduler.register(entry.connector, entry.config);
  await scheduler.scheduleAll();

  // Set M1d baselines (in production: read from Postgres 7-day rolling averages).
  healthMonitor.setScheduledInterval(connectorId, 6 * 3600); // 6-hour default

  logger.info({ message: 'Aven scraper started', connectorId });

  // ── Graceful shutdown ────────────────────────────────────

  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ message: 'Shutting down', connectorId, signal });
    await scheduler.close();
    await healthMonitor.stop();
    await redis.quit();
    process.exit(0);
  };

  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT',  () => void shutdown('SIGINT'));
}

main().catch((err: Error) => {
  logger.error({ message: 'Fatal startup error', error: err.message, stack: err.stack });
  process.exit(1);
});
