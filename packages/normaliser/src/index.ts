// Aven — Module 2: Normalisation Engine
// Entry point. Starts N worker processes, each consuming from the Redis stream,
// calling the LLM extraction pipeline (M2a→M2b→M2d→M2c→M2e), and writing to Postgres.

import * as dotenv from 'dotenv';
import * as path   from 'path';
dotenv.config({ path: path.join(__dirname, '../.env'), override: true });
import Redis from 'ioredis';
import { getPool, closePool } from './lib/db';
import { StreamConsumer }     from './lib/redis-consumer';
import { logger }             from './lib/logger';
import { extractFields }      from './m2a-extractor';
import { validateAndStandardise } from './m2b-validator';
import { redactPII }          from './m2d-redactor';
import { computeConfidence }  from './m2c-scorer';
import { routeAndWrite }      from './m2e-router';

const WORKER_COUNT = Number(process.env.WORKER_COUNT ?? 2);

async function main(): Promise<void> {
  const redisUrl = process.env.REDIS_URL ?? 'redis://localhost:6379';

  const redis = new Redis(redisUrl, {
    maxRetriesPerRequest: null,
    retryStrategy: (times: number) => times > 3 ? null : Math.min(times * 500, 2000),
    enableOfflineQueue: false,
  });

  await new Promise<void>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('Redis not reachable at ' + redisUrl)), 3000);
    redis.once('ready',  () => { clearTimeout(t); resolve(); });
    redis.once('close',  () => { clearTimeout(t); reject(new Error('Redis connection closed')); });
  }).catch((err: Error) => {
    logger.error({ message: err.message + ' — start Redis first: docker compose up redis' });
    process.exit(1);
  });

  const pool = getPool();

  // Verify DB connectivity
  try {
    await pool.query('SELECT 1');
    logger.info({ message: 'Postgres connected' });
  } catch (err) {
    logger.error({ message: 'Postgres not reachable — run: npm run db:migrate', error: (err as Error).message });
    process.exit(1);
  }

  logger.info({ message: 'Aven normaliser started', workerCount: WORKER_COUNT });

  // Launch worker pool
  const workers = Array.from({ length: WORKER_COUNT }, (_, i) =>
    runWorker(redis, `worker-${process.pid}-${i}`)
  );
  await Promise.all(workers);
}

async function runWorker(redis: Redis, workerId: string): Promise<void> {
  const pool     = getPool();
  const consumer = new StreamConsumer(redis, workerId);
  await consumer.init();

  logger.info({ message: 'Worker started', workerId });

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const item = await consumer.next();
    if (!item) continue;  // nothing in stream — loop and block again

    const { id, payload } = item;

    try {
      // M2a — LLM extraction
      const extraction = await extractFields(payload);

      // M2b — field validation & standardisation
      const validated = validateAndStandardise(extraction.fields);

      // M2d — PII redaction
      const redaction = redactPII(validated.description);
      validated.description = redaction.text;

      // M2c — confidence scoring
      const scored = computeConfidence(validated);

      // M2e — route and write to Postgres
      await routeAndWrite(pool, payload, scored, extraction, redaction, redaction.failed);

      // Acknowledge message — safe to discard from stream
      await consumer.ack(id);

    } catch (err) {
      const e = err as Error;
      logger.error({
        message:    'Worker processing error',
        workerId,
        payload_id: payload.payload_id,
        error:      e.message,
      });
      // Don't ack — message will be reclaimed after PENDING_TIMEOUT (30s)
      // and retried by another worker. After 3 worker retries, BullMQ
      // moves it to the dead letter queue.
    }
  }
}

// ── Graceful shutdown ─────────────────────────────────────

const shutdown = async (signal: string): Promise<void> => {
  logger.info({ message: 'Shutting down normaliser', signal });
  await closePool();
  process.exit(0);
};

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT',  () => void shutdown('SIGINT'));

main().catch((err: Error) => {
  logger.error({ message: 'Fatal startup error', error: err.message });
  process.exit(1);
});
