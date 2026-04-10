// Redis stream consumer for the M2 normalisation worker.
// Reads from aven:ingestion:raw using a consumer group so no message is lost.
// Each worker calls next() in a loop to get the next unprocessed payload.

import Redis from 'ioredis';
import { RawPayload } from '../types';
import { logger } from './logger';

const STREAM_KEY      = 'aven:ingestion:raw';
const GROUP_NAME      = 'm2-normalisation-workers';
const BLOCK_MS        = 5_000;   // block for 5s waiting for new messages
const PENDING_TIMEOUT = 30_000;  // reclaim messages idle > 30s (crashed workers)

export class StreamConsumer {
  private readonly consumerId: string;

  constructor(
    private readonly redis: Redis,
    consumerId?: string,
  ) {
    this.consumerId = consumerId ?? `worker-${process.pid}`;
  }

  // Create the consumer group if it doesn't exist.
  async init(): Promise<void> {
    try {
      await this.redis.xgroup('CREATE', STREAM_KEY, GROUP_NAME, '$', 'MKSTREAM');
      logger.info({ message: 'Consumer group created', group: GROUP_NAME });
    } catch (err) {
      const e = err as { message: string };
      if (e.message.includes('BUSYGROUP')) {
        // Group already exists — normal on restart.
      } else {
        throw err;
      }
    }
  }

  // Return the next message from the stream, or null if nothing is available.
  // First checks for pending (unacknowledged) messages from previous runs,
  // then falls back to BLOCK-wait for new messages.
  async next(): Promise<{ id: string; payload: RawPayload } | null> {
    // Step 1 — reclaim stale pending messages from crashed workers.
    const pending = await this.redis.xautoclaim(
      STREAM_KEY, GROUP_NAME, this.consumerId,
      PENDING_TIMEOUT, '0-0', 'COUNT', '1',
    ) as [string, [string, string[]][], string[]];

    const claimed = pending[1];
    if (claimed && claimed.length > 0) {
      return this.parseEntry(claimed[0]);
    }

    // Step 2 — read new messages, blocking up to BLOCK_MS.
    const result = await this.redis.xreadgroup(
      'GROUP', GROUP_NAME, this.consumerId,
      'COUNT', '1',
      'BLOCK', BLOCK_MS,
      'STREAMS', STREAM_KEY, '>',
    ) as Array<[string, Array<[string, string[]]>]> | null;

    if (!result || result.length === 0) return null;

    const entries = result[0][1];
    if (!entries || entries.length === 0) return null;

    return this.parseEntry(entries[0]);
  }

  // Acknowledge a successfully processed message.
  async ack(id: string): Promise<void> {
    await this.redis.xack(STREAM_KEY, GROUP_NAME, id);
  }

  private parseEntry(entry: [string, string[]]): { id: string; payload: RawPayload } | null {
    const [id, fields] = entry;
    const map: Record<string, string> = {};
    for (let i = 0; i < fields.length; i += 2) {
      map[fields[i]] = fields[i + 1];
    }
    try {
      const payload = JSON.parse(map['data']) as RawPayload;
      return { id, payload };
    } catch {
      logger.error({ message: 'Failed to parse stream entry', id });
      return null;
    }
  }
}
