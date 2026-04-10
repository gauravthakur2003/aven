// QueueWriter — the only permitted way for connectors to push to the ingestion queue.
// Direct Redis access from connectors is NOT permitted (BS-05).
// Validates the RawPayload schema before writing.

import type { Redis } from 'ioredis';
import { RawPayload } from '../types';
import { logger } from './logger';

const STREAM_NAME = 'aven:ingestion:raw';
const MAX_STREAM_LENGTH = 500_000;

const REQUIRED_FIELDS: ReadonlyArray<keyof RawPayload> = [
  'payload_id',
  'source_id',
  'source_category',
  'listing_url',
  'scrape_timestamp',
  'connector_version',
  'raw_content',
  'raw_content_type',
  'listing_images',
  'geo_region',
  'scrape_run_id',
  'http_status',
  'proxy_used',
  'requires_auth',
];

export class QueueWriter {
  constructor(private readonly redis: Redis) {}

  async push(payload: RawPayload): Promise<void> {
    this.validate(payload);

    try {
      await this.redis.xadd(
        STREAM_NAME,
        'MAXLEN', '~', String(MAX_STREAM_LENGTH),
        '*',
        // Flatten the payload as alternating field/value strings for the Redis stream.
        'payload_id',       payload.payload_id,
        'source_id',        payload.source_id,
        'source_category',  payload.source_category,
        'listing_url',      payload.listing_url,
        'scrape_timestamp', payload.scrape_timestamp,
        'connector_version',payload.connector_version,
        'raw_content',      payload.raw_content,
        'raw_content_type', payload.raw_content_type,
        'listing_images',   JSON.stringify(payload.listing_images),
        'geo_region',       payload.geo_region,
        'scrape_run_id',    payload.scrape_run_id,
        'http_status',      String(payload.http_status),
        'proxy_used',       String(payload.proxy_used),
        'requires_auth',    String(payload.requires_auth),
        'is_dealer_listing',payload.is_dealer_listing === null
                              ? 'null'
                              : String(payload.is_dealer_listing),
      );
    } catch (err) {
      logger.error({
        message:    'QueueWriter: failed to push to ingestion queue',
        payload_id: payload.payload_id,
        source_id:  payload.source_id,
        error:      (err as Error).message,
      });
      throw err;
    }
  }

  private validate(payload: RawPayload): void {
    for (const field of REQUIRED_FIELDS) {
      if (payload[field] === undefined) {
        throw new Error(`RawPayload is missing required field: ${field}`);
      }
    }

    if (!Array.isArray(payload.listing_images)) {
      throw new Error('listing_images must be an array, not null');
    }

    if (!payload.listing_url.startsWith('http')) {
      throw new Error(`listing_url does not look like a URL: ${payload.listing_url}`);
    }
  }
}
