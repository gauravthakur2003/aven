// PostgreSQL connection pool.
// All M2 sub-modules share a single pool instance via this module.

import { Pool } from 'pg';
import { logger } from './logger';

let pool: Pool | null = null;

export function getPool(): Pool {
  if (!pool) {
    const connectionString = process.env.DATABASE_URL;
    if (!connectionString) throw new Error('DATABASE_URL env var is required');

    pool = new Pool({
      connectionString,
      max:              10,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 5_000,
    });

    pool.on('error', (err: Error) => {
      logger.error({ message: 'Postgres pool error', error: err.message });
    });
  }
  return pool;
}

export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}
