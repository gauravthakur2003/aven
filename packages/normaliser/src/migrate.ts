// Migration runner — plain SQL files, no ORM.
// Usage:
//   npm run db:migrate          Run all pending migrations
//   npm run db:status           Show migration status
//   npm run db:rollback         Roll back last migration (dev only)

import 'dotenv/config';
import * as fs   from 'fs';
import * as path from 'path';
import { Pool }  from 'pg';

const MIGRATIONS_DIR = path.join(__dirname, '../../../db/migrations');

async function getPool(): Promise<Pool> {
  const url = process.env.DATABASE_URL;
  if (!url) throw new Error('DATABASE_URL env var is required');
  return new Pool({ connectionString: url });
}

async function ensureMigrationsTable(pool: Pool): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version     VARCHAR(64) PRIMARY KEY,
      applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

async function getApplied(pool: Pool): Promise<Set<string>> {
  const { rows } = await pool.query<{ version: string }>(
    'SELECT version FROM schema_migrations ORDER BY version',
  );
  return new Set(rows.map(r => r.version));
}

async function getMigrationFiles(): Promise<string[]> {
  return fs.readdirSync(MIGRATIONS_DIR)
    .filter(f => f.endsWith('.sql'))
    .sort();
}

async function runMigrations(pool: Pool): Promise<void> {
  await ensureMigrationsTable(pool);
  const applied = await getApplied(pool);
  const files   = await getMigrationFiles();
  const pending = files.filter(f => !applied.has(f));

  if (pending.length === 0) {
    console.log('All migrations are up to date.');
    return;
  }

  for (const file of pending) {
    const sql = fs.readFileSync(path.join(MIGRATIONS_DIR, file), 'utf-8');
    console.log(`Applying ${file}...`);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(sql);
      await client.query('INSERT INTO schema_migrations (version) VALUES ($1)', [file]);
      await client.query('COMMIT');
      console.log(`  ✓ ${file}`);
    } catch (err) {
      await client.query('ROLLBACK');
      console.error(`  ✗ ${file}: ${(err as Error).message}`);
      throw err;
    } finally {
      client.release();
    }
  }
  console.log(`\n${pending.length} migration(s) applied.`);
}

async function showStatus(pool: Pool): Promise<void> {
  await ensureMigrationsTable(pool);
  const applied = await getApplied(pool);
  const files   = await getMigrationFiles();

  console.log('\nMigration status:');
  for (const file of files) {
    const status = applied.has(file) ? '✓ applied' : '○ pending';
    console.log(`  ${status}  ${file}`);
  }
}

async function rollback(pool: Pool): Promise<void> {
  await ensureMigrationsTable(pool);
  const { rows } = await pool.query<{ version: string }>(
    'SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1',
  );
  if (rows.length === 0) {
    console.log('Nothing to roll back.');
    return;
  }
  const last = rows[0].version;
  console.log(`Rolling back ${last}...`);
  await pool.query('DELETE FROM schema_migrations WHERE version = $1', [last]);
  console.log(`Rolled back ${last} from tracking table.`);
  console.log('NOTE: SQL changes are not automatically reversed. Drop and recreate objects manually if needed.');
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const pool = await getPool();

  try {
    if (args.includes('--status')) {
      await showStatus(pool);
    } else if (args.includes('--rollback')) {
      await rollback(pool);
    } else {
      await runMigrations(pool);
    }
  } finally {
    await pool.end();
  }
}

main().catch((err: Error) => {
  console.error('Migration error:', err.message);
  process.exit(1);
});
