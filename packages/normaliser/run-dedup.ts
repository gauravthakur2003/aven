import { Pool } from 'pg';
import { runDeduplication } from './src/deduplicator';

const pool = new Pool({ database: 'aven_dev', user: 'gauravthakur' });
runDeduplication(pool, console.log).then(stats => {
  console.log('Done:', stats);
  pool.end();
});
