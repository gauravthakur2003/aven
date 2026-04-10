// Structured JSON logger — identical contract to M1.

type Level = 'INFO' | 'WARN' | 'ERROR' | 'DEBUG';

function log(level: Level, fields: Record<string, unknown>): void {
  process.stdout.write(JSON.stringify({ timestamp: new Date().toISOString(), level, ...fields }) + '\n');
}

export const logger = {
  info:  (fields: Record<string, unknown>) => log('INFO',  fields),
  warn:  (fields: Record<string, unknown>) => log('WARN',  fields),
  error: (fields: Record<string, unknown>) => log('ERROR', fields),
  debug: (fields: Record<string, unknown>) => log('DEBUG', fields),
};
