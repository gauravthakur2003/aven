// Structured JSON logger.
// All logs include: timestamp, level, connector_id (where applicable).
// No console.log permitted in connectors — use this module exclusively.

type LogData = Record<string, unknown>;

function log(level: string, data: LogData): void {
  const entry = JSON.stringify({
    timestamp: new Date().toISOString(),
    level,
    ...data,
  });

  if (level === 'ERROR') {
    process.stderr.write(entry + '\n');
  } else {
    process.stdout.write(entry + '\n');
  }
}

export const logger = {
  info:  (data: LogData) => log('INFO', data),
  warn:  (data: LogData) => log('WARN', data),
  error: (data: LogData) => log('ERROR', data),
};
