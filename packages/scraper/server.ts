// Aven — Live Dashboard Server
// Streams Kijiji scraping updates to the dashboard in real-time via SSE.
//
// Run:  npx ts-node server.ts
// Open: http://localhost:3000

import * as http  from 'http';
import * as fs    from 'fs';
import * as path  from 'path';
import * as child from 'child_process';
import { KijijiConnector } from './src/connectors/kijiji-ca';
import { KIJIJI_CONFIG }   from './src/config/connector-configs';
import { randomUUID }      from 'crypto';

const PORT = 3000;

// ── SSE clients ────────────────────────────────────────────
const clients = new Set<http.ServerResponse>();

function broadcast(event: string, data: unknown): void {
  const msg = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of clients) {
    try { res.write(msg); } catch { clients.delete(res); }
  }
}

// ── Live state (kept in memory for new clients) ────────────
let scraped = 0;
const recentListings: unknown[] = [];

// ── HTTP server ────────────────────────────────────────────
const server = http.createServer((req, res) => {
  const url = req.url ?? '/';

  // SSE stream
  if (url === '/events') {
    res.writeHead(200, {
      'Content-Type':                'text/event-stream',
      'Cache-Control':               'no-cache',
      'Connection':                  'keep-alive',
      'Access-Control-Allow-Origin': '*',
    });
    res.write(':\n\n'); // initial keep-alive comment
    clients.add(res);
    req.on('close', () => clients.delete(res));

    // Send current state immediately so a late-joining browser catches up.
    res.write(`event: state\ndata: ${JSON.stringify({ scraped, recentListings })}\n\n`);
    return;
  }

  // Dashboard HTML
  if (url === '/') {
    try {
      const html = fs.readFileSync(path.join(__dirname, 'dashboard.html'), 'utf-8');
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(html);
    } catch {
      res.writeHead(500);
      res.end('dashboard.html not found');
    }
    return;
  }

  res.writeHead(404);
  res.end();
});

server.listen(PORT, () => {
  const url = `http://localhost:${PORT}`;
  console.log(`\nAven Dashboard → ${url}\n`);
  child.exec(`open "${url}"`); // auto-open in default browser (macOS)
  startScraping().catch((err: Error) => console.error('Scraper error:', err.message));
});

// ── Scraper ────────────────────────────────────────────────
async function startScraping(): Promise<void> {
  const connector = new KijijiConnector({
    ...KIJIJI_CONFIG,
    pagination_depth:  5_000,
    request_delay_ms:  [500, 1_500] as [number, number],
  });

  broadcast('status', { message: 'Scraper started · Kijiji GTA' });

  for await (const payload of connector.scrape({ runId: randomUUID(), geoRegion: 'ON-GTA' })) {
    scraped++;

    const d = JSON.parse(payload.raw_content) as Record<string, unknown>;

    const listing = {
      n:           scraped,
      title:       d.title as string,
      price:       d.priceCents != null
                     ? `$${((d.priceCents as number) / 100).toLocaleString('en-CA', { minimumFractionDigits: 0 })}`
                     : 'N/A',
      priceRating: (d.priceRating as string | null) ?? null,
      year:        (d.year        as number | null)  ?? null,
      make:        (d.make        as string | null)  ?? null,
      model:       (d.model       as string | null)  ?? null,
      mileage:     d.mileageKm != null ? `${(d.mileageKm as number).toLocaleString()} km` : null,
      seller:      d._sellerType === 'delr' ? 'Dealer'
                 : d._sellerType === 'ownr' ? 'Private'
                 : null,
      url:         payload.listing_url,
      ts:          new Date().toLocaleTimeString('en-CA', { hour: '2-digit', minute: '2-digit', second: '2-digit' }),
    };

    recentListings.unshift(listing);
    if (recentListings.length > 100) recentListings.pop();

    broadcast('listing', listing);
  }

  broadcast('done', { total: scraped });
  console.log(`\nDone — ${scraped} listings scraped.\n`);
}
