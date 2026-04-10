// M2h — Car Alert Sender
// Checks all active saved searches against a newly-published listing.
// If any match, sends an email notification via Resend API.
// Called (fire-and-forget) from m2e-router after a listing goes active.

import { Pool } from 'pg';
import { logger } from './lib/logger';

const RESEND_API_KEY = process.env.RESEND_API_KEY ?? '';
const FROM_EMAIL     = process.env.ALERT_FROM_EMAIL ?? 'alerts@aven.ca';
const SITE_URL       = process.env.SITE_URL ?? 'https://aven-dashboard-production.up.railway.app';

interface SavedSearch {
  id: string; email: string; make: string | null; model: string | null;
  max_price: number | null; min_year: number | null;
}

interface ListingForAlert {
  id: string; make: string; model: string; year: number; trim: string | null;
  price: number | null; price_type: string; mileage_km: number | null;
  city: string; province: string | null; source_url: string; photo_urls: string[] | null;
}

// ── Main: check matches and send alerts ───────────────────

export async function checkAndSendAlerts(pool: Pool, listing: ListingForAlert): Promise<void> {
  if (!RESEND_API_KEY) return; // Silently skip if not configured

  let searches: SavedSearch[] = [];
  try {
    const { rows } = await pool.query<SavedSearch>(`
      SELECT id, email, make, model, max_price, min_year
      FROM public.saved_searches
      WHERE is_active = TRUE
        AND (make      IS NULL OR LOWER(make)  = LOWER($1))
        AND (max_price IS NULL OR $2 IS NULL OR $2 <= max_price)
        AND (min_year  IS NULL OR $3 IS NULL OR $3 >= min_year)
    `, [listing.make, listing.price, listing.year]);
    searches = rows;
  } catch (err) {
    // Table might not exist yet — silent skip
    logger.warn({ message: 'checkAndSendAlerts: could not query saved_searches', error: (err as Error).message });
    return;
  }

  if (searches.length === 0) return;

  for (const search of searches) {
    try {
      await sendAlertEmail(search, listing);
      logger.info({ message: 'Alert sent', to: search.email, listing_id: listing.id });
    } catch (err) {
      logger.warn({ message: 'Alert email failed', to: search.email, error: (err as Error).message });
    }
  }
}

// ── Email builder ─────────────────────────────────────────

async function sendAlertEmail(search: SavedSearch, listing: ListingForAlert): Promise<void> {
  const title    = [listing.year, listing.make, listing.model, listing.trim].filter(Boolean).join(' ');
  const price    = listing.price
    ? `$${listing.price.toLocaleString('en-CA')} CAD`
    : listing.price_type === 'CALL_FOR_PRICE' ? 'Call for price' : 'Price not listed';
  const mileage  = listing.mileage_km ? `${listing.mileage_km.toLocaleString()} km` : 'Mileage not listed';
  const location = [listing.city, listing.province].filter(Boolean).join(', ');
  const image    = listing.photo_urls?.[0] ?? '';
  const unsubUrl = `${SITE_URL}/unsubscribe?id=${search.id}`;

  const html = `
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
  <div style="max-width:560px;margin:32px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08);">
    ${image ? `<img src="${image}" style="width:100%;height:220px;object-fit:cover;display:block;" alt="${title}">` : ''}
    <div style="padding:28px 32px;">
      <div style="font-size:11px;font-weight:600;letter-spacing:2px;color:#16a34a;text-transform:uppercase;margin-bottom:8px;">New Match Found 🎯</div>
      <h1 style="font-size:22px;font-weight:700;color:#111;margin:0 0 16px;">${title}</h1>
      <table style="width:100%;border-collapse:collapse;margin-bottom:24px;">
        <tr>
          <td style="padding:10px 0;border-bottom:1px solid #f0f0f0;color:#888;font-size:13px;width:40%;">Price</td>
          <td style="padding:10px 0;border-bottom:1px solid #f0f0f0;color:#111;font-size:15px;font-weight:600;">${price}</td>
        </tr>
        <tr>
          <td style="padding:10px 0;border-bottom:1px solid #f0f0f0;color:#888;font-size:13px;">Mileage</td>
          <td style="padding:10px 0;border-bottom:1px solid #f0f0f0;color:#111;font-size:14px;">${mileage}</td>
        </tr>
        <tr>
          <td style="padding:10px 0;color:#888;font-size:13px;">Location</td>
          <td style="padding:10px 0;color:#111;font-size:14px;">${location}</td>
        </tr>
      </table>
      <a href="${listing.source_url}" style="display:block;text-align:center;background:#16a34a;color:#fff;text-decoration:none;border-radius:8px;padding:14px 24px;font-size:15px;font-weight:600;margin-bottom:24px;">View Listing →</a>
      <p style="font-size:12px;color:#aaa;text-align:center;margin:0;">
        You're receiving this because you set up a car alert on Aven.<br>
        <a href="${unsubUrl}" style="color:#aaa;">Unsubscribe</a>
      </p>
    </div>
  </div>
</body>
</html>`;

  const response = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${RESEND_API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ from: FROM_EMAIL, to: [search.email], subject: `New match: ${title} — ${price}`, html }),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Resend API ${response.status}: ${body.slice(0, 200)}`);
  }
}
