// M2f — CARFAX Canada VIN Lookup
// Fetches the free public preview from CARFAX Canada for a given VIN.
// Only called when VIN is known and fields like accidents/owners are missing.
// Gracefully returns null if the page is unavailable or the VIN yields no data.

import axios from 'axios';

export interface CarfaxResult {
  accidents:  number | null;
  owners:     number | null;
  hasLien:    boolean | null;
  stolen:     boolean | null;
  hasRecalls: boolean | null;
  source:     'carfax-ca';
  fetchedAt:  string;
}

const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Language': 'en-CA,en;q=0.9',
};

export async function lookupCarfax(vin: string): Promise<CarfaxResult | null> {
  if (!vin || vin.length !== 17) return null;
  try {
    const url = `https://www.carfax.ca/en/vehicle-history-report/${vin}`;
    const res = await axios.get(url, { headers: HEADERS, timeout: 12_000, maxRedirects: 5 });
    return parseCarfaxHtml(res.data as string, vin);
  } catch {
    // Network error, 404, or anti-bot block — not fatal, just skip
    return null;
  }
}

function parseCarfaxHtml(html: string, _vin: string): CarfaxResult | null {
  const result: CarfaxResult = {
    accidents: null, owners: null, hasLien: null, stolen: null, hasRecalls: null,
    source: 'carfax-ca', fetchedAt: new Date().toISOString(),
  };

  // Accident count  e.g. "2 accident records" / "No accident records"
  const accMatch = html.match(/(\d+)\s+accident\s+record/i);
  if (accMatch)                           result.accidents = parseInt(accMatch[1], 10);
  else if (/no accident record/i.test(html)) result.accidents = 0;

  // Owner count  e.g. "3 previous owners" / "1 owner"
  const ownMatch = html.match(/(\d+)\s+(?:previous\s+)?owner/i);
  if (ownMatch) result.owners = parseInt(ownMatch[1], 10);

  if (/lien\s+registered|active\s+lien/i.test(html))  result.hasLien    = true;
  else if (/no\s+lien\s+registered/i.test(html))       result.hasLien    = false;

  if (/reported\s+stolen|theft\s+record/i.test(html))  result.stolen     = true;
  else if (/not\s+reported\s+stolen/i.test(html))      result.stolen     = false;

  if (/open\s+recall|safety\s+recall/i.test(html))     result.hasRecalls = true;
  else if (/no\s+(?:open\s+)?recall/i.test(html))      result.hasRecalls = false;

  const hasData = result.accidents !== null || result.owners !== null
               || result.hasLien   !== null || result.stolen  !== null;
  return hasData ? result : null;
}
