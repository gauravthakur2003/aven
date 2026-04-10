// M2a Fast Path — Direct extraction from structured Kijiji JSON
// Bypasses LLM when all critical fields are already present in structured data.
// Only applies to source_id='kijiji-ca' with raw_content_type='json'.
// Falls back to LLM if eligibility check fails.

import { RawPayload, ExtractedFields } from './types';
import { ExtractionResult } from './m2a-extractor';

const NORMALISATION_VERSION = '1.0.0';

// Minimum fields required to skip LLM
const REQUIRED_FIELDS = ['make', 'model', 'year', 'priceCents'] as const;
// At least 3 of these "rich" fields must be present
const RICH_FIELDS     = ['mileageKm', 'colour', 'bodyType', 'transmission', 'fuelType', 'drivetrain'] as const;

export function isFastPathEligible(payload: RawPayload): boolean {
  if (payload.source_id !== 'kijiji-ca') return false;
  if (payload.raw_content_type !== 'json') return false;
  try {
    const d = JSON.parse(payload.raw_content) as Record<string, unknown>;
    for (const f of REQUIRED_FIELDS) { if (d[f] == null) return false; }
    if (d['make'] === 'othrmake' || d['model'] === 'othrmdl') return false;
    const year = Number(d['year']);
    if (year < 1950 || year > 2027) return false;
    if (RICH_FIELDS.filter(f => d[f] != null).length < 3) return false;
    return true;
  } catch { return false; }
}

export function fastPathExtract(payload: RawPayload): ExtractionResult {
  const t0 = Date.now();
  const d  = JSON.parse(payload.raw_content) as Record<string, unknown>;

  // Province from location string e.g. "Mississauga, ON" → 'ON'
  const locationStr     = String(d['location'] ?? '');
  const provinceMatcher = locationStr.match(/,\s*([A-Z]{2})$/);
  const province        = provinceMatcher ? provinceMatcher[1] : null;
  const city            = locationStr.replace(/,\s*[A-Z]{2}$/, '').trim() || locationStr || null;

  const priceCents = d['priceCents'] != null ? Number(d['priceCents']) : null;
  const price      = priceCents != null && priceCents > 0 ? Math.round(priceCents / 100) : null;

  // Seller type mapping
  const sellerRaw   = String(d['_sellerType'] ?? '');
  const seller_type = sellerRaw === 'delr' ? 'Dealer' : sellerRaw === 'ownr' ? 'Private' : null;

  // Drivetrain normalization
  const driveRaw   = String(d['drivetrain'] ?? '').toLowerCase();
  const drivetrain = driveRaw.includes('awd') ? 'AWD'
    : driveRaw.includes('4wd') || driveRaw.includes('4x4') ? '4WD'
    : driveRaw.includes('fwd') || driveRaw.includes('front') ? 'FWD'
    : driveRaw.includes('rwd') || driveRaw.includes('rear') ? 'RWD'
    : null;

  // Transmission normalization
  const transRaw     = String(d['transmission'] ?? '').toLowerCase();
  const transmission = transRaw.includes('auto') ? 'Automatic'
    : transRaw.includes('manual') || transRaw.includes('stick') ? 'Manual'
    : transRaw.includes('cvt') ? 'CVT'
    : null;

  const fields: ExtractedFields = {
    make:                  String(d['make'] ?? ''),
    model:                 String(d['model'] ?? ''),
    year:                  Number(d['year']),
    trim:                  d['trim']           != null ? String(d['trim'])           : null,
    body_type:             d['bodyType']       != null ? String(d['bodyType'])       : null,
    drivetrain:            drivetrain as ExtractedFields['drivetrain'],
    fuel_type:             d['fuelType']       != null ? String(d['fuelType'])       : null,
    transmission:          transmission as ExtractedFields['transmission'],
    colour_exterior:       d['colour']         != null ? String(d['colour'])         : null,
    colour_interior:       d['colourInterior'] != null ? String(d['colourInterior']) : null,
    engine:                null,
    doors:                 d['doors']          != null ? Number(d['doors'])          : null,
    seats:                 d['seats']          != null ? Number(d['seats'])          : null,
    vin:                   d['vin']            != null ? String(d['vin'])            : null,
    condition:             'Used',
    mileage_km:            d['mileageKm']      != null ? Number(d['mileageKm'])      : null,
    mileage_unit_original: 'km',
    safetied:              null,
    accidents:             null,
    owners:                null,
    price,
    price_type:            price != null ? 'PURCHASE_PRICE' : 'UNKNOWN',
    price_qualifier:       null,
    price_raw:             priceCents != null ? `$${(priceCents / 100).toLocaleString('en-CA')}` : '',
    price_currency_orig:   'CAD',
    payment_amount:        null,
    payment_frequency:     null,
    city:                  city ?? 'Unknown',
    province,
    seller_type:           seller_type as ExtractedFields['seller_type'],
    dealer_name:           null,
    listed_date:           null,
    description:           d['description'] != null ? String(d['description']).slice(0, 2000) : null,
    confidence: {
      make:       'high',
      model:      'high',
      year:       'high',
      price:      price != null ? 'high' : 'none',
      mileage_km: d['mileageKm'] != null ? 'high' : 'none',
      safetied:   'none',
      city:       city ? 'high' : 'low',
    },
  };

  return {
    fields,
    model:               'fast-path/kijiji-structured',
    promptTokens:         0,
    completionTokens:     0,
    latencyMs:            Date.now() - t0,
    normalisationVersion: NORMALISATION_VERSION,
  };
}
