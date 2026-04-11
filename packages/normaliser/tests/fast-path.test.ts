/**
 * M2a Fast Path — Unit Tests
 * Tests isFastPathEligible() and fastPathExtract() across normal, edge, and broken inputs.
 */

import { isFastPathEligible, fastPathExtract } from '../src/m2a-fast-path';
import { RawPayload } from '../src/types';

// ── Helpers ──────────────────────────────────────────────────────────────────

function makePayload(overrides: Partial<Record<string, unknown>> = {}): RawPayload {
  const data = {
    make: 'Toyota',
    model: 'Camry',
    year: 2019,
    priceCents: 2200000, // $22,000
    mileageKm: 65000,
    colour: 'White',
    bodyType: 'Sedan',
    transmission: 'Automatic',
    fuelType: 'Gasoline',
    drivetrain: 'FWD',
    location: 'Toronto, ON',
    _sellerType: 'ownr',
    ...overrides,
  };
  return {
    id: 'test-uuid',
    source_id: 'kijiji-ca',
    source_category: 'cars',
    source_url: 'https://www.kijiji.ca/v-cars-trucks/test/123',
    raw_content: JSON.stringify(data),
    raw_content_type: 'json',
    scraped_at: new Date().toISOString(),
  } as unknown as RawPayload;
}

// ── isFastPathEligible ────────────────────────────────────────────────────────

describe('isFastPathEligible', () => {

  test('returns true for a complete, valid Kijiji JSON payload', () => {
    expect(isFastPathEligible(makePayload())).toBe(true);
  });

  test('returns false for non-kijiji source', () => {
    const p = makePayload();
    (p as any).source_id = 'facebook-mp-ca';
    expect(isFastPathEligible(p)).toBe(false);
  });

  test('returns false for non-json content type', () => {
    const p = makePayload();
    (p as any).raw_content_type = 'html';
    expect(isFastPathEligible(p)).toBe(false);
  });

  test('returns false when make is missing', () => {
    expect(isFastPathEligible(makePayload({ make: null }))).toBe(false);
  });

  test('returns false when model is missing', () => {
    expect(isFastPathEligible(makePayload({ model: null }))).toBe(false);
  });

  test('returns false when year is missing', () => {
    expect(isFastPathEligible(makePayload({ year: null }))).toBe(false);
  });

  test('returns false when priceCents is missing', () => {
    expect(isFastPathEligible(makePayload({ priceCents: null }))).toBe(false);
  });

  test('returns false when make is "othrmake" (Kijiji catch-all)', () => {
    expect(isFastPathEligible(makePayload({ make: 'othrmake' }))).toBe(false);
  });

  test('returns false when model is "othrmdl" (Kijiji catch-all)', () => {
    expect(isFastPathEligible(makePayload({ model: 'othrmdl' }))).toBe(false);
  });

  test('returns false for year below 1950', () => {
    expect(isFastPathEligible(makePayload({ year: 1920 }))).toBe(false);
  });

  test('returns false for year above 2027', () => {
    expect(isFastPathEligible(makePayload({ year: 2030 }))).toBe(false);
  });

  test('returns false when fewer than 3 rich fields are present', () => {
    expect(isFastPathEligible(makePayload({
      mileageKm: null,
      colour: null,
      bodyType: null,
      // only transmission, fuelType, drivetrain remain = 3, should pass
    }))).toBe(true);

    expect(isFastPathEligible(makePayload({
      mileageKm: null,
      colour: null,
      bodyType: null,
      transmission: null, // now only 2 rich fields
    }))).toBe(false);
  });

  test('returns false for malformed JSON', () => {
    const p = makePayload();
    (p as any).raw_content = '{ not valid json }}}';
    expect(isFastPathEligible(p)).toBe(false);
  });
});

// ── fastPathExtract ───────────────────────────────────────────────────────────

describe('fastPathExtract', () => {

  test('extracts make, model, year correctly', () => {
    const result = fastPathExtract(makePayload());
    expect(result.fields.make).toBe('Toyota');
    expect(result.fields.model).toBe('Camry');
    expect(result.fields.year).toBe(2019);
  });

  test('converts priceCents to dollars correctly', () => {
    const result = fastPathExtract(makePayload({ priceCents: 2200000 }));
    expect(result.fields.price).toBe(22000);
  });

  test('sets price to null when priceCents is 0', () => {
    const result = fastPathExtract(makePayload({ priceCents: 0 }));
    expect(result.fields.price).toBeNull();
  });

  test('parses province correctly from location string', () => {
    const result = fastPathExtract(makePayload({ location: 'Mississauga, ON' }));
    expect(result.fields.province).toBe('ON');
    expect(result.fields.city).toBe('Mississauga');
  });

  test('handles location with no province gracefully', () => {
    const result = fastPathExtract(makePayload({ location: 'Toronto' }));
    expect(result.fields.province).toBeNull();
    expect(result.fields.city).toBe('Toronto');
  });

  test('normalises AWD drivetrain variants', () => {
    const r1 = fastPathExtract(makePayload({ drivetrain: 'All Wheel Drive' }));
    expect(r1.fields.drivetrain).toBe('AWD');

    const r2 = fastPathExtract(makePayload({ drivetrain: 'awd' }));
    expect(r2.fields.drivetrain).toBe('AWD');
  });

  test('normalises 4WD variants', () => {
    const r1 = fastPathExtract(makePayload({ drivetrain: '4x4' }));
    expect(r1.fields.drivetrain).toBe('4WD');

    const r2 = fastPathExtract(makePayload({ drivetrain: '4wd' }));
    expect(r2.fields.drivetrain).toBe('4WD');
  });

  test('normalises Automatic transmission', () => {
    const result = fastPathExtract(makePayload({ transmission: 'Automatic' }));
    expect(result.fields.transmission).toBe('Automatic');
  });

  test('normalises Manual / stick shift', () => {
    const r1 = fastPathExtract(makePayload({ transmission: 'manual' }));
    expect(r1.fields.transmission).toBe('Manual');

    const r2 = fastPathExtract(makePayload({ transmission: 'stick shift' }));
    expect(r2.fields.transmission).toBe('Manual');
  });

  test('maps seller type ownr → Private', () => {
    const result = fastPathExtract(makePayload({ _sellerType: 'ownr' }));
    expect(result.fields.seller_type).toBe('Private');
  });

  test('maps seller type delr → Dealer', () => {
    const result = fastPathExtract(makePayload({ _sellerType: 'delr' }));
    expect(result.fields.seller_type).toBe('Dealer');
  });

  test('sets confidence high for all present core fields', () => {
    const result = fastPathExtract(makePayload());
    expect(result.fields.confidence.make).toBe('high');
    expect(result.fields.confidence.model).toBe('high');
    expect(result.fields.confidence.year).toBe('high');
    expect(result.fields.confidence.price).toBe('high');
    expect(result.fields.confidence.mileage_km).toBe('high');
  });

  test('sets mileage confidence to none when mileageKm is missing', () => {
    const result = fastPathExtract(makePayload({ mileageKm: null }));
    expect(result.fields.confidence.mileage_km).toBe('none');
  });

  test('reports model as fast-path/kijiji-structured', () => {
    const result = fastPathExtract(makePayload());
    expect(result.model).toBe('fast-path/kijiji-structured');
  });

  test('reports zero tokens (no LLM used)', () => {
    const result = fastPathExtract(makePayload());
    expect(result.promptTokens).toBe(0);
    expect(result.completionTokens).toBe(0);
  });

  test('truncates description to 2000 chars', () => {
    const longDesc = 'a'.repeat(3000);
    const result = fastPathExtract(makePayload({ description: longDesc }));
    expect(result.fields.description!.length).toBe(2000);
  });

  test('handles missing optional fields gracefully', () => {
    const result = fastPathExtract(makePayload({
      trim: null, vin: null, doors: null, seats: null,
      colourInterior: null, description: null,
    }));
    expect(result.fields.trim).toBeNull();
    expect(result.fields.vin).toBeNull();
    expect(result.fields.doors).toBeNull();
    expect(result.fields.description).toBeNull();
  });
});
