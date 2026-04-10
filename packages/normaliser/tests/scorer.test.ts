/**
 * M2c Confidence Scorer — Unit Tests
 * Tests scoring thresholds, routing outcomes (published/review/rejected),
 * hard rejection rules, and penalty logic for missing core fields.
 */

import { scoreAndRoute } from '../src/m2c-scorer';
import { ValidatedFields } from '../src/types';

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeValidated(overrides: Partial<ValidatedFields> = {}): ValidatedFields {
  return {
    make: 'Honda',
    model: 'Civic',
    year: 2020,
    trim: 'Sport',
    body_type: 'Sedan',
    drivetrain: 'FWD',
    fuel_type: 'Gasoline',
    transmission: 'Automatic',
    colour_exterior: 'Blue',
    colour_interior: null,
    engine: null,
    doors: 4,
    seats: 5,
    vin: null,
    condition: 'Used',
    mileage_km: 42000,
    mileage_unit_original: 'km',
    safetied: true,
    accidents: 0,
    owners: 1,
    price: 18000,
    price_type: 'PURCHASE_PRICE',
    price_qualifier: null,
    price_raw: '$18,000',
    price_currency_orig: 'CAD',
    payment_amount: null,
    payment_frequency: null,
    city: 'Toronto',
    province: 'ON',
    seller_type: 'Private',
    dealer_name: null,
    listed_date: null,
    description: 'Well maintained, single owner.',
    confidence: {
      make: 'high', model: 'high', year: 'high',
      price: 'high', mileage_km: 'high', safetied: 'high', city: 'high',
    },
    warnings: [],
    ...overrides,
  };
}

// ── Routing thresholds ────────────────────────────────────────────────────────

describe('Routing thresholds', () => {

  test('all-high-confidence listing is published', () => {
    const result = scoreAndRoute(makeValidated());
    expect(result.outcome).toBe('published');
    expect(result.confidenceScore).toBeGreaterThanOrEqual(70);
  });

  test('listing with medium make/model confidence is published (with flag)', () => {
    const result = scoreAndRoute(makeValidated({
      confidence: {
        make: 'medium', model: 'medium', year: 'high',
        price: 'high', mileage_km: 'high', safetied: 'none', city: 'high',
      },
    }));
    expect(['published', 'review']).toContain(result.outcome);
  });

  test('listing missing year goes to review queue', () => {
    const result = scoreAndRoute(makeValidated({
      year: null as any,
      confidence: {
        make: 'high', model: 'high', year: 'none',
        price: 'high', mileage_km: 'low', safetied: 'none', city: 'medium',
      },
    }));
    expect(result.outcome).toBe('review');
  });

  test('listing with all-low confidence is rejected', () => {
    const result = scoreAndRoute(makeValidated({
      confidence: {
        make: 'low', model: 'low', year: 'low',
        price: 'low', mileage_km: 'low', safetied: 'none', city: 'low',
      },
    }));
    expect(result.outcome).toBe('rejected');
    expect(result.confidenceScore).toBeLessThan(50);
  });

  test('listing with all-none confidence is rejected', () => {
    const result = scoreAndRoute(makeValidated({
      make: null as any, model: null as any, year: null as any,
      price: null, city: null as any,
      confidence: {
        make: 'none', model: 'none', year: 'none',
        price: 'none', mileage_km: 'none', safetied: 'none', city: 'none',
      },
    }));
    expect(result.outcome).toBe('rejected');
  });
});

// ── Hard rejection rules ──────────────────────────────────────────────────────

describe('Hard rejection rules', () => {

  test('listing with no photos is hard-rejected', () => {
    const result = scoreAndRoute(makeValidated({ photo_urls: [] } as any));
    expect(result.outcome).toBe('rejected');
  });

  test('listing with no price and no photos is hard-rejected', () => {
    const result = scoreAndRoute(makeValidated({
      price: null,
      photo_urls: [],
    } as any));
    expect(result.outcome).toBe('rejected');
  });
});

// ── Penalty for missing core fields ──────────────────────────────────────────

describe('Core field penalties', () => {

  test('missing make drops the score by at least 15 points vs full listing', () => {
    const full    = scoreAndRoute(makeValidated());
    const noMake  = scoreAndRoute(makeValidated({ make: null as any }));
    expect(full.confidenceScore - noMake.confidenceScore).toBeGreaterThanOrEqual(15);
  });

  test('missing city drops the score', () => {
    const full   = scoreAndRoute(makeValidated());
    const noCity = scoreAndRoute(makeValidated({
      city: null as any,
      confidence: { ...makeValidated().confidence, city: 'none' },
    }));
    expect(full.confidenceScore).toBeGreaterThan(noCity.confidenceScore);
  });

  test('confidenceScore is always between 0 and 100', () => {
    const worst = scoreAndRoute(makeValidated({
      make: null as any, model: null as any, year: null as any,
      price: null, city: null as any,
      confidence: {
        make: 'none', model: 'none', year: 'none',
        price: 'none', mileage_km: 'none', safetied: 'none', city: 'none',
      },
    }));
    const best = scoreAndRoute(makeValidated());
    expect(worst.confidenceScore).toBeGreaterThanOrEqual(0);
    expect(best.confidenceScore).toBeLessThanOrEqual(100);
  });
});
