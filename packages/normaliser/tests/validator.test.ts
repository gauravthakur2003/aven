/**
 * M2b Validator — Unit Tests
 * Tests field normalisation: make aliases, province codes, drivetrain/fuel mappings,
 * year boundaries, price sanity, mileage sanity, and VIN format validation.
 */

import { validateAndStandardise } from '../src/m2b-validator';
import { ExtractedFields } from '../src/types';

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeFields(overrides: Partial<ExtractedFields> = {}): ExtractedFields {
  return {
    make: 'Toyota',
    model: 'Camry',
    year: 2019,
    trim: 'XSE',
    body_type: 'Sedan',
    drivetrain: 'FWD',
    fuel_type: 'Gasoline',
    transmission: 'Automatic',
    colour_exterior: 'White',
    colour_interior: null,
    engine: null,
    doors: 4,
    seats: 5,
    vin: null,
    condition: 'Used',
    mileage_km: 65000,
    mileage_unit_original: 'km',
    safetied: true,
    accidents: 0,
    owners: 1,
    price: 22000,
    price_type: 'PURCHASE_PRICE',
    price_qualifier: null,
    price_raw: '$22,000',
    price_currency_orig: 'CAD',
    payment_amount: null,
    payment_frequency: null,
    city: 'Toronto',
    province: 'ON',
    seller_type: 'Private',
    dealer_name: null,
    listed_date: null,
    description: 'Clean car, one owner, no accidents.',
    confidence: {
      make: 'high', model: 'high', year: 'high',
      price: 'high', mileage_km: 'high', safetied: 'high', city: 'high',
    },
    ...overrides,
  };
}

// ── Make normalisation ────────────────────────────────────────────────────────

describe('Make aliases', () => {
  test('chevy → Chevrolet', () => {
    const result = validateAndStandardise(makeFields({ make: 'chevy' }));
    expect(result.fields.make).toBe('Chevrolet');
  });

  test('vw → Volkswagen', () => {
    const result = validateAndStandardise(makeFields({ make: 'vw' }));
    expect(result.fields.make).toBe('Volkswagen');
  });

  test('volkswagon (typo) → Volkswagen', () => {
    const result = validateAndStandardise(makeFields({ make: 'volkswagon' }));
    expect(result.fields.make).toBe('Volkswagen');
  });

  test('mercedes → Mercedes-Benz', () => {
    const result = validateAndStandardise(makeFields({ make: 'mercedes' }));
    expect(result.fields.make).toBe('Mercedes-Benz');
  });

  test('benz → Mercedes-Benz', () => {
    const result = validateAndStandardise(makeFields({ make: 'benz' }));
    expect(result.fields.make).toBe('Mercedes-Benz');
  });

  test('range rover → Land Rover', () => {
    const result = validateAndStandardise(makeFields({ make: 'range rover' }));
    expect(result.fields.make).toBe('Land Rover');
  });

  test('rolls royce → Rolls-Royce', () => {
    const result = validateAndStandardise(makeFields({ make: 'rolls royce' }));
    expect(result.fields.make).toBe('Rolls-Royce');
  });

  test('unknown make passes through unchanged', () => {
    const result = validateAndStandardise(makeFields({ make: 'Rivian' }));
    expect(result.fields.make).toBe('Rivian');
  });
});

// ── Province normalisation ────────────────────────────────────────────────────

describe('Province normalisation', () => {
  test('ontario → ON', () => {
    const result = validateAndStandardise(makeFields({ province: 'ontario' as any }));
    expect(result.fields.province).toBe('ON');
  });

  test('british columbia → BC', () => {
    const result = validateAndStandardise(makeFields({ province: 'british columbia' as any }));
    expect(result.fields.province).toBe('BC');
  });

  test('québec → QC', () => {
    const result = validateAndStandardise(makeFields({ province: 'québec' as any }));
    expect(result.fields.province).toBe('QC');
  });

  test('already-correct ON passes through', () => {
    const result = validateAndStandardise(makeFields({ province: 'ON' }));
    expect(result.fields.province).toBe('ON');
  });

  test('invalid province gets nulled out', () => {
    const result = validateAndStandardise(makeFields({ province: 'XX' as any }));
    expect(result.fields.province).toBeNull();
  });
});

// ── Drivetrain normalisation ──────────────────────────────────────────────────

describe('Drivetrain normalisation', () => {
  test('all wheel drive → AWD', () => {
    const result = validateAndStandardise(makeFields({ drivetrain: 'all wheel drive' as any }));
    expect(result.fields.drivetrain).toBe('AWD');
  });

  test('4x4 → 4WD', () => {
    const result = validateAndStandardise(makeFields({ drivetrain: '4x4' as any }));
    expect(result.fields.drivetrain).toBe('4WD');
  });

  test('front wheel drive → FWD', () => {
    const result = validateAndStandardise(makeFields({ drivetrain: 'front wheel drive' as any }));
    expect(result.fields.drivetrain).toBe('FWD');
  });

  test('unknown drivetrain → null', () => {
    const result = validateAndStandardise(makeFields({ drivetrain: 'warp drive' as any }));
    expect(result.fields.drivetrain).toBeNull();
  });
});

// ── Fuel type normalisation ───────────────────────────────────────────────────

describe('Fuel type normalisation', () => {
  test('gas → Gasoline', () => {
    const result = validateAndStandardise(makeFields({ fuel_type: 'gas' }));
    expect(result.fields.fuel_type).toBe('Gasoline');
  });

  test('petrol → Gasoline', () => {
    const result = validateAndStandardise(makeFields({ fuel_type: 'petrol' }));
    expect(result.fields.fuel_type).toBe('Gasoline');
  });

  test('ev → Electric', () => {
    const result = validateAndStandardise(makeFields({ fuel_type: 'ev' }));
    expect(result.fields.fuel_type).toBe('Electric');
  });

  test('plug-in hybrid → PHEV', () => {
    const result = validateAndStandardise(makeFields({ fuel_type: 'plug-in hybrid' }));
    expect(result.fields.fuel_type).toBe('PHEV');
  });
});

// ── Year boundary checks ──────────────────────────────────────────────────────

describe('Year boundaries', () => {
  test('valid year 2015 passes through', () => {
    const result = validateAndStandardise(makeFields({ year: 2015 }));
    expect(result.warnings).not.toContain(expect.stringMatching(/year/i));
  });

  test('year 1899 triggers a warning', () => {
    const result = validateAndStandardise(makeFields({ year: 1899 }));
    expect(result.warnings.some(w => /year/i.test(w))).toBe(true);
  });

  test('year 2031 triggers a warning', () => {
    const result = validateAndStandardise(makeFields({ year: 2031 }));
    expect(result.warnings.some(w => /year/i.test(w))).toBe(true);
  });

  test('year 0 triggers a warning', () => {
    const result = validateAndStandardise(makeFields({ year: 0 }));
    expect(result.warnings.some(w => /year/i.test(w))).toBe(true);
  });
});

// ── Price sanity ──────────────────────────────────────────────────────────────

describe('Price sanity', () => {
  test('valid price $15,000 passes through', () => {
    const result = validateAndStandardise(makeFields({ price: 15000 }));
    expect(result.fields.price).toBe(15000);
  });

  test('price of $200 triggers a warning (suspiciously low)', () => {
    const result = validateAndStandardise(makeFields({ price: 200 }));
    expect(result.warnings.some(w => /price/i.test(w))).toBe(true);
  });

  test('price over $500,000 triggers a warning', () => {
    const result = validateAndStandardise(makeFields({ price: 600000 }));
    expect(result.warnings.some(w => /price/i.test(w))).toBe(true);
  });

  test('null price does not trigger a warning (free/unknown is allowed)', () => {
    const result = validateAndStandardise(makeFields({ price: null }));
    expect(result.warnings.some(w => /price/i.test(w))).toBe(false);
  });
});

// ── Mileage sanity ────────────────────────────────────────────────────────────

describe('Mileage sanity', () => {
  test('valid 65,000 km passes', () => {
    const result = validateAndStandardise(makeFields({ mileage_km: 65000 }));
    expect(result.fields.mileage_km).toBe(65000);
  });

  test('mileage over 800,000 km triggers a warning', () => {
    const result = validateAndStandardise(makeFields({ mileage_km: 900000 }));
    expect(result.warnings.some(w => /mileage/i.test(w))).toBe(true);
  });

  test('negative mileage triggers a warning', () => {
    const result = validateAndStandardise(makeFields({ mileage_km: -500 }));
    expect(result.warnings.some(w => /mileage/i.test(w))).toBe(true);
  });

  test('mileage in miles gets converted to km', () => {
    const result = validateAndStandardise(makeFields({
      mileage_km: 40000,
      mileage_unit_original: 'mi',
    }));
    // 40,000 miles ≈ 64,374 km
    expect(result.fields.mileage_km!).toBeGreaterThan(60000);
    expect(result.fields.mileage_km!).toBeLessThan(70000);
  });
});

// ── VIN validation ────────────────────────────────────────────────────────────

describe('VIN validation', () => {
  test('valid 17-char VIN passes through', () => {
    const result = validateAndStandardise(makeFields({ vin: '1HGCM82633A004352' }));
    expect(result.fields.vin).toBe('1HGCM82633A004352');
  });

  test('VIN shorter than 17 chars gets nulled out', () => {
    const result = validateAndStandardise(makeFields({ vin: '1HGCM82633A' }));
    expect(result.fields.vin).toBeNull();
  });

  test('VIN with illegal character I gets nulled out', () => {
    const result = validateAndStandardise(makeFields({ vin: '1HGCM82633I004352' }));
    expect(result.fields.vin).toBeNull();
  });

  test('null VIN is passed through as null', () => {
    const result = validateAndStandardise(makeFields({ vin: null }));
    expect(result.fields.vin).toBeNull();
  });
});
