// M2b — Field Validator & Standardiser
// Applies deterministic rules to the raw LLM output:
// type validation, unit conversion, vocabulary standardisation.
// Does NOT call any external API — purely deterministic.

import { ExtractedFields, ValidatedFields } from './types';

const NORMALISATION_VERSION = '1.0.0';

// ── Make aliases ──────────────────────────────────────────

const MAKE_ALIASES: Record<string, string> = {
  'chevy': 'Chevrolet', 'chev': 'Chevrolet', 'chevrolet': 'Chevrolet',
  'vw': 'Volkswagen', 'volkswagon': 'Volkswagen',
  'benz': 'Mercedes-Benz', 'mercedes': 'Mercedes-Benz', 'merc': 'Mercedes-Benz',
  'caddy': 'Cadillac', 'bmw': 'BMW', 'kia': 'Kia', 'gmc': 'GMC',
  'ram': 'Ram', 'jeep': 'Jeep', 'dodge': 'Dodge', 'chrysler': 'Chrysler',
  'lincoln': 'Lincoln', 'buick': 'Buick', 'mazda': 'Mazda', 'subaru': 'Subaru',
  'mitsubishi': 'Mitsubishi', 'infiniti': 'Infiniti', 'genesis': 'Genesis',
  'genesis motors': 'Genesis', 'alfa romeo': 'Alfa Romeo', 'land rover': 'Land Rover',
  'range rover': 'Land Rover', 'rolls royce': 'Rolls-Royce', 'rolls-royce': 'Rolls-Royce',
};

// ── Province aliases ───────────────────────────────────────

const PROVINCE_ALIASES: Record<string, string> = {
  'ontario': 'ON',
  'british columbia': 'BC', 'bc': 'BC',
  'alberta': 'AB', 'ab': 'AB',
  'quebec': 'QC', 'québec': 'QC', 'qc': 'QC',
  'manitoba': 'MB', 'mb': 'MB',
  'saskatchewan': 'SK', 'sk': 'SK',
  'nova scotia': 'NS', 'ns': 'NS',
  'new brunswick': 'NB', 'nb': 'NB',
  'newfoundland': 'NL', 'nl': 'NL',
  'prince edward island': 'PE', 'pei': 'PE', 'pe': 'PE',
  'yukon': 'YT', 'yt': 'YT',
  'northwest territories': 'NT', 'nt': 'NT',
  'nunavut': 'NU', 'nu': 'NU',
};

const VALID_PROVINCES = new Set(['ON','BC','AB','QC','MB','SK','NS','NB','NL','PE','YT','NT','NU']);

// ── Drivetrain aliases ────────────────────────────────────

const DRIVETRAIN_MAP: Record<string, 'FWD' | 'RWD' | 'AWD' | '4WD'> = {
  'fwd': 'FWD', 'front wheel drive': 'FWD', 'front-wheel drive': 'FWD',
  'rwd': 'RWD', 'rear wheel drive': 'RWD', 'rear-wheel drive': 'RWD',
  'awd': 'AWD', 'all wheel drive': 'AWD', 'all-wheel drive': 'AWD',
  '4wd': '4WD', '4x4': '4WD', 'four wheel drive': '4WD', 'four-wheel drive': '4WD', '4-wheel drive': '4WD',
};

// ── Fuel type aliases ─────────────────────────────────────

const FUEL_MAP: Record<string, string> = {
  'gas': 'Gasoline', 'petrol': 'Gasoline', 'gasoline': 'Gasoline',
  'diesel': 'Diesel', 'hybrid': 'Hybrid', 'plug-in hybrid': 'PHEV', 'phev': 'PHEV',
  'electric': 'Electric', 'ev': 'Electric', 'hydrogen': 'Hydrogen',
  'natural gas': 'Natural Gas', 'cng': 'Natural Gas',
};

// ── Colour aliases ────────────────────────────────────────

const COLOUR_MAP: Record<string, string> = {
  'blk': 'Black', 'blck': 'Black', 'wht': 'White', 'wt': 'White',
  'slvr': 'Silver', 'slv': 'Silver', 'sil': 'Silver', 'rd': 'Red',
  'gry': 'Grey', 'gray': 'Grey', 'grn': 'Green', 'blu': 'Blue',
  'brn': 'Brown', 'brwn': 'Brown',
};

// ── Condition aliases ─────────────────────────────────────

const CONDITION_MAP: Record<string, string> = {
  'cpo': 'Certified Pre-Owned', 'certified pre-owned': 'Certified Pre-Owned',
  'certified preowned': 'Certified Pre-Owned', 'certified': 'Certified Pre-Owned',
  'new vehicle': 'New', 'brand new': 'New', 'new': 'New',
  'used': 'Used', 'pre-owned': 'Used', 'preowned': 'Used',
  'demo': 'Demo', 'demonstrator': 'Demo',
  'salvage': 'Salvage', 'rebuilt': 'Salvage', 'unknown': 'Unknown',
};

// ── Main validator ────────────────────────────────────────

export function validateAndStandardise(raw: ExtractedFields): ValidatedFields {
  const warnings: string[] = [];
  const f = { ...raw } as ValidatedFields;
  f._validationWarnings = warnings;

  // year — 1900 is Mistral's sentinel for "unknown"; reject it and anything
  // before 1950 (no modern used car predates that) or after 2030.
  if (f.year != null) {
    const y = Number(f.year);
    if (isNaN(y) || y < 1950 || y > 2030) {
      warnings.push(`Invalid year: ${f.year}`);
      f.year = null;
    } else {
      f.year = y;
    }
  }

  if (f.make) {
    const key = f.make.toLowerCase().trim();
    f.make = MAKE_ALIASES[key] ?? toTitleCase(f.make.trim().slice(0, 128));
  }

  // model — strip leading year if present ('2019 Civic' → 'Civic')
  if (f.model) {
    f.model = toTitleCase(f.model.trim().replace(/^\d{4}\s+/, '').slice(0, 256));
  }

  // mileage_km — convert from miles if needed
  if (f.mileage_km != null) {
    let km = Number(f.mileage_km);
    if (f.mileage_unit_original === 'miles') km = Math.round(km * 1.60934);
    if (isNaN(km) || km < 0 || km > 999_999) {
      warnings.push(`Invalid mileage: ${f.mileage_km}`);
      f.mileage_km = null;
    } else {
      f.mileage_km = km;
    }
  }

  // price sanity checks (per PRD §6.2)
  if (f.price != null) {
    const p = Number(f.price);
    if (isNaN(p) || p <= 0) {
      f.price = null;
    } else {
      if (p < 500) { warnings.push(`Price floor failed: ${p}`); f.confidence.price = 'low'; }
      else if (p > 500_000) { warnings.push(`Price ceiling failed: ${p}`); f.confidence.price = 'low'; }
      else if (p < 3_000 && f.year != null && f.year > 2010) { f.confidence.price = 'medium'; }
      f.price = Math.round(p);
    }
  }

  if (f.province) {
    const key = f.province.toLowerCase().trim();
    const mapped = PROVINCE_ALIASES[key] ?? f.province.toUpperCase().trim();
    f.province = VALID_PROVINCES.has(mapped) ? mapped : null;
    if (!f.province) warnings.push(`Unknown province: ${key}`);
  }

  // vin — validate format, set to null if bad
  if (f.vin) {
    const vin = f.vin.toUpperCase().replace(/\s/g, '');
    if (!/^[A-HJ-NPR-Z0-9]{17}$/.test(vin)) {
      warnings.push(`Invalid VIN format: ${f.vin}`);
      f.vin = null;
    } else {
      f.vin = vin;
    }
  }

  if (f.condition) {
    f.condition = (CONDITION_MAP[f.condition.toLowerCase().trim()] ?? 'Unknown') as ExtractedFields['condition'];
  } else {
    f.condition = 'Unknown';
  }

  if (f.drivetrain) f.drivetrain = DRIVETRAIN_MAP[f.drivetrain.toLowerCase().trim()] ?? null;
  if (f.fuel_type) f.fuel_type = FUEL_MAP[f.fuel_type.toLowerCase().trim()] ?? toTitleCase(f.fuel_type.trim());

  if (f.colour_exterior) f.colour_exterior = COLOUR_MAP[f.colour_exterior.toLowerCase().trim()] ?? toTitleCase(f.colour_exterior.trim());
  if (f.colour_interior) f.colour_interior = COLOUR_MAP[f.colour_interior.toLowerCase().trim()] ?? toTitleCase(f.colour_interior.trim());
  if (f.city) f.city = normaliseCity(f.city.trim());

  return f;
}

// ── Helpers ───────────────────────────────────────────────

function toTitleCase(s: string): string {
  return s.replace(/\w\S*/g, w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase());
}

function normaliseCity(city: string): string {
  const lower = city.toLowerCase();
  if (['gta', 'greater toronto area', 'toronto area', 'gtaa'].includes(lower)) return 'Greater Toronto Area';
  if (['mtl', 'moncton area'].includes(lower)) return 'Montreal';
  if (['tor', 'toronto, on'].includes(lower)) return 'Toronto';
  return toTitleCase(city);
}
