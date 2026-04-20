-- Backfill province for Kijiji listings scraped before geo_region tagging was added.
-- All historical Kijiji scraping was Ontario-only, so NULL province → 'ON'.
-- Safe to re-run: WHERE clause restricts to NULLs only.

UPDATE listings
SET province = 'ON'
WHERE source_id = 'kijiji-ca'
  AND province IS NULL;
