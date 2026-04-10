// Kijiji Autos Connector (kijiji-ca) — PRIORITY 1
//
// Scrape method: Extracts per-listing structured JSON from the Apollo cache
// embedded in Kijiji's __NEXT_DATA__ script tag on each search results page.
//
// Each page yields ~46 AutosListing objects with full fields:
// title, description, price, imageUrls, url, VIN, make, model, year,
// mileage, colour, body type, drivetrain, fuel type, seller type, location.
//
// This is significantly richer than RSS or raw HTML scraping.
// M2 will receive clean JSON — minimal LLM work needed for Kijiji payloads.
//
// No authentication. No proxy required for MVP.

import axios from 'axios';
import { randomUUID } from 'crypto';
import {
  SourceConnector, SourceCategory, HealthResult,
  RawPayload, ScrapeConfig, ConnectorConfig,
} from '../../types';
import { buildUserAgent, randomDelay, PerRunDeduplicator } from '../../lib/utils';
import * as metrics from '../../lib/metrics';
import { logger } from '../../lib/logger';

const SOURCE_ID          = 'kijiji-ca';
const CONNECTOR_VERSION  = '1.2.0';
const CATEGORY: SourceCategory = 'classifieds';

const LISTING_BASE_URL = 'https://www.kijiji.ca/b-cars-trucks/city-of-toronto/c174l1700273';

export class KijijiConnector implements SourceConnector {
  readonly source_id         = SOURCE_ID;
  readonly source_name       = 'Kijiji Canada';
  readonly connector_version = CONNECTOR_VERSION;
  readonly category          = CATEGORY;

  private deduplicator = new PerRunDeduplicator();

  constructor(private readonly config: ConnectorConfig) {}

  // ── healthCheck ───────────────────────────────────────────

  async healthCheck(): Promise<HealthResult> {
    try {
      const listings = await this.fetchPageListings(1);
      if (listings.length > 0) {
        return { status: 'OK', timestamp: new Date().toISOString() };
      }
      return { status: 'DEGRADED', reason: 'Page fetched but 0 listings extracted', timestamp: new Date().toISOString() };
    } catch (err) {
      return { status: 'FAILED', reason: (err as Error).message, timestamp: new Date().toISOString() };
    }
  }

  // ── getListingUrls ────────────────────────────────────────

  async getListingUrls(): Promise<string[]> {
    const urls: string[] = [];
    for await (const payload of this.scrape({ runId: randomUUID(), geoRegion: 'ON-GTA' })) {
      urls.push(payload.listing_url);
    }
    return urls;
  }

  // ── scrape ────────────────────────────────────────────────

  async *scrape(config: ScrapeConfig): AsyncGenerator<RawPayload> {
    this.deduplicator.clear();

    for (let page = 1; page <= this.config.pagination_depth; page++) {
      metrics.requestsMade.inc({ connector_id: SOURCE_ID });

      let listings: KijijiListing[];
      let httpStatus = 200;

      try {
        listings = await this.fetchPageListings(page);
        metrics.requestsSucceeded.inc({ connector_id: SOURCE_ID });
      } catch (err: unknown) {
        const e = err as { response?: { status: number }; message: string };
        httpStatus = e.response?.status ?? 0;
        metrics.requestsFailed.inc({ connector_id: SOURCE_ID });
        logger.error({ message: 'Kijiji page fetch failed', connector_id: SOURCE_ID, page, httpStatus, error: e.message });
        break;
      }

      if (listings.length === 0) {
        logger.info({ message: 'Kijiji: no listings on page — stopping', connector_id: SOURCE_ID, page });
        break;
      }

      for (const listing of listings) {
        if (!listing.url) continue;
        if (this.deduplicator.has(listing.url)) continue;
        this.deduplicator.add(listing.url);

        yield {
          payload_id:        randomUUID(),
          source_id:         SOURCE_ID,
          source_category:   CATEGORY,
          listing_url:       listing.url,
          scrape_timestamp:  new Date().toISOString(),
          connector_version: CONNECTOR_VERSION,
          raw_content:       JSON.stringify(listing),
          raw_content_type:  'json',
          listing_images:    listing.imageUrls ?? [],
          geo_region:        config.geoRegion,
          scrape_run_id:     config.runId,
          http_status:       httpStatus,
          proxy_used:        false,
          requires_auth:     false,
          is_dealer_listing: listing._sellerType === 'delr' ? true
                           : listing._sellerType ? false
                           : null,
        };
      }

      logger.info({ message: 'Kijiji page scraped', connector_id: SOURCE_ID, page, count: listings.length });

      await randomDelay(this.config.request_delay_ms[0], this.config.request_delay_ms[1]);
    }
  }

  // ── Private: fetch and extract listings from one page ─────

  private async fetchPageListings(page: number): Promise<KijijiListing[]> {
    const url = page === 1 ? LISTING_BASE_URL : `${LISTING_BASE_URL}?page=${page}`;

    const res = await axios.get<string>(url, {
      timeout:      this.config.timeout_ms,
      responseType: 'text',
      headers: {
        'User-Agent':      buildUserAgent(),
        'Accept':          'text/html,application/xhtml+xml',
        'Accept-Language': 'en-CA,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT':             '1',
        ...(page > 1 ? { 'Referer': LISTING_BASE_URL } : {}),
      },
    });

    return this.extractListings(res.data);
  }

  private extractListings(html: string): KijijiListing[] {
    const scriptMatch = html.match(/<script id="__NEXT_DATA__"[^>]*>(.*?)<\/script>/s);
    if (!scriptMatch) return [];

    let pageData: Record<string, unknown>;
    try {
      pageData = JSON.parse(scriptMatch[1]) as Record<string, unknown>;
    } catch {
      return [];
    }

    const apollo = (pageData as {
      props?: { pageProps?: { __APOLLO_STATE__?: Record<string, unknown> } }
    })?.props?.pageProps?.__APOLLO_STATE__;

    if (!apollo) return [];

    return Object.entries(apollo)
      .filter(([key]) => key.startsWith('AutosListing:'))
      .map(([, raw]) => this.normaliseApolloListing(raw as ApolloListing))
      .filter((l): l is KijijiListing => Boolean(l.url));
  }

  private normaliseApolloListing(raw: ApolloListing): KijijiListing {
    // Flatten attributes array into a lookup map.
    const attrs: Record<string, string> = {};
    for (const attr of raw.attributes?.all ?? []) {
      attrs[attr.canonicalName] = attr.canonicalValues?.[0] ?? '';
    }

    // Price is stored in cents (e.g. 2799000 = $27,990.00).
    const priceCents = raw.price?.amount ?? null;

    return {
      id:           raw.id,
      url:          raw.url,
      title:        raw.title,
      description:  raw.description,
      imageUrls:    (raw.imageUrls ?? []).map((u) => u.replace('200-jpg', '640-jpg')),
      activatedAt:  raw.activationDate,
      location:     raw.location?.name ?? null,
      address:      raw.location?.address ?? null,
      priceCents,
      priceRating:  raw.price?.classification?.rating ?? null,
      // Structured fields from attributes
      vin:          attrs['vin']           ?? null,
      make:         attrs['carmake']       ?? null,
      model:        attrs['carmodel']      ?? null,
      year:         attrs['caryear']       ? Number(attrs['caryear']) : null,
      trim:         attrs['cartrim']       ?? null,
      mileageKm:    attrs['carmileageinkms'] ? Number(attrs['carmileageinkms']) : null,
      colour:       attrs['carcolor']      ?? null,
      bodyType:     attrs['carbodytype']   ?? null,
      drivetrain:   attrs['drivetrain']    ?? null,
      fuelType:     attrs['carfueltype']   ?? null,
      transmission: attrs['cartransmission'] ?? null,
      vehicleType:  attrs['vehicletype']   ?? null,  // 'new' | 'used'
      _sellerType:  attrs['forsaleby']     ?? null,  // 'delr' = dealer, 'ownr' = private
      isTopAd:      raw.flags?.topAd       ?? false,
      posterRating: raw.posterInfo?.rating ?? null,
    };
  }
}

// ── Types ─────────────────────────────────────────────────

interface KijijiListing {
  id:           string;
  url:          string;
  title:        string;
  description:  string;
  imageUrls:    string[];
  activatedAt:  string | null;
  location:     string | null;
  address:      string | null;
  priceCents:   number | null;
  priceRating:  string | null;
  vin:          string | null;
  make:         string | null;
  model:        string | null;
  year:         number | null;
  trim:         string | null;
  mileageKm:    number | null;
  colour:       string | null;
  bodyType:     string | null;
  drivetrain:   string | null;
  fuelType:     string | null;
  transmission: string | null;
  vehicleType:  string | null;
  _sellerType:  string | null;
  isTopAd:      boolean;
  posterRating: number | null;
}

interface ApolloListing {
  id:             string;
  url:            string;
  title:          string;
  description:    string;
  imageUrls?:     string[];
  activationDate: string;
  location?:      { name?: string; address?: string };
  price?:         { amount?: number; classification?: { rating?: string } };
  flags?:         { topAd?: boolean };
  posterInfo?:    { rating?: number };
  attributes?:    { all?: Array<{ canonicalName: string; canonicalValues?: string[] }> };
}
