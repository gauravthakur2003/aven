// Facebook Marketplace Connector (facebook-mp-ca) — PRIORITY 1, BUILD SECOND
//
// Scrape method: Playwright headless Chromium. Full JS rendering required.
// Authentication: REQUIRED. Uses a dedicated Facebook account (session cookie).
// Proxy: REQUIRED. Residential proxy with PER_REQUEST rotation.
// Stealth mode: true — Facebook has the most aggressive bot detection in scope.
//
// Expected listing count (GTA, vehicles): ~50,000–80,000 active listings.
// Highest-risk source. Budget extra engineering time for maintenance.
//
// Legal note: Facebook ToS restricts automated access.
// A formal Meta data partnership is the preferred long-term path.
// Current status: proceeding with automated collection pending legal review.

import { chromium, Browser, BrowserContext, Page } from 'playwright';
import { randomUUID } from 'crypto';
import {
  SourceConnector, SourceCategory, HealthResult,
  RawPayload, ScrapeConfig, ProxyConfig, ConnectorConfig,
} from '../../types';
import { buildUserAgent, randomResolution, randomDelay, PerRunDeduplicator } from '../../lib/utils';
import * as metrics from '../../lib/metrics';
import { logger } from '../../lib/logger';

const SOURCE_ID          = 'facebook-mp-ca';
const CONNECTOR_VERSION  = '1.0.0';
const CATEGORY: SourceCategory = 'social';

const FB_MARKETPLACE_URL = 'https://www.facebook.com/marketplace/toronto/vehicles';
// Per PRD §10: Limit detail page views to 30–50 per session before rotating.
const MAX_DETAIL_VIEWS_PER_SESSION = 40;

export class FacebookMarketplaceConnector implements SourceConnector {
  readonly source_id         = SOURCE_ID;
  readonly source_name       = 'Facebook Marketplace Canada';
  readonly connector_version = CONNECTOR_VERSION;
  readonly category          = CATEGORY;

  private browser:     Browser | null      = null;
  private context:     BrowserContext | null = null;
  private deduplicator = new PerRunDeduplicator();

  constructor(private readonly config: ConnectorConfig) {}

  // ── Interface: healthCheck ────────────────────────────────

  async healthCheck(): Promise<HealthResult> {
    if (!this.context) {
      return { status: 'DEGRADED', reason: 'Browser context not initialised', timestamp: new Date().toISOString() };
    }
    return { status: 'OK', timestamp: new Date().toISOString() };
  }

  // ── Interface: getListingUrls ─────────────────────────────

  async getListingUrls(): Promise<string[]> {
    const urls: string[] = [];
    for await (const payload of this.scrape({ runId: randomUUID(), geoRegion: 'ON-GTA' })) {
      urls.push(payload.listing_url);
    }
    return urls;
  }

  // ── Interface: initBrowser ────────────────────────────────
  // Called by the Scheduler before scrape(). Must be paired with closeBrowser().

  async initBrowser(proxyConfig: ProxyConfig): Promise<void> {
    const { width, height } = randomResolution();

    this.browser = await chromium.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-blink-features=AutomationControlled',
        `--window-size=${width},${height}`,
      ],
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
    });

    const contextOptions: Parameters<Browser['newContext']>[0] = {
      viewport:   { width, height },
      userAgent:  buildUserAgent(),
      locale:     'en-CA',
      timezoneId: 'America/Toronto',
      extraHTTPHeaders: {
        'Accept-Language': 'en-CA,en;q=0.9,fr-CA;q=0.8,fr;q=0.7',
        'DNT':             '1',
      },
    };

    if (proxyConfig.provider !== 'none') {
      contextOptions.proxy = {
        server:   `http://${proxyConfig.host}:${proxyConfig.port}`,
        username: proxyConfig.username,
        password: proxyConfig.password,
      };
    }

    this.context = await this.browser.newContext(contextOptions);

    // ── Browser fingerprint hardening (PRD §11.1) ───────────
    await this.context.addInitScript(() => {
      // Patch webdriver flag — most detectable Playwright artefact.
      Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

      // Spoof plugin list — empty array is a headless signal.
      Object.defineProperty(navigator, 'plugins', {
        get: () => [
          { name: 'Chrome PDF Plugin' },
          { name: 'Chrome PDF Viewer' },
          { name: 'Native Client' },
        ],
      });

      // Add Chrome runtime object — absent in headless by default.
      (window as unknown as Record<string, unknown>).chrome = {
        runtime: {},
        loadTimes: () => ({}),
        csi: () => ({}),
      };
    });

    // Load saved session cookie if available.
    await this.loadSessionCookie();

    logger.info({ message: 'Facebook browser initialised', connector_id: SOURCE_ID });
  }

  // ── Interface: closeBrowser ───────────────────────────────

  async closeBrowser(): Promise<void> {
    if (this.context) { await this.context.close(); this.context = null; }
    if (this.browser) { await this.browser.close(); this.browser = null; }
    logger.info({ message: 'Facebook browser closed', connector_id: SOURCE_ID });
  }

  // ── Interface: scrape ─────────────────────────────────────

  async *scrape(config: ScrapeConfig): AsyncGenerator<RawPayload> {
    if (!this.context) {
      logger.error({ message: 'Cannot scrape: browser not initialised', connector_id: SOURCE_ID });
      return;
    }

    this.deduplicator.clear();

    const page = await this.context.newPage();
    try {
      await this.sessionWarmup(page);
      yield* this.scrapeListings(page, config);
    } finally {
      await page.close();
    }
  }

  // ── Private: session warm-up ─────────────────────────────
  // Per PRD §11.2: navigate to home feed, wait, then navigate to Marketplace.

  private async sessionWarmup(page: Page): Promise<void> {
    await page.goto('https://www.facebook.com', {
      waitUntil: 'domcontentloaded',
      timeout:   this.config.timeout_ms,
    });
    await randomDelay(5_000, 15_000);
  }

  // ── Private: listing scrape loop ─────────────────────────

  private async *scrapeListings(page: Page, config: ScrapeConfig): AsyncGenerator<RawPayload> {
    metrics.requestsMade.inc({ connector_id: SOURCE_ID });

    try {
      await page.goto(FB_MARKETPLACE_URL, {
        waitUntil: 'networkidle',
        timeout:   this.config.timeout_ms,
      });
      metrics.requestsSucceeded.inc({ connector_id: SOURCE_ID });
    } catch (err) {
      metrics.requestsFailed.inc({ connector_id: SOURCE_ID });
      logger.error({ message: 'Failed to load Marketplace listing grid', error: (err as Error).message });
      return;
    }

    let scrollCount      = 0;
    let detailViewCount  = 0;

    while (scrollCount < this.config.pagination_depth) {
      // Extract all listing links currently visible on the page.
      const listingUrls: string[] = await page.evaluate(() => {
        const anchors = Array.from(
          document.querySelectorAll<HTMLAnchorElement>('a[href*="/marketplace/item/"]'),
        );
        return [...new Set(anchors.map((a) => a.href))];
      });

      for (const listingUrl of listingUrls) {
        if (this.deduplicator.has(listingUrl)) continue;
        if (detailViewCount >= MAX_DETAIL_VIEWS_PER_SESSION) break;

        this.deduplicator.add(listingUrl);

        // Per PRD §10: wait 3–10s before visiting a detail page.
        await randomDelay(3_000, 10_000);

        const detailPage = await this.context!.newPage();
        let rawHtml      = '';
        let httpStatus   = 0;
        let imageUrls:   string[] = [];

        try {
          metrics.requestsMade.inc({ connector_id: SOURCE_ID });

          const response = await detailPage.goto(listingUrl, {
            waitUntil: 'networkidle',
            timeout:   this.config.timeout_ms,
          });

          httpStatus = response?.status() ?? 200;
          await this.humanScroll(detailPage);
          rawHtml    = await detailPage.content();

          // Extract image URLs from CDN patterns used by Facebook.
          imageUrls  = await detailPage.evaluate(() =>
            Array.from(document.querySelectorAll<HTMLImageElement>('img[src]'))
              .map((img) => img.src)
              .filter((src) => src.includes('scontent') || src.includes('fbcdn')),
          );

          metrics.requestsSucceeded.inc({ connector_id: SOURCE_ID });
          detailViewCount++;
        } catch (err) {
          metrics.requestsFailed.inc({ connector_id: SOURCE_ID });
          logger.error({
            message:      'Failed to load listing detail page',
            connector_id: SOURCE_ID,
            listingUrl,
            error:        (err as Error).message,
          });
        } finally {
          await detailPage.close();
        }

        if (rawHtml) {
          const payload: RawPayload = {
            payload_id:        randomUUID(),
            source_id:         SOURCE_ID,
            source_category:   CATEGORY,
            listing_url:       listingUrl,
            scrape_timestamp:  new Date().toISOString(),
            connector_version: CONNECTOR_VERSION,
            raw_content:       rawHtml,
            raw_content_type:  'html',
            listing_images:    imageUrls,
            geo_region:        config.geoRegion,
            scrape_run_id:     config.runId,
            http_status:       httpStatus,
            proxy_used:        true,
            requires_auth:     true,
            is_dealer_listing: null,
          };
          yield payload;
        }
      }

      if (detailViewCount >= MAX_DETAIL_VIEWS_PER_SESSION) break;

      // Scroll down to trigger infinite-scroll content load.
      const hadNewContent = await this.infiniteScroll(page);
      if (!hadNewContent) break;

      scrollCount++;
      await randomDelay(this.config.request_delay_ms[0], this.config.request_delay_ms[1]);
    }
  }

  // ── Private: infinite scroll ─────────────────────────────
  // Per PRD §10: detect end-of-results by checking whether scroll triggers a network request.

  private async infiniteScroll(page: Page): Promise<boolean> {
    const prevHeight: number = await page.evaluate(() => document.body.scrollHeight);

    await page.evaluate(() => window.scrollBy({ top: 800, behavior: 'smooth' }));
    await randomDelay(2_000, 5_000);

    const newHeight: number = await page.evaluate(() => document.body.scrollHeight);
    return newHeight > prevHeight;
  }

  // ── Private: human scroll simulation ─────────────────────
  // Per PRD §11.2: scroll through page before extracting content.

  private async humanScroll(page: Page): Promise<void> {
    await page.evaluate(async () => {
      await new Promise<void>((resolve) => {
        const totalHeight = document.body.scrollHeight;
        let scrolled      = 0;
        const step        = 300 + Math.floor(Math.random() * 300);

        const timer = setInterval(() => {
          window.scrollBy(0, step);
          scrolled += step;
          if (scrolled >= totalHeight) {
            clearInterval(timer);
            resolve();
          }
        }, 100 + Math.floor(Math.random() * 200));
      });
    });
  }

  // ── Private: session cookie loader ───────────────────────
  // The Facebook session cookie is stored in the environment as a JSON array.
  // It is loaded once at browser init to restore an authenticated session.

  private async loadSessionCookie(): Promise<void> {
    const raw = process.env.FB_ACCOUNT_COOKIE;
    if (!raw || !this.context) return;

    try {
      const cookies = JSON.parse(raw) as Array<{
        name: string;
        value: string;
        domain: string;
        path: string;
        httpOnly?: boolean;
        secure?: boolean;
      }>;
      await this.context.addCookies(cookies);
      logger.info({ message: 'Facebook session cookie loaded', connector_id: SOURCE_ID });
    } catch (err) {
      logger.warn({
        message:      'Failed to parse FB_ACCOUNT_COOKIE — proceeding without session',
        connector_id: SOURCE_ID,
        error:        (err as Error).message,
      });
    }
  }
}
