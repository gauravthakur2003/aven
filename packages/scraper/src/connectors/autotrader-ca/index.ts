// AutoTrader.ca Connector (autotrader-ca) — PRIORITY 2
//
// STATUS: BLOCKED — Do not build until a signed data partnership agreement
// is in place with AutoTrader Canada / AutoScout24 GmbH.
//
// AutoTrader's Terms of Service explicitly prohibit automated data extraction
// without written consent. Building a scraper without legal clearance exposes
// Aven to legal action that could halt all data operations.
//
// Owner:    Gaurav (Business Development)
// Urgency:  HIGH — needed for Phase 3 timeline
// Contact:  AutoScout24 GmbH (AutoTrader's parent company, European HQ)
//
// This file is a placeholder only. The connector interface is defined
// so that the module boundary is clear and future implementation can
// slot in without changing anything else.

import { SourceConnector, SourceCategory, HealthResult, RawPayload, ScrapeConfig } from '../../types';
import { logger } from '../../lib/logger';

export class AutoTraderConnector implements SourceConnector {
  readonly source_id         = 'autotrader-ca';
  readonly source_name       = 'AutoTrader Canada';
  readonly connector_version = '0.0.0';
  readonly category: SourceCategory = 'dealer';

  async healthCheck(): Promise<HealthResult> {
    return {
      status:    'FAILED',
      reason:    'BLOCKED: AutoTrader data partnership required before this connector can be built.',
      timestamp: new Date().toISOString(),
    };
  }

  async getListingUrls(): Promise<string[]> {
    this.blockError();
    return [];
  }

  async *scrape(_config: ScrapeConfig): AsyncGenerator<RawPayload> {
    this.blockError();
  }

  private blockError(): never {
    const message = 'AutoTrader connector is blocked pending a signed data partnership agreement.';
    logger.error({ message, connector_id: 'autotrader-ca' });
    throw new Error(message);
  }
}
