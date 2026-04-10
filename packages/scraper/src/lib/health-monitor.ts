// M1d — Scrape Health Monitor
//
// Read-only observer. Consumes metrics emitted by connectors and the scheduler,
// computes health signals, and fires alerts when anomalies are detected.
// It does NOT modify scraper behaviour — it only observes and reports.
//
// Exposes a /metrics endpoint (Prometheus) and a /health endpoint (liveness check).

import * as http from 'http';
import { register } from './metrics';
import { logger } from './logger';

// ── Alert levels per PRD §8.2 ─────────────────────────────

export type AlertLevel = 'INFO' | 'WARNING' | 'CRITICAL' | 'OUTAGE';

export interface AlertEvent {
  level:       AlertLevel;
  connectorId: string;
  message:     string;
  timestamp:   string;
  metric?:     string;
  value?:      number;
  threshold?:  number;
}

export type AlertHandler = (event: AlertEvent) => void;

// ── Per-metric thresholds per PRD §8.1 ────────────────────

interface MetricThresholds {
  /** % of 7-day rolling average below which WARNING fires */
  listingsPerRunWarningPct:  number;
  /** % of 7-day rolling average below which CRITICAL fires */
  listingsPerRunCriticalPct: number;
  requestSuccessRateWarning:  number;  // 0–1
  requestSuccessRateCritical: number;  // 0–1
  proxyBanRateWarning:        number;  // 0–1
  proxyBanRateCritical:       number;  // 0–1
  queuePushFailureRateWarning:  number; // 0–1
  queuePushFailureRateCritical: number; // 0–1
  /** Multiple of scheduled interval above which WARNING fires */
  lastRunAgeWarningMultiplier:  number;
  lastRunAgeCriticalMultiplier: number;
}

const DEFAULT_THRESHOLDS: MetricThresholds = {
  listingsPerRunWarningPct:     0.80,
  listingsPerRunCriticalPct:    0.60,
  requestSuccessRateWarning:    0.90,
  requestSuccessRateCritical:   0.75,
  proxyBanRateWarning:          0.05,
  proxyBanRateCritical:         0.20,
  queuePushFailureRateWarning:  0.001,
  queuePushFailureRateCritical: 0.010,
  lastRunAgeWarningMultiplier:  1.5,
  lastRunAgeCriticalMultiplier: 3.0,
};

export class HealthMonitor {
  private server:       http.Server;
  private alertHandler: AlertHandler;
  private thresholds:   MetricThresholds;

  // Rolling baselines — keyed by connector_id.
  // Real implementation: compute from 7-day rolling average in Postgres/Redis.
  // For MVP: baselines are set externally or defaulted.
  private listingBaselines = new Map<string, number>();
  private scheduledIntervalSeconds = new Map<string, number>();

  constructor(alertHandler?: AlertHandler, thresholds?: Partial<MetricThresholds>) {
    this.alertHandler = alertHandler ?? this.defaultAlertHandler.bind(this);
    this.thresholds   = { ...DEFAULT_THRESHOLDS, ...thresholds };

    this.server = http.createServer(this.handleRequest.bind(this));
  }

  start(port = 9090): void {
    this.server.listen(port, () => {
      logger.info({ message: 'HealthMonitor started', port });
    });
  }

  stop(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server.close((err) => (err ? reject(err) : resolve()));
    });
  }

  // ── Baseline management ───────────────────────────────────

  setListingBaseline(connectorId: string, avgPerRun: number): void {
    this.listingBaselines.set(connectorId, avgPerRun);
  }

  setScheduledInterval(connectorId: string, intervalSeconds: number): void {
    this.scheduledIntervalSeconds.set(connectorId, intervalSeconds);
  }

  // ── Alert firing ──────────────────────────────────────────

  fire(level: AlertLevel, connectorId: string, message: string, extra?: Partial<AlertEvent>): void {
    const event: AlertEvent = {
      level,
      connectorId,
      message,
      timestamp: new Date().toISOString(),
      ...extra,
    };
    this.alertHandler(event);
  }

  // ── Threshold checks — called by the monitoring loop ─────

  checkListingsPerRun(connectorId: string, currentCount: number): void {
    const baseline = this.listingBaselines.get(connectorId);
    if (!baseline) return; // still in learning mode

    const ratio = currentCount / baseline;
    const { listingsPerRunCriticalPct, listingsPerRunWarningPct } = this.thresholds;

    if (ratio < listingsPerRunCriticalPct) {
      this.fire('CRITICAL', connectorId, `Listing count dropped to ${(ratio * 100).toFixed(0)}% of baseline`, {
        metric: 'listings_per_run', value: currentCount, threshold: baseline * listingsPerRunCriticalPct,
      });
    } else if (ratio < listingsPerRunWarningPct) {
      this.fire('WARNING', connectorId, `Listing count dropped to ${(ratio * 100).toFixed(0)}% of baseline`, {
        metric: 'listings_per_run', value: currentCount, threshold: baseline * listingsPerRunWarningPct,
      });
    }
  }

  checkRequestSuccessRate(connectorId: string, rate: number): void {
    const { requestSuccessRateCritical, requestSuccessRateWarning } = this.thresholds;
    if (rate < requestSuccessRateCritical) {
      this.fire('CRITICAL', connectorId, `Request success rate ${(rate * 100).toFixed(0)}% below critical threshold`, {
        metric: 'request_success_rate', value: rate, threshold: requestSuccessRateCritical,
      });
    } else if (rate < requestSuccessRateWarning) {
      this.fire('WARNING', connectorId, `Request success rate ${(rate * 100).toFixed(0)}% below warning threshold`, {
        metric: 'request_success_rate', value: rate, threshold: requestSuccessRateWarning,
      });
    }
  }

  checkProxyBanRate(connectorId: string, rate: number): void {
    const { proxyBanRateCritical, proxyBanRateWarning } = this.thresholds;
    if (rate > proxyBanRateCritical) {
      this.fire('CRITICAL', connectorId, `Proxy ban rate ${(rate * 100).toFixed(0)}% above critical threshold`, {
        metric: 'proxy_ban_rate', value: rate, threshold: proxyBanRateCritical,
      });
    } else if (rate > proxyBanRateWarning) {
      this.fire('WARNING', connectorId, `Proxy ban rate ${(rate * 100).toFixed(0)}% above warning threshold`, {
        metric: 'proxy_ban_rate', value: rate, threshold: proxyBanRateWarning,
      });
    }
  }

  checkQueuePushFailureRate(connectorId: string, rate: number): void {
    const { queuePushFailureRateCritical, queuePushFailureRateWarning } = this.thresholds;
    if (rate > queuePushFailureRateCritical) {
      this.fire('CRITICAL', connectorId, `Queue push failure rate ${(rate * 100).toFixed(2)}% above critical threshold`, {
        metric: 'queue_push_failure_rate', value: rate, threshold: queuePushFailureRateCritical,
      });
    } else if (rate > queuePushFailureRateWarning) {
      this.fire('WARNING', connectorId, `Queue push failure rate ${(rate * 100).toFixed(2)}% above warning threshold`, {
        metric: 'queue_push_failure_rate', value: rate, threshold: queuePushFailureRateWarning,
      });
    }
  }

  checkLastRunAge(connectorId: string, ageSeconds: number): void {
    const interval = this.scheduledIntervalSeconds.get(connectorId);
    if (!interval) return;

    const { lastRunAgeCriticalMultiplier, lastRunAgeWarningMultiplier } = this.thresholds;
    if (ageSeconds > interval * lastRunAgeCriticalMultiplier) {
      this.fire('CRITICAL', connectorId, `No successful run in ${(ageSeconds / 3600).toFixed(1)}h`, {
        metric: 'last_successful_run_age', value: ageSeconds, threshold: interval * lastRunAgeCriticalMultiplier,
      });
    } else if (ageSeconds > interval * lastRunAgeWarningMultiplier) {
      this.fire('WARNING', connectorId, `No successful run in ${(ageSeconds / 3600).toFixed(1)}h`, {
        metric: 'last_successful_run_age', value: ageSeconds, threshold: interval * lastRunAgeWarningMultiplier,
      });
    }
  }

  // ── HTTP handler ─────────────────────────────────────────

  private async handleRequest(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    if (req.url === '/metrics') {
      res.setHeader('Content-Type', register.contentType);
      res.end(await register.metrics());
    } else if (req.url === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ status: 'ok', timestamp: new Date().toISOString() }));
    } else {
      res.writeHead(404);
      res.end();
    }
  }

  // ── Default alert handler ─────────────────────────────────

  private defaultAlertHandler(event: AlertEvent): void {
    const entry = {
      message:     `[ALERT:${event.level}] ${event.message}`,
      connectorId: event.connectorId,
      alertLevel:  event.level,
      metric:      event.metric,
      value:       event.value,
      threshold:   event.threshold,
    };

    if (event.level === 'INFO') {
      logger.info(entry);
    } else if (event.level === 'WARNING') {
      logger.warn(entry);
      // TODO: post to Slack #aven-scraper-alerts channel
    } else {
      logger.error(entry);
      // TODO: trigger PagerDuty for CRITICAL / OUTAGE
    }
  }
}
