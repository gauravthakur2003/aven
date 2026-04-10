#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────
#  Aven — Platform Launcher
#  Starts: Scraper (M1) + Normaliser (M2) + Dashboard
#  Usage:  ./start.sh
#          ./start.sh --no-scraper    (skip M1, just normaliser + dashboard)
#          ./start.sh --dashboard     (dashboard only)
# ─────────────────────────────────────────────────────────
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRAPER_DIR="$ROOT/packages/scraper"
NORMALISER_DIR="$ROOT/packages/normaliser"
DASHBOARD_DIR="$ROOT/packages/dashboard"
DASHBOARD_PORT=3030
LOG_DIR="$ROOT/.logs"

SKIP_SCRAPER=false
ONLY_DASHBOARD=false

for arg in "$@"; do
  case $arg in
    --no-scraper)  SKIP_SCRAPER=true ;;
    --dashboard)   ONLY_DASHBOARD=true ;;
  esac
done

mkdir -p "$LOG_DIR"

# ── Colours ───────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; AMBER='\033[0;33m'
BLUE='\033[0;34m'; DIM='\033[2m'; RESET='\033[0m'

log()  { echo -e "${DIM}$(date +%H:%M:%S)${RESET}  $*"; }
ok()   { echo -e "${GREEN}✓${RESET}  $*"; }
warn() { echo -e "${AMBER}⚠${RESET}  $*"; }
err()  { echo -e "${RED}✗${RESET}  $*" >&2; }

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  AVEN  —  Platform Launcher"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# ── Check prerequisites ───────────────────────────────────
log "Checking dependencies…"

if ! pg_isready -q 2>/dev/null; then
  warn "PostgreSQL not running. Starting via brew…"
  brew services start postgresql@16 2>/dev/null || true
  sleep 2
fi
ok "PostgreSQL"

if ! redis-cli ping >/dev/null 2>&1; then
  warn "Redis not running. Starting via brew…"
  brew services start redis 2>/dev/null || true
  sleep 1
fi
ok "Redis"

if ! redis-cli ping >/dev/null 2>&1; then
  err "Redis still unreachable. Run: brew services start redis"
  exit 1
fi

# ── Check/run migrations ──────────────────────────────────
log "Checking database migrations…"
cd "$NORMALISER_DIR"
npx ts-node src/migrate.ts --status 2>&1 | grep -q "pending" && {
  log "Running pending migrations…"
  npx ts-node src/migrate.ts 2>&1 | tee "$LOG_DIR/migrate.log"
} || true
ok "Migrations up to date"

# ── Kill any existing Aven processes ─────────────────────
pkill -f "packages/scraper" 2>/dev/null || true
pkill -f "packages/normaliser/src/index" 2>/dev/null || true
pkill -f "packages/dashboard" 2>/dev/null || true
sleep 1

echo ""

# ── Start Dashboard ───────────────────────────────────────
log "Starting dashboard on :${DASHBOARD_PORT}…"
cd "$DASHBOARD_DIR"
npx ts-node --project tsconfig.json server.ts > "$LOG_DIR/dashboard.log" 2>&1 &
DASH_PID=$!
sleep 2

if kill -0 "$DASH_PID" 2>/dev/null; then
  ok "Dashboard  →  http://localhost:${DASHBOARD_PORT}  (PID $DASH_PID)"
else
  err "Dashboard failed to start. Check $LOG_DIR/dashboard.log"
  cat "$LOG_DIR/dashboard.log"
  exit 1
fi

if [[ "$ONLY_DASHBOARD" == "true" ]]; then
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Dashboard only mode — scraper and normaliser not started"
  echo "  Open: http://localhost:${DASHBOARD_PORT}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  open "http://localhost:${DASHBOARD_PORT}" 2>/dev/null || true
  wait
  exit 0
fi

# ── Start Normaliser (M2) ─────────────────────────────────
log "Starting normaliser (M2)…"
cd "$NORMALISER_DIR"
npx ts-node src/index.ts > "$LOG_DIR/normaliser.log" 2>&1 &
NORM_PID=$!
sleep 2

if kill -0 "$NORM_PID" 2>/dev/null; then
  ok "Normaliser  (PID $NORM_PID)"
else
  warn "Normaliser failed to start (Redis may be needed for full mode)"
  warn "Check $LOG_DIR/normaliser.log for details"
fi

# ── Start Scraper (M1) ────────────────────────────────────
if [[ "$SKIP_SCRAPER" == "false" ]]; then
  log "Starting scraper (M1)…"
  cd "$SCRAPER_DIR"
  if [[ -f "package.json" ]]; then
    npx ts-node src/index.ts > "$LOG_DIR/scraper.log" 2>&1 &
    SCRAPER_PID=$!
    sleep 2
    if kill -0 "$SCRAPER_PID" 2>/dev/null; then
      ok "Scraper  (PID $SCRAPER_PID)"
    else
      warn "Scraper failed to start. Check $LOG_DIR/scraper.log"
    fi
  else
    warn "Scraper package not found — skipping"
  fi
fi

# ── Open browser ──────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
ok "Aven is running!"
echo ""
echo -e "  Dashboard  →  ${BLUE}http://localhost:${DASHBOARD_PORT}${RESET}"
echo -e "  Logs       →  ${DIM}$LOG_DIR/${RESET}"
echo ""
echo -e "  Press ${RED}Ctrl+C${RESET} to stop all services"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

open "http://localhost:${DASHBOARD_PORT}" 2>/dev/null || true

# ── Tail logs ─────────────────────────────────────────────
cleanup() {
  echo ""
  log "Shutting down…"
  kill "$DASH_PID" 2>/dev/null || true
  [[ -n "${NORM_PID:-}" ]] && kill "$NORM_PID" 2>/dev/null || true
  [[ -n "${SCRAPER_PID:-}" ]] && kill "$SCRAPER_PID" 2>/dev/null || true
  ok "All processes stopped"
  exit 0
}
trap cleanup INT TERM

# Stream normaliser log (most useful for seeing pipeline activity)
echo ""
echo -e "${DIM}── normaliser output ───────────────────────────────${RESET}"
tail -f "$LOG_DIR/normaliser.log" 2>/dev/null &

wait
