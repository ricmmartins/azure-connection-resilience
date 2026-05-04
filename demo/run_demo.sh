#!/bin/bash
# Demo: Azure Connection Resilience — Before vs After
#
# Shows the recovery time difference with and without the monitor.
# Uses file-based notification for simplicity.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Azure Connection Resilience — Recovery Time Demo       ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""

cleanup() {
    kill $(jobs -p) 2>/dev/null || true
    rm -f /tmp/azure-drain-trigger /tmp/mock_imds_events.json
}
trap cleanup EXIT

echo -e "${BLUE}━━━ Scenario: WITHOUT Resilience Monitor ━━━${NC}"
echo ""
echo "  Timeline of a TCP black hole during Azure live migration:"
echo ""
echo "  T+0s    LB backend VM starts migrating"
echo "  T+0s    Active TCP connections go silent (no RST sent)"
echo "  T+1s    Client sends data — no ACK received"
echo "  T+1s    TCP retransmission timer starts (RTO = 200ms initially)"
echo "  T+3s    RTO doubles: 400ms, 800ms, 1.6s, 3.2s..."
echo "  T+30s   RTO hits ceiling, keeps retrying every 120s"
echo "  T+900s  tcp_retries2 (default=15) exhausted. Connection dead."
echo "  T+900s  Application finally gets error. Reconnects."
echo ""
echo -e "  ${RED}Total downtime: ~15 MINUTES${NC}"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo -e "${BLUE}━━━ Scenario: WITH Resilience Monitor ━━━${NC}"
echo ""
echo "  Timeline with the monitor + TCP_USER_TIMEOUT:"
echo ""
echo "  T-300s  Monitor detects 'Freeze' event via IMDS Scheduled Events"
echo "  T-300s  Monitor calls app's /drain endpoint"
echo "  T-299s  App closes idle connections, marks active for reconnect"
echo "  T-298s  App opens new connections (route to healthy backends)"
echo "  T+0s    LB migration happens — NO impact (connections already moved)"
echo ""
echo -e "  ${GREEN}Total downtime: 0 SECONDS (proactive drain)${NC}"
echo ""
echo "  Fallback (unplanned failure, no IMDS warning):"
echo ""
echo "  T+0s    Connection goes silent unexpectedly"
echo "  T+20s   TCP_USER_TIMEOUT fires — connection declared dead"
echo "  T+21s   App reconnects immediately"
echo ""
echo -e "  ${GREEN}Worst-case downtime: ~20 SECONDS (TCP_USER_TIMEOUT fallback)${NC}"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo -e "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                    COMPARISON                           ║${NC}"
echo -e "${BLUE}╠══════════════════════════════════════════════════════════╣${NC}"
echo -e "${BLUE}║                                                         ║${NC}"
echo -e "${BLUE}║  Scenario              │ Recovery Time │ Improvement    ║${NC}"
echo -e "${BLUE}║  ─────────────────────-┼──────────────-┼───────────── - ║${NC}"
echo -e "${BLUE}║  ${RED}No monitor (default)${BLUE}   │ ~900s (15min) │ baseline       ║${NC}"
echo -e "${BLUE}║  ${GREEN}Monitor (planned)${BLUE}      │ 0s            │ ∞ (prevented)  ║${NC}"
echo -e "${BLUE}║  ${GREEN}Monitor (unplanned)${BLUE}    │ ~20s          │ 45x faster     ║${NC}"
echo -e "${BLUE}║                                                         ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
echo ""

# Live demo if running on Azure
if curl -s -H "Metadata: true" "http://169.254.169.254/metadata/instance?api-version=2021-02-01" > /dev/null 2>&1; then
    echo -e "${GREEN}Running on Azure VM — live demo available${NC}"
    echo ""
    echo "Starting monitor with file notification..."
    
    python3 "$PROJECT_DIR/monitor.py" \
        --notify-file /tmp/azure-drain-trigger \
        --poll-interval 5 \
        --dry-run \
        --verbose &
    
    echo "Monitor running. Watching for real maintenance events..."
    echo "Press Ctrl+C to stop."
    wait
else
    echo -e "${YELLOW}Not running on Azure VM — showing simulation only.${NC}"
    echo "Deploy to an Azure VM to see live Scheduled Events detection."
fi
