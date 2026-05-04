#!/bin/bash
# TCP Tuning for Azure Connection Resilience
#
# Adjusts kernel TCP keepalive parameters to detect dead IDLE connections faster.
# Default Linux settings wait ~15 minutes (7200 + 75*9 = 7875s).
# These settings detect dead idle sockets in ~60s (30 + 10*3 = 60s).
#
# IMPORTANT: Keepalive only helps IDLE sockets (those with SO_KEEPALIVE enabled).
# For ACTIVE connections with in-flight data, TCP_USER_TIMEOUT is what matters.
# That must be set per-socket by the application (see examples below).
#
# Detection timeline with these settings:
#   - Active connections (TCP_USER_TIMEOUT=20s): dead in ~20s
#   - Idle connections (keepalive): dead in ~60s
#   - Default Linux (no tuning): dead in ~15 minutes
#
# Usage:
#   ./tcp_tuning.sh check    # Show current vs recommended
#   ./tcp_tuning.sh apply    # Apply recommended settings (needs root)
#   ./tcp_tuning.sh persist  # Apply + make permanent (survives reboot)
#   ./tcp_tuning.sh revert   # Restore original settings from backup

set -euo pipefail

# Recommended values — balanced between fast detection and false-positive avoidance.
# 5/5/3 is too aggressive: a 20s network blip would kill healthy connections.
# 30/10/3 gives 60s total detection for idle sockets, which is conservative enough
# to avoid false positives from transient Azure network hiccups.
KEEPALIVE_TIME=30     # Seconds idle before first keepalive probe (default: 7200)
KEEPALIVE_INTVL=10    # Seconds between probes (default: 75)
KEEPALIVE_PROBES=3    # Failed probes before declaring dead (default: 9)
# Total idle detection: 30 + 10*3 = 60s

# TCP_USER_TIMEOUT (the PRIMARY setting for active connections):
# Must be set per-socket by the application. Recommended: 20000ms (20s).
# This is what prevents the 15-minute stall on connections actively sending data.

BACKUP_FILE="/tmp/tcp_tuning_backup.conf"
SYSCTL_CONF="/etc/sysctl.d/99-azure-resilience.conf"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

show_current() {
    local param="$1"
    local recommended="$2"
    local current
    current=$(sysctl -n "$param" 2>/dev/null || echo "N/A")

    if [[ "$current" == "$recommended" ]]; then
        echo -e "  ${GREEN}✓${NC} $param = $current (optimal)"
    else
        echo -e "  ${RED}✗${NC} $param = $current → recommended: ${GREEN}$recommended${NC}"
    fi
}

calculate_detection_time() {
    local kt kp ki
    kt=$(sysctl -n net.ipv4.tcp_keepalive_time 2>/dev/null || echo 7200)
    ki=$(sysctl -n net.ipv4.tcp_keepalive_intvl 2>/dev/null || echo 75)
    kp=$(sysctl -n net.ipv4.tcp_keepalive_probes 2>/dev/null || echo 9)
    local total=$((kt + ki * kp))
    echo "$total"
}

cmd_check() {
    echo -e "${BLUE}TCP Keepalive Settings — Current vs Recommended${NC}"
    echo ""
    show_current "net.ipv4.tcp_keepalive_time" "$KEEPALIVE_TIME"
    show_current "net.ipv4.tcp_keepalive_intvl" "$KEEPALIVE_INTVL"
    show_current "net.ipv4.tcp_keepalive_probes" "$KEEPALIVE_PROBES"
    echo ""

    local current_detect
    current_detect=$(calculate_detection_time)
    local recommended_detect=$((KEEPALIVE_TIME + KEEPALIVE_INTVL * KEEPALIVE_PROBES))

    echo -e "  Current detection time:     ${RED}${current_detect}s ($((current_detect / 60)) min)${NC}"
    echo -e "  Recommended detection time: ${GREEN}${recommended_detect}s${NC}"
    echo ""
    echo -e "${YELLOW}IMPORTANT:${NC} These keepalive settings only help IDLE sockets."
    echo "  For ACTIVE connections (data in-flight), TCP_USER_TIMEOUT is what matters."
    echo "  It must be set per-socket by your application. Recommended: 20 seconds."
    echo ""
    echo -e "  ${BLUE}Per-socket TCP_USER_TIMEOUT examples:${NC}"
    echo "  Python:  sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT, 20000)"
    echo "  Go:      syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, 0x12, 20000) // TCP_USER_TIMEOUT=18"
    echo "  Java:    channel.setOption(ExtendedSocketOptions.TCP_USER_TIMEOUT, 20000)"
    echo "  C:       setsockopt(fd, IPPROTO_TCP, TCP_USER_TIMEOUT, &timeout, sizeof(timeout))"
    echo ""
    echo -e "  ${BLUE}Per-socket SO_KEEPALIVE (required for keepalive to apply):${NC}"
    echo "  Python:  sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)"
    echo "  Go:      conn.(*net.TCPConn).SetKeepAlive(true)"
    echo "  Java:    socket.setOption(StandardSocketOptions.SO_KEEPALIVE, true)"
}

cmd_apply() {
    if [[ $EUID -ne 0 ]]; then
        echo -e "${RED}Root required. Run: sudo $0 apply${NC}"
        exit 1
    fi

    # Backup current values
    echo "# TCP tuning backup — $(date)" > "$BACKUP_FILE"
    echo "net.ipv4.tcp_keepalive_time=$(sysctl -n net.ipv4.tcp_keepalive_time)" >> "$BACKUP_FILE"
    echo "net.ipv4.tcp_keepalive_intvl=$(sysctl -n net.ipv4.tcp_keepalive_intvl)" >> "$BACKUP_FILE"
    echo "net.ipv4.tcp_keepalive_probes=$(sysctl -n net.ipv4.tcp_keepalive_probes)" >> "$BACKUP_FILE"
    echo -e "${YELLOW}Backed up current settings → ${BACKUP_FILE}${NC}"

    # Apply
    sysctl -w net.ipv4.tcp_keepalive_time=$KEEPALIVE_TIME > /dev/null
    sysctl -w net.ipv4.tcp_keepalive_intvl=$KEEPALIVE_INTVL > /dev/null
    sysctl -w net.ipv4.tcp_keepalive_probes=$KEEPALIVE_PROBES > /dev/null

    echo -e "${GREEN}✅ TCP keepalive settings applied.${NC}"
    echo ""
    cmd_check
}

cmd_persist() {
    if [[ $EUID -ne 0 ]]; then
        echo -e "${RED}Root required. Run: sudo $0 persist${NC}"
        exit 1
    fi

    cmd_apply

    cat > "$SYSCTL_CONF" << EOF
# Azure Connection Resilience — TCP Keepalive Tuning
# Applied by tcp_tuning.sh on $(date)
# Reduces dead IDLE connection detection from ~15min to ~60s
# For active connections, set TCP_USER_TIMEOUT=20s per-socket in your app
net.ipv4.tcp_keepalive_time = $KEEPALIVE_TIME
net.ipv4.tcp_keepalive_intvl = $KEEPALIVE_INTVL
net.ipv4.tcp_keepalive_probes = $KEEPALIVE_PROBES
EOF

    echo -e "${GREEN}✅ Settings persisted to ${SYSCTL_CONF}${NC}"
    echo "  Will survive reboots."
}

cmd_revert() {
    if [[ $EUID -ne 0 ]]; then
        echo -e "${RED}Root required. Run: sudo $0 revert${NC}"
        exit 1
    fi

    if [[ ! -f "$BACKUP_FILE" ]]; then
        echo -e "${RED}No backup found at ${BACKUP_FILE}${NC}"
        echo "Restoring Linux defaults..."
        sysctl -w net.ipv4.tcp_keepalive_time=7200 > /dev/null
        sysctl -w net.ipv4.tcp_keepalive_intvl=75 > /dev/null
        sysctl -w net.ipv4.tcp_keepalive_probes=9 > /dev/null
    else
        echo "Restoring from backup..."
        while IFS='=' read -r key value; do
            [[ "$key" == \#* ]] && continue
            [[ -z "$key" ]] && continue
            sysctl -w "$key=$value" > /dev/null
        done < "$BACKUP_FILE"
    fi

    # Remove persistent config if it exists
    rm -f "$SYSCTL_CONF" 2>/dev/null || true

    echo -e "${GREEN}✅ Settings reverted.${NC}"
    cmd_check
}

# Main
case "${1:-check}" in
    check)   cmd_check ;;
    apply)   cmd_apply ;;
    persist) cmd_persist ;;
    revert)  cmd_revert ;;
    *)
        echo "Usage: $0 {check|apply|persist|revert}"
        echo ""
        echo "  check   — Show current vs recommended settings"
        echo "  apply   — Apply settings (runtime only, needs root)"
        echo "  persist — Apply + save to /etc/sysctl.d/ (survives reboot)"
        echo "  revert  — Restore original settings from backup"
        exit 1
        ;;
esac
