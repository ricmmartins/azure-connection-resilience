#!/bin/bash
# Azure TCP Tuning for Connection Resilience
#
# Applies kernel-level TCP parameters that dramatically reduce 
# "black hole" detection time during Azure live migrations.
#
# IMPORTANT: These are SYSTEM-WIDE settings. They affect ALL TCP connections.
# For production, prefer per-socket TCP_USER_TIMEOUT (set by your application).
#
# Usage:
#   sudo ./tcp_tuning.sh [apply|check|revert]

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Recommended values for Azure LB resilience
declare -A RECOMMENDED=(
    ["net.ipv4.tcp_keepalive_time"]="15"
    ["net.ipv4.tcp_keepalive_intvl"]="5"
    ["net.ipv4.tcp_keepalive_probes"]="3"
)

# Optional aggressive settings (uncomment in apply if needed)
# These reduce recovery time further but may affect other traffic
declare -A AGGRESSIVE=(
    ["net.ipv4.tcp_retries2"]="8"          # default 15, reduces max RTO accumulation
    ["net.ipv4.tcp_syn_retries"]="3"       # default 6, faster connection timeout
    ["net.ipv4.tcp_fin_timeout"]="15"      # default 60, faster FIN-WAIT-2 cleanup
)

BACKUP_FILE="/tmp/tcp_tuning_backup_$(date +%Y%m%d_%H%M%S).conf"

check_current() {
    echo -e "${YELLOW}Current TCP settings:${NC}"
    echo ""
    printf "%-35s %-10s %-12s %s\n" "Parameter" "Current" "Recommended" "Status"
    printf "%-35s %-10s %-12s %s\n" "---------" "-------" "-----------" "------"
    
    for param in "${!RECOMMENDED[@]}"; do
        current=$(sysctl -n "$param" 2>/dev/null || echo "N/A")
        recommended="${RECOMMENDED[$param]}"
        
        if [ "$current" = "$recommended" ]; then
            status="${GREEN}✅ OK${NC}"
        else
            status="${RED}⚠️  Needs tuning${NC}"
        fi
        
        printf "%-35s %-10s %-12s %b\n" "$param" "$current" "$recommended" "$status"
    done
    
    echo ""
    echo -e "${YELLOW}Detection time estimate:${NC}"
    
    keepalive_time=$(sysctl -n net.ipv4.tcp_keepalive_time)
    keepalive_intvl=$(sysctl -n net.ipv4.tcp_keepalive_intvl)
    keepalive_probes=$(sysctl -n net.ipv4.tcp_keepalive_probes)
    
    detection_time=$((keepalive_time + keepalive_intvl * keepalive_probes))
    echo "  Keepalive detection: ${detection_time}s (idle_time + interval × probes)"
    echo "  = ${keepalive_time} + ${keepalive_intvl} × ${keepalive_probes} = ${detection_time}s"
    echo ""
    
    if [ "$detection_time" -gt 60 ]; then
        echo -e "  ${RED}⚠️  Current settings allow ${detection_time}s black hole ($(( detection_time / 60 ))+ minutes)${NC}"
    else
        echo -e "  ${GREEN}✅ Detection within ${detection_time}s${NC}"
    fi
    
    echo ""
    echo -e "${YELLOW}Note:${NC} TCP_USER_TIMEOUT (per-socket) is not visible via sysctl."
    echo "      It must be set by the application on each socket."
    echo "      Recommended value: 20000ms (20 seconds)"
}

backup_current() {
    echo "# TCP settings backup - $(date)" > "$BACKUP_FILE"
    for param in "${!RECOMMENDED[@]}"; do
        current=$(sysctl -n "$param" 2>/dev/null)
        echo "${param}=${current}" >> "$BACKUP_FILE"
    done
    echo -e "Backup saved to: ${GREEN}${BACKUP_FILE}${NC}"
}

apply_settings() {
    if [ "$EUID" -ne 0 ]; then
        echo -e "${RED}Error: Must run as root (sudo ./tcp_tuning.sh apply)${NC}"
        exit 1
    fi
    
    echo -e "${YELLOW}Applying Azure connection resilience TCP tuning...${NC}"
    echo ""
    
    # Backup first
    backup_current
    echo ""
    
    # Apply recommended settings
    for param in "${!RECOMMENDED[@]}"; do
        value="${RECOMMENDED[$param]}"
        sysctl -w "${param}=${value}" > /dev/null
        echo -e "  ${GREEN}✅${NC} ${param} = ${value}"
    done
    
    echo ""
    echo -e "${GREEN}TCP tuning applied successfully.${NC}"
    echo ""
    echo "Expected black-hole detection time:"
    echo "  Keepalive: 15s + (5s × 3) = 30s"
    echo "  With TCP_USER_TIMEOUT=20s (app-level): 20s"
    echo ""
    echo -e "${YELLOW}To persist across reboots, add to /etc/sysctl.d/99-azure-resilience.conf:${NC}"
    echo ""
    for param in "${!RECOMMENDED[@]}"; do
        echo "  ${param} = ${RECOMMENDED[$param]}"
    done
    echo ""
    echo -e "${YELLOW}IMPORTANT:${NC} For best results, also set TCP_USER_TIMEOUT=20000"
    echo "in your application code (per-socket setting, not a sysctl)."
}

persist_settings() {
    if [ "$EUID" -ne 0 ]; then
        echo -e "${RED}Error: Must run as root${NC}"
        exit 1
    fi
    
    PERSIST_FILE="/etc/sysctl.d/99-azure-resilience.conf"
    
    echo "# Azure Connection Resilience - TCP Tuning" > "$PERSIST_FILE"
    echo "# Applied by azure-connection-resilience tool" >> "$PERSIST_FILE"
    echo "# $(date)" >> "$PERSIST_FILE"
    echo "" >> "$PERSIST_FILE"
    
    for param in "${!RECOMMENDED[@]}"; do
        echo "${param} = ${RECOMMENDED[$param]}" >> "$PERSIST_FILE"
    done
    
    echo -e "${GREEN}Settings persisted to ${PERSIST_FILE}${NC}"
    echo "They will survive reboots."
}

revert_settings() {
    if [ "$EUID" -ne 0 ]; then
        echo -e "${RED}Error: Must run as root${NC}"
        exit 1
    fi
    
    # Find most recent backup
    latest_backup=$(ls -t /tmp/tcp_tuning_backup_*.conf 2>/dev/null | head -1)
    
    if [ -z "$latest_backup" ]; then
        echo -e "${RED}No backup found. Reverting to Linux defaults...${NC}"
        sysctl -w net.ipv4.tcp_keepalive_time=7200 > /dev/null
        sysctl -w net.ipv4.tcp_keepalive_intvl=75 > /dev/null
        sysctl -w net.ipv4.tcp_keepalive_probes=9 > /dev/null
    else
        echo "Reverting from: $latest_backup"
        while IFS='=' read -r param value; do
            [[ "$param" =~ ^#.*$ ]] && continue
            [ -z "$param" ] && continue
            sysctl -w "${param}=${value}" > /dev/null
            echo -e "  ${GREEN}↩${NC} ${param} = ${value}"
        done < "$latest_backup"
    fi
    
    echo -e "${GREEN}Settings reverted.${NC}"
}

# Main
case "${1:-check}" in
    check)
        check_current
        ;;
    apply)
        apply_settings
        ;;
    persist)
        apply_settings
        persist_settings
        ;;
    revert)
        revert_settings
        ;;
    *)
        echo "Usage: $0 [check|apply|persist|revert]"
        echo ""
        echo "  check   - Show current settings vs recommended (default)"
        echo "  apply   - Apply recommended settings (requires sudo)"
        echo "  persist - Apply and persist across reboots (requires sudo)"
        echo "  revert  - Revert to previous settings (requires sudo)"
        exit 1
        ;;
esac
