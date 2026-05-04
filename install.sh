#!/bin/bash
# Install Azure Connection Resilience Monitor as a systemd service
#
# Usage:
#   sudo ./install.sh              # Install and start
#   sudo ./install.sh --uninstall  # Remove service

set -euo pipefail

SERVICE_NAME="azure-resilience-monitor"
INSTALL_DIR="/opt/azure-resilience"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}This script must be run as root (sudo)${NC}"
    exit 1
fi

if [[ "${1:-}" == "--uninstall" ]]; then
    echo -e "${YELLOW}Uninstalling ${SERVICE_NAME}...${NC}"
    systemctl stop "$SERVICE_NAME" 2>/dev/null || true
    systemctl disable "$SERVICE_NAME" 2>/dev/null || true
    rm -f "$SERVICE_FILE"
    rm -rf "$INSTALL_DIR"
    systemctl daemon-reload
    echo -e "${GREEN}Uninstalled.${NC}"
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${GREEN}Installing Azure Connection Resilience Monitor${NC}"
echo ""

# Create install directory
mkdir -p "$INSTALL_DIR"
cp "$SCRIPT_DIR/monitor.py" "$INSTALL_DIR/"
cp "$SCRIPT_DIR/couchbase_adapter.py" "$INSTALL_DIR/"
cp "$SCRIPT_DIR/config.yaml" "$INSTALL_DIR/"
cp "$SCRIPT_DIR/tcp_tuning.sh" "$INSTALL_DIR/"
cp "$SCRIPT_DIR/requirements.txt" "$INSTALL_DIR/"
chmod +x "$INSTALL_DIR/tcp_tuning.sh"

# Install Python dependencies
echo "Installing Python dependencies..."
pip3 install --quiet -r "$INSTALL_DIR/requirements.txt" 2>/dev/null || \
    pip install --quiet -r "$INSTALL_DIR/requirements.txt"

# Create systemd service
cat > "$SERVICE_FILE" << EOF
[Unit]
Description=Azure Connection Resilience Monitor
After=network.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/python3 ${INSTALL_DIR}/monitor.py \\
    --notify-url http://localhost:8099/drain \\
    --poll-interval 1 \\
    --metrics-port 9090 \\
    --verbose
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadOnlyPaths=/
ReadWritePaths=/tmp

[Install]
WantedBy=multi-user.target
EOF

# Apply TCP tuning
echo ""
echo -e "${YELLOW}Applying TCP keepalive tuning...${NC}"
"$INSTALL_DIR/tcp_tuning.sh" apply

# Enable and start
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl start "$SERVICE_NAME"

echo ""
echo -e "${GREEN}✅ Installation complete!${NC}"
echo ""
echo "  Service:  systemctl status ${SERVICE_NAME}"
echo "  Logs:     journalctl -u ${SERVICE_NAME} -f"
echo "  Metrics:  curl http://localhost:9090/metrics"
echo "  Config:   ${INSTALL_DIR}/config.yaml"
echo ""
echo "  To customize notification URL, edit:"
echo "    ${SERVICE_FILE}"
echo "  Then: systemctl daemon-reload && systemctl restart ${SERVICE_NAME}"
