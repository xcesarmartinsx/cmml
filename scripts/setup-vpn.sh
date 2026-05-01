#!/usr/bin/env bash
# ==============================================================================
# setup-vpn.sh — Tailscale VPN + UFW Firewall Setup for CMML Server
#
# Installs Tailscale (WireGuard-based mesh VPN) and configures UFW firewall
# to restrict public access, allowing only VPN and LAN traffic.
#
# Usage: sudo bash scripts/setup-vpn.sh
# ==============================================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[+]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
err()  { echo -e "${RED}[✗]${NC} $*" >&2; }

# Must run as root
if [[ $EUID -ne 0 ]]; then
    err "This script must be run as root (sudo)"
    exit 1
fi

LAN_SUBNET="192.168.1.0/24"
TAILSCALE_IFACE="tailscale0"
TAILSCALE_UDP_PORT="41641"

# ---------- Step 1: Install Tailscale ----------
log "Installing Tailscale..."
if command -v tailscale &>/dev/null; then
    warn "Tailscale already installed: $(tailscale version)"
else
    curl -fsSL https://tailscale.com/install.sh | sh
    log "Tailscale installed successfully"
fi

# ---------- Step 2: Start Tailscale ----------
log "Starting Tailscale with SSH enabled..."
if tailscale status &>/dev/null; then
    warn "Tailscale already running"
    tailscale status
else
    tailscale up --ssh
fi

TAILSCALE_IP=$(tailscale ip -4 2>/dev/null || echo "unknown")
log "Tailscale IP: ${TAILSCALE_IP}"

# ---------- Step 3: Install and Configure UFW ----------
log "Installing UFW..."
apt-get install -y ufw > /dev/null 2>&1

log "Configuring UFW rules..."

# Reset to clean state (non-interactive)
ufw --force reset > /dev/null 2>&1

# Default policies
ufw default deny incoming > /dev/null
ufw default allow outgoing > /dev/null

# SSH — keep open as emergency fallback
ufw allow 22/tcp comment "SSH fallback" > /dev/null

# Tailscale interface — allow all VPN traffic
ufw allow in on "${TAILSCALE_IFACE}" comment "Tailscale VPN" > /dev/null

# LAN — allow local network access
ufw allow from "${LAN_SUBNET}" comment "LAN local" > /dev/null

# Tailscale UDP — required for NAT hole-punching
ufw allow "${TAILSCALE_UDP_PORT}/udp" comment "Tailscale WireGuard" > /dev/null

# Enable firewall
ufw --force enable > /dev/null

log "UFW enabled with the following rules:"
ufw status numbered

# ---------- Step 4: Summary ----------
echo ""
echo "=============================================="
echo "  VPN + Firewall Setup Complete"
echo "=============================================="
echo ""
echo "  Tailscale IP:  ${TAILSCALE_IP}"
echo "  Firewall:      UFW active (deny incoming)"
echo ""
echo "  Allowed access:"
echo "    - SSH (port 22) from anywhere (fallback)"
echo "    - All traffic via Tailscale VPN"
echo "    - All traffic from LAN (${LAN_SUBNET})"
echo "    - UDP ${TAILSCALE_UDP_PORT} (Tailscale hole-punching)"
echo ""
echo "  Services accessible via VPN:"
echo "    http://${TAILSCALE_IP}:3001/  — Business 360°"
echo "    http://${TAILSCALE_IP}:3000/  — Dashboard ML"
echo "    http://${TAILSCALE_IP}:8001/  — API FastAPI"
echo ""
echo "  Next steps:"
echo "    1. Install Tailscale on your MacBook"
echo "    2. Login with the same account"
echo "    3. Access services via the Tailscale IP above"
echo "=============================================="
