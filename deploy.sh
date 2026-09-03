#!/bin/bash
# deploy.sh — Oracle Cloud Ubuntu 22.04 aarch64 initial setup
# Run as ubuntu user: bash deploy.sh

set -euo pipefail
PROJECT_DIR="/home/ubuntu/jaetech247"
VENV="$PROJECT_DIR/venv"

echo "=== JaeTech247 Deployment Script ==="
echo "Server: $(uname -m) | $(lsb_release -d | cut -f2)"

# 1. System packages
echo "[1] Installing system dependencies..."
sudo apt-get update -q
sudo apt-get install -y -q \
    python3.11 python3.11-venv python3.11-dev \
    build-essential libssl-dev libffi-dev \
    nginx certbot python3-certbot-nginx \
    sqlite3 git curl \
    tzdata

# Set timezone to KST (for stock market checks)
sudo timedatectl set-timezone Asia/Seoul
echo "[✓] Timezone: $(timedatectl | grep 'Time zone')"

# 2. Create project directory
echo "[2] Setting up project directory..."
mkdir -p "$PROJECT_DIR/uploads" "$PROJECT_DIR/logs"

# 3. Python virtual environment
echo "[3] Creating Python venv..."
python3.11 -m venv "$VENV"
source "$VENV/bin/activate"
pip install --upgrade pip wheel
pip install uvloop  # ARM64 compatible event loop
pip install -r "$PROJECT_DIR/requirements.txt"
echo "[✓] Python dependencies installed"

# 4. Copy .env
if [ ! -f "$PROJECT_DIR/.env" ]; then
    cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
    echo "[!] Created .env from example — EDIT BEFORE STARTING"
    echo "    nano $PROJECT_DIR/.env"
fi

# 5. Nginx config
echo "[4] Configuring Nginx..."
sudo cp "$PROJECT_DIR/nginx.conf" /etc/nginx/sites-available/jaetech247.pro
sudo ln -sf /etc/nginx/sites-available/jaetech247.pro /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx
echo "[✓] Nginx configured"

# 6. SSL (Let's Encrypt) — comment out if domain not yet pointed
# echo "[5] Obtaining SSL certificate..."
# sudo certbot --nginx -d jaetech247.pro -d www.jaetech247.pro --non-interactive --agree-tos -m admin@jaetech247.pro

# 7. Systemd service
echo "[6] Installing systemd service..."
sudo cp "$PROJECT_DIR/jaetech247.service" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable jaetech247
sudo systemctl restart jaetech247
echo "[✓] Service started"
sudo systemctl status jaetech247 --no-pager -l

echo ""
echo "=== Deployment Complete ==="
echo "API running at: http://127.0.0.1:8080"
echo "Public URL: https://jaetech247.pro"
echo ""
echo "Useful commands:"
echo "  sudo systemctl status jaetech247"
echo "  sudo journalctl -u jaetech247 -f"
echo "  sudo systemctl restart jaetech247"
