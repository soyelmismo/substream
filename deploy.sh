#!/bin/bash
# 🚀 Substream Automatic Deployment Script
# Compiles locally for Linux amd64 and deploys via SSH.

set -e

# --- Configuration (Adjust these or use .env) ---
if [ -f ".env" ]; then
    set -a
    source .env
    set +a
fi

SERVER="${DEPLOY_SERVER:-100.66.0.2}"
SSH_PORT="${DEPLOY_PORT:-2369}"
SSH_USER="${DEPLOY_USER:-root}"
PATH_REMOTE="${DEPLOY_PATH:-/root/.substream}"
SERVICE_NAME="${SERVICE_NAME:-substream}"
LISTEN_ADDR="${LISTEN_ADDR:-0.0.0.0:4533}"
TIDAL_TOKEN="${TIDAL_TOKENS:-${TIDAL_TOKEN:-${SUBSTREAM_TIDAL_TOKENS:-${SUBSTREAM_TIDAL_TOKEN:-}}}}"
DEEZER_ARL="${DEEZER_ARL:-${SUBSTREAM_DEEZER_ARL:-}}"
DEEZER_SECRET_KEY="${DEEZER_SECRET_KEY:-${SUBSTREAM_DEEZER_SECRET_KEY:-}}"
DEEZER_STREAM_FILTER="${DEEZER_STREAM_FILTER:-${SUBSTREAM_DEEZER_STREAM_FILTER:-}}"
QOBUZ_TOKEN="${QOBUZ_TOKEN:-${SUBSTREAM_QOBUZ_TOKEN:-}}"
SPOTIFY_SP_DC="${SPOTIFY_SP_DC:-${SUBSTREAM_SPOTIFY_SP_DC:-}}"
SPOTIFY_WVD_PATH="${SPOTIFY_WVD_PATH:-${SUBSTREAM_SPOTIFY_WVD_PATH:-spotify.wvd}}"

REMOTE="${SSH_USER}@${SERVER}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}#################################################${NC}"
echo -e "${GREEN}#      Substream 🌊 Automatic Deploy System     #${NC}"
echo -e "${GREEN}#################################################${NC}"

# 1. Local Build
echo -e "\n${BLUE}[1/4] Building for Linux amd64...${NC}"
GOOS=linux GOARCH=amd64 go build -o substream_linux ./cmd/substream
echo -e "${GREEN}✅ Build successful!${NC}"

# 2. Prepare Remote Server
echo -e "\n${BLUE}[2/4] Preparing Remote Server...${NC}"
ssh -p "${SSH_PORT}" "${REMOTE}" "mkdir -p ${PATH_REMOTE} && systemctl stop ${SERVICE_NAME} 2>/dev/null || true"

# 3. Upload Binary & Tools
echo -e "\n${BLUE}[3/4] Uploading Binary...${NC}"
scp -P "${SSH_PORT}" substream_linux "${REMOTE}:${PATH_REMOTE}/substream"

# Si existen certificados locales, los subimos (opcional)
if [ -f "cert.pem" ] && [ -f "key.pem" ]; then
    echo -e "${YELLOW}Uploading certificates...${NC}"
    scp -P "${SSH_PORT}" cert.pem key.pem "${REMOTE}:${PATH_REMOTE}/"
fi

# Si existe WVD local, lo subimos
LOCAL_WVD="${SPOTIFY_WVD_PATH}"
if [ -f "${LOCAL_WVD}" ]; then
    echo -e "${YELLOW}Uploading ${LOCAL_WVD} device as spotify.wvd...${NC}"
    scp -P "${SSH_PORT}" "${LOCAL_WVD}" "${REMOTE}:${PATH_REMOTE}/spotify.wvd"
elif [ -f "spotify.wvd" ]; then
    echo -e "${YELLOW}Uploading spotify.wvd device...${NC}"
    scp -P "${SSH_PORT}" spotify.wvd "${REMOTE}:${PATH_REMOTE}/spotify.wvd"
fi

REMOTE_WVD="${SPOTIFY_WVD_PATH}"
if [ -f "${LOCAL_WVD}" ] || [ -f "spotify.wvd" ]; then
    REMOTE_WVD="spotify.wvd"
fi

# 4. Remote Setup & Service Restart
echo -e "\n${BLUE}[4/4] Configuring Systemd & Restarting...${NC}"
ssh -p "${SSH_PORT}" "${REMOTE}" "bash -s" << ENDSSH
set -e
chmod +x "${PATH_REMOTE}/substream"

SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

echo "📝 Creating systemd service file..."
cat > "\$SERVICE_FILE" << EOF
[Unit]
Description=Substream
After=network.target

[Service]
Type=simple
User=${SSH_USER}
WorkingDirectory=${PATH_REMOTE}
Environment="SUBSTREAM_TIDAL_TOKEN=${TIDAL_TOKEN}"
Environment="SUBSTREAM_DEEZER_ARL=${DEEZER_ARL}"
Environment="SUBSTREAM_DEEZER_SECRET_KEY=${DEEZER_SECRET_KEY}"
Environment="SUBSTREAM_DEEZER_STREAM_FILTER=${DEEZER_STREAM_FILTER}"
Environment="SUBSTREAM_QOBUZ_TOKEN=${QOBUZ_TOKEN}"
Environment="SUBSTREAM_SPOTIFY_SP_DC=${SPOTIFY_SP_DC}"
Environment="SUBSTREAM_SPOTIFY_WVD_PATH=${REMOTE_WVD}"
ExecStart=${PATH_REMOTE}/substream --listen-addr="${LISTEN_ADDR}" --cert-path="cert.pem" --key-path="key.pem"
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

echo "🔄 Reloading systemd and restarting service..."
systemctl daemon-reload
systemctl enable ${SERVICE_NAME}
systemctl restart ${SERVICE_NAME}

echo -e "\n${GREEN}✨ DEPLOYMENT SUCCESSFUL!${NC}"
echo "-----------------------------------------------"
echo "📊 Status for ${SERVICE_NAME}:"
systemctl is-active ${SERVICE_NAME} --quiet && echo "ONLINE ✅" || echo "OFFLINE ❌"
echo "-----------------------------------------------"
ENDSSH

echo -e "\n${BLUE}Monitor logs with:${NC}"
echo -e "${YELLOW}ssh -p ${SSH_PORT} ${REMOTE} 'journalctl -u ${SERVICE_NAME} -f'${NC}\n"