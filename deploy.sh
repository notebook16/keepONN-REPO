#!/usr/bin/env bash
# Install keepONN as a systemd service on Ubuntu EC2.
# Usage:
#   sudo ./deploy.sh          # install / update
#   sudo ./deploy.sh --remove # stop and remove systemd unit
set -euo pipefail

INSTALL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_USER="${SERVICE_USER:-${SUDO_USER:-ubuntu}}"
SERVICE_NAME="keeponn"
UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}.service"

remove_service() {
  if [[ "$(id -u)" -ne 0 ]]; then
    echo "Run with sudo: sudo ./deploy.sh --remove"
    exit 1
  fi

  if systemctl is-active "${SERVICE_NAME}" &>/dev/null; then
    systemctl stop "${SERVICE_NAME}" || true
  fi
  if systemctl is-enabled "${SERVICE_NAME}" &>/dev/null; then
    systemctl disable "${SERVICE_NAME}" || true
  fi
  rm -f "${UNIT_PATH}"
  systemctl daemon-reload 2>/dev/null || true
  echo "Removed ${SERVICE_NAME} systemd unit."
  exit 0
}

if [[ "${1:-}" == "--remove" ]]; then
  remove_service
fi

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run with sudo: sudo ./deploy.sh"
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found. Install: sudo apt install -y python3 python3-venv python3-pip"
  exit 1
fi

# Fix Windows CRLF if repo was edited on Windows
for f in run.sh deploy.sh keeponn.env.example keeponn.service; do
  [[ -f "$INSTALL_DIR/$f" ]] && sed -i 's/\r$//' "$INSTALL_DIR/$f"
done

if [[ ! -d "$INSTALL_DIR/.venv" ]]; then
  echo "Creating virtualenv at $INSTALL_DIR/.venv"
  sudo -u "$SERVICE_USER" python3 -m venv "$INSTALL_DIR/.venv"
fi

echo "Installing Python dependencies..."
sudo -u "$SERVICE_USER" "$INSTALL_DIR/.venv/bin/pip" install -q -r "$INSTALL_DIR/requirements.txt"

if [[ ! -f "$INSTALL_DIR/keeponn.env" ]]; then
  cp "$INSTALL_DIR/keeponn.env.example" "$INSTALL_DIR/keeponn.env"
  sed -i 's/\r$//' "$INSTALL_DIR/keeponn.env"
  echo "Created $INSTALL_DIR/keeponn.env from example — edit Redis creds before production use."
fi

chmod +x "$INSTALL_DIR/run.sh" "$INSTALL_DIR/deploy.sh"
mkdir -p "$INSTALL_DIR/data"
chown -R "${SERVICE_USER}:${SERVICE_USER}" "$INSTALL_DIR/.venv" "$INSTALL_DIR/data"
chown "${SERVICE_USER}:${SERVICE_USER}" "$INSTALL_DIR/keeponn.env" 2>/dev/null || true

timedatectl set-timezone Asia/Kolkata 2>/dev/null || true

sed -e "s|@@INSTALL_DIR@@|${INSTALL_DIR}|g" \
    -e "s|@@SERVICE_USER@@|${SERVICE_USER}|g" \
  "$INSTALL_DIR/keeponn.service" \
  > "${UNIT_PATH}"
chmod 644 "${UNIT_PATH}"

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"
systemctl restart "${SERVICE_NAME}"

echo ""
echo "Deployed keepONN (systemd, IST timezone)"
echo "  repo:    $INSTALL_DIR"
echo "  unit:    ${UNIT_PATH}"
echo "  env:     $INSTALL_DIR/keeponn.env"
echo "  data:    $INSTALL_DIR/data/"
echo ""
systemctl status "${SERVICE_NAME}" --no-pager -l || true
echo ""
echo "Logs: sudo journalctl -u ${SERVICE_NAME} -f"
echo "Remove: sudo ./deploy.sh --remove"
