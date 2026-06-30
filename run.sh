#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${KEEPONN_ENV:-$DIR/keeponn.env}"
PYTHON="${DIR}/.venv/bin/python"

if [[ ! -x "$PYTHON" ]]; then
  echo "Missing virtualenv at $DIR/.venv — run: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt" >&2
  exit 1
fi

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

exec "$PYTHON" -u "$DIR/keep_onn.py"
