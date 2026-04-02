#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv"

cd "${ROOT_DIR}"

if [[ ! -f "${ROOT_DIR}/.env" ]]; then
  echo ".env is missing. Copy .env.example to .env and fill in your settings." >&2
  exit 1
fi

if [[ ! -d "${VENV_DIR}" ]]; then
  echo "Creating virtualenv in ${VENV_DIR}..."
  python3 -m venv "${VENV_DIR}"
fi

. "${VENV_DIR}/bin/activate"

echo "Installing/updating Python dependencies..."
pip install -r requirements.txt

echo "Starting updater loop..."
exec python -m app.updater_main
