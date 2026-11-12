#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -x .venv/bin/python ]]; then
  echo "[setup] creating virtualenv"
  python3 -m venv .venv
  .venv/bin/pip install --quiet --upgrade pip
  .venv/bin/pip install --quiet -r requirements.txt
fi

if [[ ! -d frontend/node_modules ]]; then
  echo "[setup] installing React dependencies"
  npm_config_cache="${NPM_CONFIG_CACHE:-$ROOT/.npm-cache}" npm install --prefix frontend
fi

if [[ ! -f frontend/dist/index.html ]] \
   || [[ -n "$(find frontend/src frontend/index.html -newer frontend/dist/index.html -print -quit 2>/dev/null)" ]]; then
  echo "[setup] building React UI"
  npm --prefix frontend run build
fi

echo "[run] catalog root: ${PQS_DATA_ROOT:-$ROOT/runtime}"
exec .venv/bin/uvicorn app.main:app \
  --host "${PQS_HOST:-127.0.0.1}" --port "${PQS_PORT:-8000}"
