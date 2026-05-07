#!/usr/bin/env bash
set -euo pipefail

cd /app

python main_new.py --migrate-db

exec gunicorn \
  --bind 0.0.0.0:5000 \
  --workers "${WEB_WORKERS:-1}" \
  --timeout "${WEB_TIMEOUT:-600}" \
  web.app:app
