#!/usr/bin/env bash
set -euo pipefail

cd /app

python main_new.py --migrate-db

exec python main_new.py --scheduler
