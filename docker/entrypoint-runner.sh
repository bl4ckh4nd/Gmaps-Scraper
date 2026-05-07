#!/usr/bin/env bash
set -euo pipefail

cd /app

mkdir -p "${SCRAPER_ARTIFACT_ROOT:-/data/artifacts}" "${SCRAPER_SESSION_ROOT:-/data/sessions}"

XVFB_PID=""
FLUXBOX_PID=""
X11VNC_PID=""
WEBSOCKIFY_PID=""

cleanup() {
  for pid in "$WEBSOCKIFY_PID" "$X11VNC_PID" "$FLUXBOX_PID" "$XVFB_PID"; do
    if [[ -n "${pid}" ]]; then
      kill "${pid}" >/dev/null 2>&1 || true
    fi
  done
}

trap cleanup EXIT

if [[ "${RUNNER_HEADED:-true}" == "true" ]]; then
  export DISPLAY="${DISPLAY:-:99}"
  Xvfb "${DISPLAY}" -screen 0 "${XVFB_SCREEN_SIZE:-1920x1080x24}" -ac +extension RANDR &
  XVFB_PID="$!"

  fluxbox >/tmp/fluxbox.log 2>&1 &
  FLUXBOX_PID="$!"

  if [[ "${ENABLE_NOVNC:-true}" == "true" ]]; then
    x11vnc -display "${DISPLAY}" -forever -shared -nopw -rfbport 5900 >/tmp/x11vnc.log 2>&1 &
    X11VNC_PID="$!"
    websockify --web=/usr/share/novnc/ 7900 localhost:5900 >/tmp/novnc.log 2>&1 &
    WEBSOCKIFY_PID="$!"
  fi
fi

python main_new.py --migrate-db

exec python main_new.py --runner-service
