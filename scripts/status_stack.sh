#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT_DIR/run/api.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "API status: stopped"
  exit 0
fi

api_pid="$(cat "$PID_FILE")"
if [[ -n "$api_pid" ]] && ps -p "$api_pid" >/dev/null 2>&1; then
  echo "API status: running (PID $api_pid)"
else
  echo "API status: stale PID file ($api_pid)"
fi
