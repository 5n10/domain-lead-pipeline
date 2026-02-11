#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$ROOT_DIR/run/api.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "API is not running (no PID file)."
  exit 0
fi

api_pid="$(cat "$PID_FILE")"
if [[ -z "$api_pid" ]]; then
  rm -f "$PID_FILE"
  echo "Removed empty PID file."
  exit 0
fi

if ps -p "$api_pid" >/dev/null 2>&1; then
  kill "$api_pid"
  echo "Stopped API (PID $api_pid)"
else
  echo "Process $api_pid not found. Cleaning up PID file."
fi

rm -f "$PID_FILE"
