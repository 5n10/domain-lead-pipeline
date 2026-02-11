#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="$ROOT_DIR/run"
LOG_DIR="$ROOT_DIR/logs"
PID_FILE="$PID_DIR/api.pid"

mkdir -p "$PID_DIR" "$LOG_DIR"

if [[ -f "$PID_FILE" ]]; then
  existing_pid="$(cat "$PID_FILE")"
  if [[ -n "$existing_pid" ]] && ps -p "$existing_pid" >/dev/null 2>&1; then
    echo "API already running (PID $existing_pid)"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

VENV_PYTHON="$ROOT_DIR/.venv/bin/python"
if [[ ! -x "$VENV_PYTHON" ]]; then
  echo "Missing virtualenv Python at $VENV_PYTHON"
  echo "Run: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
  exit 1
fi

API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-8000}"

cd "$ROOT_DIR"
nohup env PYTHONPATH=src "$VENV_PYTHON" scripts/run_api.py --host "$API_HOST" --port "$API_PORT" > "$LOG_DIR/api.log" 2>&1 &
api_pid=$!
echo "$api_pid" > "$PID_FILE"

sleep 1
if ! ps -p "$api_pid" >/dev/null 2>&1; then
  echo "API failed to start. See $LOG_DIR/api.log"
  rm -f "$PID_FILE"
  exit 1
fi

echo "API started (PID $api_pid)"
echo "UI: http://$API_HOST:$API_PORT"
