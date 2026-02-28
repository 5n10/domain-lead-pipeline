#!/usr/bin/env bash
# start-server.sh — Launched by macOS launchd on login.
# Waits for Docker + PostgreSQL, then starts the API server.
set -euo pipefail

PROJECT_DIR="/Users/ku3h/domain business/domain-lead-pipeline"
VENV_PYTHON="$PROJECT_DIR/.venv/bin/python3"
UVICORN="$PROJECT_DIR/.venv/bin/uvicorn"
LOG_DIR="$PROJECT_DIR/logs"

mkdir -p "$LOG_DIR"

# ── 1. Wait for Docker daemon (up to 120s) ──────────────────────────
echo "[$(date)] Waiting for Docker daemon..."
for i in $(seq 1 60); do
    if docker info >/dev/null 2>&1; then
        echo "[$(date)] Docker is ready."
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "[$(date)] ERROR: Docker not available after 120s — aborting."
        exit 1
    fi
    sleep 2
done

# ── 2. Start Docker Compose services ────────────────────────────────
echo "[$(date)] Starting Docker Compose services..."
cd "$PROJECT_DIR"
docker compose up -d 2>&1

# ── 3. Wait for PostgreSQL (up to 60s) ──────────────────────────────
echo "[$(date)] Waiting for PostgreSQL..."
for i in $(seq 1 30); do
    if docker compose exec -T postgres pg_isready -U postgres >/dev/null 2>&1; then
        echo "[$(date)] PostgreSQL is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "[$(date)] ERROR: PostgreSQL not ready after 60s — aborting."
        exit 1
    fi
    sleep 2
done

# ── 4. Rebuild frontend (in case code was updated) ──────────────────
echo "[$(date)] Building frontend..."
cd "$PROJECT_DIR/frontend"
npm run build 2>&1 || echo "[$(date)] WARNING: Frontend build failed (non-fatal)."

# ── 5. Start API server (production mode — no --reload) ─────────────
echo "[$(date)] Starting API server on port 8000..."
cd "$PROJECT_DIR"
exec "$UVICORN" src.domain_pipeline.api:app \
    --host 127.0.0.1 \
    --port 8000 \
    --workers 1 \
    --log-level info
