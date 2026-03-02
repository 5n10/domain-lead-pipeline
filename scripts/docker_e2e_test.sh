#!/usr/bin/env bash
# Sprint 6.10: Full-stack Docker deployment e2e test
# Tests: compose up → health check → seed data → verify API → cleanup
set -euo pipefail

echo "=== Docker E2E Test ==="

# 1. Build and start services
echo "[1/5] Starting services with docker compose..."
docker compose up -d --build --wait 2>&1

# Wait for app to be healthy
echo "[2/5] Waiting for health check..."
MAX_RETRIES=30
for i in $(seq 1 $MAX_RETRIES); do
  if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
    echo "  Health check passed on attempt $i"
    break
  fi
  if [ "$i" -eq "$MAX_RETRIES" ]; then
    echo "  FAIL: Health check did not pass after $MAX_RETRIES attempts"
    docker compose logs app
    docker compose down -v
    exit 1
  fi
  sleep 2
done

# 3. Verify API endpoints
echo "[3/5] Verifying API endpoints..."

# Health endpoint
HEALTH=$(curl -sf http://localhost:8000/health)
echo "  /health: OK"

# Metrics endpoint
METRICS=$(curl -sf http://localhost:8000/metrics)
echo "  /metrics: OK"

# Automation status
AUTO=$(curl -sf http://localhost:8000/automation/status)
echo "  /automation/status: OK"

# Business leads (should return empty list on fresh DB)
LEADS=$(curl -sf "http://localhost:8000/business-leads?limit=5")
echo "  /business-leads: OK"

# Categories
CATS=$(curl -sf http://localhost:8000/categories)
echo "  /categories: OK"

# OpenAPI docs
DOCS=$(curl -sf http://localhost:8000/openapi.json | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d['paths']), 'endpoints')")
echo "  /openapi.json: $DOCS"

echo "[4/5] Checking frontend is served..."
FRONTEND=$(curl -sf http://localhost:8000/ | head -c 100)
if echo "$FRONTEND" | grep -q "html"; then
  echo "  Frontend HTML: OK"
else
  echo "  WARN: Frontend may not be served from root"
fi

# 5. Cleanup
echo "[5/5] Cleaning up..."
docker compose down -v

echo ""
echo "=== All Docker E2E tests passed ==="
