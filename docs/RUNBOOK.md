# Domain Lead Pipeline -- Operational Runbook

Sprint 5.8 -- Operational procedures for running, monitoring, and troubleshooting the Domain Lead Pipeline.

---

## Setup

### Backend

```bash
git clone <repo-url> domain-lead-pipeline
cd domain-lead-pipeline
cp .env.example .env          # Edit .env with your API keys and database URL
pip install -r requirements.txt
alembic upgrade head          # Initialize / migrate the database
docker-compose up -d          # Start SearXNG meta-search instance
PYTHONPATH=src uvicorn domain_pipeline.api:app --host 0.0.0.0 --port 8000
```

### Frontend

```bash
cd frontend
npm install
npm run dev                   # Starts Vite dev server on http://localhost:5173
```

---

## Database Backup and Restore

### Backup

```bash
pg_dump -Fc -h localhost domain_leads > backup.dump
```

### Restore

```bash
pg_restore -d domain_leads backup.dump
```

For a clean restore into an empty database:

```bash
createdb domain_leads_restore
pg_restore -d domain_leads_restore backup.dump
```

---

## Monitoring

### Health Check

```
GET /health
```

Returns status of core dependencies: database connectivity, SearXNG availability, and API key presence. Use this for uptime checks and load balancer health probes.

### Pipeline Metrics

```
GET /api/metrics
```

Returns pipeline statistics including total businesses discovered, verified, scored, and exported.

### Automation Status

```
GET /api/automation/status
```

Returns the state of the automation loop and verification runner, including whether each is running, the last run time, totals processed, and the last error (if any).

### Dead Letter Queue

```
GET /api/dead-letter
```

Returns businesses that failed all verification attempts. High counts here indicate potential data quality or upstream API issues.

---

## Common Operations

### Automation Control

| Action | Method | Endpoint |
|---|---|---|
| Start automation loop | POST | `/api/automation/start` |
| Stop automation loop | POST | `/api/automation/stop` |
| Start verification loop | POST | `/api/automation/start-verification` |
| Stop verification loop | POST | `/api/automation/stop-verification` |
| Run daily target now | POST | `/api/automation/daily-target-now` |

### Manual Pipeline Actions

| Action | Method | Endpoint |
|---|---|---|
| Trigger a pipeline run | POST | `/api/actions/pipeline-run` |
| Score businesses | POST | `/api/actions/business-score` |
| Export leads | POST | `/api/actions/business-export` |

---

## Troubleshooting

### Database connection issues

1. Verify `DATABASE_URL` in `.env` is correct.
2. Confirm PostgreSQL is running: `pg_isready -h localhost -p 5432`
3. Check the `/health` endpoint for database status details.
4. Ensure the database exists: `psql -l | grep domain_leads`

### SearXNG not responding

1. Check the Docker container is running: `docker-compose ps`
2. Verify `SEARXNG_URL` matches the running instance (default: `http://localhost:8888/search`).
3. Test directly: `curl http://localhost:8888/search?q=test&format=json`
4. Restart if needed: `docker-compose restart searxng`

### Verification loop stuck

1. Check `/api/automation/status` -- look at the `verification.last_error` field.
2. If an API key is exhausted or invalid, the loop may stall on retries. Rotate the key and restart.
3. Stop and restart verification: POST `/api/automation/stop-verification`, then POST `/api/automation/start-verification`.

### High dead letter count

1. Review entries at `/api/dead-letter` to identify patterns (e.g., same city, same source).
2. Consider adjusting verification thresholds in automation settings.
3. Businesses in the dead letter queue can be requeued by updating automation settings to allow recycling (`DAILY_TARGET_ALLOW_RECYCLE=true`).

### Rate limiting (429 responses)

1. Check API usage dashboards for Google Places, Foursquare, and other providers.
2. Reduce `BATCH_SIZE` to slow down processing.
3. Increase `AUTO_RUNNER_INTERVAL_SECONDS` to space out automation runs.
4. If a specific API key is rate-limited, disable that source temporarily or rotate the key.

---

## API Key Rotation

1. Update the relevant environment variable in `.env`:
   - `GOOGLE_PLACES_API_KEY`
   - `FOURSQUARE_API_KEY`
   - `GROQ_API_KEY`
   - `OPENROUTER_API_KEY`
   - `GEMINI_API_KEY`
   - `HUNTER_API_KEY`
   - Or any other key listed in `ENV_REFERENCE.md`

2. Restart the server process. Configuration is loaded from environment variables via `load_config()` at startup. The cached config can also be refreshed by calling `reload_config()`.

3. No downtime is required for key rotation -- stop the old process and start the new one. In-flight requests will complete with the old key; new requests will use the updated key.
