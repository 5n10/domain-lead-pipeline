# Domain Lead Pipeline

Free-first pipeline for finding businesses that have a domain but no hosted website (and not parked-for-sale), then preparing outreach leads. Postgres is the source of truth.

## What it does
- Imports businesses by geography and category from OSM/Overpass.
- Extracts domains from business websites and business emails.
- Excludes common public-email domains (gmail/yahoo/outlook/etc.) from business-domain linking.
- Checks each domain with RDAP + DNS + HTTP + TCP(80/443), including `www`, and classifies:
- `hosted`, `parked`, `verified_unhosted`, `mx_missing`, `unregistered_candidate`, `dns_error`, `rdap_error`
- Unregistered domains are kept as valid business-development candidates (`unregistered_candidate`).
- Creates role-based emails for eligible domains with MX.
- Scores and exports both contact-level leads and business-level leads.
- Tracks job runs/checkpoints for resumable processing.

## Core stack (free-first)
- Data source: OSM Overpass API
- Domain checks: RDAP + DNS + HTTP probe
- Database: PostgreSQL + SQLAlchemy + Alembic
- Export: CSV (Instantly/Lemlist API adapters can be added later)

## Quickstart
1. `docker compose up -d`
2. `python3 -m venv .venv && source .venv/bin/activate`
3. `pip install -r requirements.txt`
4. `cp .env.example .env`
5. `PYTHONPATH=src alembic upgrade head`
6. `cd frontend && npm install`

## Recommended run sequence (UAE, all categories)
1. Import businesses:
`PYTHONPATH=src .venv/bin/python scripts/import_osm_area.py --area uae --categories all`
2. Sync businesses -> domains:
`PYTHONPATH=src .venv/bin/python scripts/sync_business_domains.py --scope uae --limit 500`
3. Domain checks (registered/parked/hosted/unregistered candidates):
`PYTHONPATH=src .venv/bin/python scripts/run_rdap_check.py --scope uae --limit 500 --statuses new,skipped,rdap_error,dns_error`
4. Role-email enrichment:
`PYTHONPATH=src .venv/bin/python scripts/run_email_crawl.py --limit 500`
5. Lead scoring:
`PYTHONPATH=src .venv/bin/python scripts/run_lead_scoring.py --limit 1000`
6. Export (optionally score-filtered):
`PYTHONPATH=src .venv/bin/python scripts/export_csv.py --platform csv --min-score 60`
7. Business-first scoring:
`PYTHONPATH=src .venv/bin/python scripts/run_business_lead_scoring.py --scope uae --limit 2000`
8. Business-first export:
`PYTHONPATH=src .venv/bin/python scripts/export_business_leads.py --platform csv_business --min-score 60`
- Default export now requires domain qualification (`verified_unhosted` or `unregistered_candidate`) and excludes hosted/parked signals.
- Add `--allow-unqualified-domain` only if you need a broader, lower-confidence list.

## Recheck legacy domain statuses
- If you have old data with `skipped/new` statuses, reclassify with the newer validator:
`PYTHONPATH=src .venv/bin/python scripts/run_rdap_check.py --scope uae --limit 5000 --statuses new,skipped,rdap_error,dns_error`

## Unified one-pass run
- Runs import (optional), sync, rdap, enrichment, scoring, export:
`PYTHONPATH=src .venv/bin/python scripts/run_unified_pipeline.py --area uae --categories all --sync-limit 500 --rdap-limit 500 --email-limit 500 --score-limit 1000 --platform csv --min-score 60 --business-score-limit 2000 --business-platform csv_business --business-min-score 60`
- Use `--rdap-limit 0 --email-limit 0 --score-limit 0` to skip those stages in a run.
- Use `--business-require-unhosted-domain` for strict domain-qualified business export.

## Scheduled run
- Repeats unified pipeline on an interval:
`PYTHONPATH=src .venv/bin/python scripts/run_scheduler.py --area uae --categories all --interval-seconds 1800 --sync-limit 500 --rdap-limit 500 --email-limit 500 --score-limit 1000 --min-score 60 --business-score-limit 2000 --business-platform csv_business --business-min-score 60 --metrics-out exports/metrics.json`

## Web UI (React SPA)
- API server:
`PYTHONPATH=src .venv/bin/python scripts/run_api.py --host 0.0.0.0 --port 8000`
- Frontend dev server:
`cd frontend && npm run dev`
- Open:
`http://localhost:5173`
- Optional API URL override for frontend:
`VITE_API_BASE_URL=http://localhost:8000 npm run dev`
- Optional mutation auth header for frontend (when `MUTATION_LOCALHOST_BYPASS=false`):
`VITE_MUTATION_API_KEY=<secret> npm run dev`
- Build frontend:
`cd frontend && npm run build`
- If `frontend/dist` exists, API serves it at `/` automatically.

## Keep it running
- Start in background:
`scripts/start_stack.sh`
- Check status:
`scripts/status_stack.sh`
- Stop:
`scripts/stop_stack.sh`
- Logs:
`tail -f logs/api.log`
- Mutation API auth (for non-local callers):
- `MUTATION_API_KEY=<secret>`
- `MUTATION_LOCALHOST_BYPASS=true` (keep local UI working without headers)

## Always-on runner (frontend buttons + backend loop)
- In the dashboard, use **Always-On Runner** buttons:
- `Save Settings` stores interval/limits/daily-target options.
- `Start Always-On` starts the background loop on the API process.
- `Stop` requests a loop stop.
- `Run Cycle Now` executes one manual cycle.
- `Generate Daily Target` creates or tops-up today’s target export.
- `Allow lead recycle when daily pool is exhausted` keeps daily output non-zero by reusing top leads when all candidates were previously exported.
- Keep `Import area each cycle` empty for normal continuous qualification.
- Set an area like `uae` only when you want every cycle to re-import OSM data.
- Recommended starting thresholds for current UAE dataset:
- `min_score=40`
- `require_contact=true`
- `require_domain_qualification=false` (until domain coverage improves)
- To auto-start on API boot, set in `.env`:
- `AUTO_RUNNER_ENABLED=true`
- `AUTO_RUNNER_INTERVAL_SECONDS=900`

## Metrics and operations
- Current metrics snapshot:
`PYTHONPATH=src .venv/bin/python scripts/metrics_report.py`
- Job history/checkpoints are stored in `job_runs` and `job_checkpoints`.

## Tests
- Integration tests (lead query correctness + export fill + mutation auth):
`PYTHONPATH=src DOMAIN_PIPELINE_TEST_DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/domain_leads_test pytest -q`

## Config files
- Areas: `config/areas.json`
- Category toggles: `config/categories.json`
- Domain validation knobs (`.env`):
- `DNS_TIMEOUT`, `DNS_CHECK_WWW`
- `TCP_PROBE_ENABLED`, `TCP_PROBE_TIMEOUT`, `TCP_PROBE_PORTS`

## Optional paid add-ons (later)
- WhoisXML API (replace/augment RDAP)
- DomainTools investor filter
- Hunter/Apollo enrichment
- Instantly/Lemlist direct API export
