# Contributing to Domain Lead Pipeline

This guide covers development setup, testing, and pull request conventions.

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.9+ | 3.11 recommended (matches CI) |
| Node.js | 20+ | For the React frontend |
| PostgreSQL | 16 | Or run via Docker Compose |
| Docker & Docker Compose | Latest | Optional -- provides Postgres and SearXNG |

## Development Setup

### 1. Clone and create a virtual environment

```bash
git clone <repo-url> domain-lead-pipeline && cd domain-lead-pipeline
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install ruff   # linter used in CI
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` and set at minimum `DATABASE_URL`. See `docs/ENV_REFERENCE.md` for the full list.

### 3. Start backing services

```bash
# Option A -- Docker Compose (recommended):
docker compose up -d          # starts Postgres 16 + SearXNG

# Option B -- local Postgres only:
createdb domain_leads
```

### 4. Run database migrations

```bash
PYTHONPATH=src alembic upgrade head
```

### 5. Set up the frontend

```bash
cd frontend && npm install
```

## Running the App

**Backend API:**
```bash
PYTHONPATH=src uvicorn domain_pipeline.api:app --host 0.0.0.0 --port 8000 --reload
```

**Frontend dev server:**
```bash
cd frontend && npm run dev    # http://localhost:5173
```

Override the API target with `VITE_API_BASE_URL=http://localhost:8000 npm run dev`.

**Full stack via Docker Compose:**
```bash
docker compose up -d          # Postgres + SearXNG + app on port 8000
```

## Testing

CI runs on every push and PR to `main`. All checks must pass before merging.

### Backend -- unit tests (no database needed)

```bash
PYTHONPATH=src .venv/bin/pytest tests/unit/ -v
```

### Backend -- integration tests

Create a test database, then run:

```bash
createdb domain_leads_test
PGPASSWORD=postgres psql -h localhost -U postgres -d domain_leads_test \
  -c "CREATE EXTENSION IF NOT EXISTS citext"

PYTHONPATH=src \
  DOMAIN_PIPELINE_TEST_DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/domain_leads_test \
  pytest tests/integration/ -v
```

Integration tests are skipped when `DOMAIN_PIPELINE_TEST_DATABASE_URL` is not set.

### Backend -- lint: `ruff check src/ tests/`

### Frontend

```bash
cd frontend && npx vitest run      # tests
cd frontend && npx tsc --noEmit    # type check
cd frontend && npx vite build      # build verification
```

### Quick local check

```bash
PYTHONPATH=src pytest tests/unit/ -q && ruff check src/ tests/ && \
  cd frontend && npx tsc --noEmit && npx vitest run
```

## Code Style

### Python

- Follow PEP 8. CI enforces this with **ruff**.
- Use type hints for function signatures.
- Keep imports sorted (stdlib, third-party, local -- ruff handles this).
- Source code lives under `src/domain_pipeline/`. Always use `PYTHONPATH=src`.

### TypeScript / React

- TypeScript strict mode is enabled.
- Functional components with hooks only -- no class components.
- Shared types go in `frontend/src/types.ts`.
- Tests use Vitest + React Testing Library in `frontend/src/__tests__/`.

## Pull Request Guidelines

### Branch naming

Use a descriptive prefix: `feat/`, `fix/`, `refactor/`, `docs/`, `test/`.
Example: `feat/add-whois-enrichment`

### Commit messages

Write concise, imperative-mood messages:
```
Add WhoisXML domain enrichment step
Fix RDAP timeout handling for expired domains
```

### Before opening a PR

1. Rebase on latest `main`.
2. All tests pass locally (unit, integration if applicable, frontend).
3. `ruff check src/ tests/` reports no errors.
4. `cd frontend && npx tsc --noEmit` reports no errors.
5. New or changed behavior has corresponding test coverage.

### PR description

- Summarize **what** changed and **why**.
- Reference related issue numbers.
- Note any new environment variables or database migrations.

### Review process

- At least one approval required before merging.
- CI must be green.
- Squash-merge to keep `main` history clean.

## Project Structure

```
src/domain_pipeline/     # Backend Python package
  api.py                 #   FastAPI routes
  models.py              #   SQLAlchemy ORM models
  config.py              #   Configuration loading
  pipeline.py            #   Core pipeline orchestration
  automation.py          #   Automation loop and scheduling
  domain_utils.py        #   RDAP, DNS, HTTP domain checks
  notifications.py       #   Slack / ntfy alerts
  workers/               #   Background worker modules

frontend/src/            # React + TypeScript SPA
  App.tsx                #   Root component
  api.ts                 #   Backend API client
  types.ts               #   Shared TypeScript interfaces
  components/            #   UI components
  __tests__/             #   Vitest test files

tests/                   # Backend test suite
  unit/                  #   Fast tests, no DB required
  integration/           #   Tests needing a live Postgres
  e2e/                   #   End-to-end tests
  conftest.py            #   Shared fixtures

scripts/                 # CLI entry points for pipeline stages
docs/                    # RUNBOOK.md, ENV_REFERENCE.md
config/                  # areas.json, categories.json, searxng/
```

Open an issue or start a discussion if something in this guide is unclear.
