# Domain Lead Pipeline -- Environment Variable Reference

Sprint 5.9 -- Complete reference for all environment variables used by the Domain Lead Pipeline.

All variables are set in the `.env` file at the project root. Copy `.env.example` to `.env` and fill in the values appropriate for your environment.

---

## 1. Database

### DATABASE_URL
- **Required**: Yes
- **Default**: `postgresql://localhost:5432/domain_leads`
- **Description**: PostgreSQL connection string for the main application database. Used by SQLAlchemy and Alembic for all database operations. The `.env.example` ships with the psycopg2 dialect: `postgresql+psycopg2://postgres:postgres@localhost:5432/domain_leads`.

### DOMAIN_PIPELINE_TEST_DATABASE_URL
- **Required**: No
- **Default**: None
- **Description**: PostgreSQL connection string used exclusively by the test suite. Integration and end-to-end tests are skipped when this variable is not set. Should point to a separate database from production to avoid data loss.

---

## 2. API Keys

### GOOGLE_PLACES_API_KEY
- **Required**: No
- **Default**: None
- **Description**: API key for Google Places API (New). Enables business verification via Google Places lookups. Free tier provides 10,000 calls/month on the Essentials plan. Obtain from the Google Cloud Console with the "Places API (New)" enabled.

### FOURSQUARE_API_KEY
- **Required**: No
- **Default**: None
- **Description**: API key for the Foursquare Places API. Used as an alternative or supplementary source for business verification. Free tier provides 10,000 calls/month.

### GROQ_API_KEY
- **Required**: No
- **Default**: None
- **Description**: API key for Groq LLM inference. One of three supported LLM providers for AI-powered business verification. Any one of GROQ_API_KEY, OPENROUTER_API_KEY, or GEMINI_API_KEY enables LLM verification.

### OPENROUTER_API_KEY
- **Required**: No
- **Default**: None
- **Description**: API key for OpenRouter, a unified LLM gateway. Used for LLM-based business verification as an alternative to Groq or Gemini.

### GEMINI_API_KEY
- **Required**: No
- **Default**: None
- **Description**: API key for Google Gemini. Used for LLM-based business verification as an alternative to Groq or OpenRouter.

### HUNTER_API_KEY
- **Required**: No
- **Default**: None
- **Description**: API key for Hunter.io email finder service. Used to discover contact email addresses for verified businesses.

### APOLLO_API_KEY
- **Required**: No
- **Default**: None
- **Description**: API key for Apollo.io. Used for enriching business contact data alongside or instead of Hunter.

### WHOISXML_API_KEY
- **Required**: No
- **Default**: None
- **Description**: API key for WhoisXML API. Provides WHOIS data for domain ownership lookups during the verification pipeline.

### DOMAINTOOLS_API_KEY
- **Required**: No
- **Default**: None
- **Description**: API key for DomainTools. Alternative provider for WHOIS and domain intelligence data.

### INSTANTLY_API_KEY
- **Required**: No
- **Default**: None
- **Description**: API key for Instantly.ai cold email platform. Used for exporting verified leads directly into outreach campaigns.

### LEMLIST_API_KEY
- **Required**: No
- **Default**: None
- **Description**: API key for Lemlist outreach platform. Alternative to Instantly for exporting leads into email campaigns.

### GOOGLE_SEARCH_API_KEY
- **Required**: No
- **Default**: None
- **Description**: API key for Google Custom Search JSON API. Used for web search queries when SearXNG is unavailable or as a supplementary search source.

### GOOGLE_SEARCH_CX
- **Required**: No
- **Default**: None
- **Description**: Google Custom Search Engine ID (CX). Required alongside GOOGLE_SEARCH_API_KEY to specify which search engine configuration to use.

---

## 3. Services

### SEARXNG_URL
- **Required**: No
- **Default**: `http://localhost:8888/search`
- **Description**: URL of the SearXNG meta-search instance. SearXNG aggregates results from multiple search engines and is the primary search backend for the pipeline. Start via `docker-compose up -d`.

### RDAP_BASE_URL
- **Required**: No
- **Default**: `https://rdap.org/domain/`
- **Description**: Base URL for RDAP (Registration Data Access Protocol) queries. Used to look up domain registration information during verification.

### OVERPASS_ENDPOINT
- **Required**: No
- **Default**: `https://overpass-api.de/api/interpreter`
- **Description**: URL of the OpenStreetMap Overpass API endpoint. Used to discover businesses from OSM data.

### OVERPASS_TIMEOUT
- **Required**: No
- **Default**: `180`
- **Description**: Timeout in seconds for Overpass API requests.

### OVERPASS_ENDPOINTS
- **Required**: No
- **Default**: `https://overpass-api.de/api/interpreter,https://overpass.kumi.systems/api/interpreter,https://overpass.nchc.org.tw/api/interpreter`
- **Description**: Comma-separated list of Overpass API endpoints for failover. The pipeline cycles through these when one endpoint is unavailable.

### OVERPASS_FILTER_CHUNK
- **Required**: No
- **Default**: `3`
- **Description**: Number of filter categories to include per Overpass query chunk.

### OVERPASS_ELEMENT_TYPES
- **Required**: No
- **Default**: `node`
- **Description**: OSM element types to query (e.g., `node`, `way`, `relation`).

### OVERPASS_RETRIES
- **Required**: No
- **Default**: `3`
- **Description**: Number of retry attempts for failed Overpass queries.

### OVERPASS_RETRY_DELAY
- **Required**: No
- **Default**: `5`
- **Description**: Delay in seconds between Overpass query retries.

### OVERPASS_SLEEP
- **Required**: No
- **Default**: `1`
- **Description**: Sleep duration in seconds between successive Overpass requests to avoid rate limiting.

### OVERPASS_BBOX_SPLIT
- **Required**: No
- **Default**: `1`
- **Description**: Number of bounding box subdivisions for Overpass queries. Higher values split large areas into smaller queries.

---

## 4. Security

### MUTATION_API_KEY
- **Required**: No (but strongly recommended in production)
- **Default**: None
- **Description**: API key required for all state-mutating (POST/PUT/DELETE) API endpoints. When set, requests must include this key in the `X-API-Key` header. Without this or localhost bypass, mutation endpoints will return 401.

### MUTATION_LOCALHOST_BYPASS
- **Required**: No
- **Default**: `false`
- **Description**: When set to `true`, mutation endpoints skip API key validation for requests originating from localhost (127.0.0.1 / ::1). Intended for local development only. Must be `false` in production.

---

## 5. Notifications

### NTFY_TOPIC
- **Required**: No
- **Default**: None
- **Description**: Topic name on ntfy.sh for push notifications. When set, the pipeline sends notifications for automation events (pipeline runs, daily targets, errors). Free, no registration required.

### NTFY_SERVER
- **Required**: No
- **Default**: `https://ntfy.sh`
- **Description**: Base URL of the ntfy server. Change this if you are self-hosting ntfy instead of using the public instance.

### SLACK_WEBHOOK_URL
- **Required**: No
- **Default**: None
- **Description**: Slack incoming webhook URL for sending notifications. Works alongside or instead of ntfy. Receives the same automation event notifications.

---

## 6. Google Sheets

### GOOGLE_SHEETS_CREDENTIALS_FILE
- **Required**: No
- **Default**: None
- **Description**: File path to the Google service account credentials JSON file. Required for exporting leads to Google Sheets. Obtain by creating a service account in the Google Cloud Console and downloading its key file.

### GOOGLE_SHEETS_SPREADSHEET_ID
- **Required**: No
- **Default**: None
- **Description**: ID of the target Google Sheets spreadsheet for lead exports. Found in the spreadsheet URL: `https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit`. The service account must have Editor access to this spreadsheet.

---

## 7. Frontend

### VITE_API_BASE_URL
- **Required**: No
- **Default**: `http://localhost:8000` (hardcoded fallback in frontend)
- **Description**: Base URL of the backend API server, used by the Vite-powered frontend. Set this when the backend runs on a different host or port than the default.

### VITE_MUTATION_API_KEY
- **Required**: No
- **Default**: `""` (empty string)
- **Description**: The mutation API key passed from the frontend to the backend in `X-API-Key` headers. Must match the backend's `MUTATION_API_KEY` value for mutation requests to succeed.

### FRONTEND_ORIGINS
- **Required**: No
- **Default**: `http://localhost:5173,http://127.0.0.1:5173,http://localhost:5174,http://127.0.0.1:5174,http://localhost:5175,http://127.0.0.1:5175,http://localhost:8000,http://127.0.0.1:8000,http://host.docker.internal:5174,http://host.docker.internal:5173`
- **Description**: Comma-separated list of allowed CORS origins for the backend API. Add your frontend's production URL here when deploying.

---

## 8. Logging

### LOG_LEVEL
- **Required**: No
- **Default**: `INFO`
- **Description**: Python logging level. Accepted values: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Controls verbosity of application logs.

### LOG_FORMAT
- **Required**: No
- **Default**: `json`
- **Description**: Log output format. Set to `json` for structured JSON logs (recommended for production and log aggregation) or any other value for plain text output.

---

## 9. Export

### EXPORT_DIR
- **Required**: No
- **Default**: `./exports`
- **Description**: Directory path where CSV and other export files are written. Created automatically if it does not exist. Use an absolute path in production.

---

## 10. Processing

### BATCH_SIZE
- **Required**: No
- **Default**: `100`
- **Description**: Number of records to process per batch in pipeline operations. Lower values reduce memory usage and API burst load; higher values improve throughput.

### DNS_TIMEOUT
- **Required**: No
- **Default**: `5`
- **Description**: Timeout in seconds for DNS resolution queries during domain verification.

### DNS_CHECK_WWW
- **Required**: No
- **Default**: `true`
- **Description**: When `true`, the pipeline also checks the `www.` subdomain in addition to the bare domain during DNS verification.

### HTTP_TIMEOUT
- **Required**: No
- **Default**: `10`
- **Description**: Timeout in seconds for HTTP requests made during domain availability and content checks.

### HTTP_USER_AGENT
- **Required**: No
- **Default**: `domain-lead-pipeline/0.1`
- **Description**: User-Agent header string sent with HTTP requests made by the pipeline.

### TCP_PROBE_ENABLED
- **Required**: No
- **Default**: `false`
- **Description**: When `true`, enables TCP port probing as an additional check for domain liveness. Useful when HTTP checks are blocked but the server is listening.

### TCP_PROBE_TIMEOUT
- **Required**: No
- **Default**: `3`
- **Description**: Timeout in seconds for each TCP probe connection attempt.

### TCP_PROBE_PORTS
- **Required**: No
- **Default**: `80,443`
- **Description**: Comma-separated list of TCP ports to probe when TCP probing is enabled.

---

## 11. Automation

### AUTO_RUNNER_ENABLED
- **Required**: No
- **Default**: `false`
- **Description**: When `true`, the automation loop starts automatically on server boot. When `false`, automation must be started manually via POST `/api/automation/start`.

### AUTO_RUNNER_INTERVAL_SECONDS
- **Required**: No
- **Default**: `900` (15 minutes)
- **Description**: Interval in seconds between automated pipeline runs. Minimum value is 30 seconds (enforced in code).

### AUTO_DAILY_TARGET_ENABLED
- **Required**: No
- **Default**: `true`
- **Description**: When `true`, the daily target export runs as part of the automation loop, exporting the top leads each day.

### DAILY_TARGET_COUNT
- **Required**: No
- **Default**: `100`
- **Description**: Number of leads to include in each daily target export.

### DAILY_TARGET_MIN_SCORE
- **Required**: No
- **Default**: `40.0`
- **Description**: Minimum lead score required for inclusion in the daily target export. Leads below this score are excluded.

### DAILY_TARGET_PLATFORM_PREFIX
- **Required**: No
- **Default**: `daily`
- **Description**: Prefix used for naming daily target export batches (e.g., `daily-2026-03-02`).

### DAILY_TARGET_REQUIRE_CONTACT
- **Required**: No
- **Default**: `true`
- **Description**: When `true`, only leads with a discovered contact email are included in the daily target export.

### DAILY_TARGET_REQUIRE_DOMAIN_QUALIFICATION
- **Required**: No
- **Default**: `false`
- **Description**: When `true`, only leads with a qualified domain status are included in the daily target export.

### DAILY_TARGET_REQUIRE_UNHOSTED_DOMAIN
- **Required**: No
- **Default**: `false`
- **Description**: When `true`, only leads whose domains are not hosted (i.e., available or parked) are included in the daily target export.

### DAILY_TARGET_ALLOW_RECYCLE
- **Required**: No
- **Default**: `true`
- **Description**: When `true`, previously exported leads can be re-exported in future daily targets if they still meet the criteria. When `false`, each lead is exported only once.
