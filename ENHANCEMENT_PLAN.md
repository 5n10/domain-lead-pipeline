# Domain Lead Pipeline - Enhancement Plan

## Executive Summary

This document is a comprehensive audit and enhancement plan for the Domain Lead Pipeline system. It covers every layer — verification, scoring, data quality, architecture, and frontend — with prioritized, actionable recommendations.

**Current state (Feb 2026):**
- 100,040 businesses (52K UAE, 48K Canada)
- 81,271 (81%) have no website URL
- Only 666 verified (0.8% coverage)
- Confidence: 0 high, 386 medium, 280 low, 80,605 unverified
- Only 636 businesses score above 30 (minimum useful range)
- Only 3.5% have email contacts

**Goal:** Rapidly verify all 80K+ businesses, produce high-confidence leads with contact info, and make the system self-sustaining.

---

## Part 1: Critical Issues (Fix First)

### 1.1 DDG Search Is Effectively Broken (97.5% Inconclusive)

**Problem:** `_search_web()` scrapes `https://html.duckduckgo.com/html/` but DDG is aggressively blocking scraper traffic. 78 out of 80 businesses returned zero results.

**Root cause:** DDG's HTML endpoint detects automated requests despite the custom User-Agent. The 1.5s delay between requests isn't enough to avoid detection. Requests likely receive empty HTML or redirects.

**Fix options (choose one):**

| Option | Effort | Reliability | Speed |
|--------|--------|-------------|-------|
| A. Use SearXNG self-hosted instance | Medium | Very high | ~100/min |
| B. Use Brave Search API (free tier: 2K/mo) | Low | High | ~60/min |
| C. Use DuckDuckGo API via proxy rotation | Medium | Medium | ~30/min |
| D. Drop DDG, rely on other layers | None | N/A | N/A |

**Recommendation:** Option A (SearXNG). Deploy a SearXNG instance via Docker, configure DDG + Bing + Brave as backends. It handles rate limiting internally, rotates across engines, and returns structured JSON. Free, self-hosted, unlimited.

```yaml
# docker-compose addition
searxng:
  image: searxng/searxng:latest
  ports: ["8888:8080"]
  volumes: ["./config/searxng:/etc/searxng"]
```

Then replace `_search_web()` with a SearXNG API call:
```python
resp = httpx.get("http://localhost:8888/search", params={"q": query, "format": "json"})
```

### 1.2 Google Search Is 100% Blocked

**Problem:** Direct scraping of google.com is detected immediately. All 12 businesses returned "no_results". Google has sophisticated bot detection (CAPTCHA, fingerprinting) that cannot be bypassed with simple HTTP requests.

**Fix options:**

| Option | Effort | Reliability | Speed |
|--------|--------|-------------|-------|
| A. Use SearXNG (includes Google as backend) | Already done if 1.1.A | High | Combined |
| B. Use Google Custom Search JSON API (free: 100/day) | Low | Very high | 100/day |
| C. Use Serper.dev API ($50/mo for 50K queries) | Low | Very high | ~200/min |
| D. Drop Google, rely on SearXNG + LLM | None | N/A | N/A |

**Recommendation:** If adopting SearXNG (1.1.A), Google is included as a backend engine. Otherwise, Serper.dev offers the best cost/reliability ratio if budget allows.

### 1.3 LLM Verification Returns 85% "not_sure"

**Problem:** The LLM prompt asks for "highly confident" answers about whether a business has a website. LLMs without internet access can't actually verify this — they can only guess based on business name and category patterns.

**Root cause analysis:**
- LLM has no internet access — it's guessing, not searching
- The "highly confident" instruction makes it default to "not_sure" for any ambiguous case
- For a small retail shop in UAE ("Al Noor Textiles"), the LLM has no way to know if it has a website

**Fix options:**

| Option | Effort | Reliability | Impact |
|--------|--------|-------------|--------|
| A. Change LLM role: verify found URLs instead of searching | Medium | High | Major |
| B. Loosen prompt: accept "likely" instead of "highly confident" | Low | Medium | Minor |
| C. Give LLM search context: pass SearXNG results to LLM for analysis | Medium | Very high | Major |
| D. Replace with rule-based heuristics for small businesses | Low | Medium | Medium |

**Recommendation:** Option C — use the LLM as an **analysis layer**, not a search layer. Feed it the SearXNG results + business info and ask it to determine which result (if any) is the business's actual website. This gives it real data to analyze instead of guessing.

Revised flow:
```
SearXNG Search → 10 results → LLM analyzes → picks official website or confirms "no website"
```

This makes LLM results **conclusive** nearly 100% of the time, because it has actual data to work with.

### 1.4 Score Distribution Severely Skewed (99.4% Score 0-10)

**Problem:** Nearly all businesses score 0-10 because:
1. **Confidence caps** (unverified = cap 35, low = cap 45) affect 80,885 businesses
2. **No contacts** cap at 5.0 — affects 84,943 businesses (84.9%)
3. **These stack**: even a "trades" business with no contacts and unverified = min(5, 35) = 5

**Root cause:** The scoring system correctly penalizes unverified businesses, but since verification is barely running (0.8% coverage) and contact enrichment hasn't happened at scale, almost every business is capped.

**Fix:** This is a **throughput problem**, not a scoring problem. The scoring logic is sound — we need to:
1. Verify more businesses faster (Parts 1.1-1.3)
2. Enrich contacts at scale (Part 2)
3. Then scores will naturally distribute properly

---

## Part 2: Scaling Throughput

### 2.1 Scale Domain Guess to 1000/batch

**Current:** 100/batch, processes ~300 businesses per verification cycle.
**Problem:** At 100/batch with 3s pause, processing 80K businesses takes ~67 hours.

**Recommendation:**
- Increase `domain_guess_batch` to **1000**
- Increase `business_parallelism` to **15** (from 8)
- Domain Guess has 84% website discovery rate — it's the single most impactful layer
- At 1000/batch and 15 parallel businesses: ~5-10 minutes per batch
- Full 80K coverage in **~14-16 hours** (vs current ~67 hours)

```python
# automation.py VerificationSettings
domain_guess_batch: int = 1000  # was 100
```

### 2.2 Priority Processing Order

**Current:** Verification processes businesses by `lead_score DESC, created_at`. Since most score 0-10, there's no meaningful priority.

**Better approach:** Process in waves by category value:

| Wave | Categories | Count | Why |
|------|-----------|-------|-----|
| 1 | trades, contractors | ~900 | Highest value leads (no DIY websites) |
| 2 | professional_services | ~5,900 | Often need websites |
| 3 | retail (specialized) | ~10,000 | Non-chain retail |
| 4 | health, auto | ~7,500 | Local services |
| 5 | Everything else | ~57,000 | Bulk processing |

Implementation: Add `category_priority` mapping and use it in the verification query ORDER BY.

### 2.3 Contact Enrichment Pipeline

**Current state:** Only 15.1% of businesses have any contacts. 84.9% have zero contacts, meaning they're capped at score 5.0 regardless of verification status.

**The enrichment bottleneck:**
- OSM data only has contacts for 15% of businesses
- Google Places API requires a paid key
- Foursquare API requires a paid key
- Hunter.io gives 25 lookups per run

**Recommendations:**

1. **Google Places bulk enrichment** — If API key available, this is the highest-value enrichment source. Run at 200/batch, prioritize verified no-website businesses first.

2. **Hunter.io domain-to-email** — For businesses where Domain Guess found a website, use Hunter.io to find email contacts. These are the highest-quality leads (have a website they could improve).

3. **Phone number scraping from discovered websites** — For the 3,595 businesses where Domain Guess found websites, scrape the website for phone numbers and emails. This is FREE and can be automated.

4. **Foursquare enrichment** — Secondary to Google Places, useful for cross-validation.

### 2.4 Verification Layer Consolidation (SearXNG-Based)

After implementing SearXNG (1.1.A), consolidate the verification pipeline:

**Current (6 layers, 3 broken):**
```
Domain Guess → LLM (broken) → DDG (broken) → Google Search (broken) → Google Places → Foursquare
```

**Proposed (4 layers, all working):**
```
Layer 1: Domain Guess (HTTP HEAD, ~500/min, FREE)
Layer 2: SearXNG Search (multi-engine, ~100/min, FREE)
Layer 3: LLM Analysis of SearXNG results (~50/min, API key)
Layer 4: Google Places API (enrichment + verification, API key)
```

- **Layer 1** catches 84% of businesses with websites (existing, working great)
- **Layer 2** replaces DDG + Google Search with one reliable source
- **Layer 3** becomes an analysis layer (not search layer) — nearly 100% conclusive
- **Layer 4** adds phone numbers + verifies for high-value leads

This gives us **2 conclusive sources minimum** (Domain Guess + SearXNG) for every business = **"high" confidence** achievable at scale.

---

## Part 3: Confidence System Improvements

### 3.1 Current Confidence Logic (Review)

```
high:       2+ conclusive sources
medium:     1 conclusive source
low:        checked but only inconclusive results
unverified: never checked
```

Conclusive: `{no_website, no_match, has_website}`
Inconclusive: `{no_results, no_candidates, blocked, poor_match}`

### 3.2 Problem Analysis

With current broken layers:
- Domain Guess is the **only** layer producing conclusive results at scale
- DDG/LLM/Google Search produce almost exclusively inconclusive results
- Businesses checked by Domain Guess alone = "medium" at best
- Getting to "high" requires a **second** conclusive source, which barely exists

### 3.3 Recommended Changes

**Option A: Fix the layers (recommended long-term)**
- Implement SearXNG (Part 1) to get a reliable second search source
- Make LLM analysis conclusive by feeding it real search data
- Result: Domain Guess + SearXNG+LLM = 2 conclusive sources = "high"

**Option B: Adjust confidence algorithm (quick win)**
- Count Domain Guess `no_match` as **strong evidence** (it tested 10-20 HTTP candidates)
- If Domain Guess `no_match` AND business has no OSM website: "medium" (already works)
- If Domain Guess `no_match` AND LLM even with "not_sure": upgrade to "medium" (already works since DG is conclusive)
- Consider: Domain Guess with 15+ candidates checked could count as 1.5x weight

**Option C: Add weighted confidence (most accurate)**
Replace binary conclusive/inconclusive with weighted scores:

```python
CONFIDENCE_WEIGHTS = {
    "domain_guess": {"no_match": 0.7, "has_website": 1.0, "no_candidates": 0.1},
    "searxng":      {"no_website": 0.9, "has_website": 1.0, "no_results": 0.1},
    "llm_analysis": {"no_website": 0.8, "has_website": 0.9, "not_sure": 0.2},
    "google_places": {"no_website": 0.9, "has_website": 1.0},
}

total_weight = sum of all source weights for this business
if total_weight >= 1.5: "high"
elif total_weight >= 0.7: "medium"
elif total_weight > 0: "low"
else: "unverified"
```

This means: Domain Guess `no_match` (0.7) + LLM `not_sure` (0.2) = 0.9 = "medium". Domain Guess `no_match` (0.7) + SearXNG `no_website` (0.9) = 1.6 = "high".

**Recommendation:** Implement Option C (weighted confidence). It's the most accurate and future-proof.

### 3.4 Score Cap Adjustments

| Confidence | Current Cap | Proposed Cap | Reasoning |
|------------|-------------|--------------|-----------|
| unverified | 35 | 35 | Keep strict — encourages verification |
| low | 45 | 50 | Slightly less penalty — business was checked |
| medium | No cap | No cap | One conclusive source is reasonable |
| high | No cap | No cap | Multiple sources confirmed |

---

## Part 4: Data Quality Issues

### 4.1 Chain/Franchise Detection (869 Parked Domain Businesses)

**Problem:** 869 businesses have `website_url` pointing to domains flagged as parked by RDAP. Investigation shows many are chain/franchise websites (e.g., Shoppers Drug Mart, McDonald's Delivery) that may have triggered parking detection incorrectly.

**Fix:** Improve parking detection:
- Add whitelist for known chain domains
- Cross-reference with brand:wikidata tag
- Validate parked status with actual HTTP check before flagging

### 4.2 Top Website Domains Are Chains

The top 20 website domains found are almost all chains/institutions:
- schoolweb.tdsb.on.ca (417), www.toronto.ca (272), www.shoppersdrugmart.ca (187), www.mcdelivery.ae (164)

**These should be flagged as chain businesses** and either:
- Auto-disqualified from lead scoring (score = 0)
- Or separated into a "chain" category for different handling

**Recommendation:** Build a chain domain list from the top 100 most-repeated website domains and use it in the scoring disqualification logic.

### 4.3 Duplicate Business Detection

**Not currently implemented.** OSM data may contain duplicates (same business, different nodes). Should deduplicate by:
- Same name + same city + within 100m radius
- Same phone number
- Same email address

### 4.4 Business Name Quality

Some OSM entries have poor names that won't generate good domain candidates:
- Single-word names like "Pharmacy" or "Restaurant"
- Names in Arabic script only (domain_guess only generates Latin candidates)
- Names with special characters or numbers

**Recommendation:** Add a name quality score and skip domain_guess for businesses with very generic names (< 2 significant words after cleaning).

---

## Part 5: Architecture Improvements

### 5.1 Add SearXNG to Docker Compose

```yaml
services:
  searxng:
    image: searxng/searxng:latest
    container_name: searxng
    restart: unless-stopped
    ports:
      - "8888:8080"
    volumes:
      - ./config/searxng:/etc/searxng
    environment:
      - SEARXNG_BASE_URL=http://localhost:8888
```

### 5.2 Worker Architecture Refactor

**Current:** Each worker independently queries the DB for businesses to process, with different filters and sorting.

**Better:** Central queue system:
1. A scheduler determines which businesses need what verification
2. Workers pull from a priority queue
3. Results are written back to a results queue
4. A single consumer updates the DB

This prevents duplicate processing and enables better prioritization.

**Simple implementation:** Use PostgreSQL advisory locks or a `processing_queue` table:
```sql
CREATE TABLE verification_queue (
    business_id UUID REFERENCES businesses(id),
    layer TEXT NOT NULL,
    priority INT DEFAULT 0,
    status TEXT DEFAULT 'pending',
    claimed_at TIMESTAMP,
    UNIQUE(business_id, layer)
);
```

### 5.3 Connection Pool Tuning

**Current:** `pool_size=10, max_overflow=20` (total 30 connections).

**Analysis:** With business_parallelism=8 in domain_guess + verification thread + pipeline thread + API requests, we're using ~12-15 connections simultaneously. Pool is adequate.

**Recommendation:** No change needed, but add connection pool monitoring:
```python
from sqlalchemy import event
@event.listens_for(_engine, "checkout")
def receive_checkout(dbapi_connection, connection_record, connection_proxy):
    logger.debug("Pool checkout: %s active", _engine.pool.checkedout())
```

### 5.4 Async Migration (Future)

The current system uses synchronous SQLAlchemy + threads. For higher throughput:
- Migrate to `asyncio` + `httpx.AsyncClient` for HTTP-heavy workers
- Use `sqlalchemy.ext.asyncio` for DB operations
- Replace ThreadPoolExecutor with asyncio.gather()

**Impact:** 2-3x throughput improvement for IO-bound workers (domain_guess, search).
**Effort:** High (rewrite all workers). Not recommended until current fixes are exhausted.

---

## Part 6: Frontend Enhancements

### 6.1 Verification Progress Dashboard

**Current:** Basic live totals in the verification banner.

**Enhancement:** Add a real-time progress visualization:
- Progress bar: "4,297 / 81,271 verified (5.3%)" with ETA
- Per-layer throughput chart (businesses/minute over last hour)
- Error rate indicator (% inconclusive by layer)

### 6.2 Lead Quality Funnel

Add a funnel visualization showing:
```
100,040 Total Businesses
  └─ 81,271 No Website (81%)
      └─ 4,297 Verified (5.3%)
          └─ 666 Have Confidence (0.8%)
              └─ 636 Score > 30 (0.8%)
                  └─ 338 Score > 50 (0.4%)
                      └─ 15,097 Have Contacts (15.1%)
                          └─ ??? Exportable Leads
```

This shows the user exactly where the bottleneck is and what needs improvement.

### 6.3 Business Detail View

Click on a lead to see:
- All verification results with timestamps
- Score breakdown (which points came from where)
- Contact information
- Domain status
- Raw OSM data
- Verification history timeline

### 6.4 Bulk Action Controls

- "Verify All Trades Businesses" button
- "Enrich All Verified + No Contact" button
- "Export All Score > 50 + Has Email" button
- Preset action combos for common workflows

### 6.5 Mobile Responsive

The sidebar layout works but needs:
- Better touch targets (buttons too small)
- Verification stats condensed for mobile
- Collapsible sections

---

## Part 7: Implementation Priority

### Phase 1: Quick Wins (1-2 days)

| # | Task | Impact | Effort |
|---|------|--------|--------|
| 1 | Increase domain_guess batch to 1000, parallelism to 15 | HIGH | 5 min |
| 2 | Loosen LLM prompt (accept "likely" answers) | MEDIUM | 30 min |
| 3 | Add chain domain detection from top-100 repeated domains | MEDIUM | 1 hr |
| 4 | Add verification progress bar to dashboard | LOW | 1 hr |
| 5 | Build lead quality funnel visualization | LOW | 2 hrs |

### Phase 2: Infrastructure (3-5 days)

| # | Task | Impact | Effort |
|---|------|--------|--------|
| 6 | Deploy SearXNG in Docker Compose | CRITICAL | 2 hrs |
| 7 | Replace DDG + Google Search workers with SearXNG worker | CRITICAL | 4 hrs |
| 8 | Rewrite LLM as analysis layer (feed SearXNG results) | HIGH | 4 hrs |
| 9 | Implement weighted confidence scoring | HIGH | 3 hrs |
| 10 | Add website scraping for phone/email discovery | HIGH | 4 hrs |

### Phase 3: Scale (1 week)

| # | Task | Impact | Effort |
|---|------|--------|--------|
| 11 | Priority processing by category waves | MEDIUM | 2 hrs |
| 12 | Verification queue system | MEDIUM | 8 hrs |
| 13 | Duplicate business detection | MEDIUM | 4 hrs |
| 14 | Business detail view in frontend | LOW | 4 hrs |
| 15 | Bulk action controls | LOW | 3 hrs |

### Phase 4: Polish (ongoing)

| # | Task | Impact | Effort |
|---|------|--------|--------|
| 16 | Async migration for workers | MEDIUM | 2 weeks |
| 17 | Mobile responsive improvements | LOW | 2 days |
| 18 | Additional data sources (Yellow Pages, industry directories) | MEDIUM | 1 week |
| 19 | Automated daily reporting | LOW | 1 day |
| 20 | A/B testing for domain guess candidates | LOW | 3 days |

---

## Part 8: Projected Impact

### After Phase 1 (Quick Wins)
- Domain Guess processes 80K businesses in ~14 hours (vs current ~67 hours)
- ~67K businesses get website URLs found (84% hit rate)
- ~13K businesses confirmed no website
- Score distribution improves: ~2,000 businesses score 30+

### After Phase 2 (Infrastructure)
- All 80K businesses verified by 2+ conclusive sources
- Confidence distribution: ~60K high, ~10K medium, ~5K low, ~5K unverified
- LLM returns ~90% conclusive (vs current 15%)
- Score distribution: ~5,000 businesses score 40+ (exportable range)

### After Phase 3 (Scale)
- Fully automated: new businesses verified within 24 hours of import
- Contact enrichment pipeline running at scale
- 10,000+ qualified leads with email/phone + high confidence
- Self-sustaining system requiring minimal manual intervention

---

## Appendix: Current Metrics Snapshot

```
Total Businesses:     100,040
No Website:            81,271 (81.2%)
Verified:               4,297 (5.3% of no-website)

Confidence:
  High:                     0
  Medium:                 386
  Low:                    280
  Unverified:          80,605

Verification Sources:
  Domain Guess:         4,297 (84% find rate)
  DDG Search:              80 (97.5% inconclusive)
  LLM:                     95 (85% inconclusive)
  Google Search:           12 (100% inconclusive)
  Google Places:            0
  Foursquare:               0

Contacts:
  Has email:            3,546 (3.5%)
  Has phone:           16,442 (16.4%)
  Has any:             15,097 (15.1%)
  No contacts:         84,943 (84.9%)

Scores:
  0-10:                99,404 (99.4%)
  30-40:                    2
  40-50:                  296
  50-60:                  338
```
