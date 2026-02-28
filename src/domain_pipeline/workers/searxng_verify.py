"""SearXNG meta-search verification worker.

Searches a local SearXNG instance (aggregating DuckDuckGo, Bing, Brave, Mojeek,
Qwant, Google) for each business to verify whether it truly lacks a website.

Replaces the broken DDG HTML scraper and Google Search scraper with a single,
self-hosted meta-search engine that returns results from MULTIPLE engines in
one request.

Completely FREE — no API keys, no rate limits, no blocking risk.
Requires a SearXNG instance running (see docker-compose.yml).

Stores results in raw["searxng_verified"], raw["searxng_result"], etc.
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests as http_requests
from sqlalchemy import not_, or_, select

from ..db import session_scope
from ..jobs import complete_job, fail_job, start_job
from ..models import Business, City

# Reuse matching logic from web_search_verify — battle-tested functions
from .web_search_verify import (
    DIRECTORY_DOMAINS,
    PUBLIC_EMAIL_DOMAINS_QUICK,
    _build_search_queries,
    _domain_contains_name,
    _extract_business_website,
    _get_domain_from_url,
    _is_directory_or_social,
    _name_words,
    _normalize_name,
    _result_matches_business,
)

logger = logging.getLogger(__name__)

JOB_NAME = "searxng_verify_websites"

# Default SearXNG instance URL
SEARXNG_URL = "http://localhost:8888/search"

# Rate limiting
DELAY_BETWEEN_SEARCHES = 0.3   # 300ms between SearXNG queries (local, so fast)
DELAY_BETWEEN_BUSINESSES = 0.5  # 500ms between businesses


def _search_searxng(
    query: str,
    max_results: int = 20,
    searxng_url: str = SEARXNG_URL,
    timeout: float = 10.0,
) -> list[dict]:
    """Search via local SearXNG JSON API.

    Returns list of dicts with 'title', 'href', 'body' keys
    (same format as the DDG worker for compatibility).
    """
    try:
        resp = http_requests.get(
            searxng_url,
            params={
                "q": query,
                "format": "json",
                "categories": "general",
            },
            timeout=timeout,
        )
        if resp.status_code != 200:
            logger.warning("SearXNG returned status %d for '%s'", resp.status_code, query)
            return []

        data = resp.json()
        results = []
        seen_urls = set()

        for item in data.get("results", [])[:max_results]:
            url = item.get("url", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            results.append({
                "title": item.get("title", ""),
                "href": url,
                "body": item.get("content", ""),
                "engine": item.get("engine", "unknown"),
                "engines": item.get("engines", []),
            })

        return results

    except http_requests.RequestException as exc:
        logger.warning("SearXNG request failed for '%s': %s", query, exc)
        return []
    except (ValueError, KeyError) as exc:
        logger.warning("SearXNG response parse error for '%s': %s", query, exc)
        return []


def _analyze_results(
    results: list[dict],
    business_name: str,
) -> tuple[Optional[str], dict]:
    """Analyze SearXNG results to find business website and build metadata.

    Returns:
        (website_url_or_none, metadata_dict)
    """
    website = _extract_business_website(results, business_name)

    # Collect engine coverage stats
    engines_seen = set()
    for r in results:
        engines_seen.update(r.get("engines", []))
        if r.get("engine"):
            engines_seen.add(r["engine"])

    # Count non-directory results for quality assessment
    real_results = [r for r in results if not _is_directory_or_social(r.get("href", ""))]

    metadata = {
        "total_results": len(results),
        "engines": sorted(engines_seen),
        "engine_count": len(engines_seen),
        "non_directory_results": len(real_results),
    }

    return website, metadata


def _process_one_business(
    biz_id: str,
    biz_name: str,
    city_name: str | None,
    raw: dict | None,
    searxng_url: str,
) -> dict:
    """Process a single business — search SearXNG and analyze results.

    Thread-safe: does not touch the DB, returns results dict.
    """
    raw = dict(raw) if raw else {}

    # Build multiple search queries
    search_queries = _build_search_queries(biz_name, city_name)

    # Try each query, aggregate all unique results
    all_results = []
    seen_urls = set()
    used_query = search_queries[0]
    total_raw_results = 0

    for q in search_queries:
        results = _search_searxng(q, max_results=20, searxng_url=searxng_url)
        total_raw_results += len(results)
        for r in results:
            url = r.get("href", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_results.append(r)
        if all_results:
            used_query = q
            break  # First query with results is usually best
        time.sleep(DELAY_BETWEEN_SEARCHES)

    if not all_results:
        # No results from any query — inconclusive
        raw["searxng_verified"] = True
        raw["searxng_result"] = "no_results"
        raw["searxng_search_query"] = used_query
        raw["searxng_result_count"] = 0
        return {
            "biz_id": biz_id,
            "raw": raw,
            "website": None,
            "outcome": "inconclusive",
        }

    # Analyze results
    website, metadata = _analyze_results(all_results, biz_name)

    raw["searxng_verified"] = True
    raw["searxng_search_query"] = used_query
    raw["searxng_result_count"] = len(all_results)
    raw["searxng_engines"] = metadata["engines"]
    raw["searxng_engine_count"] = metadata["engine_count"]

    if website:
        raw["searxng_result"] = "has_website"
        raw["searxng_website"] = website
        return {
            "biz_id": biz_id,
            "raw": raw,
            "website": website,
            "outcome": "has_website",
        }
    else:
        raw["searxng_result"] = "no_website"
        raw["searxng_non_directory_count"] = metadata["non_directory_results"]
        return {
            "biz_id": biz_id,
            "raw": raw,
            "website": None,
            "outcome": "no_website",
        }


def run_batch(
    limit: int | None = None,
    min_score: float = 0.0,
    scope: str | None = None,
    searxng_url: str = SEARXNG_URL,
    business_parallelism: int = 5,
) -> dict:
    """Verify whether potential leads have websites via SearXNG meta-search.

    For each business with lead_score >= min_score and no website_url:
    1. Search SearXNG (aggregating DDG, Bing, Brave, Mojeek, etc.)
    2. Analyze results — filter out directories/social media
    3. If a real business website is found, set business.website_url
    4. Track result in business.raw["searxng_verified"]

    FREE — uses local SearXNG instance. No API keys, no rate limits.

    Args:
        limit: Max businesses to verify. None = 200 default.
        min_score: Only verify businesses scoring at or above this.
        scope: Job scope tag.
        searxng_url: URL of the SearXNG instance.
        business_parallelism: Number of businesses to search concurrently.

    Returns:
        Dict with processing stats.
    """
    effective_limit = limit if limit is not None else 200

    with session_scope() as session:
        run = start_job(session, JOB_NAME, scope=scope)

        try:
            stmt = (
                select(Business, City)
                .outerjoin(City, Business.city_id == City.id)
                .where(Business.name.isnot(None))
                .where(Business.name != "")
                .where(or_(Business.website_url.is_(None), Business.website_url == ""))
                .where(Business.lead_score >= min_score)
                .where(
                    or_(
                        Business.raw.is_(None),
                        not_(Business.raw.has_key("searxng_verified")),
                    )
                )
                .order_by(Business.lead_score.desc(), Business.created_at)
            )

            if effective_limit is not None and effective_limit > 0:
                stmt = stmt.limit(effective_limit)

            rows = session.execute(stmt).all()

            if not rows:
                result = {
                    "processed": 0,
                    "websites_found": 0,
                    "no_website_confirmed": 0,
                    "inconclusive": 0,
                    "errors": 0,
                }
                complete_job(session, run, processed_count=0, details=result)
                return result

            # Prepare tasks
            tasks = []
            for business, city in rows:
                biz_name = (business.name or "").strip()
                if not biz_name:
                    continue
                city_name = city.name if city else None
                tasks.append((business.id, biz_name, city_name, business.raw))

            # Process in parallel
            results_map: dict[str, dict] = {}
            processed = 0
            websites_found = 0
            no_website_confirmed = 0
            inconclusive = 0
            errors = 0

            with ThreadPoolExecutor(max_workers=business_parallelism) as executor:
                futures = {}
                for biz_id, biz_name, city_name, raw in tasks:
                    future = executor.submit(
                        _process_one_business,
                        biz_id, biz_name, city_name, raw, searxng_url,
                    )
                    futures[future] = biz_id

                for future in as_completed(futures):
                    biz_id = futures[future]
                    try:
                        result = future.result()
                        results_map[biz_id] = result

                        if result["outcome"] == "has_website":
                            websites_found += 1
                        elif result["outcome"] == "no_website":
                            no_website_confirmed += 1
                        elif result["outcome"] == "inconclusive":
                            inconclusive += 1

                        processed += 1

                    except Exception as exc:
                        logger.warning("SearXNG error for business %s: %s", biz_id, exc)
                        errors += 1
                        processed += 1

                    if processed % 50 == 0:
                        logger.info(
                            "SearXNG progress: %d/%d processed, "
                            "%d websites, %d no-website, %d inconclusive",
                            processed, len(tasks), websites_found,
                            no_website_confirmed, inconclusive,
                        )

            # Apply results back to DB
            biz_lookup = {business.id: business for business, _ in rows}
            for biz_id, result in results_map.items():
                business = biz_lookup.get(biz_id)
                if not business:
                    continue
                business.raw = result["raw"]
                business.scored_at = None  # Force rescore
                if result["website"]:
                    business.website_url = result["website"]

            session.flush()

            details = {
                "min_score": min_score,
                "websites_found": websites_found,
                "no_website_confirmed": no_website_confirmed,
                "inconclusive": inconclusive,
                "errors": errors,
            }
            complete_job(session, run, processed_count=processed, details=details)

            logger.info(
                "SearXNG batch complete: %d processed, %d websites found, "
                "%d confirmed no website, %d inconclusive",
                processed, websites_found, no_website_confirmed, inconclusive,
            )

            return {
                "processed": processed,
                "websites_found": websites_found,
                "no_website_confirmed": no_website_confirmed,
                "inconclusive": inconclusive,
                "errors": errors,
            }

        except Exception as exc:
            fail_job(session, run, error=str(exc))
            raise
