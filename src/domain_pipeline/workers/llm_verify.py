"""LLM website verification worker — analysis layer.

Instead of blindly guessing, this worker:
1. Fetches search results from SearXNG for each business
2. Feeds those results to an LLM (Groq/Gemini/OpenRouter)
3. Asks the LLM to analyze the evidence and determine if the business has a website

This dramatically improves accuracy: the LLM goes from ~15% conclusive (guessing)
to ~90% conclusive (analyzing real search data).
"""
from __future__ import annotations

import json
import logging
import time
from typing import Optional

from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_result

import requests
from sqlalchemy import not_, or_, select

from ..db import session_scope
from ..jobs import complete_job, fail_job, start_job
from ..models import Business, City

logger = logging.getLogger(__name__)

JOB_NAME = "llm_verify_websites"

# SearXNG URL for fetching search context
SEARXNG_URL = "http://localhost:8888/search"

# Domains that are directories, NOT business websites
DIRECTORY_INDICATORS = {
    "yelp.com", "yelp.ca", "facebook.com", "instagram.com", "linkedin.com",
    "twitter.com", "x.com", "yellowpages.com", "yellowpages.ca", "tripadvisor.com",
    "google.com", "maps.google.com", "mapquest.com", "foursquare.com",
    "youtube.com", "tiktok.com", "pinterest.com", "wikipedia.org",
    "booking.com", "zomato.com", "ubereats.com", "doordash.com",
    "bayut.com", "dubizzle.com", "canada411.ca",
}


def _fetch_search_context(business_name: str, city_name: str | None) -> list[dict]:
    """Fetch search results from SearXNG for LLM analysis."""
    query = f"{business_name} {city_name}" if city_name else business_name
    try:
        resp = requests.get(
            SEARXNG_URL,
            params={"q": query, "format": "json", "categories": "general"},
            timeout=10,
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
        results = []
        for item in data.get("results", [])[:15]:
            results.append({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "snippet": (item.get("content", "") or "")[:200],
            })
        return results
    except Exception:
        return []


def _format_search_results(results: list[dict]) -> str:
    """Format search results into a concise text block for the LLM."""
    if not results:
        return "No search results found."
    lines = []
    for i, r in enumerate(results, 1):
        lines.append(f"{i}. [{r['title']}]({r['url']})")
        if r.get("snippet"):
            lines.append(f"   {r['snippet']}")
    return "\n".join(lines)


def is_error_status(result):
    return isinstance(result, dict) and result.get("status") == "error"

@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_result(is_error_status)
)
def _analyze_with_llm(
    business_name: str,
    city_name: Optional[str],
    category: Optional[str],
    search_results: list[dict],
    api_key: str,
    provider: str,
) -> dict:
    """Ask an LLM to analyze search results and determine if the business has a website.

    Returns a dict with: status ("has_website", "no_website", "not_sure"), website_url, reason
    """
    location = city_name if city_name else "unknown location"
    biz_category = category if category else "business"
    search_text = _format_search_results(search_results)

    sys_prompt = (
        "You are an expert web researcher analyzing search engine results to determine "
        "if a specific business has its own official website.\n\n"
        "RULES:\n"
        "- A real website is a domain the business owns (e.g. joespizza.com, villagecobbler.ca)\n"
        "- Directory listings (Yelp, Facebook, YellowPages, Google Maps, TripAdvisor, etc.) are NOT real websites\n"
        "- Social media pages (instagram.com/business, facebook.com/business) are NOT real websites\n"
        "- If a search result URL contains the business name and is NOT a directory, it's likely their website\n"
        "- Chain/franchise businesses (McDonald's, Subway, etc.) should be marked 'has_website'\n\n"
        "Return ONLY a JSON object with:\n"
        "- status: 'has_website' if search results show they have an official site, "
        "'no_website' if results clearly show no official site exists, "
        "or 'not_sure' if evidence is insufficient\n"
        "- website_url: the official website URL if found, otherwise null\n"
        "- reason: brief explanation (1 sentence)"
    )

    user_prompt = (
        f"Business: {business_name}\n"
        f"Location: {location}\n"
        f"Category: {biz_category}\n\n"
        f"Search Results:\n{search_text}"
    )

    headers = {"Content-Type": "application/json"}

    try:
        if provider == "openrouter":
            url = "https://openrouter.ai/api/v1/chat/completions"
            headers["Authorization"] = f"Bearer {api_key}"
            payload = {
                "model": "google/gemini-2.5-flash",
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "temperature": 0.1
            }
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]

        elif provider == "gemini":
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"
            payload = {
                "system_instruction": {"parts": [{"text": sys_prompt}]},
                "contents": [{"parts": [{"text": user_prompt}]}],
                "generationConfig": {
                    "temperature": 0.1,
                    "responseMimeType": "application/json"
                }
            }
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            content = data["candidates"][0]["content"]["parts"][0]["text"]

        elif provider == "groq":
            url = "https://api.groq.com/openai/v1/chat/completions"
            headers["Authorization"] = f"Bearer {api_key}"
            payload = {
                "model": "llama-3.3-70b-versatile",
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "temperature": 0.1
            }
            resp = requests.post(url, headers=headers, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]

        else:
            raise ValueError(f"Unknown provider: {provider}")

        result = json.loads(content)

        status = result.get("status", "not_sure")
        if status not in ("has_website", "no_website", "not_sure"):
            status = "not_sure"

        return {
            "status": status,
            "website_url": result.get("website_url"),
            "reason": result.get("reason", "")
        }

    except Exception as exc:
        logger.warning("LLM analysis failed for '%s' using %s: %s", business_name, provider, exc)
        return {"status": "error", "error": str(exc)}


def run_batch(
    limit: Optional[int] = None,
    min_score: float = 30.0,
    scope: Optional[str] = None,
) -> dict:
    """Analyze search results via LLM to verify whether businesses have websites.

    For each business:
    1. Fetch search results from SearXNG
    2. Feed results to LLM for analysis
    3. LLM determines: has_website / no_website / not_sure

    Args:
        limit: Max businesses to verify. None = 100 default.
        min_score: Only verify businesses scoring at or above this threshold.
        scope: Job scope tag.

    Returns:
        Dict with processing stats.
    """
    from ..config import load_config

    config = load_config()

    # Determine which provider to use
    provider = None
    api_key = None

    if config.openrouter_api_key:
        provider = "openrouter"
        api_key = config.openrouter_api_key
    elif config.gemini_api_key:
        provider = "gemini"
        api_key = config.gemini_api_key
    elif config.groq_api_key:
        provider = "groq"
        api_key = config.groq_api_key

    if not provider or not api_key:
        logger.warning("No LLM API keys configured. Skipping LLM verification.")
        return {
            "processed": 0,
            "error": "No LLM API keys configured in .env (OPENROUTER_API_KEY, GEMINI_API_KEY, GROQ_API_KEY)"
        }

    batch_size = 100
    if limit is not None and limit > 0:
        batch_size = limit
    elif limit is not None and limit <= 0:
        batch_size = None

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
                        not_(Business.raw.has_key("llm_verified")),
                    )
                )
                .order_by(Business.lead_score.desc(), Business.created_at)
            )

            if batch_size is not None:
                stmt = stmt.limit(batch_size)

            rows = session.execute(stmt).all()

            if not rows:
                complete_job(session, run, processed_count=0, details={
                    "min_score": min_score,
                    "websites_found": 0,
                    "no_website_confirmed": 0,
                    "not_sure": 0,
                    "errors": 0,
                    "provider": provider
                })
                return {
                    "processed": 0,
                    "websites_found": 0,
                    "no_website_confirmed": 0,
                    "not_sure": 0,
                    "errors": 0,
                    "provider": provider
                }

            processed = 0
            websites_found = 0
            no_website_confirmed = 0
            not_sure = 0
            errors = 0
            consecutive_rate_limits = 0
            MAX_CONSECUTIVE_RATE_LIMITS = 3  # Bail out after 3 consecutive 429s

            for business, city in rows:
                city_name = city.name if city else None
                biz_name = (business.name or "").strip()
                biz_category = (business.category or "").strip()

                if not biz_name:
                    processed += 1
                    continue

                raw = dict(business.raw) if business.raw else {}

                # Step 1: Fetch search results from SearXNG
                search_results = _fetch_search_context(biz_name, city_name)

                # Step 2: Ask LLM to analyze the search results
                try:
                    result = _analyze_with_llm(
                        business_name=biz_name,
                        city_name=city_name,
                        category=biz_category,
                        search_results=search_results,
                        api_key=api_key,
                        provider=provider
                    )
                    consecutive_rate_limits = 0  # Reset on success
                except Exception as llm_exc:
                    # Handle rate limits and other API errors gracefully —
                    # mark as error and continue with next business instead
                    # of crashing the entire batch.
                    logger.warning(
                        "LLM analysis exception for '%s': %s — skipping",
                        biz_name, llm_exc,
                    )
                    raw["llm_verified"] = True
                    raw["llm_verify_result"] = "error"
                    raw["llm_error"] = str(llm_exc)[:200]
                    raw["llm_error_count"] = raw.get("llm_error_count", 0) + 1
                    business.raw = raw
                    business.scored_at = None
                    errors += 1
                    processed += 1
                    # If we're hitting rate limits, bail out early
                    if "429" in str(llm_exc) or "rate" in str(llm_exc).lower():
                        consecutive_rate_limits += 1
                        if consecutive_rate_limits >= MAX_CONSECUTIVE_RATE_LIMITS:
                            logger.warning(
                                "LLM verify: %d consecutive rate limits — "
                                "aborting batch early (%d/%d processed)",
                                consecutive_rate_limits, processed, len(rows),
                            )
                            break
                    continue

                status = result.get("status")

                if status == "error":
                    raw["llm_verified"] = True
                    raw["llm_error"] = result.get("error")
                    raw["llm_error_count"] = raw.get("llm_error_count", 0) + 1
                    raw["llm_verify_result"] = "error"
                    business.raw = raw
                    errors += 1

                elif status == "has_website":
                    website = result.get("website_url")
                    if website:
                        business.website_url = website

                    raw["llm_verified"] = True
                    raw["llm_verify_result"] = "has_website"
                    raw["llm_website"] = website
                    raw["llm_reason"] = result.get("reason")
                    raw["llm_search_results_count"] = len(search_results)
                    business.raw = raw
                    websites_found += 1

                elif status == "no_website":
                    raw["llm_verified"] = True
                    raw["llm_verify_result"] = "no_website"
                    raw["llm_reason"] = result.get("reason")
                    raw["llm_search_results_count"] = len(search_results)
                    business.raw = raw
                    no_website_confirmed += 1

                else:  # not_sure
                    raw["llm_verified"] = True
                    raw["llm_verify_result"] = "not_sure"
                    raw["llm_reason"] = result.get("reason")
                    raw["llm_search_results_count"] = len(search_results)
                    business.raw = raw
                    not_sure += 1

                business.scored_at = None
                processed += 1

                if processed % 10 == 0:
                    session.flush()
                    logger.info(
                        "LLM analysis progress (%s): %d/%d processed, "
                        "%d websites found, %d confirmed no website",
                        provider, processed, len(rows), websites_found, no_website_confirmed,
                    )

                # Rate limiting
                if provider == "gemini":
                    time.sleep(2.0)
                else:
                    time.sleep(0.5)

            details = {
                "min_score": min_score,
                "websites_found": websites_found,
                "no_website_confirmed": no_website_confirmed,
                "not_sure": not_sure,
                "errors": errors,
                "provider": provider
            }
            complete_job(session, run, processed_count=processed, details=details)

            return {
                "processed": processed,
                "websites_found": websites_found,
                "no_website_confirmed": no_website_confirmed,
                "not_sure": not_sure,
                "errors": errors,
                "provider": provider
            }

        except Exception as exc:
            fail_job(session, run, error=str(exc))
            raise
