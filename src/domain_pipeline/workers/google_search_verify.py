"""Google Search website verification worker.

Searches Google for each lead candidate to verify whether the business
truly lacks a website. This is an ADDITIONAL verification stage on top of DDG.

If search results contain a real business website (not a directory listing),
sets business.website_url so the business gets disqualified from leads.

Completely FREE — no API key, no registration required.
Uses direct HTML scraping of Google search results.
More conservative rate limiting than DDG to avoid blocks.
"""
from __future__ import annotations

import logging
import random
import re
import time
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse

from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

import requests as http_requests
from bs4 import BeautifulSoup
from sqlalchemy import not_, or_, select

from ..db import session_scope
from ..jobs import complete_job, fail_job, start_job
from ..models import Business, City

# Reuse matching/filtering logic from web_search_verify
from .web_search_verify import (
    DIRECTORY_DOMAINS,
    PUBLIC_EMAIL_DOMAINS_QUICK,
    _extract_business_website,
    _get_domain_from_url,
    _result_matches_business,
)

logger = logging.getLogger(__name__)

JOB_NAME = "google_search_verify_websites"

# Google TLDs by country for localized results
_GOOGLE_DOMAINS = {
    "CA": "www.google.ca",
    "AE": "www.google.ae",
    "US": "www.google.com",
}

_USER_AGENTS = [
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    ),
]


def return_empty_on_error(retry_state):
    return []

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=3, min=3, max=15),
    retry=retry_if_exception_type(http_requests.RequestException),
    retry_error_callback=return_empty_on_error
)
def _search_google(query: str, country: str | None = None, max_results: int = 10) -> list[dict]:
    """Search Google via HTML scraping.

    Returns list of dicts with 'title', 'href', 'body' keys.
    Uses direct HTML scraping since googlesearch-python library is broken
    (Google blocks its scraping pattern).

    Returns empty list with 'blocked' status on CAPTCHA/403.
    """
    google_domain = _GOOGLE_DOMAINS.get(country or "", "www.google.com")
    user_agent = random.choice(_USER_AGENTS)

    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    params = {
        "q": query,
        "num": str(max_results),
        "hl": "en",
    }

    resp = http_requests.get(
        f"https://{google_domain}/search",
        params=params,
        headers=headers,
        timeout=(5, 15),  # (connect_timeout, read_timeout)
        allow_redirects=True,
    )

    # Google CAPTCHA or block detection
    if resp.status_code == 429 or resp.status_code == 403:
        logger.warning(
            "Google blocked request (status %d) for '%s'",
            resp.status_code, query,
        )
        return []

    if resp.status_code != 200:
        logger.warning("Google returned status %d for '%s'", resp.status_code, query)
        return []

    # Check for CAPTCHA in response body
    if "captcha" in resp.text.lower() or "unusual traffic" in resp.text.lower():
        logger.warning("Google CAPTCHA detected for '%s'", query)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    results = []

    # Google organic results are in divs with class 'g'
    for div in soup.find_all("div", class_="g"):
        # Find the link
        link = div.find("a", href=True)
        if not link:
            continue

        href = link.get("href", "")
        # Skip Google's own links and non-http links
        if not href.startswith("http"):
            continue
        if "google.com" in href or "google.ca" in href or "google.ae" in href:
            continue

        # Extract title
        title_el = div.find("h3")
        title = title_el.get_text(strip=True) if title_el else ""

        # Extract snippet
        snippet_el = div.find("div", class_="VwiC3b") or div.find("span", class_="aCOpRe")
        body = snippet_el.get_text(strip=True) if snippet_el else ""

        results.append({
            "title": title,
            "href": href,
            "body": body,
        })

        if len(results) >= max_results:
            break

    # Fallback: try alternative selectors if no results found
    if not results:
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            if not href.startswith("http"):
                continue
            if "google.com" in href or "google.ca" in href or "google.ae" in href:
                continue

            domain = _get_domain_from_url(href)
            if not domain:
                continue
            if domain in {"accounts.google.com", "support.google.com", "policies.google.com"}:
                continue

            title = a_tag.get_text(strip=True)
            if not title or len(title) < 3:
                continue

            results.append({
                "title": title,
                "href": href,
                "body": "",
            })

            if len(results) >= max_results:
                break

    return results


def _build_google_queries(biz_name: str, city_name: str | None, category: str | None, country: str | None) -> list[str]:
        """Generate search queries for Google with different strategies.

        1. Full name + city (broad)
        2. Full name + "website" (looking for official site)
        3. Full name + category + country (contextual)
        """
        queries = []

        # Query 1: Full name + city (broad match)
        if city_name:
            queries.append(f"{biz_name} {city_name}")
        else:
            queries.append(biz_name)

        # Query 2: Full name + "website" keyword
        queries.append(f"{biz_name} website")

        # Query 3: Full name + category + country context
        if category and country:
            queries.append(f"{biz_name} {category} {country}")
        elif category:
            queries.append(f"{biz_name} {category}")

        # Deduplicate while preserving order
        seen = set()
        unique = []
        for q in queries:
            if q not in seen:
                seen.add(q)
                unique.append(q)
        return unique


def run_batch(
    limit: Optional[int] = None,
    min_score: float = 30.0,
    scope: Optional[str] = None,
) -> dict:
    """Verify whether potential leads have websites via Google Search.

    For each business with lead_score >= min_score and no website_url:
    1. Search Google for the business name
    2. Analyze results — filter out directories/social media
    3. If a real business website is found, set business.website_url
    4. Track result in business.raw["google_search_verified"]

    This is an ADDITIONAL verification stage on top of DDG.
    FREE, no API key, but rate limited to ~15 businesses/min.

    Args:
        limit: Max businesses to verify. None = 50 default, 0 = unlimited.
        min_score: Only verify businesses scoring at or above this threshold.
        scope: Job scope tag.

    Returns:
        Dict with processing stats.
    """
    batch_size = 50  # Conservative default for Google
    if limit is not None and limit > 0:
        batch_size = limit
    elif limit is not None and limit <= 0:
        batch_size = None

    with session_scope() as session:
        run = start_job(session, JOB_NAME, scope=scope)

        try:
            # Find leads that haven't been Google-search-verified yet
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
                        not_(Business.raw.has_key("google_search_verified")),
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
                    "inconclusive": 0,
                    "blocked": 0,
                    "errors": 0,
                })
                return {
                    "processed": 0,
                    "websites_found": 0,
                    "no_website_confirmed": 0,
                    "inconclusive": 0,
                    "blocked": 0,
                    "errors": 0,
                }

            processed = 0
            websites_found = 0
            no_website_confirmed = 0
            inconclusive = 0
            blocked = 0
            errors = 0
            consecutive_blocks = 0

            for business, city in rows:
                # Stop if Google is consistently blocking us
                if consecutive_blocks >= 3:
                    logger.warning(
                        "Google blocking detected (%d consecutive), stopping batch early",
                        consecutive_blocks,
                    )
                    break

                city_name = city.name if city else None
                country = city.country if city else None
                biz_name = (business.name or "").strip()
                if not biz_name:
                    processed += 1
                    continue

                category = (business.category or "").strip() or None

                # Build multiple search queries
                search_queries = _build_google_queries(biz_name, city_name, category, country)

                # Try each query until we get results
                results = []
                query_used = search_queries[0]
                was_blocked = False

                for q in search_queries:
                    results = _search_google(q, country=country, max_results=10)
                    query_used = q
                    if results:
                        consecutive_blocks = 0
                        break
                    # Check if this was a block vs genuine no results
                    # (empty results from _search_google could be a block)
                    time.sleep(2.0 + random.uniform(0, 1.5))

                raw = dict(business.raw) if business.raw else {}

                if not results:
                    # Could be blocked or genuine no results — mark as inconclusive
                    raw["google_search_verified"] = True
                    raw["google_search_result"] = "no_results"
                    raw["google_search_query"] = query_used
                    business.raw = raw
                    business.scored_at = None
                    inconclusive += 1
                    blocked += 1
                    consecutive_blocks += 1
                    processed += 1
                    # Longer wait if potentially blocked
                    time.sleep(4.0 + random.uniform(0, 2.0))
                    continue

                # Analyze results
                website = _extract_business_website(results, biz_name)

                if website:
                    # Found a real website — disqualify this lead
                    business.website_url = website
                    raw["google_search_verified"] = True
                    raw["google_search_result"] = "has_website"
                    raw["google_search_website"] = website
                    raw["google_search_query"] = query_used
                    raw["google_search_result_count"] = len(results)
                    business.raw = raw
                    websites_found += 1
                    logger.debug(
                        "Google found website for '%s': %s", biz_name, website,
                    )
                else:
                    # No business website in results — genuine lead candidate
                    raw["google_search_verified"] = True
                    raw["google_search_result"] = "no_website"
                    raw["google_search_query"] = query_used
                    raw["google_search_result_count"] = len(results)
                    business.raw = raw
                    no_website_confirmed += 1

                business.scored_at = None
                processed += 1

                if processed % 25 == 0:
                    session.flush()
                    logger.info(
                        "Google verification progress: %d/%d processed, "
                        "%d have websites, %d confirmed no website",
                        processed, len(rows), websites_found, no_website_confirmed,
                    )

                # Conservative rate limiting for Google (3-5 seconds between requests)
                time.sleep(3.0 + random.uniform(0, 2.0))

            details = {
                "min_score": min_score,
                "websites_found": websites_found,
                "no_website_confirmed": no_website_confirmed,
                "inconclusive": inconclusive,
                "blocked": blocked,
                "errors": errors,
            }
            complete_job(session, run, processed_count=processed, details=details)

            return {
                "processed": processed,
                "websites_found": websites_found,
                "no_website_confirmed": no_website_confirmed,
                "inconclusive": inconclusive,
                "blocked": blocked,
                "errors": errors,
            }

        except Exception as exc:
            fail_job(session, run, error=str(exc))
            raise
