"""Foursquare Places API enrichment & website verification worker.

Uses the Foursquare Places API v3 to:
1. Enrich businesses with phone numbers, websites, categories (run_batch)
2. Verify whether potential leads actually have websites (verify_websites)

Free tier: 10,000 API calls/month. No credit card required.
Get API key at: https://foursquare.com/developers/signup
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any, Optional

from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

import requests
from sqlalchemy import exists, not_, or_, select

from ..config import load_config
from ..db import session_scope
from ..jobs import complete_job, fail_job, start_job
from ..models import Business, BusinessContact, City

logger = logging.getLogger(__name__)

JOB_NAME = "foursquare_enrich"
VERIFY_JOB_NAME = "foursquare_verify_websites"

PLACES_SEARCH_URL = "https://api.foursquare.com/v3/places/search"


class FoursquareClient:
    """Foursquare Places API v3 client."""

    def __init__(self, api_key: str) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": api_key,
            "Accept": "application/json",
        })
        self._calls_made = 0

    @property
    def calls_made(self) -> int:
        return self._calls_made

    def return_none_on_error(retry_state):
        return None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.RequestException),
        retry_error_callback=return_none_on_error
    )
    def search(
        self,
        query: str,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
    ) -> Optional[dict[str, Any]]:
        """Search for a place. Returns the top result or None."""
        params: dict[str, Any] = {
            "query": query,
            "limit": 1,
            "fields": "fsq_id,name,location,tel,website,categories,rating",
        }

        if lat is not None and lon is not None:
            params["ll"] = f"{lat},{lon}"
            params["radius"] = 2000

        resp = self.session.get(
            PLACES_SEARCH_URL,
            params=params,
            timeout=10,
        )
        self._calls_made += 1

        if resp.status_code == 429:
            resp.raise_for_status()

        if resp.status_code != 200:
            logger.warning(
                "Foursquare API error %d: %s",
                resp.status_code,
                resp.text[:200],
            )
            return None

        data = resp.json()
        results = data.get("results", [])
        if not results:
            return None

        return results[0]


def _is_good_match(business: Business, place: dict) -> bool:
    """Validate that the Foursquare result matches our business."""
    place_name = (place.get("name") or "").lower()
    business_name = (business.name or "").lower()

    if not place_name or not business_name:
        return False

    stop_words = {"the", "a", "an", "and", "&", "of", "in", "at", "to", "for", "-", "le", "la", "les", "de", "du"}
    biz_words = set(business_name.split()) - stop_words
    place_words = set(place_name.split()) - stop_words

    if not biz_words:
        return False

    overlap = biz_words & place_words
    return len(overlap) >= max(1, len(biz_words) * 0.5)


def _build_search_query(business: Business, city_name: Optional[str] = None) -> str:
    """Build search query from business name + city."""
    parts = []
    if business.name:
        parts.append(business.name)
    if business.address:
        parts.append(business.address)
    elif city_name:
        parts.append(city_name)
    return " ".join(parts)


def run_batch(
    limit: Optional[int] = None,
    scope: Optional[str] = None,
    priority: str = "no_contacts",
) -> dict:
    """Enrich businesses using Foursquare Places API.

    Args:
        limit: Max businesses to process.
        scope: Job scope tag.
        priority: "no_contacts", "no_phone", or "all".

    Returns:
        Dict with processing stats.
    """
    config = load_config()

    if not config.foursquare_api_key:
        return {
            "error": "FOURSQUARE_API_KEY not configured",
            "processed": 0,
            "enriched": 0,
            "phones_added": 0,
        }

    batch_size = config.batch_size
    if limit is not None and limit > 0:
        batch_size = limit
    elif limit is not None and limit <= 0:
        batch_size = None

    client = FoursquareClient(config.foursquare_api_key)

    with session_scope() as session:
        run = start_job(session, JOB_NAME, scope=scope or priority)

        try:
            stmt = (
                select(Business, City)
                .outerjoin(City, Business.city_id == City.id)
                .where(Business.name.isnot(None))
                .where(Business.name != "")
                .where(
                    or_(
                        Business.raw.is_(None),
                        not_(Business.raw.has_key("foursquare")),
                    )
                )
            )

            if priority == "no_contacts":
                has_any_contact = exists(
                    select(BusinessContact.id)
                    .where(BusinessContact.business_id == Business.id)
                )
                stmt = stmt.where(not_(has_any_contact))
            elif priority == "no_phone":
                has_phone = exists(
                    select(BusinessContact.id)
                    .where(BusinessContact.business_id == Business.id)
                    .where(BusinessContact.contact_type == "phone")
                )
                stmt = stmt.where(not_(has_phone))

            stmt = stmt.order_by(
                Business.website_url.isnot(None).asc(),
                Business.created_at,
            )

            if batch_size is not None:
                stmt = stmt.limit(batch_size)

            rows = session.execute(stmt).all()

            if not rows:
                complete_job(session, run, processed_count=0, details={
                    "priority": priority, "enriched": 0, "phones_added": 0, "api_calls": 0,
                })
                return {"processed": 0, "enriched": 0, "phones_added": 0, "api_calls": 0}

            processed = 0
            enriched = 0
            phones_added = 0

            for business, city in rows:
                city_name = city.name if city else None
                query = _build_search_query(business, city_name)
                if not query.strip():
                    processed += 1
                    continue

                place = client.search(
                    query=query,
                    lat=float(business.lat) if business.lat is not None else None,
                    lon=float(business.lon) if business.lon is not None else None,
                )

                if place and _is_good_match(business, place):
                    raw = dict(business.raw) if business.raw else {}
                    enrichment = {
                        "fsq_id": place.get("fsq_id"),
                        "name": place.get("name"),
                        "phone": place.get("tel"),
                        "website": place.get("website"),
                        "rating": place.get("rating"),
                        "categories": [
                            c.get("name") for c in (place.get("categories") or [])
                        ],
                    }
                    raw["foursquare"] = enrichment
                    business.raw = raw
                    business.scored_at = None
                    enriched += 1

                    # Add phone if found and not already stored
                    phone = (place.get("tel") or "").strip()
                    if phone:
                        existing = session.execute(
                            select(BusinessContact.id)
                            .where(BusinessContact.business_id == business.id)
                            .where(BusinessContact.contact_type == "phone")
                            .where(BusinessContact.value == phone)
                        ).scalar()
                        if not existing:
                            session.add(BusinessContact(
                                business_id=business.id,
                                contact_type="phone",
                                value=phone,
                                source="foursquare",
                            ))
                            phones_added += 1

                processed += 1
                if processed % 50 == 0:
                    session.flush()
                    logger.info(
                        "Foursquare enrichment: %d/%d processed, %d enriched, %d phones",
                        processed, len(rows), enriched, phones_added,
                    )
                time.sleep(0.15)

            details = {
                "priority": priority, "enriched": enriched,
                "phones_added": phones_added, "api_calls": client.calls_made,
            }
            complete_job(session, run, processed_count=processed, details=details)
            return {
                "processed": processed, "enriched": enriched,
                "phones_added": phones_added, "api_calls": client.calls_made,
            }

        except Exception as exc:
            fail_job(session, run, error=str(exc), details={"api_calls": client.calls_made})
            raise


def verify_websites(
    limit: Optional[int] = None,
    min_score: float = 30.0,
    scope: Optional[str] = None,
) -> dict:
    """Verify whether leads have websites via Foursquare.

    Same pattern as google_places.verify_websites but uses Foursquare API.
    """
    config = load_config()

    if not config.foursquare_api_key:
        return {
            "error": "FOURSQUARE_API_KEY not configured",
            "processed": 0,
            "websites_found": 0,
            "no_website_confirmed": 0,
            "no_match": 0,
        }

    batch_size = config.batch_size
    if limit is not None and limit > 0:
        batch_size = limit
    elif limit is not None and limit <= 0:
        batch_size = None

    client = FoursquareClient(config.foursquare_api_key)

    with session_scope() as session:
        run = start_job(session, VERIFY_JOB_NAME, scope=scope)

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
                        not_(Business.raw.has_key("foursquare_verified")),
                    )
                )
                .order_by(Business.lead_score.desc(), Business.created_at)
            )

            if batch_size is not None:
                stmt = stmt.limit(batch_size)

            rows = session.execute(stmt).all()

            if not rows:
                complete_job(session, run, processed_count=0, details={
                    "min_score": min_score, "websites_found": 0,
                    "no_website_confirmed": 0, "no_match": 0, "api_calls": 0,
                })
                return {
                    "processed": 0, "websites_found": 0,
                    "no_website_confirmed": 0, "no_match": 0, "api_calls": 0,
                }

            processed = 0
            websites_found = 0
            no_website_confirmed = 0
            no_match = 0

            for business, city in rows:
                city_name = city.name if city else None
                query = _build_search_query(business, city_name)
                if not query.strip():
                    processed += 1
                    continue

                place = client.search(
                    query=query,
                    lat=float(business.lat) if business.lat is not None else None,
                    lon=float(business.lon) if business.lon is not None else None,
                )

                raw = dict(business.raw) if business.raw else {}

                if not place:
                    raw["foursquare_verified"] = True
                    raw["foursquare_verify_result"] = "no_match"
                    business.raw = raw
                    business.scored_at = None
                    no_match += 1
                    processed += 1
                    time.sleep(0.15)
                    continue

                if not _is_good_match(business, place):
                    raw["foursquare_verified"] = True
                    raw["foursquare_verify_result"] = "poor_match"
                    raw["foursquare_verify_name"] = place.get("name")
                    business.raw = raw
                    business.scored_at = None
                    no_match += 1
                    processed += 1
                    time.sleep(0.15)
                    continue

                website = (place.get("website") or "").strip()

                if website:
                    business.website_url = website
                    raw["foursquare_verified"] = True
                    raw["foursquare_verify_result"] = "has_website"
                    raw["foursquare_website"] = website
                    raw["foursquare_verify_name"] = place.get("name")
                    business.raw = raw
                    business.scored_at = None
                    websites_found += 1
                else:
                    raw["foursquare_verified"] = True
                    raw["foursquare_verify_result"] = "no_website"
                    raw["foursquare_verify_name"] = place.get("name")
                    business.raw = raw
                    business.scored_at = None
                    no_website_confirmed += 1

                # Enrich with phone if available
                phone = (place.get("tel") or "").strip()
                if phone:
                    existing = session.execute(
                        select(BusinessContact.id)
                        .where(BusinessContact.business_id == business.id)
                        .where(BusinessContact.contact_type == "phone")
                        .where(BusinessContact.value == phone)
                    ).scalar()
                    if not existing:
                        session.add(BusinessContact(
                            business_id=business.id,
                            contact_type="phone",
                            value=phone,
                            source="foursquare",
                        ))

                processed += 1
                if processed % 50 == 0:
                    session.flush()
                    logger.info(
                        "Foursquare verification: %d/%d, %d websites, %d no website, %d no match",
                        processed, len(rows), websites_found, no_website_confirmed, no_match,
                    )
                time.sleep(0.15)

            details = {
                "min_score": min_score, "websites_found": websites_found,
                "no_website_confirmed": no_website_confirmed, "no_match": no_match,
                "api_calls": client.calls_made,
            }
            complete_job(session, run, processed_count=processed, details=details)
            return {
                "processed": processed, "websites_found": websites_found,
                "no_website_confirmed": no_website_confirmed, "no_match": no_match,
                "api_calls": client.calls_made,
            }

        except Exception as exc:
            fail_job(session, run, error=str(exc), details={"api_calls": client.calls_made})
            raise
