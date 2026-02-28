"""Google Places API enrichment & website verification worker.

Uses the Google Places API (New) to:
1. Enrich businesses with phone numbers, ratings, Place IDs (run_batch)
2. Verify whether potential leads actually have websites (verify_websites)

The verification step is CRITICAL for lead quality. OSM only tags websites
for ~14% of businesses. Google Places knows the real website for most
businesses — if Google says a business has a website, it's NOT a lead.

Free tier: 10,000 calls/month on Essentials SKUs.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Optional
from urllib.parse import quote_plus

from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

import requests
from sqlalchemy import and_, exists, func, not_, or_, select

from ..config import load_config
from ..db import session_scope
from ..domain_utils import normalize_domain
from ..jobs import complete_job, fail_job, start_job
from ..models import Business, BusinessContact, City

logger = logging.getLogger(__name__)

JOB_NAME = "google_places_enrich"

# Fields we request from Places API (Essentials tier = free up to 10K/mo)
# Using Places API (New) Text Search
PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
PLACE_DETAILS_URL = "https://places.googleapis.com/v1/places"

# Field masks control pricing — keep to Essentials-tier fields
SEARCH_FIELD_MASK = ",".join([
    "places.id",
    "places.displayName",
    "places.formattedAddress",
    "places.nationalPhoneNumber",
    "places.internationalPhoneNumber",
    "places.websiteUri",
    "places.rating",
    "places.userRatingCount",
    "places.googleMapsUri",
    "places.location",
])


class PlacesClient:
    """Google Places API (New) client with session pooling."""

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key,
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
    def text_search(
        self,
        query: str,
        location_lat: Optional[float] = None,
        location_lon: Optional[float] = None,
    ) -> Optional[dict[str, Any]]:
        """Search for a place by text query.

        Returns the top result or None if no match found.
        Uses the Essentials-tier Text Search (New) endpoint.
        """
        body: dict[str, Any] = {
            "textQuery": query,
            "maxResultCount": 1,
        }

        # Add location bias if coordinates available (improves accuracy)
        if location_lat is not None and location_lon is not None:
            body["locationBias"] = {
                "circle": {
                    "center": {
                        "latitude": location_lat,
                        "longitude": location_lon,
                    },
                    "radius": 2000.0,  # 2km radius
                }
            }

        resp = self.session.post(
            PLACES_TEXT_SEARCH_URL,
            json=body,
            headers={"X-Goog-FieldMask": SEARCH_FIELD_MASK},
            timeout=10,
        )
        self._calls_made += 1

        if resp.status_code == 429:
            resp.raise_for_status()

        if resp.status_code != 200:
            logger.warning(
                "Google Places API error %d: %s",
                resp.status_code,
                resp.text[:200],
            )
            return None

        data = resp.json()
        places = data.get("places", [])
        if not places:
            return None

        return places[0]


def _build_search_query(business: Business, city_name: Optional[str] = None) -> str:
    """Build a search query from business name + address/city.

    The more specific the query, the better the match accuracy.
    """
    parts = []

    if business.name:
        parts.append(business.name)

    # Add address for disambiguation
    if business.address:
        parts.append(business.address)
    elif city_name:
        parts.append(city_name)

    return " ".join(parts)


def _is_good_match(business: Business, place: dict) -> bool:
    """Basic validation that the Places result matches our business.

    Prevents enriching business A with data from business B.
    """
    place_name = (place.get("displayName", {}).get("text") or "").lower()
    business_name = (business.name or "").lower()

    if not place_name or not business_name:
        return False

    # Check if names share significant overlap
    # Split both into words and check overlap
    biz_words = set(business_name.split())
    place_words = set(place_name.split())

    # Remove very common words
    stop_words = {"the", "a", "an", "and", "&", "of", "in", "at", "to", "for", "-", "le", "la", "les", "de", "du"}
    biz_words -= stop_words
    place_words -= stop_words

    if not biz_words:
        return False

    overlap = biz_words & place_words
    overlap_ratio = len(overlap) / len(biz_words)

    # At least 50% of business name words should appear in the place name
    return overlap_ratio >= 0.5


def _extract_contacts(place: dict) -> dict[str, Any]:
    """Extract enrichment data from a Places API result."""
    result: dict[str, Any] = {
        "google_place_id": place.get("id"),
        "google_maps_url": place.get("googleMapsUri"),
        "phone": None,
        "website": None,
        "rating": None,
        "review_count": None,
        "place_name": (place.get("displayName", {}).get("text")),
    }

    # Phone — prefer national format
    phone = place.get("nationalPhoneNumber") or place.get("internationalPhoneNumber")
    if phone:
        result["phone"] = phone.strip()

    # Website
    website = place.get("websiteUri")
    if website:
        result["website"] = website.strip()

    # Rating
    rating = place.get("rating")
    if rating is not None:
        result["rating"] = float(rating)

    review_count = place.get("userRatingCount")
    if review_count is not None:
        result["review_count"] = int(review_count)

    return result


def enrich_business(
    business: Business,
    city_name: Optional[str],
    client: PlacesClient,
    session,
) -> Optional[dict]:
    """Enrich a single business with Google Places data.

    Returns the enrichment data dict if successful, None if no match.
    """
    query = _build_search_query(business, city_name)
    if not query.strip():
        return None

    place = client.text_search(
        query=query,
        location_lat=float(business.lat) if business.lat is not None else None,
        location_lon=float(business.lon) if business.lon is not None else None,
    )

    if not place:
        return None

    # Validate match quality
    if not _is_good_match(business, place):
        logger.debug(
            "Skipping poor match for '%s': got '%s'",
            business.name,
            place.get("displayName", {}).get("text"),
        )
        return None

    enrichment = _extract_contacts(place)

    # Add phone as BusinessContact if we found one and business doesn't have it
    if enrichment["phone"]:
        existing_phone = session.execute(
            select(BusinessContact.id)
            .where(BusinessContact.business_id == business.id)
            .where(BusinessContact.contact_type == "phone")
            .where(BusinessContact.value == enrichment["phone"])
        ).scalar()

        if not existing_phone:
            session.add(BusinessContact(
                business_id=business.id,
                contact_type="phone",
                value=enrichment["phone"],
                source="google_places",
            ))

    # Add website to raw data if business doesn't have one
    # (Don't set website_url — that would change lead eligibility)
    # Store it in raw for reference
    raw = business.raw or {}
    raw["google_places"] = enrichment
    business.raw = raw
    business.scored_at = None

    return enrichment


def run_batch(
    limit: Optional[int] = None,
    scope: Optional[str] = None,
    priority: str = "no_contacts",
) -> dict:
    """Enrich businesses using Google Places API.

    Args:
        limit: Max businesses to process (None = use config batch_size).
                Set to 0 for unlimited.
        scope: Job scope tag.
        priority: Which businesses to prioritize:
            - "no_contacts": Businesses with no phone/email (default, highest impact)
            - "no_phone": Businesses that have email but no phone
            - "all": Any business without google_places enrichment

    Returns:
        Dict with processing stats.
    """
    config = load_config()

    if not config.google_places_api_key:
        return {
            "error": "GOOGLE_PLACES_API_KEY not configured",
            "processed": 0,
            "enriched": 0,
            "phones_added": 0,
        }

    batch_size = config.batch_size
    if limit is not None and limit > 0:
        batch_size = limit
    elif limit is not None and limit <= 0:
        batch_size = None  # Unlimited

    client = PlacesClient(config.google_places_api_key)

    with session_scope() as session:
        run = start_job(session, JOB_NAME, scope=scope or priority)

        try:
            # Build query based on priority
            stmt = (
                select(Business, City)
                .outerjoin(City, Business.city_id == City.id)
                .where(Business.name.isnot(None))
                .where(Business.name != "")
            )

            # Exclude already-enriched businesses
            # Check raw JSONB for google_places key
            stmt = stmt.where(
                or_(
                    Business.raw.is_(None),
                    not_(Business.raw.has_key("google_places")),
                )
            )

            if priority == "no_contacts":
                # Businesses with NO contacts at all — highest impact
                has_any_contact = exists(
                    select(BusinessContact.id)
                    .where(BusinessContact.business_id == Business.id)
                )
                stmt = stmt.where(not_(has_any_contact))
            elif priority == "no_phone":
                # Businesses that have email but no phone
                has_phone = exists(
                    select(BusinessContact.id)
                    .where(BusinessContact.business_id == Business.id)
                    .where(BusinessContact.contact_type == "phone")
                )
                stmt = stmt.where(not_(has_phone))

            # Prefer businesses without websites (our lead targets)
            stmt = stmt.order_by(
                # No-website businesses first
                Business.website_url.isnot(None).asc(),
                Business.created_at,
            )

            if batch_size is not None:
                stmt = stmt.limit(batch_size)

            rows = session.execute(stmt).all()

            if not rows:
                complete_job(session, run, processed_count=0, details={
                    "priority": priority,
                    "enriched": 0,
                    "phones_added": 0,
                    "api_calls": 0,
                })
                return {
                    "processed": 0,
                    "enriched": 0,
                    "phones_added": 0,
                    "api_calls": 0,
                }

            processed = 0
            enriched = 0
            phones_added = 0

            for business, city in rows:
                city_name = city.name if city else None
                result = enrich_business(business, city_name, client, session)

                if result:
                    enriched += 1
                    if result.get("phone"):
                        phones_added += 1

                processed += 1

                # Commit every 50 businesses to avoid losing work
                if processed % 50 == 0:
                    session.flush()
                    logger.info(
                        "Google Places enrichment progress: %d/%d processed, "
                        "%d enriched, %d phones added, %d API calls",
                        processed, len(rows), enriched, phones_added, client.calls_made,
                    )

                # Respect rate limits — Google allows ~10 QPS on free tier
                # Small delay to stay well under limit
                time.sleep(0.15)

            details = {
                "priority": priority,
                "enriched": enriched,
                "phones_added": phones_added,
                "api_calls": client.calls_made,
            }
            complete_job(session, run, processed_count=processed, details=details)

            return {
                "processed": processed,
                "enriched": enriched,
                "phones_added": phones_added,
                "api_calls": client.calls_made,
            }

        except Exception as exc:
            fail_job(session, run, error=str(exc), details={
                "api_calls": client.calls_made,
            })
            raise


VERIFY_JOB_NAME = "google_places_verify_websites"


def verify_websites(
    limit: Optional[int] = None,
    min_score: float = 30.0,
    scope: Optional[str] = None,
) -> dict:
    """Verify whether potential leads actually have websites via Google Places.

    This is the critical quality gate for leads. The system's biggest weakness
    is that OSM only has website URLs for ~14% of businesses. Most businesses
    DO have websites — they're just not tagged in OSM. Google Places knows.

    For each business with lead_score >= min_score and no website_url:
    1. Search Google Places for the business
    2. If Google returns a websiteUri → set business.website_url
    3. Business gets excluded from leads on next rescore

    This should be run AFTER enrichment and scoring, as a final quality filter.

    Args:
        limit: Max businesses to verify. None = config batch_size, 0 = unlimited.
        min_score: Only verify businesses scoring at or above this threshold.
        scope: Job scope tag.

    Returns:
        Dict with processing stats.
    """
    config = load_config()

    if not config.google_places_api_key:
        return {
            "error": "GOOGLE_PLACES_API_KEY not configured",
            "processed": 0,
            "websites_found": 0,
            "no_website_confirmed": 0,
            "no_match": 0,
        }

    batch_size = config.batch_size
    if limit is not None and limit > 0:
        batch_size = limit
    elif limit is not None and limit <= 0:
        batch_size = None  # Unlimited

    client = PlacesClient(config.google_places_api_key)

    with session_scope() as session:
        run = start_job(session, VERIFY_JOB_NAME, scope=scope)

        try:
            # Find potential leads that haven't been website-verified yet.
            # We use raw JSONB to track verification status:
            #   raw["google_places_verified"] = True means we've checked this business.
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
                        not_(Business.raw.has_key("google_places_verified")),
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
                    "no_match": 0,
                    "api_calls": 0,
                })
                return {
                    "processed": 0,
                    "websites_found": 0,
                    "no_website_confirmed": 0,
                    "no_match": 0,
                    "api_calls": 0,
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

                place = client.text_search(
                    query=query,
                    location_lat=float(business.lat) if business.lat is not None else None,
                    location_lon=float(business.lon) if business.lon is not None else None,
                )

                raw = business.raw or {}

                if not place:
                    # No Google Places result — can't verify
                    raw["google_places_verified"] = True
                    raw["google_places_verify_result"] = "no_match"
                    business.raw = raw
                    business.scored_at = None
                    no_match += 1
                    processed += 1
                    time.sleep(0.15)
                    continue

                # Check match quality
                if not _is_good_match(business, place):
                    raw["google_places_verified"] = True
                    raw["google_places_verify_result"] = "poor_match"
                    raw["google_places_verify_name"] = (
                        place.get("displayName", {}).get("text")
                    )
                    business.raw = raw
                    business.scored_at = None
                    no_match += 1
                    processed += 1
                    time.sleep(0.15)
                    continue

                # Good match — check for website
                website = (place.get("websiteUri") or "").strip()

                if website:
                    # Google confirms this business HAS a website.
                    # Set website_url so it gets excluded from leads on rescore.
                    business.website_url = website
                    raw["google_places_verified"] = True
                    raw["google_places_verify_result"] = "has_website"
                    raw["google_places_website"] = website
                    raw["google_places_verify_name"] = (
                        place.get("displayName", {}).get("text")
                    )
                    business.raw = raw
                    websites_found += 1
                    logger.debug(
                        "Website found for '%s': %s", business.name, website,
                    )
                else:
                    # Google Places matched but no website — genuine lead candidate!
                    raw["google_places_verified"] = True
                    raw["google_places_verify_result"] = "no_website"
                    raw["google_places_verify_name"] = (
                        place.get("displayName", {}).get("text")
                    )
                    business.raw = raw
                    no_website_confirmed += 1

                # Also enrich with phone if available and not already stored
                enrichment = _extract_contacts(place)
                if enrichment.get("phone"):
                    existing_phone = session.execute(
                        select(BusinessContact.id)
                        .where(BusinessContact.business_id == business.id)
                        .where(BusinessContact.contact_type == "phone")
                        .where(BusinessContact.value == enrichment["phone"])
                    ).scalar()
                    if not existing_phone:
                        session.add(BusinessContact(
                            business_id=business.id,
                            contact_type="phone",
                            value=enrichment["phone"],
                            source="google_places",
                        ))

                # Store full enrichment data
                raw["google_places"] = enrichment
                business.raw = raw
                business.scored_at = None

                processed += 1

                # Commit every 50
                if processed % 50 == 0:
                    session.flush()
                    logger.info(
                        "Website verification progress: %d/%d processed, "
                        "%d have websites, %d confirmed no website, "
                        "%d no match, %d API calls",
                        processed, len(rows), websites_found,
                        no_website_confirmed, no_match, client.calls_made,
                    )

                time.sleep(0.15)

            details = {
                "min_score": min_score,
                "websites_found": websites_found,
                "no_website_confirmed": no_website_confirmed,
                "no_match": no_match,
                "api_calls": client.calls_made,
            }
            complete_job(session, run, processed_count=processed, details=details)

            return {
                "processed": processed,
                "websites_found": websites_found,
                "no_website_confirmed": no_website_confirmed,
                "no_match": no_match,
                "api_calls": client.calls_made,
            }

        except Exception as exc:
            fail_job(session, run, error=str(exc), details={
                "api_calls": client.calls_made,
            })
            raise
