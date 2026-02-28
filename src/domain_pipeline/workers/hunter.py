"""Hunter.io email enrichment worker.

Uses Hunter.io Domain Search API to find email addresses for lead domains.
Free tier: 25 searches/month. Paid plans from $49/mo for 500 searches.

Get API key at: https://hunter.io/users/sign_up
"""
from __future__ import annotations

import logging
import time
from typing import Any, Optional

import requests
from sqlalchemy import exists, not_, or_, select

from ..config import load_config
from ..db import session_scope
from ..domain_utils import is_public_email_domain
from ..jobs import complete_job, fail_job, start_job
from ..models import Business, BusinessContact, BusinessDomainLink, Domain

logger = logging.getLogger(__name__)

JOB_NAME = "hunter_email_enrichment"

DOMAIN_SEARCH_URL = "https://api.hunter.io/v2/domain-search"


class HunterClient:
    """Hunter.io API client."""

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.session = requests.Session()
        self._calls_made = 0

    @property
    def calls_made(self) -> int:
        return self._calls_made

    def domain_search(self, domain: str) -> Optional[dict[str, Any]]:
        """Search for emails associated with a domain.

        Returns the API response data or None on failure.
        """
        try:
            resp = self.session.get(
                DOMAIN_SEARCH_URL,
                params={"domain": domain, "api_key": self.api_key},
                timeout=10,
            )
            self._calls_made += 1

            if resp.status_code == 429:
                logger.warning("Hunter.io rate limited")
                return None

            if resp.status_code == 402:
                logger.warning("Hunter.io quota exhausted")
                return None

            if resp.status_code != 200:
                logger.warning(
                    "Hunter.io error %d: %s",
                    resp.status_code,
                    resp.text[:200],
                )
                return None

            return resp.json().get("data")

        except requests.RequestException as exc:
            logger.warning("Hunter.io request failed: %s", exc)
            return None


def run_batch(
    limit: Optional[int] = None,
    scope: Optional[str] = None,
) -> dict:
    """Enrich lead businesses with email contacts via Hunter.io.

    Targets businesses that have a non-public domain (from BusinessDomainLink)
    but few email contacts. Uses Hunter.io domain-search API to find real
    email addresses.

    Args:
        limit: Max businesses to process. None = 25 (conservative for free tier).
        scope: Job scope tag.

    Returns:
        Dict with processing stats.
    """
    config = load_config()

    if not config.hunter_api_key:
        return {
            "error": "HUNTER_API_KEY not configured",
            "processed": 0,
            "emails_found": 0,
            "contacts_created": 0,
        }

    batch_size = 25  # Conservative default matching free tier
    if limit is not None and limit > 0:
        batch_size = limit
    elif limit is not None and limit <= 0:
        batch_size = None

    client = HunterClient(config.hunter_api_key)

    with session_scope() as session:
        run = start_job(session, JOB_NAME, scope=scope)

        try:
            # Find leads with domains but not yet Hunter-enriched
            # Prioritize those with phone but no email (need email for outreach)
            has_email = exists(
                select(BusinessContact.id)
                .where(BusinessContact.business_id == Business.id)
                .where(BusinessContact.contact_type == "email")
            )

            stmt = (
                select(Business)
                .where(Business.lead_score >= 30)
                .where(
                    or_(
                        Business.raw.is_(None),
                        not_(Business.raw.has_key("hunter_enriched")),
                    )
                )
                # Must have at least one non-public domain
                .where(
                    exists(
                        select(BusinessDomainLink.id)
                        .join(Domain, Domain.id == BusinessDomainLink.domain_id)
                        .where(BusinessDomainLink.business_id == Business.id)
                    )
                )
                .order_by(
                    has_email.asc(),  # No-email businesses first
                    Business.lead_score.desc(),
                )
            )

            if batch_size is not None:
                stmt = stmt.limit(batch_size)

            businesses = session.execute(stmt).scalars().all()

            if not businesses:
                complete_job(session, run, processed_count=0, details={
                    "emails_found": 0, "contacts_created": 0, "api_calls": 0,
                })
                return {"processed": 0, "emails_found": 0, "contacts_created": 0, "api_calls": 0}

            processed = 0
            emails_found = 0
            contacts_created = 0

            for business in businesses:
                # Get domains for this business
                domain_rows = session.execute(
                    select(Domain.domain)
                    .join(BusinessDomainLink, BusinessDomainLink.domain_id == Domain.id)
                    .where(BusinessDomainLink.business_id == business.id)
                ).scalars().all()

                hunter_result = None
                searched_domain = None

                for domain_name in domain_rows:
                    if not domain_name or is_public_email_domain(domain_name.lower()):
                        continue
                    searched_domain = domain_name.lower()
                    hunter_result = client.domain_search(searched_domain)
                    if hunter_result:
                        break
                    time.sleep(0.5)

                raw = dict(business.raw) if business.raw else {}
                raw["hunter_enriched"] = True

                if hunter_result and hunter_result.get("emails"):
                    found_emails = hunter_result["emails"]
                    raw["hunter_domain"] = searched_domain
                    raw["hunter_emails_count"] = len(found_emails)
                    raw["hunter_organization"] = hunter_result.get("organization")
                    business.raw = raw

                    for email_data in found_emails:
                        email = (email_data.get("value") or "").strip().lower()
                        if not email:
                            continue
                        emails_found += 1

                        # Only add high-confidence emails
                        confidence = email_data.get("confidence", 0)
                        if confidence < 50:
                            continue

                        existing = session.execute(
                            select(BusinessContact.id)
                            .where(BusinessContact.business_id == business.id)
                            .where(BusinessContact.contact_type == "email")
                            .where(BusinessContact.value == email)
                        ).scalar()

                        if not existing:
                            session.add(BusinessContact(
                                business_id=business.id,
                                contact_type="email",
                                value=email,
                                source="hunter",
                            ))
                            contacts_created += 1
                else:
                    raw["hunter_domain"] = searched_domain
                    raw["hunter_emails_count"] = 0
                    business.raw = raw

                business.scored_at = None
                processed += 1
                if processed % 10 == 0:
                    session.flush()
                    logger.info(
                        "Hunter enrichment: %d/%d, %d emails found, %d contacts created",
                        processed, len(businesses), emails_found, contacts_created,
                    )
                time.sleep(0.5)

            details = {
                "emails_found": emails_found,
                "contacts_created": contacts_created,
                "api_calls": client.calls_made,
            }
            complete_job(session, run, processed_count=processed, details=details)
            return {
                "processed": processed,
                "emails_found": emails_found,
                "contacts_created": contacts_created,
                "api_calls": client.calls_made,
            }

        except Exception as exc:
            fail_job(session, run, error=str(exc), details={"api_calls": client.calls_made})
            raise
