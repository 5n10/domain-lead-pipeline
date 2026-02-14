from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select

from ..config import load_config
from ..db import session_scope
from ..jobs import complete_job, fail_job, start_job
from ..models import (
    Business,
    BusinessContact,
    BusinessDomainLink,
    Contact,
    Domain,
    Organization,
)

ROLE_PREFIXES = {"info", "admin", "sales", "support", "contact"}
HIGH_PRIORITY_CATEGORIES = {"trades", "contractors"}
MEDIUM_PRIORITY_CATEGORIES = {"professional_services", "retail", "health", "food", "auto"}


def _score_contact(contact: Contact, domain: Domain, features: dict) -> tuple[float, dict]:
    if domain.status in {"hosted", "parked"}:
        return 0.0, {
            "domain_status": domain.status,
            "disqualified": True,
            "disqualification_reason": "hosted_or_parked_domain",
            "source": contact.source,
        }

    score = 0.0
    reasons = {
        "domain_status": domain.status,
        "categories": sorted(features.get("categories", [])),
        "has_no_website_business": bool(features.get("has_no_website_business", False)),
        "has_phone": bool(features.get("has_phone", False)),
        "source": contact.source,
    }

    if contact.source == "role":
        score += 10

    if contact.email and "@" in contact.email:
        prefix = contact.email.split("@", 1)[0].lower()
        if prefix in ROLE_PREFIXES:
            score += 10
            reasons["role_prefix"] = prefix

    if domain.status == "verified_unhosted":
        score += 20
    elif domain.status in {"checked", "mx_missing", "no_mx"}:
        score += 15
    elif domain.status == "enriched":
        score += 20
    elif domain.status == "unregistered_candidate":
        score += 10

    if features.get("has_no_website_business"):
        score += 25

    if features.get("has_phone"):
        score += 20

    categories = set(features.get("categories", []))
    if categories & HIGH_PRIORITY_CATEGORIES:
        score += 25
    elif categories & MEDIUM_PRIORITY_CATEGORIES:
        score += 10
    elif categories:
        score += 5

    return min(score, 100.0), reasons


def run_batch(limit: Optional[int] = None, force_rescore: bool = False) -> int:
    config = load_config()
    # When limit is None, use config.batch_size. When limit <= 0, process all (no limit)
    if limit is None:
        batch_size = config.batch_size
    elif limit <= 0:
        batch_size = None  # No limit
    else:
        batch_size = limit

    with session_scope() as session:
        run = start_job(session, "lead_scoring")
        try:
            stmt = (
                select(Contact, Organization, Domain)
                .join(Organization, Contact.org_id == Organization.id)
                .join(Domain, Organization.domain_id == Domain.id)
                .where(Contact.email.isnot(None))
                .order_by(Contact.created_at)
                .limit(batch_size)
            )
            if not force_rescore:
                stmt = stmt.where(Contact.scored_at.is_(None))

            rows = session.execute(stmt).all()
            if not rows:
                complete_job(session, run, processed_count=0)
                return 0

            domain_ids = sorted({domain.id for _, _, domain in rows})
            link_rows = session.execute(
                select(BusinessDomainLink.domain_id, Business.id, Business.category, Business.website_url)
                .join(Business, Business.id == BusinessDomainLink.business_id)
                .where(BusinessDomainLink.domain_id.in_(domain_ids))
            ).all()

            features_by_domain: dict = {
                domain_id: {
                    "categories": set(),
                    "has_no_website_business": False,
                    "has_phone": False,
                    "business_ids": set(),
                }
                for domain_id in domain_ids
            }

            for domain_id, business_id, category, website_url in link_rows:
                feature = features_by_domain[domain_id]
                feature["business_ids"].add(business_id)
                if category:
                    feature["categories"].add(category)
                if not website_url:
                    feature["has_no_website_business"] = True

            all_business_ids = sorted(
                {
                    business_id
                    for feature in features_by_domain.values()
                    for business_id in feature["business_ids"]
                }
            )

            phone_business_ids: set = set()
            if all_business_ids:
                phone_rows = session.execute(
                    select(BusinessContact.business_id)
                    .where(BusinessContact.business_id.in_(all_business_ids))
                    .where(BusinessContact.contact_type == "phone")
                ).all()
                phone_business_ids = {business_id for business_id, in phone_rows}

            for feature in features_by_domain.values():
                feature["has_phone"] = any(
                    business_id in phone_business_ids for business_id in feature["business_ids"]
                )
                feature.pop("business_ids", None)

            processed = 0
            for contact, _, domain in rows:
                features = features_by_domain.get(
                    domain.id,
                    {
                        "categories": set(),
                        "has_no_website_business": False,
                        "has_phone": False,
                    },
                )
                score, reasons = _score_contact(contact, domain, features)
                contact.lead_score = score
                contact.score_reasons = reasons
                contact.scored_at = datetime.now(timezone.utc)
                processed += 1

            complete_job(session, run, processed_count=processed, details={"force_rescore": force_rescore})
            return processed
        except Exception as exc:
            fail_job(session, run, error=str(exc), details={"force_rescore": force_rescore})
            raise
