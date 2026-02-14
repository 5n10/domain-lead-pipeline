from __future__ import annotations

import logging
from typing import Iterable, Optional

from sqlalchemy import select

from ..config import load_config
from ..db import session_scope
from ..jobs import complete_job, fail_job, start_job
from ..models import Contact, Domain, Organization, WhoisCheck

logger = logging.getLogger(__name__)

ROLE_PREFIXES = [
    "info",
    "admin",
    "sales",
    "support",
    "contact",
]


def ensure_org(session, domain_row: Domain) -> Organization:
    if domain_row.organizations:
        return domain_row.organizations[0]
    org = Organization(domain_id=domain_row.id, name=domain_row.domain)
    session.add(org)
    return org


def create_contacts(session, org: Organization, emails: Iterable[str]) -> int:
    existing = {
        row.email.lower()
        for row in session.execute(
            select(Contact.email).where(Contact.org_id == org.id, Contact.email.isnot(None))
        ).scalars()
        if row
    }

    created = 0
    for email in emails:
        if email in existing:
            continue
        contact = Contact(org_id=org.id, email=email, source="role", confidence=0.2)
        session.add(contact)
        created += 1
    return created


def latest_whois_check(session, domain_id) -> Optional[WhoisCheck]:
    return (
        session.execute(
            select(WhoisCheck)
            .where(WhoisCheck.domain_id == domain_id)
            .order_by(WhoisCheck.checked_at.desc())
            .limit(1)
        )
        .scalars()
        .first()
    )


def build_role_emails(domain: str) -> list[str]:
    return [f"{prefix}@{domain}" for prefix in ROLE_PREFIXES]


def run_batch(limit: Optional[int] = None, scope: Optional[str] = None) -> int:
    config = load_config()
    processed = 0

    with session_scope() as session:
        run = start_job(session, "enrich_role_emails", scope=scope)
        # When limit is None, use config batch size; when limit <= 0, process all items
        if limit is None:
            batch_size = config.batch_size
        elif limit <= 0:
            batch_size = None  # No limit
        else:
            batch_size = limit

        try:
            stmt = (
                select(Domain)
                .where(Domain.status.in_(["verified_unhosted", "checked"]))
                .order_by(Domain.created_at)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
            domains = session.execute(stmt).scalars().all()

            for domain_row in domains:
                whois_check = latest_whois_check(session, domain_row.id)
                if not whois_check or not whois_check.has_mx:
                    domain_row.status = "mx_missing"
                    processed += 1
                    continue

                org = ensure_org(session, domain_row)
                emails = build_role_emails(domain_row.domain)
                created = create_contacts(session, org, emails)

                if created > 0:
                    domain_row.status = "enriched"
                else:
                    domain_row.status = "no_contacts"

                processed += 1

            complete_job(session, run, processed_count=processed)
        except Exception as exc:
            fail_job(session, run, error=str(exc))
            raise

    return processed


if __name__ == "__main__":
    count = run_batch()
    logger.info("Processed %d domains", count)
