from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlalchemy import and_, or_, select
from sqlalchemy.dialects.postgresql import insert

from ..config import load_config
from ..db import session_scope
from ..domain_utils import extract_domain_from_email, is_public_email_domain, normalize_domain
from ..jobs import complete_job, fail_job, get_checkpoint, set_checkpoint, start_job
from ..models import Business, BusinessContact, BusinessDomainLink, Domain


JOB_NAME = "sync_business_domains"
CURSOR_KEY = "business_cursor"


def _parse_cursor(value: Optional[str]) -> tuple[Optional[datetime], Optional[UUID]]:
    if not value:
        return None, None
    try:
        ts_raw, id_raw = value.split("|", 1)
        return datetime.fromisoformat(ts_raw), UUID(id_raw)
    except (ValueError, TypeError):
        # Invalid cursor format - return None to restart from beginning
        return None, None


def _make_cursor(ts: datetime, business_id: UUID) -> str:
    return f"{ts.isoformat()}|{business_id}"


def _sync_batch(session, businesses: list[Business]) -> tuple[int, int]:
    business_ids = [row.id for row in businesses]
    email_rows = session.execute(
        select(BusinessContact.business_id, BusinessContact.value)
        .where(BusinessContact.business_id.in_(business_ids))
        .where(BusinessContact.contact_type == "email")
    ).all()

    emails_by_business: dict[UUID, list[str]] = {}
    for business_id, email in email_rows:
        emails_by_business.setdefault(business_id, []).append(email)

    business_domain_sources: dict[UUID, dict[str, str]] = {}
    all_domains: set[str] = set()

    for business in businesses:
        discovered: dict[str, str] = {}

        if business.website_url:
            web_domain = normalize_domain(business.website_url)
            if web_domain:
                discovered[web_domain] = "website"

        for email in emails_by_business.get(business.id, []):
            email_domain = extract_domain_from_email(email)
            if email_domain and not is_public_email_domain(email_domain) and email_domain not in discovered:
                discovered[email_domain] = "email"

        if discovered:
            business_domain_sources[business.id] = discovered
            all_domains.update(discovered.keys())

    domains_inserted = 0
    links_inserted = 0

    if all_domains:
        domain_values = [{"domain": domain} for domain in sorted(all_domains)]
        insert_domain_stmt = (
            insert(Domain)
            .values(domain_values)
            .on_conflict_do_nothing(index_elements=["domain"])
        )
        domain_result = session.execute(insert_domain_stmt)
        domains_inserted = domain_result.rowcount or 0

        domain_rows = session.execute(
            select(Domain.id, Domain.domain).where(Domain.domain.in_(sorted(all_domains)))
        ).all()
        domain_map = {domain: domain_id for domain_id, domain in domain_rows}

        link_values = []
        for business_id, source_map in business_domain_sources.items():
            for domain, source in source_map.items():
                domain_id = domain_map.get(domain)
                if not domain_id:
                    continue
                link_values.append(
                    {
                        "business_id": business_id,
                        "domain_id": domain_id,
                        "source": source,
                    }
                )

        if link_values:
            insert_link_stmt = (
                insert(BusinessDomainLink)
                .values(link_values)
                .on_conflict_do_nothing(index_elements=["business_id", "domain_id"])
            )
            link_result = session.execute(insert_link_stmt)
            links_inserted = link_result.rowcount or 0

    return domains_inserted, links_inserted


def run_batch(limit: Optional[int] = None, scope: Optional[str] = None, reset_cursor: bool = False) -> dict:
    config = load_config()
    max_items = config.batch_size if limit is None else max(limit, 0)

    with session_scope() as session:
        run = start_job(session, JOB_NAME, scope=scope)

        try:
            if reset_cursor:
                set_checkpoint(session, JOB_NAME, scope, CURSOR_KEY, "", job_run_id=run.id)

            cursor_value = get_checkpoint(session, JOB_NAME, scope, CURSOR_KEY)
            cursor_ts, cursor_id = _parse_cursor(cursor_value)

            processed_total = 0
            domains_total = 0
            links_total = 0

            while processed_total < max_items:
                remaining = max_items - processed_total
                chunk_size = min(config.batch_size, remaining)

                stmt = select(Business).order_by(Business.created_at, Business.id).limit(chunk_size)
                if cursor_ts and cursor_id:
                    stmt = stmt.where(
                        or_(
                            Business.created_at > cursor_ts,
                            and_(Business.created_at == cursor_ts, Business.id > cursor_id),
                        )
                    )

                businesses = session.execute(stmt).scalars().all()
                if not businesses:
                    break

                batch_domains, batch_links = _sync_batch(session, businesses)
                domains_total += batch_domains
                links_total += batch_links
                processed_total += len(businesses)

                last = businesses[-1]
                cursor_ts = last.created_at
                cursor_id = last.id
                set_checkpoint(
                    session,
                    JOB_NAME,
                    scope,
                    CURSOR_KEY,
                    _make_cursor(last.created_at, last.id),
                    details={"last_business_name": last.name or ""},
                    job_run_id=run.id,
                )

            details = {
                "domains_inserted": domains_total,
                "links_inserted": links_total,
            }
            if cursor_id:
                details["cursor_business_id"] = str(cursor_id)

            complete_job(session, run, processed_count=processed_total, details=details)
            return {
                "processed": processed_total,
                "domains_inserted": domains_total,
                "links_inserted": links_total,
            }
        except Exception as exc:
            fail_job(session, run, error=str(exc))
            raise
