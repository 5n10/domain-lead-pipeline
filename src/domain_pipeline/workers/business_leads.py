from __future__ import annotations

import csv
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from sqlalchemy import and_, exists, func, not_, or_, select
from sqlalchemy.orm import Session

from ..config import load_config
from ..db import session_scope
from ..domain_utils import (
    PUBLIC_EMAIL_DOMAINS,
    PUBLIC_EMAIL_DOMAIN_PREFIXES,
    extract_domain_from_email,
    is_public_email_domain,
)
from ..jobs import complete_job, fail_job, start_job
from ..models import (
    Business,
    BusinessContact,
    BusinessDomainLink,
    BusinessOutreachExport,
    City,
    Domain,
)

# Category priority sets
HIGH_PRIORITY_CATEGORIES = {"trades", "contractors"}
MEDIUM_PRIORITY_CATEGORIES = {"professional_services", "retail", "health", "food", "auto"}

# Domain status sets
VERIFIED_UNHOSTED_DOMAIN_STATUSES = {"verified_unhosted", "mx_missing", "checked", "no_mx", "enriched", "no_contacts"}
UNREGISTERED_CANDIDATE_STATUSES = {"unregistered_candidate"}
HOSTED_DOMAIN_STATUSES = {"hosted"}
PARKED_DOMAIN_STATUSES = {"parked"}
UNKNOWN_DOMAIN_STATUSES = {"new", "rdap_error", "dns_error", "skipped"}

# Scoring weights (extracted from _score_business logic)
SCORE_NO_WEBSITE = 25
SCORE_BUSINESS_EMAIL = 20
SCORE_ANY_EMAIL = 5
SCORE_PHONE = 15
SCORE_VERIFIED_UNHOSTED_DOMAIN = 35
SCORE_UNREGISTERED_DOMAIN = 20
SCORE_ANY_DOMAIN = 10
SCORE_HIGH_PRIORITY_CATEGORY = 20
SCORE_MEDIUM_PRIORITY_CATEGORY = 10
SCORE_ANY_CATEGORY = 5


def _base_business_query():
    return select(Business).where(or_(Business.website_url.is_(None), Business.website_url == ""))


def _non_public_domain_expr():
    lowered = func.lower(Domain.domain)
    conditions = [not_(lowered.in_(tuple(PUBLIC_EMAIL_DOMAINS)))]
    conditions.extend(not_(lowered.like(f"{prefix}%")) for prefix in PUBLIC_EMAIL_DOMAIN_PREFIXES)
    return and_(*conditions)


def _business_has_contact_expr():
    return exists(
        select(BusinessContact.id)
        .where(BusinessContact.business_id == Business.id)
        .where(BusinessContact.contact_type.in_(("email", "phone")))
    )


def _business_has_domain_status_expr(statuses: set[str]):
    if not statuses:
        return None
    return exists(
        select(BusinessDomainLink.id)
        .join(Domain, Domain.id == BusinessDomainLink.domain_id)
        .where(BusinessDomainLink.business_id == Business.id)
        .where(_non_public_domain_expr())
        .where(Domain.status.in_(tuple(statuses)))
    )


def business_eligibility_filters(
    require_contact: bool,
    require_unhosted_domain: bool,
    require_domain_qualification: bool,
    exclude_hosted_email_domain: bool = True,
):
    filters = []
    if require_contact:
        filters.append(_business_has_contact_expr())

    # Exclude businesses whose email domain is hosted or parked — they DO have
    # a website, even though OSM didn't tag it. On by default for lead quality.
    # BUT: only exclude if the business has NO qualified (unhosted/unregistered)
    # domains — a business with both hosted and unhosted domains is still a lead.
    if exclude_hosted_email_domain:
        hosted_parked_expr = _business_has_domain_status_expr(
            HOSTED_DOMAIN_STATUSES | PARKED_DOMAIN_STATUSES
        )
        qualified_expr = _business_has_domain_status_expr(
            VERIFIED_UNHOSTED_DOMAIN_STATUSES | UNREGISTERED_CANDIDATE_STATUSES
        )
        if hosted_parked_expr is not None:
            if qualified_expr is not None:
                # Exclude only if has hosted/parked AND does NOT have any qualified domains
                filters.append(not_(and_(hosted_parked_expr, not_(qualified_expr))))
            else:
                filters.append(not_(hosted_parked_expr))

    # Only exclude hosted/parked domains when domain qualification is required
    if require_domain_qualification or require_unhosted_domain:
        hosted_parked_expr = _business_has_domain_status_expr(HOSTED_DOMAIN_STATUSES | PARKED_DOMAIN_STATUSES)
        if hosted_parked_expr is not None:
            filters.append(not_(hosted_parked_expr))

    qualification_expr = _business_has_domain_status_expr(
        VERIFIED_UNHOSTED_DOMAIN_STATUSES | UNREGISTERED_CANDIDATE_STATUSES
    )
    if (require_domain_qualification or require_unhosted_domain) and qualification_expr is not None:
        filters.append(qualification_expr)

    return filters


def load_business_features(session: Session, business_ids: list) -> dict:
    features = {
        business_id: {
            "emails": set(),
            "business_emails": set(),
            "free_emails": set(),
            "phones": set(),
            "domains": set(),
            "verified_unhosted_domains": set(),
            "unregistered_domains": set(),
            "hosted_domains": set(),
            "parked_domains": set(),
            "unknown_domains": set(),
            "domain_status_counts": {},
        }
        for business_id in business_ids
    }

    if not business_ids:
        return features

    contact_rows = session.execute(
        select(BusinessContact.business_id, BusinessContact.contact_type, BusinessContact.value).where(
            BusinessContact.business_id.in_(business_ids)
        )
    ).all()
    for business_id, contact_type, value in contact_rows:
        if not value:
            continue
        if contact_type == "email":
            email = value.strip().lower()
            features[business_id]["emails"].add(email)
            email_domain = extract_domain_from_email(email)
            if email_domain and not is_public_email_domain(email_domain):
                features[business_id]["business_emails"].add(email)
            else:
                features[business_id]["free_emails"].add(email)
        elif contact_type == "phone":
            features[business_id]["phones"].add(value.strip())

    domain_rows = session.execute(
        select(BusinessDomainLink.business_id, Domain.domain, Domain.status)
        .join(Domain, Domain.id == BusinessDomainLink.domain_id)
        .where(BusinessDomainLink.business_id.in_(business_ids))
    ).all()
    for business_id, domain, status in domain_rows:
        if not domain:
            continue
        normalized = domain.strip().lower()
        if is_public_email_domain(normalized):
            continue
        features[business_id]["domains"].add(normalized)
        status_key = (status or "unknown").strip()
        features[business_id]["domain_status_counts"][status_key] = (
            features[business_id]["domain_status_counts"].get(status_key, 0) + 1
        )

        if status in VERIFIED_UNHOSTED_DOMAIN_STATUSES:
            features[business_id]["verified_unhosted_domains"].add(normalized)
        elif status in UNREGISTERED_CANDIDATE_STATUSES:
            features[business_id]["unregistered_domains"].add(normalized)
        elif status in HOSTED_DOMAIN_STATUSES:
            features[business_id]["hosted_domains"].add(normalized)
        elif status in PARKED_DOMAIN_STATUSES:
            features[business_id]["parked_domains"].add(normalized)
        else:
            if status in UNKNOWN_DOMAIN_STATUSES or not status:
                features[business_id]["unknown_domains"].add(normalized)

    return features


DOMAIN_LIKE_TLDS = {
    ".com", ".ca", ".ae", ".qa", ".io", ".co", ".net", ".org",
    ".biz", ".info", ".us", ".uk", ".app", ".dev", ".shop", ".store",
}


def _name_looks_like_domain(name: str) -> bool:
    """Check if business name looks like a domain name (e.g. 'iRepair.ca')."""
    if not name:
        return False
    clean = name.strip().lower().replace(" ", "")
    return any(clean.endswith(tld) or tld + "/" in clean for tld in DOMAIN_LIKE_TLDS)


def _is_branded_chain(business: Business) -> bool:
    """Detect branded chains/franchises from OSM tags.

    Businesses with brand/brand:wikidata/operator:wikidata tags are
    known chains that definitely have corporate websites.
    """
    raw = business.raw or {}
    # brand:wikidata is the strongest signal — a well-known entity
    if raw.get("brand:wikidata") or raw.get("operator:wikidata"):
        return True
    # brand tag alone is also a strong signal (less strict)
    if raw.get("brand"):
        return True
    return False


def _score_business(business: Business, feature: dict) -> tuple[float, dict]:
    score = 0.0
    has_email = bool(feature["emails"])
    has_business_email = bool(feature["business_emails"])
    has_phone = bool(feature["phones"])
    has_domain = bool(feature["domains"])
    has_hosted_domain = bool(feature["hosted_domains"])
    has_parked_domain = bool(feature["parked_domains"])
    has_verified_unhosted_domain = bool(feature["verified_unhosted_domains"])
    has_unregistered_candidate_domain = bool(feature["unregistered_domains"])
    has_unknown_domain = bool(feature["unknown_domains"])
    has_any_contact = has_email or has_phone

    # --- Disqualification checks (before scoring) ---

    # 1. Branded chains (Tim Hortons, Starbucks, etc.) definitely have websites
    is_chain = _is_branded_chain(business)
    if is_chain:
        reasons = _build_reasons(business, feature, disqualify_reason="branded_chain")
        return 0.0, reasons

    # 2. Business name looks like a domain (iRepair.ca, SuperMart.ae)
    name_is_domain = _name_looks_like_domain(business.name)

    # 3. Hosted/parked domain from email — business has a real website
    has_qualified = has_verified_unhosted_domain or has_unregistered_candidate_domain
    if not business.website_url and (has_hosted_domain or has_parked_domain) and not has_qualified:
        reasons = _build_reasons(business, feature, disqualify_reason="hosted_email_domain")
        return 0.0, reasons

    # --- Positive scoring ---

    # Base: no website = lead signal (but weak without contacts)
    if not business.website_url:
        score += SCORE_NO_WEBSITE

    # Contact signals — these are CRITICAL for lead quality
    if has_business_email:
        score += SCORE_BUSINESS_EMAIL
    elif has_email:
        score += SCORE_ANY_EMAIL
    if has_phone:
        score += SCORE_PHONE

    # Domain signals
    if has_verified_unhosted_domain:
        score += SCORE_VERIFIED_UNHOSTED_DOMAIN
    elif has_unregistered_candidate_domain:
        score += SCORE_UNREGISTERED_DOMAIN
    elif has_hosted_domain or has_parked_domain:
        score += 0  # Already has web presence
    elif has_domain:
        score += SCORE_ANY_DOMAIN

    # Category signals
    category = (business.category or "").strip()
    if category in HIGH_PRIORITY_CATEGORIES:
        score += SCORE_HIGH_PRIORITY_CATEGORY
    elif category in MEDIUM_PRIORITY_CATEGORIES:
        score += SCORE_MEDIUM_PRIORITY_CATEGORY
    elif category:
        score += SCORE_ANY_CATEGORY

    # --- Quality caps ---

    # Unknown domains (RDAP hasn't checked yet) from business emails are very
    # likely hosted. Cap aggressively until RDAP confirms status.
    if not business.website_url and has_unknown_domain and not has_qualified:
        score = min(score, 10.0)

    # Business name looks like a domain — likely has website, cap score
    if name_is_domain:
        score = min(score, 15.0)

    # Businesses with NO contacts are extremely low quality — you can't reach them.
    # They only get base "no website" + category points but are nearly useless.
    if not has_any_contact:
        score = min(score, 5.0)

    # General caps for domain situations
    if has_hosted_domain and not has_qualified:
        score = min(score, 60.0)
    if has_unknown_domain and not has_qualified:
        score = min(score, 35.0)

    disqualify_reason = None
    if not has_any_contact:
        disqualify_reason = "no_contacts"
    elif name_is_domain:
        disqualify_reason = "name_is_domain"

    reasons = _build_reasons(business, feature, disqualify_reason=disqualify_reason)
    return min(score, 100.0), reasons


def _build_reasons(business: Business, feature: dict, disqualify_reason: str = None) -> dict:
    """Build the score_reasons JSON for a business."""
    category = (business.category or "").strip()
    has_hosted = bool(feature["hosted_domains"])
    has_parked = bool(feature["parked_domains"])
    has_qualified = bool(feature["verified_unhosted_domains"]) or bool(feature["unregistered_domains"])
    is_chain = _is_branded_chain(business)
    raw = business.raw or {}

    return {
        "category": category or None,
        "has_email": bool(feature["emails"]),
        "has_business_email": bool(feature["business_emails"]),
        "has_phone": bool(feature["phones"]),
        "has_hosted_domain": has_hosted,
        "has_parked_domain": has_parked,
        "disqualify_reason": disqualify_reason,
        "disqualified_hosted_email_domain": (
            not business.website_url and (has_hosted or has_parked) and not has_qualified
        ),
        "is_branded_chain": is_chain,
        "brand": raw.get("brand"),
        "name_looks_like_domain": _name_looks_like_domain(business.name),
        "domain_count": len(feature["domains"]),
        "verified_unhosted_domain_count": len(feature["verified_unhosted_domains"]),
        "unregistered_domain_count": len(feature["unregistered_domains"]),
        "unknown_domain_count": len(feature["unknown_domains"]),
        "domains": sorted(feature["domains"]),
        "verified_unhosted_domains": sorted(feature["verified_unhosted_domains"]),
        "unregistered_domains": sorted(feature["unregistered_domains"]),
        "unknown_domains": sorted(feature["unknown_domains"]),
        "hosted_domains": sorted(feature["hosted_domains"]),
        "parked_domains": sorted(feature["parked_domains"]),
        "domain_status_counts": feature["domain_status_counts"],
    }


def score_businesses(limit: Optional[int] = None, scope: Optional[str] = None, force_rescore: bool = False) -> int:
    config = load_config()
    # When limit is None or 0, score ALL businesses (no limit)
    batch_size = None if (limit is None or limit <= 0) else limit

    with session_scope() as session:
        run = start_job(
            session,
            "score_business_leads",
            scope=scope,
            details={"force_rescore": force_rescore},
        )
        try:

            stale_contact_exists = exists(
                select(BusinessContact.id)
                .where(BusinessContact.business_id == Business.id)
                .where(Business.scored_at.isnot(None))
                .where(BusinessContact.created_at > Business.scored_at)
            )
            stale_domain_link_exists = exists(
                select(BusinessDomainLink.id)
                .where(BusinessDomainLink.business_id == Business.id)
                .where(Business.scored_at.isnot(None))
                .where(BusinessDomainLink.created_at > Business.scored_at)
            )
            stale_domain_update_exists = exists(
                select(Domain.id)
                .join(BusinessDomainLink, BusinessDomainLink.domain_id == Domain.id)
                .where(BusinessDomainLink.business_id == Business.id)
                .where(Business.scored_at.isnot(None))
                .where(Domain.updated_at > Business.scored_at)
            )

            stmt = _base_business_query().order_by(Business.created_at)
            if batch_size is not None:
                stmt = stmt.limit(batch_size)
            if not force_rescore:
                stmt = stmt.where(
                    or_(
                        Business.scored_at.is_(None),
                        stale_contact_exists,
                        stale_domain_link_exists,
                        stale_domain_update_exists,
                    )
                )

            businesses = session.execute(stmt).scalars().all()
            if not businesses:
                complete_job(session, run, processed_count=0, details={"force_rescore": force_rescore})
                return 0

            business_ids = [business.id for business in businesses]
            feature_map = load_business_features(session, business_ids)

            processed = 0
            for business in businesses:
                feature = feature_map[business.id]
                score, reasons = _score_business(business, feature)
                business.lead_score = score
                business.score_reasons = reasons
                business.scored_at = datetime.now(timezone.utc)
                processed += 1

            complete_job(session, run, processed_count=processed, details={"force_rescore": force_rescore})
            return processed
        except Exception as exc:
            fail_job(session, run, error=str(exc), details={"force_rescore": force_rescore})
            raise


def export_business_leads(
    platform: str,
    min_score: Optional[float] = None,
    limit: Optional[int] = None,
    require_contact: bool = True,
    require_unhosted_domain: bool = False,
    require_domain_qualification: bool = True,
    exclude_hosted_email_domain: bool = True,
    max_written: Optional[int] = None,
    exclude_previously_exported: bool = False,
) -> Optional[Path]:
    config = load_config()
    export_dir = Path(config.export_dir)
    export_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = export_dir / f"business_leads_{platform}_{timestamp}.csv"
    row_limit = None if (limit is None or limit <= 0) else limit
    final_limit = row_limit
    if max_written is not None:
        final_limit = max_written if final_limit is None else min(final_limit, max_written)

    with session_scope() as session:
        run = start_job(
            session,
            "export_business_leads",
            scope=platform,
            details={
                "min_score": min_score,
                "require_contact": require_contact,
                "require_unhosted_domain": require_unhosted_domain,
                "require_domain_qualification": require_domain_qualification,
                "limit": row_limit,
                "final_limit": final_limit,
                "max_written": max_written,
                "exclude_previously_exported": exclude_previously_exported,
            },
        )
        try:
            if row_limit == 0:
                complete_job(session, run, processed_count=0)
                return None
            if max_written is not None and max_written <= 0:
                complete_job(session, run, processed_count=0)
                return None

            exported_same_platform_exists = exists(
                select(BusinessOutreachExport.id)
                .where(BusinessOutreachExport.business_id == Business.id)
                .where(BusinessOutreachExport.platform == platform)
            )
            exported_any_platform_exists = exists(
                select(BusinessOutreachExport.id).where(BusinessOutreachExport.business_id == Business.id)
            )

            stmt = (
                select(Business, City)
                .outerjoin(City, Business.city_id == City.id)
                .where(or_(Business.website_url.is_(None), Business.website_url == ""))
                .where(Business.lead_score.isnot(None))
                .where(not_(exported_same_platform_exists))
                .order_by(Business.lead_score.desc(), Business.created_at)
            )
            if min_score is not None:
                stmt = stmt.where(Business.lead_score >= min_score)
            if exclude_previously_exported:
                stmt = stmt.where(not_(exported_any_platform_exists))
            for expression in business_eligibility_filters(
                require_contact=require_contact,
                require_unhosted_domain=require_unhosted_domain,
                require_domain_qualification=require_domain_qualification,
                exclude_hosted_email_domain=exclude_hosted_email_domain,
            ):
                stmt = stmt.where(expression)
            if final_limit is not None:
                stmt = stmt.limit(final_limit)

            rows = session.execute(stmt).all()
            if not rows:
                complete_job(session, run, processed_count=0)
                return None

            business_ids = [business.id for business, _ in rows]
            feature_map = load_business_features(session, business_ids)

            written_rows = 0
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(
                    [
                        "business_name",
                        "category",
                        "address",
                        "city",
                        "country",
                        "emails",
                        "business_emails",
                        "free_emails",
                        "phones",
                        "domains",
                        "verified_unhosted_domains",
                        "unregistered_domains",
                        "unknown_domains",
                        "hosted_domains",
                        "parked_domains",
                        "lead_score",
                        "source",
                        "source_id",
                    ]
                )

                for business, city in rows:
                    feature = feature_map[business.id]

                    writer.writerow(
                        [
                            business.name or "",
                            business.category or "",
                            business.address or "",
                            city.name if city else "",
                            city.country if city else "",
                            ";".join(sorted(feature["emails"])),
                            ";".join(sorted(feature["business_emails"])),
                            ";".join(sorted(feature["free_emails"])),
                            ";".join(sorted(feature["phones"])),
                            ";".join(sorted(feature["domains"])),
                            ";".join(sorted(feature["verified_unhosted_domains"])),
                            ";".join(sorted(feature["unregistered_domains"])),
                            ";".join(sorted(feature["unknown_domains"])),
                            ";".join(sorted(feature["hosted_domains"])),
                            ";".join(sorted(feature["parked_domains"])),
                            float(business.lead_score) if business.lead_score is not None else "",
                            business.source,
                            business.source_id,
                        ]
                    )
                    session.add(
                        BusinessOutreachExport(
                            business_id=business.id,
                            platform=platform,
                            status="queued",
                        )
                    )
                    written_rows += 1

            session.flush()

            if written_rows == 0:
                path.unlink(missing_ok=True)
                complete_job(session, run, processed_count=0)
                return None

            complete_job(
                session,
                run,
                processed_count=written_rows,
                details={
                    "min_score": min_score,
                    "require_contact": require_contact,
                    "require_unhosted_domain": require_unhosted_domain,
                    "require_domain_qualification": require_domain_qualification,
                    "limit": row_limit,
                    "final_limit": final_limit,
                    "max_written": max_written,
                    "exclude_previously_exported": exclude_previously_exported,
                },
            )
            return path
        except Exception as exc:
            fail_job(
                session,
                run,
                error=str(exc),
                details={
                    "min_score": min_score,
                    "require_contact": require_contact,
                    "require_unhosted_domain": require_unhosted_domain,
                    "require_domain_qualification": require_domain_qualification,
                    "limit": row_limit,
                    "final_limit": final_limit,
                    "max_written": max_written,
                    "exclude_previously_exported": exclude_previously_exported,
                },
            )
            raise


def daily_platform_name(for_date: Optional[date] = None, prefix: str = "daily") -> str:
    target_date = for_date or datetime.now(timezone.utc).date()
    return f"{prefix}_{target_date.strftime('%Y%m%d')}"


def daily_target_summary(
    platform_prefix: str = "daily",
    for_date: Optional[date] = None,
    target_count: Optional[int] = None,
) -> dict:
    platform = daily_platform_name(for_date=for_date, prefix=platform_prefix)
    with session_scope() as session:
        exported_count = int(
            session.execute(
                select(func.count(BusinessOutreachExport.id)).where(BusinessOutreachExport.platform == platform)
            ).scalar()
            or 0
        )
    effective_target = target_count if target_count is not None else exported_count
    return {
        "platform": platform,
        "date": (for_date or datetime.now(timezone.utc).date()).isoformat(),
        "target_count": int(effective_target),
        "generated_count": exported_count,
        "remaining_count": max(int(effective_target) - exported_count, 0),
        "completed": exported_count >= int(effective_target),
    }


def ensure_daily_target_generated(
    target_count: int,
    min_score: Optional[float] = None,
    platform_prefix: str = "daily",
    for_date: Optional[date] = None,
    require_contact: bool = True,
    require_unhosted_domain: bool = False,
    require_domain_qualification: bool = True,
    exclude_hosted_email_domain: bool = True,
    allow_recycle: bool = True,
) -> dict:
    summary = daily_target_summary(
        platform_prefix=platform_prefix,
        for_date=for_date,
        target_count=target_count,
    )
    if summary["remaining_count"] <= 0:
        return {**summary, "created_now": 0, "export_path": None}

    first_path = export_business_leads(
        platform=summary["platform"],
        min_score=min_score,
        require_contact=require_contact,
        require_unhosted_domain=require_unhosted_domain,
        require_domain_qualification=require_domain_qualification,
        exclude_hosted_email_domain=exclude_hosted_email_domain,
        max_written=summary["remaining_count"],
        exclude_previously_exported=True,
    )
    updated = daily_target_summary(
        platform_prefix=platform_prefix,
        for_date=for_date,
        target_count=target_count,
    )
    recycled_path = None
    if allow_recycle and updated["remaining_count"] > 0:
        recycled_path = export_business_leads(
            platform=summary["platform"],
            min_score=min_score,
            require_contact=require_contact,
            require_unhosted_domain=require_unhosted_domain,
            require_domain_qualification=require_domain_qualification,
            exclude_hosted_email_domain=exclude_hosted_email_domain,
            max_written=updated["remaining_count"],
            exclude_previously_exported=False,
        )
        updated = daily_target_summary(
            platform_prefix=platform_prefix,
            for_date=for_date,
            target_count=target_count,
        )

    return {
        **updated,
        "created_now": max(updated["generated_count"] - summary["generated_count"], 0),
        "export_path": str(recycled_path or first_path) if (recycled_path or first_path) else None,
        "allow_recycle": allow_recycle,
    }
