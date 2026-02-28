from __future__ import annotations

import csv
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import requests
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
VERIFIED_UNHOSTED_DOMAIN_STATUSES = {"verified_unhosted", "checked", "no_mx", "enriched", "no_contacts"}
UNREGISTERED_CANDIDATE_STATUSES = {"unregistered_candidate"}
HOSTED_DOMAIN_STATUSES = {"hosted"}
PARKED_DOMAIN_STATUSES = {"parked"}
# Domains with DNS records but no web server — business likely has a website
# elsewhere or is using the domain for email only. NOT a lead opportunity.
REGISTERED_DOMAIN_STATUSES = {"registered_no_web", "registered_dns_only", "mx_missing"}
UNKNOWN_DOMAIN_STATUSES = {"new", "rdap_error", "dns_error", "skipped"}

# Scoring weights
# NOTE: Domain-based boosts (SCORE_VERIFIED_UNHOSTED_DOMAIN, SCORE_UNREGISTERED_DOMAIN)
# were REMOVED because email domain ≠ business website domain. Having info@company.ae
# tells us nothing about whether the business has a website at company.ae or elsewhere.
# The only reliable signals are: contacts (email/phone), category, and OSM website tag.
SCORE_NO_WEBSITE = 25
SCORE_BUSINESS_EMAIL = 20
SCORE_ANY_EMAIL = 5
SCORE_PHONE = 15
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

    # Exclude businesses whose email domain is hosted, parked, or registered
    # (has DNS records) — they likely have a website somewhere, even though
    # OSM didn't tag it. ANY domain with DNS records indicates active use.
    if exclude_hosted_email_domain:
        active_domain_expr = _business_has_domain_status_expr(
            HOSTED_DOMAIN_STATUSES | PARKED_DOMAIN_STATUSES | REGISTERED_DOMAIN_STATUSES
        )
        if active_domain_expr is not None:
            filters.append(not_(active_domain_expr))

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
            "registered_domains": set(),
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
        elif status in REGISTERED_DOMAIN_STATUSES:
            features[business_id]["registered_domains"].add(normalized)
        else:
            if status in UNKNOWN_DOMAIN_STATUSES or not status:
                features[business_id]["unknown_domains"].add(normalized)

    return features


DOMAIN_LIKE_TLDS = {
    ".com", ".ca", ".ae", ".qa", ".io", ".co", ".net", ".org",
    ".biz", ".info", ".us", ".uk", ".app", ".dev", ".shop", ".store",
}

# Verification tracking — used across workers to determine verification coverage
VERIFICATION_KEYS = [
    "llm_verified",
    "domain_guess_verified",
    "ddg_verified",
    "google_places_verified",
    "foursquare_verified",
    "google_search_verified",
    "searxng_verified",
]

# Verification results that represent CONCLUSIVE checks (found actual search results)
_CONCLUSIVE_RESULTS = {"no_website", "no_match", "has_website"}
# Verification results that are INCONCLUSIVE (no search results returned)
_INCONCLUSIVE_RESULTS = {"no_results", "no_candidates", "blocked", "poor_match"}

# Mapping from verification key → result key in raw JSONB
_RESULT_KEY_MAP = {
    "llm_verified": "llm_verify_result",
    "domain_guess_verified": "domain_guess_result",
    "ddg_verified": "ddg_verify_result",
    "google_places_verified": "google_places_verify_result",
    "foursquare_verified": "foursquare_verify_result",
    "google_search_verified": "google_search_result",
    "searxng_verified": "searxng_result",
}

# Weighted confidence scores per source and result type.
# Higher weight = more trustworthy evidence. Used instead of binary conclusive/inconclusive.
CONFIDENCE_WEIGHTS: dict[str, dict[str, float]] = {
    "domain_guess_verified": {
        "no_match": 0.7,       # Tested 10-20 HTTP candidates, none matched
        "has_website": 1.0,    # Found a working domain
        "no_candidates": 0.1,  # Couldn't generate any domain candidates
    },
    "searxng_verified": {
        "no_website": 0.9,     # Multi-engine search found only directories
        "has_website": 1.0,    # Found official website in search results
        "no_results": 0.1,     # Search returned nothing (inconclusive)
    },
    "llm_verified": {
        "no_website": 0.8,     # LLM analyzed search results, concluded no website
        "has_website": 0.9,    # LLM identified official website from results
        "not_sure": 0.2,       # LLM couldn't determine from evidence
        "no_results": 0.1,     # No search results to analyze
    },
    "ddg_verified": {
        "no_website": 0.6,     # DDG alone is less reliable
        "has_website": 0.8,
        "no_results": 0.05,    # DDG often returns nothing (broken scraper)
    },
    "google_search_verified": {
        "no_website": 0.6,
        "has_website": 0.8,
        "no_results": 0.05,    # Google blocks scraping frequently
        "blocked": 0.0,
    },
    "google_places_verified": {
        "no_website": 0.9,     # API-quality data, very reliable
        "has_website": 1.0,
    },
    "foursquare_verified": {
        "no_website": 0.7,
        "has_website": 0.9,
    },
}


def compute_verification_count(raw: dict | None) -> int:
    """Count how many verification sources have checked this business."""
    if not raw:
        return 0
    return sum(1 for key in VERIFICATION_KEYS if raw.get(key))


def get_verification_sources(raw: dict | None) -> list[str]:
    """Return list of verification source names that have checked this business."""
    if not raw:
        return []
    return [key.replace("_verified", "") for key in VERIFICATION_KEYS if raw.get(key)]


def compute_verification_weight(raw: dict | None) -> float:
    """Compute the total weighted confidence score from all verification sources.

    Each source/result combination has a weight reflecting its reliability.
    Returns total weight (0.0 if unverified).
    """
    if not raw:
        return 0.0

    total = 0.0
    for vkey in VERIFICATION_KEYS:
        if not raw.get(vkey):
            continue
        result_key = _RESULT_KEY_MAP.get(vkey)
        if not result_key:
            # Legacy data without result key — treat as medium-weight evidence
            total += 0.5
            continue
        result_value = raw.get(result_key, "")
        weights = CONFIDENCE_WEIGHTS.get(vkey, {})
        total += weights.get(result_value, 0.1)  # default 0.1 for unknown results

    return total


def compute_verification_confidence(raw: dict | None) -> str:
    """Compute verification confidence level using weighted scoring.

    Returns one of: "high", "medium", "low", "unverified".

    Weights are assigned per source and result type. E.g.:
    - Domain Guess no_match (0.7) + SearXNG no_website (0.9) = 1.6 → "high"
    - Domain Guess no_match (0.7) alone = 0.7 → "medium"
    - Domain Guess no_match (0.7) + LLM not_sure (0.2) = 0.9 → "medium"
    - DDG no_results (0.05) only = 0.05 → "low"

    Thresholds: high >= 1.5, medium >= 0.7, low > 0, unverified = 0
    """
    if not raw:
        return "unverified"

    # Check if any verification source has run
    has_any = any(raw.get(vkey) for vkey in VERIFICATION_KEYS)
    if not has_any:
        return "unverified"

    weight = compute_verification_weight(raw)
    if weight >= 1.5:
        return "high"
    if weight >= 0.7:
        return "medium"
    return "low"


def _name_looks_like_domain(name: str) -> bool:
    """Check if business name looks like a domain name (e.g. 'iRepair.ca')."""
    if not name:
        return False
    clean = name.strip().lower().replace(" ", "")
    return any(clean.endswith(tld) or tld + "/" in clean for tld in DOMAIN_LIKE_TLDS)


logger = logging.getLogger(__name__)

WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
_wikidata_chain_cache: Optional[set[str]] = None


def _load_wikidata_chains() -> set[str]:
    """Query Wikidata SPARQL for known business chains/franchises.

    Returns a set of lowercase English business names. Cached after first call.
    Falls back to empty set on failure (Wikidata down, network issues, etc.).
    """
    global _wikidata_chain_cache
    if _wikidata_chain_cache is not None:
        return _wikidata_chain_cache

    query = """
    SELECT DISTINCT ?label WHERE {
      { ?item wdt:P31 wd:Q507619 . }
      UNION
      { ?item wdt:P31 wd:Q126793 . }
      ?item rdfs:label ?label .
      FILTER(LANG(?label) = "en")
    }
    """
    try:
        resp = requests.get(
            WIKIDATA_SPARQL_URL,
            params={"query": query, "format": "json"},
            headers={"User-Agent": "domain-lead-pipeline/0.1"},
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            names = {
                binding["label"]["value"].strip().lower()
                for binding in data.get("results", {}).get("bindings", [])
                if binding.get("label", {}).get("value")
            }
            logger.info("Loaded %d chain names from Wikidata", len(names))
            _wikidata_chain_cache = names
            return names
    except Exception as exc:
        logger.warning("Wikidata chain query failed: %s", exc)

    _wikidata_chain_cache = set()
    return _wikidata_chain_cache


def _is_wikidata_chain(business_name: str) -> bool:
    """Check if business name matches a known Wikidata chain."""
    if not business_name:
        return False
    chains = _load_wikidata_chains()
    if not chains:
        return False
    normalized = business_name.strip().lower()
    if normalized in chains:
        return True
    # Substring match for "Tim Hortons #1234" → "tim hortons"
    for chain in chains:
        if len(chain) >= 4 and chain in normalized:
            return True
    return False


def _is_branded_chain(business: Business) -> bool:
    """Detect branded chains/franchises from OSM tags and Wikidata.

    Businesses with brand/brand:wikidata/operator:wikidata tags are
    known chains that definitely have corporate websites.
    Also checks against Wikidata's database of chain stores/franchises.
    """
    raw = business.raw or {}
    # brand:wikidata is the strongest signal — a well-known entity
    if raw.get("brand:wikidata") or raw.get("operator:wikidata"):
        return True
    # brand tag alone is also a strong signal (less strict)
    if raw.get("brand"):
        return True
    # Check against Wikidata known chains database
    if _is_wikidata_chain(business.name):
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
    has_registered_domain = bool(feature["registered_domains"])
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

    # 3. Any domain with DNS records = business likely has a website somewhere.
    # This covers: hosted, parked, registered_no_web, registered_dns_only, mx_missing.
    # The email domain tells us the business owns/uses that domain — if it has
    # ANY DNS records, the business is technically active online.
    has_any_active_domain = has_hosted_domain or has_parked_domain or has_registered_domain
    if not business.website_url and has_any_active_domain:
        reasons = _build_reasons(business, feature, disqualify_reason="active_domain")
        return 0.0, reasons

    # --- Positive scoring ---
    # Only reliable signals: contacts, category, and OSM website tag absence.
    # Domain status is NOT a reliable signal for "business needs a website".

    # Base: no website in OSM = potential lead signal
    if not business.website_url:
        score += SCORE_NO_WEBSITE

    # Contact signals — these are CRITICAL for lead quality
    if has_business_email:
        score += SCORE_BUSINESS_EMAIL
    elif has_email:
        score += SCORE_ANY_EMAIL
    if has_phone:
        score += SCORE_PHONE

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
    # likely hosted/registered. Cap aggressively until RDAP confirms status.
    if not business.website_url and has_unknown_domain:
        score = min(score, 10.0)

    # Business name looks like a domain — likely has website, cap score
    if name_is_domain:
        score = min(score, 15.0)

    # Businesses with NO contacts are extremely low quality — you can't reach them.
    if not has_any_contact:
        score = min(score, 5.0)

    # --- Verification confidence caps ---
    # Unverified leads cannot reach export territory. Only conclusive verification
    # (actual search results confirming no website) earns full score potential.
    confidence = compute_verification_confidence(business.raw)
    if confidence == "unverified":
        score = min(score, 35.0)
    elif confidence == "low":
        score = min(score, 50.0)
    # "medium" and "high" — no cap

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
    has_registered = bool(feature["registered_domains"])
    has_any_active_domain = has_hosted or has_parked or has_registered
    is_chain = _is_branded_chain(business)
    raw = business.raw or {}

    return {
        "category": category or None,
        "has_email": bool(feature["emails"]),
        "has_business_email": bool(feature["business_emails"]),
        "has_phone": bool(feature["phones"]),
        "has_hosted_domain": has_hosted,
        "has_parked_domain": has_parked,
        "has_registered_domain": has_registered,
        "has_any_active_domain": has_any_active_domain,
        "disqualify_reason": disqualify_reason,
        "is_branded_chain": is_chain,
        "brand": raw.get("brand"),
        "name_looks_like_domain": _name_looks_like_domain(business.name),
        "domain_count": len(feature["domains"]),
        "verified_unhosted_domain_count": len(feature["verified_unhosted_domains"]),
        "unregistered_domain_count": len(feature["unregistered_domains"]),
        "registered_domain_count": len(feature["registered_domains"]),
        "unknown_domain_count": len(feature["unknown_domains"]),
        "domains": sorted(feature["domains"]),
        "verified_unhosted_domains": sorted(feature["verified_unhosted_domains"]),
        "unregistered_domains": sorted(feature["unregistered_domains"]),
        "registered_domains": sorted(feature["registered_domains"]),
        "unknown_domains": sorted(feature["unknown_domains"]),
        "hosted_domains": sorted(feature["hosted_domains"]),
        "parked_domains": sorted(feature["parked_domains"]),
        "domain_status_counts": feature["domain_status_counts"],
        "verification_confidence": compute_verification_confidence(raw),
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

            # --- Score businesses that already have a website_url ---
            # These came from OSM with a website tag or were enriched by
            # Google Places/DDG/Foursquare. They should be disqualified (score=0)
            # so they never appear in leads.
            has_website_conditions = [
                Business.website_url.isnot(None),
                Business.website_url != "",
            ]
            if force_rescore:
                has_website_stmt = (
                    select(Business)
                    .where(*has_website_conditions)
                    .order_by(Business.created_at)
                )
            else:
                has_website_stmt = (
                    select(Business)
                    .where(*has_website_conditions)
                    .where(Business.scored_at.is_(None))
                    .order_by(Business.created_at)
                )
            if batch_size is not None:
                has_website_stmt = has_website_stmt.limit(batch_size)

            has_website_businesses = session.execute(has_website_stmt).scalars().all()
            for business in has_website_businesses:
                business.lead_score = 0.0
                business.score_reasons = {
                    "disqualify_reason": "has_website",
                    "website_url": business.website_url,
                }
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
                        "registered_domains",
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
                            ";".join(sorted(feature["registered_domains"])),
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
