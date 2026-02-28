from __future__ import annotations

from contextlib import asynccontextmanager
import hmac
import ipaddress
import os
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy import exists, func, not_, or_, select
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.staticfiles import StaticFiles

from .automation import AutomationController
from .config import load_config
from .db import session_scope
from .metrics import collect_metrics
from .models import (
    Business,
    BusinessOutreachExport,
    City,
    JobRun,
)
from .pipeline import run_once
from .workers.business_domain_sync import run_batch as sync_business_domains
from .workers.rdap_check import run_batch as run_rdap_checks
from .workers.business_leads import (
    VERIFICATION_KEYS,
    business_eligibility_filters,
    compute_verification_confidence,
    compute_verification_count,
    export_business_leads,
    get_verification_sources,
    load_business_features,
    score_businesses,
)
from .workers.google_places import run_batch as run_google_places_enrich, verify_websites
from .workers.web_search_verify import run_batch as run_ddg_verify
from .workers.llm_verify import run_batch as run_llm_verify
from .workers.foursquare import run_batch as run_foursquare_enrich, verify_websites as verify_websites_foursquare
from .workers.domain_guess import run_batch as run_domain_guess
from .workers.hunter import run_batch as run_hunter_enrich
from .workers.sheets_export import export_to_sheets
from .notifications import send_notification


# Allowed configuration file paths for validation
ALLOWED_CONFIG_FILES = ["config/areas.json", "config/categories.json"]

# Input validation limits
MAX_CATEGORY_LENGTH = 50
MAX_CITY_LENGTH = 100
MAX_PLATFORM_LENGTH = 50


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security headers to all responses."""
    
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        # Prevent clickjacking
        response.headers["X-Frame-Options"] = "DENY"
        # Prevent MIME type sniffing
        response.headers["X-Content-Type-Options"] = "nosniff"
        # Referrer policy
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        # Content Security Policy for API
        response.headers["Content-Security-Policy"] = "default-src 'self'"
        return response


def _parse_origins() -> list[str]:
    raw = os.getenv(
        "FRONTEND_ORIGINS",
        ",".join(
            [
                "http://localhost:5173",
                "http://127.0.0.1:5173",
                "http://localhost:5174",
                "http://127.0.0.1:5174",
                "http://localhost:5175",
                "http://127.0.0.1:5175",
                "http://localhost:8000",
                "http://127.0.0.1:8000",
                "http://host.docker.internal:5174",
                "http://host.docker.internal:5173",
                "http://host.docker.internal:8000",
            ]
        ),
    )
    return [entry.strip() for entry in raw.split(",") if entry.strip()]


def _export_dir() -> Path:
    config = load_config()
    path = Path(config.export_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _is_loopback_host(host: Optional[str]) -> bool:
    if not host:
        return False
    candidate = host.strip()
    if not candidate:
        return False
    if candidate == "localhost":
        return True
    try:
        return ipaddress.ip_address(candidate).is_loopback
    except ValueError:
        return False


def require_mutation_auth(
    request: Request,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None),
) -> None:
    config = load_config()
    client_host = request.client.host if request.client else None
    if config.mutation_localhost_bypass and _is_loopback_host(client_host):
        return

    token = x_api_key
    if not token and authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()

    expected = config.mutation_api_key
    if not expected:
        raise HTTPException(status_code=401, detail="Mutation API key is required")
    if not token or not hmac.compare_digest(token, expected):
        raise HTTPException(status_code=401, detail="Invalid mutation API key")


def _validate_string_param(value: Optional[str], param_name: str, max_length: int = 100) -> None:
    """Validate string query parameters to prevent abuse.
    
    Empty or whitespace-only strings are treated as None (no filter applied).
    """
    if value is None:
        return
    # Treat empty/whitespace-only strings as None (no filter) for optional parameters
    stripped = value.strip()
    if not stripped:
        return
    # Validate length on the original value to catch potential padding attacks
    if len(value) > max_length:
        raise HTTPException(status_code=400, detail=f"Parameter '{param_name}' exceeds maximum length of {max_length}")
    # Reject parameters with control characters that could indicate injection attempts
    # Using string for efficient membership testing
    if any(c in '\x00\n\r' for c in value):
        raise HTTPException(
            status_code=400,
            detail=f"Parameter '{param_name}' contains control characters (null bytes, newlines, or carriage returns) which are not allowed"
        )


def _validate_file_path(file_path: str, param_name: str) -> None:
    """Validate file paths to prevent directory traversal.
    
    Uses a whitelist approach - only files in ALLOWED_CONFIG_FILES are permitted.
    """
    if file_path not in ALLOWED_CONFIG_FILES:
        raise HTTPException(status_code=400, detail=f"Invalid {param_name}: must be one of {ALLOWED_CONFIG_FILES}")


class PipelineRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    area: Optional[str] = Field(None, max_length=100)
    categories: str = Field("all", max_length=500)
    areas_file: str = Field("config/areas.json", max_length=100, pattern=r"^config/[a-zA-Z0-9_-]+\.json$")
    categories_file: str = Field("config/categories.json", max_length=100, pattern=r"^config/[a-zA-Z0-9_-]+\.json$")
    sync_limit: Optional[int] = Field(None, ge=1, le=10000)
    rdap_limit: Optional[int] = Field(None, ge=1, le=10000)
    rdap_statuses: Optional[list[str]] = None
    email_limit: Optional[int] = Field(None, ge=1, le=10000)
    score_limit: Optional[int] = Field(None, ge=1, le=10000)
    min_score: Optional[float] = Field(None, ge=0.0)
    platform: str = Field("csv", max_length=50)
    business_score_limit: Optional[int] = Field(None, ge=1, le=10000)
    business_platform: str = Field("csv_business", max_length=50)
    business_min_score: Optional[float] = Field(None, ge=0.0)
    business_require_unhosted_domain: bool = False
    business_require_contact: bool = True
    business_require_domain_qualification: bool = True


class BusinessScoreRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: Optional[int] = Field(None, ge=1, le=100000)
    scope: Optional[str] = Field(None, max_length=50)
    force_rescore: bool = False


class BusinessExportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    platform: str = Field("csv_business", max_length=50)
    min_score: Optional[float] = Field(None, ge=0.0)
    limit: Optional[int] = Field(None, ge=1, le=10000)
    require_contact: bool = True
    require_unhosted_domain: bool = False
    require_domain_qualification: bool = True
    exclude_hosted_email_domain: bool = True


class GooglePlacesEnrichRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: Optional[int] = Field(None, ge=1, le=1000)
    priority: str = Field("no_contacts", max_length=20)
    rescore: bool = True


class GooglePlacesVerifyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: Optional[int] = Field(None, ge=1, le=1000)
    min_score: float = Field(30.0, ge=0.0)
    rescore: bool = True


class DomainGuessRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: Optional[int] = Field(None, ge=1, le=1000)
    min_score: float = Field(0.0, ge=0.0)
    rescore: bool = True


class DDGVerifyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: Optional[int] = Field(None, ge=1, le=1000)
    min_score: float = Field(30.0, ge=0.0)
    rescore: bool = True


class LLMVerifyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: Optional[int] = Field(None, ge=1, le=1000)
    min_score: float = Field(30.0, ge=0.0)
    rescore: bool = True


class GoogleSearchVerifyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: Optional[int] = Field(None, ge=1, le=1000)
    min_score: float = Field(30.0, ge=0.0)
    rescore: bool = True


class SearXNGVerifyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: Optional[int] = Field(None, ge=1, le=2000)
    min_score: float = Field(0.0, ge=0.0)
    rescore: bool = True


class FoursquareEnrichRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: Optional[int] = Field(None, ge=1, le=1000)
    priority: str = Field("no_contacts", max_length=20)
    rescore: bool = True


class FoursquareVerifyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: Optional[int] = Field(None, ge=1, le=1000)
    min_score: float = Field(30.0, ge=0.0)
    rescore: bool = True


class HunterEnrichRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: Optional[int] = Field(None, ge=1, le=100)


class SheetsExportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    min_score: Optional[float] = Field(None, ge=0.0)
    limit: Optional[int] = Field(None, ge=1, le=10000)
    require_contact: bool = True
    require_unhosted_domain: bool = False
    require_domain_qualification: bool = True


class TestNotificationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: str = Field("Test Notification", max_length=100)
    message: str = Field("Domain Lead Pipeline test notification", max_length=500)


class AutomationSettingsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    interval_seconds: Optional[int] = Field(None, ge=60, le=86400)
    area: Optional[str] = Field(None, max_length=100)
    categories: Optional[str] = Field(None, max_length=500)
    areas_file: Optional[str] = Field(None, max_length=100, pattern=r"^config/[a-zA-Z0-9_-]+\.json$")
    categories_file: Optional[str] = Field(None, max_length=100, pattern=r"^config/[a-zA-Z0-9_-]+\.json$")
    sync_limit: Optional[int] = Field(None, ge=1, le=10000)
    rdap_limit: Optional[int] = Field(None, ge=1, le=10000)
    rdap_statuses: Optional[list[str]] = None
    email_limit: Optional[int] = Field(None, ge=1, le=10000)
    score_limit: Optional[int] = Field(None, ge=1, le=10000)
    platform: Optional[str] = Field(None, max_length=50)
    min_score: Optional[float] = Field(None, ge=0.0)
    business_score_limit: Optional[int] = Field(None, ge=1, le=10000)
    business_platform: Optional[str] = Field(None, max_length=50)
    business_min_score: Optional[float] = Field(None, ge=0.0)
    business_require_unhosted_domain: Optional[bool] = None
    business_require_contact: Optional[bool] = None
    business_require_domain_qualification: Optional[bool] = None
    daily_target_enabled: Optional[bool] = None
    daily_target_count: Optional[int] = Field(None, ge=1, le=1000)
    daily_target_min_score: Optional[float] = Field(None, ge=0.0)
    daily_target_platform_prefix: Optional[str] = Field(None, max_length=50)
    daily_target_require_contact: Optional[bool] = None
    daily_target_require_domain_qualification: Optional[bool] = None
    daily_target_require_unhosted_domain: Optional[bool] = None
    daily_target_allow_recycle: Optional[bool] = None


class VerificationSettingsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    domain_guess_batch: Optional[int] = Field(None, ge=1, le=5000)
    domain_guess_min_score: Optional[float] = Field(None, ge=0.0)
    searxng_batch: Optional[int] = Field(None, ge=1, le=2000)
    searxng_min_score: Optional[float] = Field(None, ge=0.0)
    ddg_batch: Optional[int] = Field(None, ge=1, le=1000)
    ddg_min_score: Optional[float] = Field(None, ge=0.0)
    llm_batch: Optional[int] = Field(None, ge=1, le=500)
    llm_min_score: Optional[float] = Field(None, ge=0.0)
    google_search_batch: Optional[int] = Field(None, ge=1, le=500)
    google_search_min_score: Optional[float] = Field(None, ge=0.0)
    rescore_after_batch: Optional[bool] = None
    pause_between_batches: Optional[int] = Field(None, ge=1, le=300)
    pause_when_idle: Optional[int] = Field(None, ge=10, le=3600)


automation_controller = AutomationController()


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if automation_controller.auto_start_enabled:
            automation_controller.start()
        # Always start continuous verification on boot
        automation_controller.start_verification()
        try:
            yield
        finally:
            automation_controller.stop_verification()
            automation_controller.stop()

    app = FastAPI(title="Domain Lead Pipeline API", version="0.1.0", lifespan=lifespan)
    
    # Add security headers middleware
    app.add_middleware(SecurityHeadersMiddleware)
    
    # Configure CORS with specific methods and headers instead of wildcards
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_parse_origins(),
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization", "X-API-Key"],
    )

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/api/metrics")
    def api_metrics() -> dict:
        return collect_metrics()

    @app.get("/api/jobs")
    def api_jobs(limit: int = Query(default=50, ge=1, le=500)) -> list[dict]:
        with session_scope() as session:
            rows = session.execute(
                select(JobRun).order_by(JobRun.started_at.desc()).limit(limit)
            ).scalars().all()
        return [
            {
                "id": str(row.id),
                "job_name": row.job_name,
                "scope": row.scope,
                "status": row.status,
                "started_at": row.started_at.isoformat() if row.started_at else None,
                "finished_at": row.finished_at.isoformat() if row.finished_at else None,
                "processed_count": row.processed_count,
                "details": row.details,
                "error": row.error,
            }
            for row in rows
        ]

    @app.get("/api/leads/business")
    def api_business_leads(
        min_score: Optional[float] = Query(default=None),
        category: Optional[str] = Query(default=None),
        city: Optional[str] = Query(default=None),
        min_confidence: Optional[str] = Query(default=None),
        require_contact: bool = Query(default=False),
        require_unhosted_domain: bool = Query(default=False),
        require_domain_qualification: bool = Query(default=False),
        require_no_website: bool = Query(default=True),
        exclude_hosted_email_domain: bool = Query(default=True),
        only_unexported: bool = Query(default=False),
        only_verified: bool = Query(default=False),
        platform: str = Query(default="csv_business"),
        limit: int = Query(default=200, ge=1, le=2000),
        offset: int = Query(default=0, ge=0),
    ) -> dict:
        # Validate string parameters
        _validate_string_param(category, "category", max_length=MAX_CATEGORY_LENGTH)
        _validate_string_param(city, "city", max_length=MAX_CITY_LENGTH)
        _validate_string_param(platform, "platform", max_length=MAX_PLATFORM_LENGTH)
        
        with session_scope() as session:
            exported_for_platform_exists = exists(
                select(BusinessOutreachExport.id)
                .where(BusinessOutreachExport.business_id == Business.id)
                .where(BusinessOutreachExport.platform == platform)
            )

            shared_filters = [Business.lead_score.isnot(None)]
            if require_no_website:
                shared_filters.append(or_(Business.website_url.is_(None), Business.website_url == ""))
            if min_score is not None:
                shared_filters.append(Business.lead_score >= min_score)
            if category:
                shared_filters.append(Business.category == category)
            if city:
                shared_filters.append(City.name.ilike(f"%{city}%"))
            if only_unexported:
                shared_filters.append(not_(exported_for_platform_exists))
            if only_verified:
                # At least one verification source must have checked this business
                shared_filters.append(
                    or_(
                        *[Business.raw.has_key(key) for key in VERIFICATION_KEYS]
                    )
                )
            shared_filters.extend(
                business_eligibility_filters(
                    require_contact=require_contact,
                    require_unhosted_domain=require_unhosted_domain,
                    require_domain_qualification=require_domain_qualification,
                    exclude_hosted_email_domain=exclude_hosted_email_domain,
                )
            )

            count_stmt = select(func.count(Business.id)).select_from(Business).outerjoin(City, Business.city_id == City.id)
            for expression in shared_filters:
                count_stmt = count_stmt.where(expression)
            total_candidates = int(session.execute(count_stmt).scalar() or 0)

            stmt = (
                select(Business, City)
                .outerjoin(City, Business.city_id == City.id)
                .order_by(Business.lead_score.desc(), Business.created_at)
                .limit(limit)
                .offset(offset)
            )
            for expression in shared_filters:
                stmt = stmt.where(expression)

            rows = session.execute(stmt).all()
            business_ids = [business.id for business, _ in rows]
            feature_map = load_business_features(session, business_ids)

            exported_ids = set(
                session.execute(
                    select(BusinessOutreachExport.business_id)
                    .where(BusinessOutreachExport.business_id.in_(business_ids))
                    .where(BusinessOutreachExport.platform == platform)
                )
                .scalars()
                .all()
            )

            all_items = [
                {
                    "id": str(business.id),
                    "name": business.name,
                    "category": business.category,
                    "address": business.address,
                    "city": city_row.name if city_row else None,
                    "country": city_row.country if city_row else None,
                    "lead_score": float(business.lead_score) if business.lead_score is not None else None,
                    "scored_at": business.scored_at.isoformat() if business.scored_at else None,
                    "source": business.source,
                    "source_id": business.source_id,
                    "emails": sorted(feature_map[business.id]["emails"]),
                    "business_emails": sorted(feature_map[business.id]["business_emails"]),
                    "free_emails": sorted(feature_map[business.id]["free_emails"]),
                    "phones": sorted(feature_map[business.id]["phones"]),
                    "domains": sorted(feature_map[business.id]["domains"]),
                    "verified_unhosted_domains": sorted(feature_map[business.id]["verified_unhosted_domains"]),
                    "unregistered_domains": sorted(feature_map[business.id]["unregistered_domains"]),
                    "unknown_domains": sorted(feature_map[business.id]["unknown_domains"]),
                    "hosted_domains": sorted(feature_map[business.id]["hosted_domains"]),
                    "parked_domains": sorted(feature_map[business.id]["parked_domains"]),
                    "domain_status_counts": feature_map[business.id]["domain_status_counts"],
                    "exported": business.id in exported_ids,
                    "verification_count": compute_verification_count(business.raw),
                    "verification_sources": get_verification_sources(business.raw),
                    "verification_confidence": compute_verification_confidence(business.raw),
                }
                for business, city_row in rows
            ]

            # Client-side confidence filter (computed field, not in DB)
            if min_confidence:
                confidence_rank = {"high": 3, "medium": 2, "low": 1, "unverified": 0}
                min_rank = confidence_rank.get(min_confidence, 0)
                items = [item for item in all_items if confidence_rank.get(item["verification_confidence"], 0) >= min_rank]
            else:
                items = all_items

            return {
                "total_candidates": total_candidates,
                "returned": len(items),
                "items": items,
            }

    @app.get("/api/leads/business/categories")
    def api_business_categories() -> list[str]:
        with session_scope() as session:
            rows = session.execute(
                select(Business.category)
                .where(Business.category.isnot(None))
                .group_by(Business.category)
                .order_by(Business.category)
            ).scalars().all()
        return [row for row in rows if row]

    @app.get("/api/leads/business/cities")
    def api_business_cities(limit: int = Query(default=200, ge=1, le=2000)) -> list[str]:
        with session_scope() as session:
            rows = session.execute(
                select(City.name)
                .where(City.name.isnot(None))
                .group_by(City.name)
                .order_by(City.name)
                .limit(limit)
            ).scalars().all()
        return [row for row in rows if row]

    @app.post("/api/actions/pipeline-run", dependencies=[Depends(require_mutation_auth)])
    def api_run_pipeline(payload: PipelineRunRequest) -> dict:
        # Validate file paths to prevent directory traversal
        _validate_file_path(payload.areas_file, "areas_file")
        _validate_file_path(payload.categories_file, "categories_file")

        if not automation_controller._run_lock.acquire(blocking=False):
            raise HTTPException(status_code=409, detail="Pipeline is already running")
        try:
            return run_once(
                area=payload.area,
                categories=payload.categories,
                areas_file=payload.areas_file,
                categories_file=payload.categories_file,
                sync_limit=payload.sync_limit,
                rdap_limit=payload.rdap_limit,
                rdap_statuses=payload.rdap_statuses,
                email_limit=payload.email_limit,
                score_limit=payload.score_limit,
                platform=payload.platform,
                min_score=payload.min_score,
                business_score_limit=payload.business_score_limit,
                business_platform=payload.business_platform,
                business_min_score=payload.business_min_score,
                business_require_unhosted_domain=payload.business_require_unhosted_domain,
                business_require_contact=payload.business_require_contact,
                business_require_domain_qualification=payload.business_require_domain_qualification,
            )
        finally:
            automation_controller._run_lock.release()

    @app.post("/api/actions/business-score", dependencies=[Depends(require_mutation_auth)])
    def api_score_businesses(payload: BusinessScoreRequest) -> dict:
        processed = score_businesses(
            limit=payload.limit,
            scope=payload.scope,
            force_rescore=payload.force_rescore,
        )
        return {"processed": processed}

    @app.post("/api/actions/validate-domains", dependencies=[Depends(require_mutation_auth)])
    def api_validate_domains(
        sync_limit: Optional[int] = Query(default=None),
        rdap_limit: Optional[int] = Query(default=None),
        rescore: bool = Query(default=True),
    ) -> dict:
        """Bulk domain sync + RDAP check + rescore.

        Syncs email domains for all businesses, runs RDAP checks to detect
        hosted/parked/unhosted domains, then rescores affected businesses.
        This catches businesses that appear to have no website in OSM data
        but actually have a website discoverable from their email domain.
        """
        synced = sync_business_domains(limit=sync_limit, reset_cursor=False)
        # Disable auto_rescore since we explicitly call score_businesses below
        rdap_processed = run_rdap_checks(
            limit=rdap_limit,
            statuses=["new", "skipped", "rdap_error", "dns_error"],
            auto_rescore=False,
        )
        scored = 0
        if rescore:
            scored = score_businesses(limit=None, force_rescore=False)
        return {
            "synced": synced,
            "rdap_processed": rdap_processed,
            "rescored": scored,
        }

    @app.post("/api/actions/enrich-google-places", dependencies=[Depends(require_mutation_auth)])
    def api_enrich_google_places(payload: GooglePlacesEnrichRequest) -> dict:
        """Enrich businesses with Google Places data (phone, website, rating).

        Free tier: 10,000 calls/month on Essentials SKUs.
        Prioritizes businesses without contacts for maximum lead impact.
        Optionally rescores businesses after enrichment to reflect new contacts.
        """
        result = run_google_places_enrich(
            limit=payload.limit,
            priority=payload.priority,
        )
        if payload.rescore and result.get("phones_added", 0) > 0:
            rescored = score_businesses(limit=None, force_rescore=False)
            result["rescored"] = rescored
        return result

    @app.post("/api/actions/verify-websites", dependencies=[Depends(require_mutation_auth)])
    def api_verify_websites(payload: GooglePlacesVerifyRequest) -> dict:
        """Verify whether potential leads actually have websites via Google Places.

        This is the critical quality gate. For each business scoring >= min_score
        with no website_url, searches Google Places. If Google confirms a website
        exists, sets website_url so the business is excluded from leads on rescore.

        Businesses that Google confirms have NO website are genuine lead candidates.
        """
        result = verify_websites(
            limit=payload.limit,
            min_score=payload.min_score,
        )
        if payload.rescore and result.get("processed", 0) > 0:
            rescored = score_businesses(limit=None, force_rescore=False)
            result["rescored"] = rescored
        return result

    @app.post("/api/actions/domain-guess", dependencies=[Depends(require_mutation_auth)])
    def api_domain_guess(payload: DomainGuessRequest) -> dict:
        """Guess domains from business names and check via HTTP HEAD.

        FREE — no API key, no rate limits. ~500 businesses/minute.
        Generates candidate domains from business names (e.g. "GTA Heating" →
        gtaheatingandcooling.com) and checks if they resolve to a live site.
        """
        result = run_domain_guess(
            limit=payload.limit,
            min_score=payload.min_score,
        )
        if payload.rescore and result.get("processed", 0) > 0:
            rescored = score_businesses(limit=None, force_rescore=False)
            result["rescored"] = rescored
        return result

    @app.post("/api/actions/verify-websites-ddg", dependencies=[Depends(require_mutation_auth)])
    def api_verify_websites_ddg(payload: DDGVerifyRequest) -> dict:
        """Verify whether leads have websites via DuckDuckGo search.

        Free, no API key required. Searches the web for each business to
        determine if they actually have a website we missed.
        """
        result = run_ddg_verify(
            limit=payload.limit,
            min_score=payload.min_score,
        )
        if payload.rescore and result.get("processed", 0) > 0:
            rescored = score_businesses(limit=None, force_rescore=False)
            result["rescored"] = rescored
        return result

    @app.post("/api/actions/verify-websites-llm", dependencies=[Depends(require_mutation_auth)])
    def api_verify_websites_llm(payload: LLMVerifyRequest) -> dict:
        """Verify whether leads have websites via an LLM.

        Requires OPENROUTER_API_KEY, GEMINI_API_KEY, or GROQ_API_KEY.
        Uses language models to determine if a business has a real website.
        """
        result = run_llm_verify(
            limit=payload.limit,
            min_score=payload.min_score,
        )
        if payload.rescore and result.get("processed", 0) > 0:
            rescored = score_businesses(limit=None, force_rescore=False)
            result["rescored"] = rescored
        return result

    @app.post("/api/actions/verify-websites-google-search", dependencies=[Depends(require_mutation_auth)])
    def api_verify_websites_google_search(payload: GoogleSearchVerifyRequest) -> dict:
        """Verify whether leads have websites via Google Search scraping.

        Free, no API key required. Additional verification stage that searches
        Google for each business to determine if they have a website.
        More conservative rate limiting than DDG to avoid blocks.
        """
        from .workers.google_search_verify import run_batch as run_google_search_verify

        result = run_google_search_verify(
            limit=payload.limit,
            min_score=payload.min_score,
        )
        if payload.rescore and result.get("processed", 0) > 0:
            rescored = score_businesses(limit=None, force_rescore=False)
            result["rescored"] = rescored
        return result

    @app.post("/api/actions/verify-websites-searxng", dependencies=[Depends(require_mutation_auth)])
    def api_verify_websites_searxng(payload: SearXNGVerifyRequest) -> dict:
        """Verify whether leads have websites via SearXNG meta-search.

        Uses local SearXNG instance aggregating DDG, Bing, Brave, Mojeek, etc.
        FREE — no API keys, no rate limits, no blocking risk.
        Replaces broken DDG and Google Search scrapers.
        """
        from .workers.searxng_verify import run_batch as run_searxng_verify

        result = run_searxng_verify(
            limit=payload.limit,
            min_score=payload.min_score,
        )
        if payload.rescore and result.get("processed", 0) > 0:
            rescored = score_businesses(limit=None, force_rescore=False)
            result["rescored"] = rescored
        return result

    @app.post("/api/actions/enrich-foursquare", dependencies=[Depends(require_mutation_auth)])
    def api_enrich_foursquare(payload: FoursquareEnrichRequest) -> dict:
        """Enrich businesses with Foursquare Places data (phone, website, rating).

        Free tier: 10,000 calls/month. Requires FOURSQUARE_API_KEY.
        """
        result = run_foursquare_enrich(
            limit=payload.limit,
            priority=payload.priority,
        )
        if payload.rescore and result.get("phones_added", 0) > 0:
            rescored = score_businesses(limit=None, force_rescore=False)
            result["rescored"] = rescored
        return result

    @app.post("/api/actions/verify-websites-foursquare", dependencies=[Depends(require_mutation_auth)])
    def api_verify_websites_foursquare(payload: FoursquareVerifyRequest) -> dict:
        """Verify whether leads have websites via Foursquare Places API.

        Supplementary to Google Places verification. Requires FOURSQUARE_API_KEY.
        """
        result = verify_websites_foursquare(
            limit=payload.limit,
            min_score=payload.min_score,
        )
        if payload.rescore and result.get("processed", 0) > 0:
            rescored = score_businesses(limit=None, force_rescore=False)
            result["rescored"] = rescored
        return result

    @app.post("/api/actions/hunter-enrich", dependencies=[Depends(require_mutation_auth)])
    def api_hunter_enrich(payload: HunterEnrichRequest) -> dict:
        """Enrich lead businesses with email contacts via Hunter.io.

        Free tier: 25 searches/month. Requires HUNTER_API_KEY.
        """
        return run_hunter_enrich(limit=payload.limit)

    @app.post("/api/actions/export-google-sheets", dependencies=[Depends(require_mutation_auth)])
    def api_export_google_sheets(payload: SheetsExportRequest) -> dict:
        """Export leads directly to a Google Sheet.

        Requires GOOGLE_SHEETS_CREDENTIALS_FILE and GOOGLE_SHEETS_SPREADSHEET_ID.
        """
        return export_to_sheets(
            min_score=payload.min_score,
            limit=payload.limit,
            require_contact=payload.require_contact,
            require_unhosted_domain=payload.require_unhosted_domain,
            require_domain_qualification=payload.require_domain_qualification,
        )

    @app.post("/api/actions/test-notification", dependencies=[Depends(require_mutation_auth)])
    def api_test_notification(payload: TestNotificationRequest) -> dict:
        """Send a test push notification via ntfy.sh.

        Requires NTFY_TOPIC to be configured.
        """
        success = send_notification(
            title=payload.title,
            message=payload.message,
            priority="default",
            tags=["test"],
        )
        return {"sent": success}

    @app.post("/api/actions/business-export", dependencies=[Depends(require_mutation_auth)])
    def api_export_businesses(payload: BusinessExportRequest) -> dict:
        path = export_business_leads(
            platform=payload.platform,
            min_score=payload.min_score,
            limit=payload.limit,
            require_contact=payload.require_contact,
            require_unhosted_domain=payload.require_unhosted_domain,
            require_domain_qualification=payload.require_domain_qualification,
            exclude_hosted_email_domain=payload.exclude_hosted_email_domain,
        )
        return {"path": str(path) if path else None}

    @app.post("/api/actions/reset-ddg-verification", dependencies=[Depends(require_mutation_auth)])
    def api_reset_ddg_verification() -> dict:
        """Clear ALL existing DDG verification data and force rescore.

        The duckduckgo_search library (v8.1.1) was broken — it returned 0 results
        for ALL queries. This means every DDG-verified business has fake data.
        This endpoint clears all DDG verification flags so businesses can be
        re-verified with the working HTML scraper.

        Also forces rescore with new confidence caps.
        """
        with session_scope() as sess:
            # Find all businesses with DDG verification data
            businesses = sess.execute(
                select(Business).where(Business.raw.has_key("ddg_verified"))
            ).scalars().all()

            cleared = 0
            for biz in businesses:
                raw = dict(biz.raw) if biz.raw else {}
                # Remove all DDG-related keys
                for key in ["ddg_verified", "ddg_verify_result", "ddg_website",
                           "ddg_search_query", "ddg_result_count"]:
                    raw.pop(key, None)
                biz.raw = raw
                cleared += 1

                if cleared % 500 == 0:
                    sess.flush()

        # Force rescore all businesses with new confidence caps
        rescored = score_businesses(limit=None, force_rescore=True)

        return {
            "ddg_cleared": cleared,
            "rescored": rescored,
        }

    @app.get("/api/automation/status")
    def api_automation_status() -> dict:
        return automation_controller.status()

    @app.post("/api/automation/start", dependencies=[Depends(require_mutation_auth)])
    def api_automation_start(payload: Optional[AutomationSettingsRequest] = None) -> dict:
        updates = payload.model_dump(exclude_none=True) if payload else {}
        return automation_controller.start(updates=updates)

    @app.post("/api/automation/stop", dependencies=[Depends(require_mutation_auth)])
    def api_automation_stop() -> dict:
        return automation_controller.stop()

    @app.post("/api/automation/run-now", dependencies=[Depends(require_mutation_auth)])
    def api_automation_run_now() -> dict:
        return automation_controller.run_now()

    @app.post("/api/automation/daily-target-now", dependencies=[Depends(require_mutation_auth)])
    def api_automation_daily_target_now() -> dict:
        return automation_controller.run_daily_target_now()

    @app.post("/api/automation/settings", dependencies=[Depends(require_mutation_auth)])
    def api_automation_update_settings(payload: AutomationSettingsRequest) -> dict:
        updates = payload.model_dump(exclude_none=True)
        # Validate file paths if present
        if "areas_file" in updates and updates["areas_file"]:
            _validate_file_path(updates["areas_file"], "areas_file")
        if "categories_file" in updates and updates["categories_file"]:
            _validate_file_path(updates["categories_file"], "categories_file")
        automation_controller.update_settings(updates)
        return automation_controller.status()

    # --- Continuous Verification endpoints ---

    @app.post("/api/automation/start-verification", dependencies=[Depends(require_mutation_auth)])
    def api_start_verification(payload: Optional[VerificationSettingsRequest] = None) -> dict:
        """Start the continuous verification loop.

        Runs domain guess, LLM verify, DDG search, and Google Search in tight
        batches on a background thread. Rescores after each batch. Keeps running
        until stopped.
        """
        updates = payload.model_dump(exclude_none=True) if payload else {}
        return automation_controller.start_verification(updates=updates)

    @app.post("/api/automation/stop-verification", dependencies=[Depends(require_mutation_auth)])
    def api_stop_verification() -> dict:
        """Stop the continuous verification loop."""
        return automation_controller.stop_verification()

    @app.post("/api/automation/verification-settings", dependencies=[Depends(require_mutation_auth)])
    def api_verification_settings(payload: VerificationSettingsRequest) -> dict:
        """Update continuous verification settings."""
        updates = payload.model_dump(exclude_none=True)
        automation_controller.update_verify_settings(updates)
        return automation_controller.status()

    @app.get("/api/exports/files")
    def api_export_files() -> list[dict]:
        files = []
        for path in _export_dir().glob("*.csv"):
            stat = path.stat()
            files.append(
                {
                    "name": path.name,
                    "size": stat.st_size,
                    "modified_at": stat.st_mtime,
                }
            )
        files.sort(key=lambda entry: entry["modified_at"], reverse=True)
        return files

    @app.get("/api/exports/files/{filename}")
    def api_download_export(filename: str):
        if "/" in filename or "\\" in filename:
            raise HTTPException(status_code=400, detail="Invalid filename")

        export_dir = _export_dir()
        path = export_dir / filename
        # Prevent symlink escape: resolved path must stay within export dir
        if not path.resolve().is_relative_to(export_dir.resolve()):
            raise HTTPException(status_code=400, detail="Invalid filename")
        if not path.exists() or not path.is_file():
            raise HTTPException(status_code=404, detail="File not found")

        return FileResponse(path, media_type="text/csv", filename=filename)

    frontend_dist = Path(__file__).resolve().parents[2] / "frontend" / "dist"
    if frontend_dist.exists():
        app.mount("/", StaticFiles(directory=str(frontend_dist), html=True), name="frontend")

    return app


app = create_app()
