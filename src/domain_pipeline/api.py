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
from pydantic import BaseModel
from sqlalchemy import exists, func, not_, or_, select
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
from .workers.business_leads import (
    business_eligibility_filters,
    export_business_leads,
    load_business_features,
    score_businesses,
)


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


class PipelineRunRequest(BaseModel):
    area: Optional[str] = None
    categories: str = "all"
    areas_file: str = "config/areas.json"
    categories_file: str = "config/categories.json"
    sync_limit: Optional[int] = None
    rdap_limit: Optional[int] = None
    rdap_statuses: Optional[list[str]] = None
    email_limit: Optional[int] = None
    score_limit: Optional[int] = None
    min_score: Optional[float] = None
    platform: str = "csv"
    business_score_limit: Optional[int] = None
    business_platform: str = "csv_business"
    business_min_score: Optional[float] = None
    business_require_unhosted_domain: bool = False
    business_require_contact: bool = True
    business_require_domain_qualification: bool = True


class BusinessScoreRequest(BaseModel):
    limit: Optional[int] = None
    scope: Optional[str] = None
    force_rescore: bool = False


class BusinessExportRequest(BaseModel):
    platform: str = "csv_business"
    min_score: Optional[float] = None
    limit: Optional[int] = None
    require_contact: bool = True
    require_unhosted_domain: bool = False
    require_domain_qualification: bool = True


class AutomationSettingsRequest(BaseModel):
    interval_seconds: Optional[int] = None
    area: Optional[str] = None
    categories: Optional[str] = None
    areas_file: Optional[str] = None
    categories_file: Optional[str] = None
    sync_limit: Optional[int] = None
    rdap_limit: Optional[int] = None
    rdap_statuses: Optional[list[str]] = None
    email_limit: Optional[int] = None
    score_limit: Optional[int] = None
    platform: Optional[str] = None
    min_score: Optional[float] = None
    business_score_limit: Optional[int] = None
    business_platform: Optional[str] = None
    business_min_score: Optional[float] = None
    business_require_unhosted_domain: Optional[bool] = None
    business_require_contact: Optional[bool] = None
    business_require_domain_qualification: Optional[bool] = None
    daily_target_enabled: Optional[bool] = None
    daily_target_count: Optional[int] = None
    daily_target_min_score: Optional[float] = None
    daily_target_platform_prefix: Optional[str] = None
    daily_target_require_contact: Optional[bool] = None
    daily_target_require_domain_qualification: Optional[bool] = None
    daily_target_require_unhosted_domain: Optional[bool] = None
    daily_target_allow_recycle: Optional[bool] = None


automation_controller = AutomationController()


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if automation_controller.auto_start_enabled:
            automation_controller.start()
        try:
            yield
        finally:
            automation_controller.stop()

    app = FastAPI(title="Domain Lead Pipeline API", version="0.1.0", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_parse_origins(),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
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
        require_contact: bool = Query(default=True),
        require_unhosted_domain: bool = Query(default=False),
        require_domain_qualification: bool = Query(default=True),
        only_unexported: bool = Query(default=False),
        platform: str = Query(default="csv_business"),
        limit: int = Query(default=200, ge=1, le=2000),
        offset: int = Query(default=0, ge=0),
    ) -> dict:
        no_website = or_(Business.website_url.is_(None), Business.website_url == "")

        with session_scope() as session:
            exported_for_platform_exists = exists(
                select(BusinessOutreachExport.id)
                .where(BusinessOutreachExport.business_id == Business.id)
                .where(BusinessOutreachExport.platform == platform)
            )

            shared_filters = [no_website, Business.lead_score.isnot(None)]
            if min_score is not None:
                shared_filters.append(Business.lead_score >= min_score)
            if category:
                shared_filters.append(Business.category == category)
            if city:
                shared_filters.append(City.name.ilike(f"%{city}%"))
            if only_unexported:
                shared_filters.append(not_(exported_for_platform_exists))
            shared_filters.extend(
                business_eligibility_filters(
                    require_contact=require_contact,
                    require_unhosted_domain=require_unhosted_domain,
                    require_domain_qualification=require_domain_qualification,
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

            items = [
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
                }
                for business, city_row in rows
            ]

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

    @app.post("/api/actions/business-score", dependencies=[Depends(require_mutation_auth)])
    def api_score_businesses(payload: BusinessScoreRequest) -> dict:
        processed = score_businesses(
            limit=payload.limit,
            scope=payload.scope,
            force_rescore=payload.force_rescore,
        )
        return {"processed": processed}

    @app.post("/api/actions/business-export", dependencies=[Depends(require_mutation_auth)])
    def api_export_businesses(payload: BusinessExportRequest) -> dict:
        path = export_business_leads(
            platform=payload.platform,
            min_score=payload.min_score,
            limit=payload.limit,
            require_contact=payload.require_contact,
            require_unhosted_domain=payload.require_unhosted_domain,
            require_domain_qualification=payload.require_domain_qualification,
        )
        return {"path": str(path) if path else None}

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
        automation_controller.update_settings(updates)
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

        path = _export_dir() / filename
        if not path.exists() or not path.is_file():
            raise HTTPException(status_code=404, detail="File not found")

        return FileResponse(path, media_type="text/csv", filename=filename)

    frontend_dist = Path(__file__).resolve().parents[2] / "frontend" / "dist"
    if frontend_dist.exists():
        app.mount("/", StaticFiles(directory=str(frontend_dist), html=True), name="frontend")

    return app


app = create_app()
