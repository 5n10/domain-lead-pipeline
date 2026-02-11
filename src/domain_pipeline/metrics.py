from __future__ import annotations

from sqlalchemy import and_, case, func, or_, select

from .db import session_scope
from .models import (
    Business,
    BusinessOutreachExport,
    Contact,
    Domain,
    JobRun,
    OutreachExport,
)


def collect_metrics() -> dict:
    with session_scope() as session:
        no_website_condition = or_(Business.website_url.is_(None), Business.website_url == "")
        business_totals = session.execute(
            select(
                func.count(Business.id),
                func.sum(case((no_website_condition, 1), else_=0)),
                func.sum(case((Business.lead_score.isnot(None), 1), else_=0)),
                func.sum(
                    case(
                        (
                            and_(no_website_condition, Business.lead_score.isnot(None)),
                            1,
                        ),
                        else_=0,
                    )
                ),
            )
        ).first()

        domain_status_rows = session.execute(
            select(Domain.status, func.count(Domain.id)).group_by(Domain.status)
        ).all()

        contact_totals = session.execute(
            select(
                func.count(Contact.id),
                func.sum(case((Contact.lead_score.isnot(None), 1), else_=0)),
            )
        ).first()

        export_totals = session.execute(
            select(
                func.count(OutreachExport.id),
                func.sum(case((OutreachExport.status == "queued", 1), else_=0)),
            )
        ).first()

        business_export_totals = session.execute(
            select(
                func.count(BusinessOutreachExport.id),
                func.sum(case((BusinessOutreachExport.status == "queued", 1), else_=0)),
            )
        ).first()

        recent_jobs = session.execute(
            select(JobRun.job_name, JobRun.status, JobRun.started_at, JobRun.finished_at, JobRun.processed_count)
            .order_by(JobRun.started_at.desc())
            .limit(10)
        ).all()

    business_total = int(business_totals[0] or 0)
    no_website_total = int(business_totals[1] or 0)
    businesses_scored = int(business_totals[2] or 0)
    no_website_scored = int(business_totals[3] or 0)
    contacts_total = int(contact_totals[0] or 0)
    contacts_scored = int(contact_totals[1] or 0)
    exports_total = int(export_totals[0] or 0)
    exports_queued = int(export_totals[1] or 0)
    business_exports_total = int(business_export_totals[0] or 0)
    business_exports_queued = int(business_export_totals[1] or 0)

    return {
        "businesses": {
            "total": business_total,
            "no_website": no_website_total,
            "scored": businesses_scored,
            "no_website_scored": no_website_scored,
            "no_website_unscored": max(no_website_total - no_website_scored, 0),
        },
        "domains": {
            status: count for status, count in domain_status_rows
        },
        "contacts": {
            "total": contacts_total,
            "scored": contacts_scored,
            "unscored": max(contacts_total - contacts_scored, 0),
        },
        "exports": {
            "total": exports_total,
            "queued": exports_queued,
        },
        "business_exports": {
            "total": business_exports_total,
            "queued": business_exports_queued,
        },
        "recent_jobs": [
            {
                "job_name": job_name,
                "status": status,
                "started_at": started_at.isoformat() if started_at else None,
                "finished_at": finished_at.isoformat() if finished_at else None,
                "processed_count": processed_count,
            }
            for job_name, status, started_at, finished_at, processed_count in recent_jobs
        ],
    }
