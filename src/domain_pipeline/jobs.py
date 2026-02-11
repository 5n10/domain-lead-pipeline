from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from .models import JobCheckpoint, JobRun

GLOBAL_SCOPE = "__global__"


def normalize_scope(scope: Optional[str]) -> str:
    cleaned = (scope or "").strip()
    return cleaned or GLOBAL_SCOPE


def start_job(session: Session, job_name: str, scope: Optional[str] = None, details: Optional[dict] = None) -> JobRun:
    run = JobRun(job_name=job_name, scope=normalize_scope(scope), status="running", details=details)
    session.add(run)
    session.flush()
    return run


def complete_job(session: Session, run: JobRun, processed_count: int = 0, details: Optional[dict] = None) -> None:
    run.status = "success"
    run.processed_count = processed_count
    run.finished_at = datetime.now(timezone.utc)
    if details is not None:
        run.details = details


def fail_job(session: Session, run: JobRun, error: str, details: Optional[dict] = None) -> None:
    run.status = "failed"
    run.error = error[:4000]
    run.finished_at = datetime.now(timezone.utc)
    if details is not None:
        run.details = details


def set_checkpoint(
    session: Session,
    job_name: str,
    scope: Optional[str],
    checkpoint_key: str,
    checkpoint_value: str,
    details: Optional[dict] = None,
    job_run_id: Optional[uuid.UUID] = None,
) -> None:
    normalized_scope = normalize_scope(scope)
    stmt = (
        insert(JobCheckpoint)
        .values(
            job_run_id=job_run_id,
            job_name=job_name,
            scope=normalized_scope,
            checkpoint_key=checkpoint_key,
            checkpoint_value=checkpoint_value,
            details=details,
        )
        .on_conflict_do_update(
            constraint="job_checkpoints_unique_scope_key_uidx",
            set_={
                "checkpoint_value": checkpoint_value,
                "details": details,
                "job_run_id": job_run_id,
                "updated_at": func.now(),
            },
        )
    )
    session.execute(stmt)


def get_checkpoint(session: Session, job_name: str, scope: Optional[str], checkpoint_key: str) -> Optional[str]:
    normalized_scope = normalize_scope(scope)
    stmt = (
        select(JobCheckpoint.checkpoint_value)
        .where(JobCheckpoint.job_name == job_name)
        .where(JobCheckpoint.scope == normalized_scope)
        .where(JobCheckpoint.checkpoint_key == checkpoint_key)
        .order_by(JobCheckpoint.updated_at.desc(), JobCheckpoint.id.desc())
        .limit(1)
    )
    return session.execute(stmt).scalar_one_or_none()
