"""Verification retry and dead-letter logic.

Businesses that fail verification are retried up to MAX_RETRIES times
with exponential backoff. After exhausting retries, they are marked
as dead-letter (verification_retry_count >= MAX_RETRIES).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update

from .db import session_scope
from .models import Business

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 300  # 5 minutes base


def backoff_seconds(retry_count: int) -> int:
    """Exponential backoff: 5min, 20min, 80min."""
    return BASE_BACKOFF_SECONDS * (4 ** retry_count)


def record_verification_error(business_id, error: str) -> None:
    """Record a verification error on a business, incrementing retry count."""
    now = datetime.now(timezone.utc)
    with session_scope() as session:
        session.execute(
            update(Business)
            .where(Business.id == business_id)
            .values(
                verification_retry_count=Business.verification_retry_count + 1,
                verification_error=error[:1000],
                verification_error_at=now,
            )
        )


def clear_verification_error(business_id) -> None:
    """Clear verification error after a successful retry."""
    with session_scope() as session:
        session.execute(
            update(Business)
            .where(Business.id == business_id)
            .values(
                verification_error=None,
                verification_error_at=None,
            )
        )


def get_retryable_businesses(limit: int = 50) -> list[dict]:
    """Get businesses eligible for retry (failed but under MAX_RETRIES, backoff elapsed).

    Returns list of dicts with id, name, retry_count, error.
    """
    now = datetime.now(timezone.utc)
    with session_scope() as session:
        results = []
        for retry_count in range(MAX_RETRIES):
            backoff = timedelta(seconds=backoff_seconds(retry_count))
            cutoff = now - backoff
            rows = session.execute(
                select(
                    Business.id,
                    Business.name,
                    Business.verification_retry_count,
                    Business.verification_error,
                )
                .where(Business.verification_retry_count == retry_count + 1)
                .where(Business.verification_error.isnot(None))
                .where(Business.verification_error_at <= cutoff)
                .limit(limit - len(results))
            ).all()
            for biz_id, name, count, error in rows:
                results.append({
                    "id": biz_id,
                    "name": name,
                    "retry_count": count,
                    "error": error,
                })
            if len(results) >= limit:
                break
        return results


def reset_for_retry(business_ids: list) -> int:
    """Clear verification flags on businesses so they re-enter the verification queue.

    Clears the specific verification source that failed (identified by error prefix).
    For simplicity, clears scored_at so they get re-verified in the next batch.
    """
    if not business_ids:
        return 0
    with session_scope() as session:
        session.execute(
            update(Business)
            .where(Business.id.in_(business_ids))
            .values(
                scored_at=None,
                verification_error=None,
                verification_error_at=None,
            )
        )
    return len(business_ids)


def get_dead_letter_businesses(limit: int = 200, offset: int = 0) -> list[dict]:
    """Get businesses that have exhausted all retries (dead-letter queue)."""
    with session_scope() as session:
        rows = session.execute(
            select(
                Business.id,
                Business.name,
                Business.category,
                Business.verification_retry_count,
                Business.verification_error,
                Business.verification_error_at,
            )
            .where(Business.verification_retry_count >= MAX_RETRIES)
            .where(Business.verification_error.isnot(None))
            .order_by(Business.verification_error_at.desc())
            .limit(limit)
            .offset(offset)
        ).all()
        return [
            {
                "id": str(biz_id),
                "name": name,
                "category": category,
                "retry_count": retry_count,
                "error": error,
                "error_at": error_at.isoformat() if error_at else None,
            }
            for biz_id, name, category, retry_count, error, error_at in rows
        ]


def dead_letter_count() -> int:
    """Count businesses in the dead-letter queue."""
    from sqlalchemy import func
    with session_scope() as session:
        return int(
            session.execute(
                select(func.count(Business.id))
                .where(Business.verification_retry_count >= MAX_RETRIES)
                .where(Business.verification_error.isnot(None))
            ).scalar() or 0
        )
