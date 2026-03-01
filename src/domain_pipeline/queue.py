"""Verification queue with PostgreSQL advisory lock claim pattern.

Advisory locks prevent duplicate processing without the complexity of
row-level locking or external message brokers. Each business_id's UUID
is converted to a pair of int32 values used as the advisory lock key.

Usage:
    from domain_pipeline.queue import enqueue, claim_batch, complete, fail

    # Enqueue businesses for a verification layer
    enqueue(session, business_ids, layer="domain_guess")

    # Worker claims a batch (returns list of VerificationQueueItem)
    items = claim_batch(session, layer="domain_guess", limit=50, worker_id="worker-1")

    # After processing
    complete(session, item_id)         # success
    fail(session, item_id, "error")    # failure
"""
from __future__ import annotations

import logging
import struct
import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func, select, text, update
from sqlalchemy.orm import Session

from .models import VerificationQueueItem

logger = logging.getLogger(__name__)

# Verification layer ordering (lower = higher priority)
LAYER_ORDER = {
    "domain_guess": 0,
    "searxng": 1,
    "llm": 2,
    "ddg": 3,
    "google_search": 4,
}


def _uuid_to_lock_key(business_id: uuid.UUID) -> tuple[int, int]:
    """Convert a UUID to a pair of int32 values for pg_try_advisory_xact_lock.

    Uses the first 8 bytes of the UUID, split into two 4-byte signed integers.
    """
    b = business_id.bytes[:8]
    key1 = struct.unpack(">i", b[:4])[0]
    key2 = struct.unpack(">i", b[4:8])[0]
    return key1, key2


def enqueue(
    session: Session,
    business_ids: list[uuid.UUID],
    layer: str,
    priority: Optional[int] = None,
) -> int:
    """Add businesses to the verification queue for a given layer.

    Skips businesses that already have a pending/claimed item for the same layer.
    Returns number of items enqueued.
    """
    if not business_ids:
        return 0

    if priority is None:
        priority = LAYER_ORDER.get(layer, 5)

    # Find businesses already queued for this layer (pending or claimed)
    existing = set(
        row[0]
        for row in session.execute(
            select(VerificationQueueItem.business_id).where(
                VerificationQueueItem.business_id.in_(business_ids),
                VerificationQueueItem.layer == layer,
                VerificationQueueItem.status.in_(["pending", "claimed"]),
            )
        ).all()
    )

    new_items = []
    for bid in business_ids:
        if bid not in existing:
            new_items.append(
                VerificationQueueItem(
                    business_id=bid,
                    layer=layer,
                    priority=priority,
                )
            )

    if new_items:
        session.add_all(new_items)
        session.flush()

    return len(new_items)


def claim_batch(
    session: Session,
    layer: str,
    limit: int = 50,
    worker_id: str = "default",
) -> list[VerificationQueueItem]:
    """Claim a batch of pending queue items using advisory locks.

    For each candidate item, attempts to acquire a transaction-scoped
    advisory lock on the business_id. If successful, the item is marked
    as 'claimed'. Items that can't be locked (claimed by another worker)
    are skipped.

    Returns list of claimed items (caller should process then call complete/fail).
    """
    now = datetime.now(timezone.utc)

    # Get candidate items ordered by priority and creation time
    candidates = session.execute(
        select(VerificationQueueItem)
        .where(
            VerificationQueueItem.layer == layer,
            VerificationQueueItem.status == "pending",
        )
        .order_by(
            VerificationQueueItem.priority.asc(),
            VerificationQueueItem.created_at.asc(),
        )
        .limit(limit * 2)  # Fetch extra in case some are locked
    ).scalars().all()

    claimed = []
    for item in candidates:
        if len(claimed) >= limit:
            break

        # Try to acquire advisory lock (transaction-scoped, auto-released on commit/rollback)
        key1, key2 = _uuid_to_lock_key(item.business_id)
        locked = session.execute(
            text("SELECT pg_try_advisory_xact_lock(:key1, :key2)"),
            {"key1": key1, "key2": key2},
        ).scalar()

        if locked:
            item.status = "claimed"
            item.claimed_at = now
            item.claimed_by = worker_id
            item.attempts += 1
            claimed.append(item)

    if claimed:
        session.flush()

    return claimed


def complete(session: Session, item_id: uuid.UUID) -> None:
    """Mark a queue item as completed."""
    session.execute(
        update(VerificationQueueItem)
        .where(VerificationQueueItem.id == item_id)
        .values(
            status="completed",
            completed_at=datetime.now(timezone.utc),
        )
    )


def fail(session: Session, item_id: uuid.UUID, error: str) -> None:
    """Mark a queue item as failed."""
    session.execute(
        update(VerificationQueueItem)
        .where(VerificationQueueItem.id == item_id)
        .values(
            status="failed",
            error=error[:1000],
            completed_at=datetime.now(timezone.utc),
        )
    )


def requeue_failed(session: Session, layer: Optional[str] = None, max_attempts: int = 3) -> int:
    """Re-queue failed items that haven't exceeded max attempts."""
    query = (
        update(VerificationQueueItem)
        .where(
            VerificationQueueItem.status == "failed",
            VerificationQueueItem.attempts < max_attempts,
        )
        .values(
            status="pending",
            claimed_at=None,
            claimed_by=None,
            error=None,
        )
    )
    if layer:
        query = query.where(VerificationQueueItem.layer == layer)

    result = session.execute(query)
    return result.rowcount


def queue_depth(session: Session, layer: Optional[str] = None) -> dict[str, int]:
    """Get queue depth by status, optionally filtered by layer."""
    query = select(
        VerificationQueueItem.status,
        func.count(VerificationQueueItem.id),
    ).group_by(VerificationQueueItem.status)

    if layer:
        query = query.where(VerificationQueueItem.layer == layer)

    rows = session.execute(query).all()
    return {status: count for status, count in rows}


def cleanup_completed(session: Session, older_than_hours: int = 24) -> int:
    """Delete completed queue items older than the specified hours."""
    from datetime import timedelta
    from sqlalchemy import delete

    cutoff = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
    result = session.execute(
        delete(VerificationQueueItem).where(
            VerificationQueueItem.status == "completed",
            VerificationQueueItem.completed_at < cutoff,
        )
    )
    return result.rowcount


def enqueue_stale_businesses(
    session: Session,
    stale_days: int = 90,
    limit: int = 200,
) -> int:
    """Re-queue businesses whose last verification is older than stale_days.

    Targets businesses that have been scored (scored_at is set) but whose
    scored_at timestamp is older than the threshold. Enqueues them for
    the first verification layer (domain_guess) to re-enter the pipeline.

    Returns number of businesses enqueued.
    """
    from datetime import timedelta
    from .models import Business

    cutoff = datetime.now(timezone.utc) - timedelta(days=stale_days)

    # Find businesses scored before the cutoff, not already in queue
    already_queued = (
        select(VerificationQueueItem.business_id)
        .where(VerificationQueueItem.status.in_(["pending", "claimed"]))
        .correlate(VerificationQueueItem)
    )

    stale_biz_ids = session.execute(
        select(Business.id)
        .where(
            Business.scored_at.isnot(None),
            Business.scored_at < cutoff,
            Business.id.notin_(already_queued),
        )
        .order_by(Business.scored_at.asc())
        .limit(limit)
    ).scalars().all()

    if not stale_biz_ids:
        return 0

    return enqueue(session, stale_biz_ids, layer="domain_guess", priority=10)
