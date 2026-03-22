"""Integration tests for the verification queue system.

Tests the full claim/process/complete cycle and queue maintenance.
Requires DOMAIN_PIPELINE_TEST_DATABASE_URL.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from domain_pipeline.models import Business, City, VerificationQueueItem
from domain_pipeline.queue import (
    claim_batch,
    cleanup_completed,
    complete,
    enqueue,
    fail,
    queue_depth,
    requeue_failed,
)


def _make_business(db: Session, name: str, city_id) -> Business:
    row = Business(
        source="osm",
        source_id=f"queue-test-{name.lower().replace(' ', '-')}",
        name=name,
        category="trades",
        city_id=city_id,
    )
    db.add(row)
    db.flush()
    return row


def test_enqueue_claim_complete_cycle(db_session: Session, city):
    """Full lifecycle: enqueue → claim → complete."""
    biz = _make_business(db_session, "Queue Test Biz", city.id)
    db_session.commit()

    # Enqueue
    count = enqueue(db_session, [biz.id], layer="domain_guess")
    db_session.commit()
    assert count == 1

    # Claim
    items = claim_batch(db_session, layer="domain_guess", limit=10, worker_id="test-worker")
    db_session.commit()
    assert len(items) == 1
    assert items[0].business_id == biz.id
    assert items[0].status == "claimed"
    assert items[0].claimed_by == "test-worker"

    # Complete
    complete(db_session, items[0].id)
    db_session.commit()

    item = db_session.execute(
        select(VerificationQueueItem).where(VerificationQueueItem.id == items[0].id)
    ).scalar_one()
    assert item.status == "completed"
    assert item.completed_at is not None


def test_enqueue_skips_duplicates(db_session: Session, city):
    """Enqueueing the same business+layer twice doesn't create duplicates."""
    biz = _make_business(db_session, "Dedup Biz", city.id)
    db_session.commit()

    first = enqueue(db_session, [biz.id], layer="searxng")
    db_session.commit()
    second = enqueue(db_session, [biz.id], layer="searxng")
    db_session.commit()

    assert first == 1
    assert second == 0


def test_enqueue_different_layers(db_session: Session, city):
    """Same business can be queued for different layers."""
    biz = _make_business(db_session, "Multi Layer Biz", city.id)
    db_session.commit()

    count1 = enqueue(db_session, [biz.id], layer="domain_guess")
    count2 = enqueue(db_session, [biz.id], layer="searxng")
    db_session.commit()

    assert count1 == 1
    assert count2 == 1


def test_claim_empty_queue(db_session: Session):
    """Claiming from an empty queue returns empty list."""
    items = claim_batch(db_session, layer="domain_guess", limit=10)
    assert items == []


def test_fail_and_requeue(db_session: Session, city):
    """Failed items can be requeued for retry."""
    biz = _make_business(db_session, "Fail Biz", city.id)
    db_session.commit()

    enqueue(db_session, [biz.id], layer="llm")
    db_session.commit()

    items = claim_batch(db_session, layer="llm", limit=10)
    db_session.commit()
    assert len(items) == 1

    fail(db_session, items[0].id, "test error")
    db_session.commit()

    item = db_session.execute(
        select(VerificationQueueItem).where(VerificationQueueItem.id == items[0].id)
    ).scalar_one()
    assert item.status == "failed"
    assert item.error == "test error"

    # Requeue
    requeued = requeue_failed(db_session, layer="llm")
    db_session.commit()
    assert requeued == 1

    item = db_session.execute(
        select(VerificationQueueItem).where(VerificationQueueItem.id == items[0].id)
    ).scalar_one()
    assert item.status == "pending"


def test_queue_depth(db_session: Session, city):
    """queue_depth returns counts by status."""
    biz1 = _make_business(db_session, "Depth Biz 1", city.id)
    biz2 = _make_business(db_session, "Depth Biz 2", city.id)
    db_session.commit()

    enqueue(db_session, [biz1.id, biz2.id], layer="ddg")
    db_session.commit()

    depth = queue_depth(db_session, layer="ddg")
    assert depth.get("pending", 0) == 2


def test_cleanup_completed(db_session: Session, city):
    """cleanup_completed removes old completed items."""
    biz = _make_business(db_session, "Cleanup Biz", city.id)
    db_session.commit()

    enqueue(db_session, [biz.id], layer="domain_guess")
    db_session.commit()

    items = claim_batch(db_session, layer="domain_guess", limit=10)
    db_session.commit()

    complete(db_session, items[0].id)
    db_session.commit()

    # With older_than_hours=0, everything completed should be cleaned
    deleted = cleanup_completed(db_session, older_than_hours=0)
    db_session.commit()
    assert deleted == 1


def test_claim_respects_limit(db_session: Session, city):
    """claim_batch respects the limit parameter."""
    businesses = [_make_business(db_session, f"Limit Biz {i}", city.id) for i in range(5)]
    db_session.commit()

    enqueue(db_session, [b.id for b in businesses], layer="searxng")
    db_session.commit()

    items = claim_batch(db_session, layer="searxng", limit=2, worker_id="test")
    db_session.commit()
    assert len(items) == 2
