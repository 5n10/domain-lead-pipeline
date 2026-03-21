"""Queue-aware batch runner for verification workers.

Wraps existing workers with queue claim/complete/fail semantics.
Workers continue to use their existing DB queries internally,
but the queue tracks what has been processed and prevents duplicates.

Usage (in verification loop or standalone script):

    from domain_pipeline.queue_runner import run_queued_batch

    result = run_queued_batch("domain_guess", limit=200, min_score=0.0)
"""
from __future__ import annotations

import logging
from typing import Any, Callable

from .db import session_scope
from .queue import (
    claim_batch,
    complete,
    enqueue_stale_businesses,
    fail,
    queue_depth,
    requeue_failed,
)

logger = logging.getLogger(__name__)


def run_queued_batch(
    layer: str,
    limit: int = 50,
    min_score: float = 0.0,
    worker_id: str = "default",
) -> dict[str, Any]:
    """Run a verification batch using the queue.

    1. Claims items from the queue for the given layer.
    2. Runs the appropriate worker's run_batch (which queries businesses directly).
    3. Marks queue items as completed or failed.

    This is an incremental adoption path — the queue tracks progress
    while workers still operate via their existing DB queries.
    """
    with session_scope() as session:
        items = claim_batch(session, layer=layer, limit=limit, worker_id=worker_id)

    if not items:
        return {"layer": layer, "claimed": 0, "processed": 0}

    # Run the actual worker
    worker_fn = _get_worker(layer)
    try:
        result = worker_fn(limit=limit, min_score=min_score)
        processed = result.get("processed", 0) if isinstance(result, dict) else 0

        # Mark all claimed items as completed
        with session_scope() as session:
            for item in items:
                complete(session, item.id)

        return {
            "layer": layer,
            "claimed": len(items),
            "processed": processed,
            "worker_result": result,
        }
    except Exception as e:
        # Mark all claimed items as failed
        error_msg = str(e)[:500]
        with session_scope() as session:
            for item in items:
                fail(session, item.id, error_msg)
        logger.exception("Queue batch failed for layer %s: %s", layer, e)
        return {
            "layer": layer,
            "claimed": len(items),
            "processed": 0,
            "error": error_msg,
        }


def _get_worker(layer: str) -> Callable:
    """Lazy-import the worker function for a given layer."""
    if layer == "domain_guess":
        from .workers.domain_guess import run_batch
        return run_batch
    elif layer == "searxng":
        from .workers.searxng_verify import run_batch
        return run_batch
    elif layer == "llm":
        from .workers.llm_verify import run_batch
        return run_batch
    elif layer == "ddg":
        from .workers.web_search_verify import run_batch
        return run_batch
    elif layer == "google_search":
        from .workers.google_search_verify import run_batch
        return run_batch
    else:
        raise ValueError(f"Unknown verification layer: {layer}")


def maintain_queue(stale_days: int = 90, stale_limit: int = 200) -> dict[str, Any]:
    """Periodic queue maintenance: requeue failed, enqueue stale, get stats.

    Call this periodically (e.g. once per hour) to keep the queue healthy.
    """
    with session_scope() as session:
        requeued = requeue_failed(session)
        stale_enqueued = enqueue_stale_businesses(session, stale_days=stale_days, limit=stale_limit)
        depth = queue_depth(session)

    return {
        "requeued_failed": requeued,
        "stale_enqueued": stale_enqueued,
        "queue_depth": depth,
    }
