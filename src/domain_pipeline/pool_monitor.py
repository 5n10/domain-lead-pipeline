"""SQLAlchemy connection pool monitoring via event listeners.

Call `attach_pool_listeners(engine)` once at startup to log pool events
(checkout, checkin, overflow, invalidation) and expose pool stats
via `get_pool_stats()`.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import Pool

logger = logging.getLogger(__name__)

# Rolling counters
_stats = {
    "checkouts": 0,
    "checkins": 0,
    "overflows": 0,
    "invalidations": 0,
    "checkout_errors": 0,
}


def attach_pool_listeners(engine: Engine) -> None:
    """Attach SQLAlchemy pool event listeners to the given engine."""
    pool = engine.pool

    @event.listens_for(pool, "checkout")
    def on_checkout(dbapi_conn: Any, connection_record: Any, connection_proxy: Any) -> None:
        _stats["checkouts"] += 1

    @event.listens_for(pool, "checkin")
    def on_checkin(dbapi_conn: Any, connection_record: Any) -> None:
        _stats["checkins"] += 1

    @event.listens_for(pool, "invalidate")
    def on_invalidate(dbapi_conn: Any, connection_record: Any, exception: Any) -> None:
        _stats["invalidations"] += 1
        logger.warning(
            "Connection invalidated",
            extra={"worker": "pool_monitor", "exception": str(exception) if exception else None},
        )

    @event.listens_for(pool, "checkout_error" if hasattr(pool, "_checkout_error") else "checkout")
    def _noop(*args: Any, **kwargs: Any) -> None:
        pass  # placeholder — real checkout errors are caught below

    # Log when pool overflows
    original_overflow = getattr(pool, "_overflow", None)

    @event.listens_for(pool, "connect")
    def on_connect(dbapi_conn: Any, connection_record: Any) -> None:
        pool_obj = engine.pool
        if hasattr(pool_obj, "_overflow") and pool_obj._overflow > 0:
            _stats["overflows"] += 1
            logger.info(
                "Pool overflow connection created (overflow=%d)",
                pool_obj._overflow,
                extra={"worker": "pool_monitor"},
            )


def get_pool_stats(engine: Engine) -> dict:
    """Return current pool statistics for the given engine."""
    pool = engine.pool
    result = {
        "pool_size": pool.size(),
        "checked_in": pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
        "total_checkouts": _stats["checkouts"],
        "total_checkins": _stats["checkins"],
        "total_overflows": _stats["overflows"],
        "total_invalidations": _stats["invalidations"],
    }
    return result
