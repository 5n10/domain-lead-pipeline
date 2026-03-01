from __future__ import annotations

import logging
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from .config import load_config
from .pool_monitor import attach_pool_listeners, get_pool_stats


logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


_config = load_config()
_engine = create_engine(
    _config.database_url,
    pool_pre_ping=True,
    pool_size=10,       # More connections for concurrent verification threads
    max_overflow=20,    # Allow bursts up to 30 total connections
    pool_recycle=3600,  # Recycle connections after 1 hour to prevent stale connections
    pool_timeout=30,    # Timeout after 30 seconds waiting for a connection from the pool
)
attach_pool_listeners(_engine)
SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False)


def pool_stats() -> dict:
    """Return current connection pool statistics."""
    return get_pool_stats(_engine)


@contextmanager
def session_scope():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        logger.error("Database transaction failed, rolling back: %s", e)
        session.rollback()
        raise
    finally:
        session.close()
