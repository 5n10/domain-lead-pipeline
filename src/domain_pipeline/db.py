from __future__ import annotations

from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from .config import load_config


class Base(DeclarativeBase):
    pass


_config = load_config()
_engine = create_engine(_config.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False)


@contextmanager
def session_scope():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
