from __future__ import annotations

import os
from threading import Lock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

import domain_pipeline.models  # noqa: F401
from domain_pipeline.db import Base
import domain_pipeline.db as db_module
import domain_pipeline.api as api_module
from domain_pipeline.models import City


class _AutomationStub:
    auto_start_enabled = False

    def __init__(self):
        self._run_lock = Lock()

    def start(self, updates=None):
        return self.status()

    def stop(self):
        return self.status()

    def run_now(self):
        return {"trigger": "manual", "busy": False}

    def run_daily_target_now(self):
        return {"ok": True}

    def update_settings(self, updates):
        return None

    def start_verification(self, updates=None):
        return self.status()

    def stop_verification(self):
        return self.status()

    def update_verify_settings(self, updates):
        return None

    def status(self):
        return {
            "running": False,
            "busy": False,
            "settings": {},
            "last_run_started_at": None,
            "last_run_finished_at": None,
            "last_error": None,
            "last_result": None,
            "run_count": 0,
            "verification": {
                "running": False,
                "settings": {},
                "last_started_at": None,
                "last_finished_at": None,
                "last_error": None,
                "batch_count": 0,
                "totals": {},
            },
        }


@pytest.fixture(scope="session")
def test_database_url() -> str:
    url = os.getenv("DOMAIN_PIPELINE_TEST_DATABASE_URL")
    if not url:
        pytest.skip("DOMAIN_PIPELINE_TEST_DATABASE_URL is not set")
    return url


@pytest.fixture(scope="session")
def test_engine(test_database_url: str):
    engine = create_engine(test_database_url, pool_pre_ping=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS citext"))
    yield engine
    engine.dispose()


@pytest.fixture(autouse=True)
def _bind_test_db(monkeypatch, test_engine):
    TestSessionLocal = sessionmaker(bind=test_engine, expire_on_commit=False)
    monkeypatch.setattr(db_module, "_engine", test_engine, raising=False)
    monkeypatch.setattr(db_module, "SessionLocal", TestSessionLocal, raising=False)
    Base.metadata.drop_all(bind=test_engine)
    Base.metadata.create_all(bind=test_engine)
    yield
    Base.metadata.drop_all(bind=test_engine)


@pytest.fixture
def db_session() -> Session:
    session = db_module.SessionLocal()
    try:
        yield session
        session.commit()
    finally:
        session.close()


@pytest.fixture
def city(db_session: Session) -> City:
    row = City(name="Dubai", country="AE")
    db_session.add(row)
    db_session.flush()
    return row


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(api_module, "automation_controller", _AutomationStub(), raising=True)
    app = api_module.create_app()
    with TestClient(app) as test_client:
        yield test_client
