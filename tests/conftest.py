from __future__ import annotations

import json
import os
from threading import Lock

import pytest
import responses
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
    # Disable rate limiting in tests
    app.state.limiter.enabled = False
    with TestClient(app) as test_client:
        yield test_client


# ---------------------------------------------------------------------------
# Shared HTTP mock fixtures for worker tests
# ---------------------------------------------------------------------------

SEARXNG_RESPONSE_WEBSITE_FOUND = {
    "results": [
        {
            "title": "Joe's Pizza - Official Site",
            "url": "https://joespizza.com",
            "content": "Welcome to Joe's Pizza, serving New York since 1975.",
            "engine": "duckduckgo",
            "engines": ["duckduckgo", "bing"],
        },
        {
            "title": "Joe's Pizza - Yelp",
            "url": "https://www.yelp.com/biz/joes-pizza-new-york",
            "content": "Joe's Pizza reviews.",
            "engine": "google",
            "engines": ["google"],
        },
    ],
}

SEARXNG_RESPONSE_NO_WEBSITE = {
    "results": [
        {
            "title": "Al Baraka Trading - Yelp",
            "url": "https://www.yelp.com/biz/al-baraka-trading",
            "content": "Al Baraka Trading reviews.",
            "engine": "duckduckgo",
            "engines": ["duckduckgo"],
        },
        {
            "title": "Al Baraka Trading - Facebook",
            "url": "https://www.facebook.com/albarakatrading",
            "content": "Al Baraka Trading page.",
            "engine": "bing",
            "engines": ["bing"],
        },
    ],
}

SEARXNG_RESPONSE_EMPTY = {"results": []}

GOOGLE_PLACES_RESPONSE = {
    "places": [
        {
            "displayName": {"text": "Joe's Pizza"},
            "formattedAddress": "123 Broadway, New York, NY",
            "websiteUri": "https://joespizza.com",
            "nationalPhoneNumber": "+1 212-555-1234",
            "types": ["restaurant"],
            "id": "ChIJ_test123",
        }
    ]
}

GOOGLE_PLACES_RESPONSE_NO_WEBSITE = {
    "places": [
        {
            "displayName": {"text": "Local Shop"},
            "formattedAddress": "456 Main St",
            "types": ["store"],
            "id": "ChIJ_test456",
        }
    ]
}

FOURSQUARE_SEARCH_RESPONSE = {
    "results": [
        {
            "fsq_id": "abc123",
            "name": "Joe's Pizza",
            "location": {"formatted_address": "123 Broadway, New York, NY"},
            "categories": [{"name": "Pizza Place"}],
            "tel": "+12125551234",
            "website": "https://joespizza.com",
        }
    ]
}

FOURSQUARE_SEARCH_RESPONSE_NO_WEBSITE = {
    "results": [
        {
            "fsq_id": "def456",
            "name": "Local Shop",
            "location": {"formatted_address": "456 Main St"},
            "categories": [{"name": "Shop"}],
        }
    ]
}

HUNTER_RESPONSE = {
    "data": {
        "domain": "joespizza.com",
        "organization": "Joe's Pizza",
        "emails": [
            {
                "value": "info@joespizza.com",
                "type": "generic",
                "confidence": 95,
                "first_name": None,
                "last_name": None,
            }
        ],
    }
}

DDG_HTML_RESULTS = """
<html><body>
<div class="result">
  <a class="result__a" href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fjoespizza.com">
    Joe's Pizza - Official Site
  </a>
  <a class="result__snippet">Welcome to Joe's Pizza.</a>
</div>
<div class="result">
  <a class="result__a" href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.yelp.com%2Fbiz%2Fjoes-pizza">
    Joe's Pizza - Yelp
  </a>
  <a class="result__snippet">Reviews for Joe's Pizza.</a>
</div>
</body></html>
"""

DDG_HTML_NO_RESULTS = "<html><body></body></html>"

LLM_RESPONSE_HAS_WEBSITE = {
    "choices": [
        {
            "message": {
                "content": json.dumps({
                    "status": "has_website",
                    "website_url": "https://joespizza.com",
                    "reason": "Search results show joespizza.com as the official website.",
                })
            }
        }
    ]
}

LLM_RESPONSE_NO_WEBSITE = {
    "choices": [
        {
            "message": {
                "content": json.dumps({
                    "status": "no_website",
                    "website_url": None,
                    "reason": "No official website found in search results.",
                })
            }
        }
    ]
}

LLM_RESPONSE_NOT_SURE = {
    "choices": [
        {
            "message": {
                "content": json.dumps({
                    "status": "not_sure",
                    "website_url": None,
                    "reason": "Insufficient evidence to determine.",
                })
            }
        }
    ]
}


@pytest.fixture
def mock_searxng_website_found():
    """Mock SearXNG returning results with a business website."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "http://localhost:8888/search",
            json=SEARXNG_RESPONSE_WEBSITE_FOUND,
            status=200,
        )
        yield rsps


@pytest.fixture
def mock_searxng_no_website():
    """Mock SearXNG returning results with only directory listings."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "http://localhost:8888/search",
            json=SEARXNG_RESPONSE_NO_WEBSITE,
            status=200,
        )
        yield rsps


@pytest.fixture
def mock_searxng_empty():
    """Mock SearXNG returning no results."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "http://localhost:8888/search",
            json=SEARXNG_RESPONSE_EMPTY,
            status=200,
        )
        yield rsps


@pytest.fixture
def mock_searxng_error():
    """Mock SearXNG returning a server error."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "http://localhost:8888/search",
            json={"error": "Internal server error"},
            status=500,
        )
        yield rsps


@pytest.fixture
def mock_ddg_results():
    """Mock DuckDuckGo HTML endpoint returning search results."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "https://html.duckduckgo.com/html/",
            body=DDG_HTML_RESULTS,
            status=200,
        )
        yield rsps


@pytest.fixture
def mock_ddg_empty():
    """Mock DuckDuckGo HTML endpoint returning no results."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "https://html.duckduckgo.com/html/",
            body=DDG_HTML_NO_RESULTS,
            status=200,
        )
        yield rsps


@pytest.fixture
def mock_ddg_rate_limited():
    """Mock DuckDuckGo HTML endpoint returning 429."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "https://html.duckduckgo.com/html/",
            status=429,
        )
        yield rsps


@pytest.fixture
def mock_google_places():
    """Mock Google Places API returning a result with website."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            "https://places.googleapis.com/v1/places:searchText",
            json=GOOGLE_PLACES_RESPONSE,
            status=200,
        )
        yield rsps


@pytest.fixture
def mock_google_places_no_website():
    """Mock Google Places API returning a result without website."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            "https://places.googleapis.com/v1/places:searchText",
            json=GOOGLE_PLACES_RESPONSE_NO_WEBSITE,
            status=200,
        )
        yield rsps


@pytest.fixture
def mock_foursquare():
    """Mock Foursquare API returning a result with website."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "https://api.foursquare.com/v3/places/search",
            json=FOURSQUARE_SEARCH_RESPONSE,
            status=200,
        )
        yield rsps


@pytest.fixture
def mock_foursquare_no_website():
    """Mock Foursquare API returning a result without website."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "https://api.foursquare.com/v3/places/search",
            json=FOURSQUARE_SEARCH_RESPONSE_NO_WEBSITE,
            status=200,
        )
        yield rsps


@pytest.fixture
def mock_hunter():
    """Mock Hunter API returning email results."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            "https://api.hunter.io/v2/domain-search",
            json=HUNTER_RESPONSE,
            status=200,
        )
        yield rsps


@pytest.fixture
def mock_llm_has_website():
    """Mock LLM API (OpenRouter/Groq) returning has_website verdict."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            "https://api.groq.com/openai/v1/chat/completions",
            json=LLM_RESPONSE_HAS_WEBSITE,
            status=200,
        )
        rsps.add(
            responses.POST,
            "https://openrouter.ai/api/v1/chat/completions",
            json=LLM_RESPONSE_HAS_WEBSITE,
            status=200,
        )
        yield rsps


@pytest.fixture
def mock_llm_no_website():
    """Mock LLM API returning no_website verdict."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            "https://api.groq.com/openai/v1/chat/completions",
            json=LLM_RESPONSE_NO_WEBSITE,
            status=200,
        )
        rsps.add(
            responses.POST,
            "https://openrouter.ai/api/v1/chat/completions",
            json=LLM_RESPONSE_NO_WEBSITE,
            status=200,
        )
        yield rsps


@pytest.fixture
def mock_llm_not_sure():
    """Mock LLM API returning not_sure verdict."""
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.POST,
            "https://api.groq.com/openai/v1/chat/completions",
            json=LLM_RESPONSE_NOT_SURE,
            status=200,
        )
        rsps.add(
            responses.POST,
            "https://openrouter.ai/api/v1/chat/completions",
            json=LLM_RESPONSE_NOT_SURE,
            status=200,
        )
        yield rsps
