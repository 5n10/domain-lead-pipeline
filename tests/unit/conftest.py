"""Shared fixtures for unit tests.

These fixtures create in-memory model instances without requiring a database.
They use simple mock objects or detached SQLAlchemy instances.

Overrides the root conftest's autouse _bind_test_db fixture so unit tests
can run without DOMAIN_PIPELINE_TEST_DATABASE_URL being set.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from domain_pipeline.models import Business, City, Domain


@pytest.fixture(autouse=True)
def _bind_test_db():
    """No-op override: unit tests don't need a real database."""
    yield


def _make_uuid() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def make_business():
    """Factory fixture to create detached Business instances for unit tests."""

    def _factory(
        name: str = "Test Business",
        category: str = "trades",
        website_url: str | None = None,
        lead_score: float | None = None,
        raw: dict | None = None,
        source: str = "osm",
        source_id: str | None = None,
        city_id: uuid.UUID | None = None,
    ) -> Business:
        return Business(
            id=_make_uuid(),
            source=source,
            source_id=source_id or f"test-{uuid.uuid4().hex[:8]}",
            name=name,
            category=category,
            website_url=website_url,
            lead_score=lead_score,
            raw=raw,
            city_id=city_id,
        )

    return _factory


@pytest.fixture
def make_city():
    """Factory fixture to create detached City instances."""

    def _factory(
        name: str = "Dubai",
        country: str = "AE",
    ) -> City:
        return City(
            id=_make_uuid(),
            name=name,
            country=country,
        )

    return _factory


@pytest.fixture
def make_domain():
    """Factory fixture to create detached Domain instances."""

    def _factory(
        domain: str = "example.com",
        status: str = "new",
    ) -> Domain:
        return Domain(
            id=_make_uuid(),
            domain=domain,
            status=status,
        )

    return _factory


@pytest.fixture
def empty_features():
    """Return a feature dict with all empty sets (no contacts, no domains)."""
    return {
        "emails": set(),
        "business_emails": set(),
        "free_emails": set(),
        "phones": set(),
        "domains": set(),
        "verified_unhosted_domains": set(),
        "unregistered_domains": set(),
        "hosted_domains": set(),
        "parked_domains": set(),
        "registered_domains": set(),
        "unknown_domains": set(),
        "domain_status_counts": {},
    }
