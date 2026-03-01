"""Integration tests for all GET API endpoints.

Tests /api/metrics, /api/jobs, /api/leads/business/categories,
/api/leads/business/cities.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from domain_pipeline.models import Business, City, JobRun


def utc_now():
    return datetime.now(timezone.utc)


def _make_business(db: Session, name: str, score: float, city_id, category: str = "trades", website_url=None, raw=None, verification_confidence=None):
    row = Business(
        source="osm",
        source_id=f"seed-{name.lower().replace(' ', '-')}",
        name=name,
        category=category,
        website_url=website_url,
        lead_score=score,
        scored_at=utc_now(),
        city_id=city_id,
        raw=raw,
        verification_confidence=verification_confidence,
    )
    db.add(row)
    db.flush()
    return row


class TestMetricsEndpoint:
    def test_metrics_returns_200_empty_db(self, client):
        resp = client.get("/api/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert "businesses" in data
        assert "domains" in data
        assert "contacts" in data
        assert "exports" in data
        assert "verification" in data
        assert "confidence_distribution" in data
        assert "recent_jobs" in data

    def test_metrics_counts_businesses(self, client, db_session: Session, city):
        _make_business(db_session, "Biz A", 50, city.id)
        _make_business(db_session, "Biz B", 30, city.id)
        _make_business(db_session, "Has Website", 0, city.id, website_url="https://example.com")
        db_session.commit()

        resp = client.get("/api/metrics")
        data = resp.json()
        assert data["businesses"]["total"] == 3
        assert data["businesses"]["no_website"] == 2
        assert data["businesses"]["scored"] == 3

    def test_metrics_confidence_distribution(self, client, db_session: Session, city):
        _make_business(db_session, "Verified Biz", 50, city.id, raw={
            "domain_guess_verified": True,
            "domain_guess_result": "no_match",
            "searxng_verified": True,
            "searxng_result": "no_website",
        }, verification_confidence="high")
        _make_business(db_session, "Unverified Biz", 30, city.id, raw={}, verification_confidence=None)
        db_session.commit()

        resp = client.get("/api/metrics")
        data = resp.json()
        dist = data["confidence_distribution"]
        assert dist["high"] >= 1
        assert dist["unverified"] >= 1


class TestJobsEndpoint:
    def test_jobs_returns_200_empty(self, client):
        resp = client.get("/api/jobs")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_jobs_returns_recent_jobs(self, client, db_session: Session):
        job = JobRun(
            job_name="test_job",
            status="completed",
            processed_count=42,
        )
        db_session.add(job)
        db_session.commit()

        resp = client.get("/api/jobs")
        data = resp.json()
        assert len(data) == 1
        assert data[0]["job_name"] == "test_job"
        assert data[0]["status"] == "completed"
        assert data[0]["processed_count"] == 42

    def test_jobs_limit_param(self, client, db_session: Session):
        for i in range(5):
            db_session.add(JobRun(job_name=f"job_{i}", status="completed", processed_count=i))
        db_session.commit()

        resp = client.get("/api/jobs", params={"limit": 2})
        data = resp.json()
        assert len(data) == 2

    def test_jobs_limit_validation(self, client):
        resp = client.get("/api/jobs", params={"limit": 0})
        assert resp.status_code == 422


class TestCategoriesEndpoint:
    def test_categories_empty_db(self, client):
        resp = client.get("/api/leads/business/categories")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_categories_returns_unique_sorted(self, client, db_session: Session, city):
        _make_business(db_session, "Biz A", 50, city.id, category="trades")
        _make_business(db_session, "Biz B", 40, city.id, category="retail")
        _make_business(db_session, "Biz C", 30, city.id, category="trades")  # duplicate
        _make_business(db_session, "Biz D", 20, city.id, category="auto")
        db_session.commit()

        resp = client.get("/api/leads/business/categories")
        data = resp.json()
        assert data == ["auto", "retail", "trades"]


class TestCitiesEndpoint:
    def test_cities_empty_db(self, client):
        resp = client.get("/api/leads/business/cities")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_cities_returns_city_names(self, client, db_session: Session):
        db_session.add(City(name="Abu Dhabi", country="AE"))
        db_session.add(City(name="Dubai", country="AE"))
        db_session.add(City(name="Sharjah", country="AE"))
        db_session.commit()

        resp = client.get("/api/leads/business/cities")
        data = resp.json()
        assert "Abu Dhabi" in data
        assert "Dubai" in data
        assert "Sharjah" in data

    def test_cities_limit_param(self, client, db_session: Session):
        for i in range(5):
            db_session.add(City(name=f"City {i}", country="AE"))
        db_session.commit()

        resp = client.get("/api/leads/business/cities", params={"limit": 2})
        data = resp.json()
        assert len(data) == 2


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] in ("ok", "degraded")
        assert "pool" in data
        assert "checks" in data
        assert "database" in data["checks"]
        assert data["checks"]["database"]["status"] == "ok"
        assert "searxng" in data["checks"]
        assert "api_keys" in data["checks"]
        assert isinstance(data["checks"]["api_keys"], dict)


class TestAutomationStatusEndpoint:
    def test_automation_status_returns_200(self, client):
        resp = client.get("/api/automation/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "running" in data
        assert "verification" in data
