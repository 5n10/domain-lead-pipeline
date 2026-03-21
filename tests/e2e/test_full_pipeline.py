"""End-to-end smoke test: import businesses through the full pipeline.

Tests the complete flow using the API:
1. Health check passes
2. Insert seed businesses into the database
3. Score businesses via API
4. Fetch leads via API and verify scored results
5. Export leads via API
6. Verify metrics reflect the changes
7. Check business detail endpoint

Requires DOMAIN_PIPELINE_TEST_DATABASE_URL to be set.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy.orm import Session

from domain_pipeline.models import Business, City

E2E_API_KEY = "e2e-test-key-12345"


@pytest.fixture(autouse=True)
def _set_mutation_key(monkeypatch):
    """Ensure mutation API key is set and config cache is cleared."""
    monkeypatch.setenv("MUTATION_API_KEY", E2E_API_KEY)
    import domain_pipeline.config_manager as cm
    monkeypatch.setattr(cm, "_cached_config", None)
    yield
    monkeypatch.setattr(cm, "_cached_config", None)


def utc_now():
    return datetime.now(timezone.utc)


SEED_BUSINESSES = [
    {"name": "Ahmed's Plumbing", "category": "trades", "website_url": None},
    {"name": "Dubai Auto Repair", "category": "auto", "website_url": None},
    {"name": "Golden Bakery", "category": "food", "website_url": None},
    {"name": "Al Noor Electronics", "category": "retail", "website_url": None},
    {"name": "Desert Landscaping", "category": "trades", "website_url": None},
    {"name": "Marina Dental Clinic", "category": "health", "website_url": None},
    {"name": "Star Cleaning Services", "category": "services", "website_url": None},
    {"name": "Palm Auto Glass", "category": "auto", "website_url": None},
    {"name": "Sunrise Restaurant", "category": "food", "website_url": "https://sunrise-restaurant.com"},
    {"name": "City Gym", "category": "fitness", "website_url": "https://citygym.ae"},
]


def _seed_database(db: Session) -> tuple[City, list[Business]]:
    """Insert a city and 10 test businesses."""
    city = City(name="Dubai", country="AE")
    db.add(city)
    db.flush()

    businesses = []
    for i, biz_data in enumerate(SEED_BUSINESSES):
        row = Business(
            source="osm",
            source_id=f"e2e-seed-{i}",
            name=biz_data["name"],
            category=biz_data["category"],
            website_url=biz_data["website_url"],
            city_id=city.id,
            raw={"osm_id": f"node/{1000 + i}", "tags": {"name": biz_data["name"]}},
        )
        db.add(row)
        businesses.append(row)

    db.commit()
    return city, businesses


class TestFullPipelineSmoke:
    """Full pipeline smoke test: seed -> score -> leads -> export -> metrics."""

    def test_health_check(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] in ("ok", "degraded")
        assert data["checks"]["database"]["status"] == "ok"

    def _post(self, client, url, **kwargs):
        """POST with mutation API key header."""
        headers = kwargs.pop("headers", {})
        headers["X-API-Key"] = E2E_API_KEY
        return client.post(url, headers=headers, **kwargs)

    def test_full_pipeline_flow(self, client, db_session: Session):
        # ── 1. Seed database ──────────────────────────────────────
        city, businesses = _seed_database(db_session)
        no_website_ids = [str(b.id) for b in businesses if b.website_url is None]
        assert len(businesses) == 10
        assert len(no_website_ids) == 8

        # ── 2. Check metrics before scoring ───────────────────────
        resp = client.get("/api/metrics")
        assert resp.status_code == 200
        metrics = resp.json()
        assert metrics["businesses"]["total"] == 10
        assert metrics["businesses"]["no_website"] == 8

        # ── 3. Score businesses via API ───────────────────────────
        resp = self._post(client, "/api/actions/business-score", json={
            "limit": 100,
            "force_rescore": False,
        })
        assert resp.status_code == 200
        score_result = resp.json()
        assert score_result["processed"] >= 8  # At least 8 no-website businesses

        # ── 4. Verify leads now appear with scores ────────────────
        resp = client.get("/api/leads/business", params={
            "limit": 50,
            "require_no_website": True,
        })
        assert resp.status_code == 200
        leads = resp.json()
        assert leads["total_candidates"] >= 1
        assert leads["returned"] >= 1

        # Check that returned leads have scores
        for lead in leads["items"]:
            assert lead["lead_score"] is not None
            assert lead["name"] is not None

        # ── 5. Check categories endpoint ──────────────────────────
        resp = client.get("/api/leads/business/categories")
        assert resp.status_code == 200
        categories = resp.json()
        assert "trades" in categories
        assert "auto" in categories

        # ── 6. Check cities endpoint ──────────────────────────────
        resp = client.get("/api/leads/business/cities")
        assert resp.status_code == 200
        cities = resp.json()
        assert "Dubai" in cities

        # ── 7. Export businesses via API ──────────────────────────
        resp = self._post(client, "/api/actions/business-export", json={
            "platform": "e2e_test",
            "min_score": 0,
            "require_contact": False,
            "require_unhosted_domain": False,
            "require_domain_qualification": False,
        })
        assert resp.status_code == 200
        export_result = resp.json()
        assert "path" in export_result

        # ── 8. Check business detail for first lead ───────────────
        first_biz_id = str(businesses[0].id)
        resp = client.get(f"/api/leads/business/{first_biz_id}")
        assert resp.status_code == 200
        detail = resp.json()
        assert detail["name"] == "Ahmed's Plumbing"
        assert detail["category"] == "trades"
        assert "score_reasons" in detail
        assert "verification_timeline" in detail

        # ── 9. Verify metrics updated post-score ──────────────────
        resp = client.get("/api/metrics")
        assert resp.status_code == 200
        metrics_after = resp.json()
        assert metrics_after["businesses"]["scored"] >= 8

        # ── 10. Check jobs endpoint records our actions ───────────
        resp = client.get("/api/jobs", params={"limit": 10})
        assert resp.status_code == 200
        jobs = resp.json()
        job_names = [j["job_name"] for j in jobs]
        assert "score_business_leads" in job_names
