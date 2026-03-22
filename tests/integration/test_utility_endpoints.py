"""Integration tests for utility API endpoints.

Tests health, metrics, jobs, leads, dead-letter, duplicates,
exports, notifications, and daily report.
"""
from __future__ import annotations


def _enable_bypass(monkeypatch):
    monkeypatch.setenv("MUTATION_LOCALHOST_BYPASS", "true")
    monkeypatch.setenv("MUTATION_API_KEY", "test-secret")
    from domain_pipeline.config_manager import reload_config
    reload_config()


def test_health_endpoint(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"


def test_metrics_endpoint(client):
    resp = client.get("/api/metrics")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)


def test_jobs_endpoint(client):
    resp = client.get("/api/jobs")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


def test_leads_business_endpoint(client):
    resp = client.get("/api/leads/business")
    assert resp.status_code == 200
    data = resp.json()
    assert "items" in data
    assert "total_candidates" in data
    assert "returned" in data


def test_leads_business_categories(client):
    resp = client.get("/api/leads/business/categories")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


def test_leads_business_cities(client):
    resp = client.get("/api/leads/business/cities")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


def test_dead_letter_endpoint(client):
    resp = client.get("/api/dead-letter")
    assert resp.status_code == 200
    data = resp.json()
    assert "items" in data
    assert "total_count" in data


def test_duplicates_endpoint(client):
    resp = client.get("/api/duplicates")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)


def test_run_dedup(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/run-dedup")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)


def test_export_files_endpoint(client):
    resp = client.get("/api/exports/files")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


def test_export_nonexistent_file_returns_404(client):
    resp = client.get("/api/exports/files/nonexistent.csv")
    assert resp.status_code == 404


def test_test_notification(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/test-notification", json={"title": "Test", "message": "Hello"})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)


def test_daily_report(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/daily-report")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)


def test_leads_with_filters(client):
    resp = client.get(
        "/api/leads/business",
        params={
            "min_score": 50,
            "require_contact": "true",
            "require_domain_qualification": "false",
            "require_unhosted_domain": "false",
            "limit": 10,
        },
    )
    assert resp.status_code == 200


def test_health_includes_db_check(client):
    resp = client.get("/health")
    data = resp.json()
    assert "checks" in data
    assert "database" in data["checks"]


def test_export_google_sheets(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/export-google-sheets", json={})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
