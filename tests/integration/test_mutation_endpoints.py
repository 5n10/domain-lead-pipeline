"""Integration tests for mutation API endpoints.

Tests pipeline-run, business-score, validate-domains, business-export,
reset-ddg-verification.
"""
from __future__ import annotations


def _enable_bypass(monkeypatch):
    monkeypatch.setenv("MUTATION_LOCALHOST_BYPASS", "true")
    monkeypatch.setenv("MUTATION_API_KEY", "test-secret")
    from domain_pipeline.config_manager import reload_config
    reload_config()


def test_business_score_empty_db(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/business-score", json={"limit": 1})
    assert resp.status_code == 200
    assert resp.json() == {"processed": 0}


def test_business_score_with_force_rescore(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/business-score", json={"limit": 1, "force_rescore": True})
    assert resp.status_code == 200
    assert "processed" in resp.json()


def test_validate_domains_empty_db(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/validate-domains", json={"limit": 1})
    assert resp.status_code == 200
    data = resp.json()
    assert "processed" in data


def test_business_export_empty_db(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/business-export", json={"platform": "test_export"})
    assert resp.status_code == 200
    data = resp.json()
    assert "path" in data or "rows_written" in data


def test_reset_ddg_verification_empty_db(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/reset-ddg-verification", json={"limit": 1})
    assert resp.status_code == 200
    data = resp.json()
    assert "reset_count" in data


def test_mutation_rejects_without_auth(client, monkeypatch):
    monkeypatch.setenv("MUTATION_LOCALHOST_BYPASS", "false")
    monkeypatch.setenv("MUTATION_API_KEY", "test-secret")
    from domain_pipeline.config_manager import reload_config
    reload_config()
    resp = client.post("/api/actions/business-score", json={"limit": 1})
    assert resp.status_code == 401


def test_mutation_accepts_api_key_header(client, monkeypatch):
    monkeypatch.setenv("MUTATION_LOCALHOST_BYPASS", "false")
    monkeypatch.setenv("MUTATION_API_KEY", "test-secret")
    from domain_pipeline.config_manager import reload_config
    reload_config()
    resp = client.post(
        "/api/actions/business-score",
        json={"limit": 1},
        headers={"X-API-Key": "test-secret"},
    )
    assert resp.status_code == 200


def test_mutation_accepts_bearer_token(client, monkeypatch):
    monkeypatch.setenv("MUTATION_LOCALHOST_BYPASS", "false")
    monkeypatch.setenv("MUTATION_API_KEY", "test-secret")
    from domain_pipeline.config_manager import reload_config
    reload_config()
    resp = client.post(
        "/api/actions/business-score",
        json={"limit": 1},
        headers={"Authorization": "Bearer test-secret"},
    )
    assert resp.status_code == 200


def test_pipeline_run_returns_200(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/pipeline-run", json={})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
