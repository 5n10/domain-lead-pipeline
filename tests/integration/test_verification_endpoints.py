"""Integration tests for verification action API endpoints.

Tests domain-guess, verify-websites, verify-ddg, verify-llm,
verify-google-search, verify-searxng, verify-foursquare.
"""
from __future__ import annotations


def _enable_bypass(monkeypatch):
    monkeypatch.setenv("MUTATION_LOCALHOST_BYPASS", "true")
    monkeypatch.setenv("MUTATION_API_KEY", "test-secret")
    from domain_pipeline.config_manager import reload_config
    reload_config()


def test_domain_guess_empty_db(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/domain-guess", json={"limit": 1})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
    assert "processed" in data


def test_verify_websites_google_places(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/verify-websites", json={"limit": 1})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)


def test_verify_websites_ddg_empty_db(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/verify-websites-ddg", json={"limit": 1})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
    assert "processed" in data


def test_verify_websites_llm_empty_db(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/verify-websites-llm", json={"limit": 1})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
    assert "processed" in data


def test_verify_websites_google_search_empty_db(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/verify-websites-google-search", json={"limit": 1})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
    assert "processed" in data


def test_verify_websites_searxng_empty_db(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/verify-websites-searxng", json={"limit": 1})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
    assert "processed" in data


def test_verify_websites_foursquare(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/verify-websites-foursquare", json={"limit": 1})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)


def test_domain_guess_rejects_invalid_limit(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/domain-guess", json={"limit": -1})
    assert resp.status_code == 422


def test_verify_ddg_with_min_score(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/verify-websites-ddg", json={"limit": 5, "min_score": 50.0})
    assert resp.status_code == 200
