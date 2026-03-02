"""Integration tests for enrichment API endpoints.

Tests enrich-google-places, enrich-foursquare, hunter-enrich.
"""
from __future__ import annotations


def _enable_bypass(monkeypatch):
    monkeypatch.setenv("MUTATION_LOCALHOST_BYPASS", "true")
    monkeypatch.setenv("MUTATION_API_KEY", "test-secret")
    from domain_pipeline.config_manager import reload_config
    reload_config()


def test_enrich_google_places(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/enrich-google-places", json={"limit": 1})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
    # Either "processed" or "error" (if no API key)
    assert "processed" in data or "error" in data


def test_enrich_google_places_with_priority(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/enrich-google-places", json={"limit": 1, "priority": "all"})
    assert resp.status_code == 200


def test_enrich_foursquare(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/enrich-foursquare", json={"limit": 1})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
    assert "processed" in data or "error" in data


def test_hunter_enrich(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/hunter-enrich", json={"limit": 1})
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, dict)
    assert "processed" in data or "error" in data


def test_enrich_rejects_invalid_limit(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/actions/enrich-google-places", json={"limit": 0})
    assert resp.status_code == 422


def test_enrichment_requires_auth(client, monkeypatch):
    monkeypatch.setenv("MUTATION_LOCALHOST_BYPASS", "false")
    monkeypatch.setenv("MUTATION_API_KEY", "test-secret")
    from domain_pipeline.config_manager import reload_config
    reload_config()
    resp = client.post("/api/actions/enrich-google-places", json={"limit": 1})
    assert resp.status_code == 401
