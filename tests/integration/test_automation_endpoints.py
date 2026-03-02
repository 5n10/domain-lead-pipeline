"""Integration tests for automation API endpoints.

Tests status, start, stop, run-now, daily-target-now, settings,
start-verification, stop-verification, verification-settings.
"""
from __future__ import annotations


def _enable_bypass(monkeypatch):
    monkeypatch.setenv("MUTATION_LOCALHOST_BYPASS", "true")
    monkeypatch.setenv("MUTATION_API_KEY", "test-secret")
    from domain_pipeline.config_manager import reload_config
    reload_config()


def test_automation_status(client):
    resp = client.get("/api/automation/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "running" in data
    assert "busy" in data
    assert "verification" in data


def test_automation_start(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/automation/start")
    assert resp.status_code == 200
    data = resp.json()
    assert "running" in data


def test_automation_stop(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/automation/stop")
    assert resp.status_code == 200
    data = resp.json()
    assert "running" in data


def test_automation_run_now(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/automation/run-now")
    assert resp.status_code == 200
    data = resp.json()
    assert "trigger" in data


def test_automation_daily_target_now(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/automation/daily-target-now")
    assert resp.status_code == 200
    data = resp.json()
    assert "ok" in data


def test_automation_settings(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/automation/settings", json={"interval_seconds": 300})
    assert resp.status_code == 200


def test_automation_start_verification(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/automation/start-verification")
    assert resp.status_code == 200
    data = resp.json()
    assert "running" in data


def test_automation_stop_verification(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/automation/stop-verification")
    assert resp.status_code == 200
    data = resp.json()
    assert "running" in data


def test_automation_verification_settings(client, monkeypatch):
    _enable_bypass(monkeypatch)
    resp = client.post("/api/automation/verification-settings", json={"domain_guess_batch": 100})
    assert resp.status_code == 200


def test_automation_status_no_auth_required(client, monkeypatch):
    """GET /api/automation/status should not require auth."""
    monkeypatch.setenv("MUTATION_LOCALHOST_BYPASS", "false")
    monkeypatch.setenv("MUTATION_API_KEY", "test-secret")
    from domain_pipeline.config_manager import reload_config
    reload_config()
    resp = client.get("/api/automation/status")
    assert resp.status_code == 200


def test_automation_start_requires_auth(client, monkeypatch):
    monkeypatch.setenv("MUTATION_LOCALHOST_BYPASS", "false")
    monkeypatch.setenv("MUTATION_API_KEY", "test-secret")
    from domain_pipeline.config_manager import reload_config
    reload_config()
    resp = client.post("/api/automation/start")
    assert resp.status_code == 401
