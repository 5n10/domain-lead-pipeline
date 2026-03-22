"""Unit tests for Hunter.io email enrichment worker.

Tests HunterClient.domain_search() with mocked HTTP and client call counting.
No database required.
"""
from __future__ import annotations

import responses

from domain_pipeline.workers.hunter import DOMAIN_SEARCH_URL, HunterClient


class TestHunterClientDomainSearch:
    """Tests for HunterClient.domain_search() HTTP client."""

    @responses.activate
    def test_happy_path_returns_data(self):
        responses.add(
            responses.GET,
            "https://api.hunter.io/v2/domain-search",
            json={
                "data": {
                    "domain": "example.com",
                    "organization": "Example Inc",
                    "emails": [
                        {
                            "value": "info@example.com",
                            "type": "generic",
                            "confidence": 95,
                        }
                    ],
                }
            },
            status=200,
        )
        client = HunterClient(api_key="test-key")
        result = client.domain_search("example.com")
        assert result is not None
        assert result["domain"] == "example.com"
        assert len(result["emails"]) == 1
        assert client.calls_made == 1

    @responses.activate
    def test_no_emails_returns_empty_data(self):
        responses.add(
            responses.GET,
            "https://api.hunter.io/v2/domain-search",
            json={"data": {"domain": "noemail.com", "emails": []}},
            status=200,
        )
        client = HunterClient(api_key="test-key")
        result = client.domain_search("noemail.com")
        assert result is not None
        assert result["emails"] == []

    @responses.activate
    def test_rate_limited_returns_none(self):
        responses.add(
            responses.GET,
            "https://api.hunter.io/v2/domain-search",
            status=429,
        )
        client = HunterClient(api_key="test-key")
        result = client.domain_search("test.com")
        assert result is None
        assert client.calls_made == 1

    @responses.activate
    def test_quota_exhausted_returns_none(self):
        responses.add(
            responses.GET,
            "https://api.hunter.io/v2/domain-search",
            status=402,
        )
        client = HunterClient(api_key="test-key")
        result = client.domain_search("test.com")
        assert result is None

    @responses.activate
    def test_server_error_returns_none(self):
        responses.add(
            responses.GET,
            "https://api.hunter.io/v2/domain-search",
            status=500,
        )
        client = HunterClient(api_key="test-key")
        result = client.domain_search("test.com")
        assert result is None

    @responses.activate
    def test_connection_error_returns_none(self):
        import requests as req
        responses.add(
            responses.GET,
            "https://api.hunter.io/v2/domain-search",
            body=req.exceptions.ConnectionError("Connection refused"),
        )
        client = HunterClient(api_key="test-key")
        result = client.domain_search("test.com")
        assert result is None

    @responses.activate
    def test_passes_api_key_in_params(self):
        responses.add(
            responses.GET,
            DOMAIN_SEARCH_URL,
            json={"data": {"emails": []}},
            status=200,
        )
        client = HunterClient(api_key="my-secret-key")
        client.domain_search("test.com")
        assert "api_key=my-secret-key" in responses.calls[0].request.url

    @responses.activate
    def test_no_data_key_returns_none(self):
        """domain_search returns None when response has no 'data' key."""
        responses.add(
            responses.GET,
            DOMAIN_SEARCH_URL,
            json={"meta": {"results": 0}},
            status=200,
        )
        client = HunterClient(api_key="test-key")
        result = client.domain_search("nodata.com")
        assert result is None
        assert client.calls_made == 1


class TestHunterClientCallCounting:
    """Tests for HunterClient call tracking behavior."""

    def test_calls_made_starts_at_zero(self):
        """A fresh client has zero calls made."""
        client = HunterClient(api_key="fake-key")
        assert client.calls_made == 0

    @responses.activate
    def test_calls_made_increments_on_success(self):
        """calls_made increments after each successful API call."""
        responses.add(
            responses.GET,
            DOMAIN_SEARCH_URL,
            json={"data": {"domain": "a.com", "emails": []}},
            status=200,
        )
        responses.add(
            responses.GET,
            DOMAIN_SEARCH_URL,
            json={"data": {"domain": "b.com", "emails": []}},
            status=200,
        )
        client = HunterClient(api_key="fake-key")
        client.domain_search("a.com")
        assert client.calls_made == 1
        client.domain_search("b.com")
        assert client.calls_made == 2

    @responses.activate
    def test_calls_made_increments_on_error_status(self):
        """calls_made increments even on error status codes (request completed)."""
        responses.add(
            responses.GET,
            DOMAIN_SEARCH_URL,
            status=500,
        )
        client = HunterClient(api_key="fake-key")
        client.domain_search("error.com")
        assert client.calls_made == 1

    @responses.activate
    def test_calls_made_increments_on_rate_limit(self):
        """calls_made increments on 429 rate limit (request completed)."""
        responses.add(
            responses.GET,
            DOMAIN_SEARCH_URL,
            status=429,
        )
        client = HunterClient(api_key="fake-key")
        client.domain_search("ratelimited.com")
        assert client.calls_made == 1

    @responses.activate
    def test_calls_made_not_incremented_on_connection_error(self):
        """calls_made is NOT incremented on connection errors (no response)."""
        import requests as req

        responses.add(
            responses.GET,
            DOMAIN_SEARCH_URL,
            body=req.exceptions.ConnectionError("Connection refused"),
        )
        client = HunterClient(api_key="fake-key")
        client.domain_search("unreachable.com")
        # Exception is raised before self._calls_made += 1
        assert client.calls_made == 0
