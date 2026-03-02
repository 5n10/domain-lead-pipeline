"""Unit tests for Google Search verification worker.

Tests _search_google() with mocked HTTP, _build_google_queries() as pure function,
and _GOOGLE_DOMAINS / _USER_AGENTS constants. No database required.
"""
from __future__ import annotations

import responses

from domain_pipeline.workers.google_search_verify import (
    _GOOGLE_DOMAINS,
    _USER_AGENTS,
    _build_google_queries,
    _search_google,
)


class TestSearchGoogle:
    """Tests for _search_google() — Google HTML scraper."""

    @responses.activate
    def test_parses_organic_results(self):
        html = """
        <html><body>
        <div class="g">
            <a href="https://example.com"><h3>Example Site</h3></a>
            <div class="VwiC3b">An example website.</div>
        </div>
        </body></html>
        """
        responses.add(
            responses.GET,
            "https://www.google.com/search",
            body=html,
            status=200,
        )
        results = _search_google("test query")
        assert len(results) >= 1
        assert results[0]["href"] == "https://example.com"
        assert results[0]["title"] == "Example Site"

    @responses.activate
    def test_empty_page_returns_empty(self):
        responses.add(
            responses.GET,
            "https://www.google.com/search",
            body="<html><body></body></html>",
            status=200,
        )
        results = _search_google("test query")
        assert results == []

    @responses.activate
    def test_403_returns_empty(self):
        responses.add(
            responses.GET,
            "https://www.google.com/search",
            status=403,
        )
        results = _search_google("test query")
        assert results == []

    @responses.activate
    def test_429_returns_empty(self):
        responses.add(
            responses.GET,
            "https://www.google.com/search",
            status=429,
        )
        results = _search_google("test query")
        assert results == []

    @responses.activate
    def test_captcha_detected(self):
        html = "<html><body>unusual traffic from your computer network</body></html>"
        responses.add(
            responses.GET,
            "https://www.google.com/search",
            body=html,
            status=200,
        )
        results = _search_google("test query")
        assert results == []

    @responses.activate
    def test_country_uses_localized_domain(self):
        responses.add(
            responses.GET,
            "https://www.google.ca/search",
            body="<html><body></body></html>",
            status=200,
        )
        _search_google("test query", country="CA")
        assert "google.ca" in responses.calls[0].request.url

    @responses.activate
    def test_filters_google_links(self):
        html = """
        <html><body>
        <div class="g">
            <a href="https://google.com/settings"><h3>Google Settings</h3></a>
        </div>
        <div class="g">
            <a href="https://example.com"><h3>Real Site</h3></a>
        </div>
        </body></html>
        """
        responses.add(
            responses.GET,
            "https://www.google.com/search",
            body=html,
            status=200,
        )
        results = _search_google("test query")
        hrefs = [r["href"] for r in results]
        assert "https://example.com" in hrefs
        assert not any("google.com" in h for h in hrefs)

    @responses.activate
    def test_max_results_limits_output(self):
        divs = ""
        for i in range(20):
            divs += f'<div class="g"><a href="https://site{i}.com"><h3>Site {i}</h3></a></div>'
        html = f"<html><body>{divs}</body></html>"
        responses.add(
            responses.GET,
            "https://www.google.com/search",
            body=html,
            status=200,
        )
        results = _search_google("test query", max_results=3)
        assert len(results) == 3


class TestBuildGoogleQueries:
    """Tests for _build_google_queries() — pure function."""

    def test_with_city(self):
        queries = _build_google_queries("Joe's Pizza", "New York", None, None)
        assert len(queries) >= 2
        assert any("New York" in q for q in queries)
        assert any("website" in q for q in queries)

    def test_without_city(self):
        queries = _build_google_queries("Joe's Pizza", None, None, None)
        assert len(queries) >= 1
        assert "Joe's Pizza" in queries[0]

    def test_with_category_and_country(self):
        queries = _build_google_queries("Test Biz", "Dubai", "restaurant", "AE")
        assert any("restaurant" in q and "AE" in q for q in queries)

    def test_no_duplicates(self):
        queries = _build_google_queries("Test", "City", None, None)
        assert len(queries) == len(set(queries))

    def test_always_includes_website_query(self):
        queries = _build_google_queries("Test", None, None, None)
        assert any("website" in q for q in queries)


class TestGoogleSearchConstants:
    """Tests for _GOOGLE_DOMAINS and _USER_AGENTS constants."""

    def test_google_domains_is_dict(self):
        """_GOOGLE_DOMAINS should be a dict mapping country codes to domains."""
        assert isinstance(_GOOGLE_DOMAINS, dict)
        assert len(_GOOGLE_DOMAINS) > 0

    def test_ae_domain(self):
        """UAE should map to google.ae."""
        assert _GOOGLE_DOMAINS["AE"] == "www.google.ae"

    def test_us_domain(self):
        """US should map to google.com."""
        assert _GOOGLE_DOMAINS["US"] == "www.google.com"

    def test_ca_domain(self):
        """Canada should map to google.ca."""
        assert _GOOGLE_DOMAINS["CA"] == "www.google.ca"

    def test_user_agents_is_nonempty_list(self):
        """_USER_AGENTS should be a non-empty list of Chrome UA strings."""
        assert isinstance(_USER_AGENTS, list)
        assert len(_USER_AGENTS) >= 2
        for ua in _USER_AGENTS:
            assert "Chrome" in ua
            assert "Mozilla" in ua
