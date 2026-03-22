"""Unit tests for SearXNG verification worker.

Tests _search_searxng(), _analyze_results(), _process_one_business()
with mocked HTTP responses. No database required.
"""
from __future__ import annotations

import responses

from domain_pipeline.workers.searxng_verify import (
    _analyze_results,
    _process_one_business,
    _search_searxng,
)


class TestSearchSearxng:
    """Tests for the _search_searxng() HTTP client function."""

    @responses.activate
    def test_happy_path_returns_results(self):
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            json={
                "results": [
                    {
                        "title": "Test Result",
                        "url": "https://example.com",
                        "content": "A test result.",
                        "engine": "duckduckgo",
                        "engines": ["duckduckgo", "bing"],
                    }
                ]
            },
            status=200,
        )
        results = _search_searxng("test query", searxng_url="http://localhost:8888/search")
        assert len(results) == 1
        assert results[0]["title"] == "Test Result"
        assert results[0]["href"] == "https://example.com"
        assert results[0]["body"] == "A test result."
        assert results[0]["engine"] == "duckduckgo"
        assert results[0]["engines"] == ["duckduckgo", "bing"]

    @responses.activate
    def test_empty_results(self):
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            json={"results": []},
            status=200,
        )
        results = _search_searxng("test query", searxng_url="http://localhost:8888/search")
        assert results == []

    @responses.activate
    def test_server_error_returns_empty(self):
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            json={"error": "Internal error"},
            status=500,
        )
        results = _search_searxng("test query", searxng_url="http://localhost:8888/search")
        assert results == []

    @responses.activate
    def test_timeout_returns_empty(self):
        import requests
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            body=requests.exceptions.ConnectionError("Connection refused"),
        )
        results = _search_searxng("test query", searxng_url="http://localhost:8888/search")
        assert results == []

    @responses.activate
    def test_deduplicates_urls(self):
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            json={
                "results": [
                    {"title": "A", "url": "https://example.com", "content": "", "engine": "ddg", "engines": ["ddg"]},
                    {"title": "B", "url": "https://example.com", "content": "", "engine": "bing", "engines": ["bing"]},
                    {"title": "C", "url": "https://other.com", "content": "", "engine": "brave", "engines": ["brave"]},
                ]
            },
            status=200,
        )
        results = _search_searxng("test query", searxng_url="http://localhost:8888/search")
        assert len(results) == 2
        urls = [r["href"] for r in results]
        assert "https://example.com" in urls
        assert "https://other.com" in urls

    @responses.activate
    def test_max_results_limits_output(self):
        many_results = [
            {"title": f"Result {i}", "url": f"https://site{i}.com", "content": "", "engine": "ddg", "engines": ["ddg"]}
            for i in range(30)
        ]
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            json={"results": many_results},
            status=200,
        )
        results = _search_searxng("test query", max_results=5, searxng_url="http://localhost:8888/search")
        assert len(results) == 5


class TestAnalyzeResults:
    """Tests for the _analyze_results() function."""

    def test_finds_website_from_domain_match(self):
        results = [
            {"href": "https://joespizza.com", "title": "Joe's Pizza", "body": "", "engines": ["ddg"]},
        ]
        website, metadata = _analyze_results(results, "Joe's Pizza")
        assert website is not None
        assert "joespizza" in website.lower()
        assert metadata["total_results"] == 1

    def test_no_website_when_only_directories(self):
        results = [
            {"href": "https://www.yelp.com/biz/test", "title": "Test - Yelp", "body": "", "engines": ["ddg"]},
            {"href": "https://www.facebook.com/test", "title": "Test - Facebook", "body": "", "engines": ["bing"]},
        ]
        website, metadata = _analyze_results(results, "Test Business")
        assert website is None
        assert metadata["non_directory_results"] == 0

    def test_metadata_tracks_engines(self):
        results = [
            {"href": "https://example.com", "title": "Example", "body": "", "engine": "duckduckgo", "engines": ["duckduckgo", "bing"]},
            {"href": "https://other.com", "title": "Other", "body": "", "engine": "brave", "engines": ["brave"]},
        ]
        _, metadata = _analyze_results(results, "Example")
        assert metadata["engine_count"] >= 2
        assert "duckduckgo" in metadata["engines"]

    def test_empty_results(self):
        website, metadata = _analyze_results([], "Test Business")
        assert website is None
        assert metadata["total_results"] == 0


class TestProcessOneBusiness:
    """Tests for _process_one_business() — the per-business worker function."""

    @responses.activate
    def test_website_found_outcome(self):
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            json={
                "results": [
                    {
                        "title": "Morton Motors - Official Site",
                        "url": "https://mortonmotors.ca",
                        "content": "Morton Motors in Toronto.",
                        "engine": "ddg",
                        "engines": ["ddg", "bing"],
                    }
                ]
            },
            status=200,
        )
        result = _process_one_business(
            biz_id="test-id",
            biz_name="Morton Motors",
            city_name="Toronto",
            raw=None,
            searxng_url="http://localhost:8888/search",
        )
        assert result["outcome"] == "has_website"
        assert result["website"] is not None
        assert "mortonmotors" in result["website"].lower()
        assert result["raw"]["searxng_verified"] is True
        assert result["raw"]["searxng_result"] == "has_website"

    @responses.activate
    def test_no_website_outcome(self):
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            json={
                "results": [
                    {
                        "title": "Local Shop - Yelp",
                        "url": "https://www.yelp.com/biz/local-shop",
                        "content": "Reviews.",
                        "engine": "ddg",
                        "engines": ["ddg"],
                    }
                ]
            },
            status=200,
        )
        result = _process_one_business(
            biz_id="test-id",
            biz_name="Local Shop",
            city_name="Dubai",
            raw={},
            searxng_url="http://localhost:8888/search",
        )
        assert result["outcome"] == "no_website"
        assert result["website"] is None
        assert result["raw"]["searxng_verified"] is True
        assert result["raw"]["searxng_result"] == "no_website"

    @responses.activate
    def test_no_results_inconclusive(self):
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            json={"results": []},
            status=200,
        )
        result = _process_one_business(
            biz_id="test-id",
            biz_name="Unknown Business",
            city_name="Nowhere",
            raw=None,
            searxng_url="http://localhost:8888/search",
        )
        assert result["outcome"] == "inconclusive"
        assert result["website"] is None
        assert result["raw"]["searxng_result"] == "no_results"

    @responses.activate
    def test_error_returns_inconclusive(self):
        import requests as req
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            body=req.exceptions.ConnectionError("Connection refused"),
        )
        result = _process_one_business(
            biz_id="test-id",
            biz_name="Test Biz",
            city_name="City",
            raw=None,
            searxng_url="http://localhost:8888/search",
        )
        assert result["outcome"] == "inconclusive"
        assert result["raw"]["searxng_result"] == "no_results"

    @responses.activate
    def test_preserves_existing_raw_data(self):
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            json={"results": []},
            status=200,
        )
        existing_raw = {"domain_guess_verified": True, "domain_guess_result": "no_match"}
        result = _process_one_business(
            biz_id="test-id",
            biz_name="Test Biz",
            city_name="City",
            raw=existing_raw,
            searxng_url="http://localhost:8888/search",
        )
        assert result["raw"]["domain_guess_verified"] is True
        assert result["raw"]["searxng_verified"] is True
