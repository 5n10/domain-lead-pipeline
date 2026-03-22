"""Unit tests for LLM verification worker.

Tests _fetch_search_context(), _format_search_results(), _analyze_with_llm()
with mocked HTTP responses. No database required.
"""
from __future__ import annotations

import json

import pytest
import responses

from domain_pipeline.workers.llm_verify import (
    _analyze_with_llm,
    _fetch_search_context,
    _format_search_results,
)


class TestFetchSearchContext:
    """Tests for _fetch_search_context() — SearXNG context fetching."""

    @responses.activate
    def test_happy_path(self):
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            json={
                "results": [
                    {
                        "title": "Joe's Pizza",
                        "url": "https://joespizza.com",
                        "content": "Official website of Joe's Pizza.",
                    },
                    {
                        "title": "Joe's Pizza - Yelp",
                        "url": "https://www.yelp.com/biz/joes-pizza",
                        "content": "Reviews for Joe's Pizza.",
                    },
                ]
            },
            status=200,
        )
        results = _fetch_search_context("Joe's Pizza", "New York")
        assert len(results) == 2
        assert results[0]["title"] == "Joe's Pizza"
        assert results[0]["url"] == "https://joespizza.com"

    @responses.activate
    def test_no_results(self):
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            json={"results": []},
            status=200,
        )
        results = _fetch_search_context("Unknown Biz", "Nowhere")
        assert results == []

    @responses.activate
    def test_server_error_returns_empty(self):
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            status=500,
        )
        results = _fetch_search_context("Test", "City")
        assert results == []

    @responses.activate
    def test_connection_error_returns_empty(self):
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            body=ConnectionError("Connection refused"),
        )
        results = _fetch_search_context("Test", "City")
        assert results == []

    @responses.activate
    def test_truncates_long_content(self):
        long_content = "A" * 500
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            json={
                "results": [
                    {"title": "Test", "url": "https://test.com", "content": long_content}
                ]
            },
            status=200,
        )
        results = _fetch_search_context("Test", "City")
        assert len(results[0]["snippet"]) <= 200

    @responses.activate
    def test_limits_to_15_results(self):
        many_results = [
            {"title": f"Result {i}", "url": f"https://site{i}.com", "content": f"Content {i}"}
            for i in range(30)
        ]
        responses.add(
            responses.GET,
            "http://localhost:8888/search",
            json={"results": many_results},
            status=200,
        )
        results = _fetch_search_context("Test", "City")
        assert len(results) == 15


class TestFormatSearchResults:
    """Tests for _format_search_results() text formatting."""

    def test_formats_results(self):
        results = [
            {"title": "Test Site", "url": "https://test.com", "snippet": "A test."},
            {"title": "Other Site", "url": "https://other.com", "snippet": "Another."},
        ]
        text = _format_search_results(results)
        assert "1. [Test Site](https://test.com)" in text
        assert "2. [Other Site](https://other.com)" in text
        assert "A test." in text

    def test_empty_results(self):
        text = _format_search_results([])
        assert "No search results" in text

    def test_missing_snippet(self):
        results = [{"title": "Test", "url": "https://test.com", "snippet": ""}]
        text = _format_search_results(results)
        assert "[Test]" in text


class TestAnalyzeWithLlm:
    """Tests for _analyze_with_llm() with mocked LLM API responses."""

    @responses.activate
    def test_groq_has_website(self):
        responses.add(
            responses.POST,
            "https://api.groq.com/openai/v1/chat/completions",
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps({
                                "status": "has_website",
                                "website_url": "https://joespizza.com",
                                "reason": "Found official website.",
                            })
                        }
                    }
                ]
            },
            status=200,
        )
        result = _analyze_with_llm(
            business_name="Joe's Pizza",
            city_name="New York",
            category="restaurant",
            search_results=[{"title": "Joe's Pizza", "url": "https://joespizza.com", "snippet": ""}],
            api_key="test-key",
            provider="groq",
        )
        assert result["status"] == "has_website"
        assert result["website_url"] == "https://joespizza.com"

    @responses.activate
    def test_groq_no_website(self):
        responses.add(
            responses.POST,
            "https://api.groq.com/openai/v1/chat/completions",
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps({
                                "status": "no_website",
                                "website_url": None,
                                "reason": "No official website found.",
                            })
                        }
                    }
                ]
            },
            status=200,
        )
        result = _analyze_with_llm(
            business_name="Local Shop",
            city_name="Dubai",
            category="retail",
            search_results=[],
            api_key="test-key",
            provider="groq",
        )
        assert result["status"] == "no_website"
        assert result["website_url"] is None

    @responses.activate
    def test_openrouter_provider(self):
        responses.add(
            responses.POST,
            "https://openrouter.ai/api/v1/chat/completions",
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps({
                                "status": "not_sure",
                                "website_url": None,
                                "reason": "Insufficient evidence.",
                            })
                        }
                    }
                ]
            },
            status=200,
        )
        result = _analyze_with_llm(
            business_name="Test",
            city_name="City",
            category="business",
            search_results=[],
            api_key="test-key",
            provider="openrouter",
        )
        assert result["status"] == "not_sure"

    @responses.activate
    def test_gemini_provider(self):
        responses.add(
            responses.POST,
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent",
            json={
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": json.dumps({
                                        "status": "has_website",
                                        "website_url": "https://test.com",
                                        "reason": "Found it.",
                                    })
                                }
                            ]
                        }
                    }
                ]
            },
            status=200,
        )
        result = _analyze_with_llm(
            business_name="Test",
            city_name="City",
            category="business",
            search_results=[],
            api_key="test-key",
            provider="gemini",
        )
        assert result["status"] == "has_website"

    @responses.activate
    def test_api_error_retries_and_raises(self):
        """LLM worker retries on error status; after max attempts it raises RetryError."""
        from tenacity import RetryError

        # Add enough responses for all retry attempts
        for _ in range(4):
            responses.add(
                responses.POST,
                "https://api.groq.com/openai/v1/chat/completions",
                status=500,
            )
        with pytest.raises(RetryError):
            _analyze_with_llm(
                business_name="Test",
                city_name="City",
                category="business",
                search_results=[],
                api_key="test-key",
                provider="groq",
            )

    def test_unknown_provider_retries_and_raises(self):
        """Unknown provider returns error dict, which triggers retry loop."""
        from tenacity import RetryError

        with pytest.raises(RetryError):
            _analyze_with_llm(
                business_name="Test",
                city_name="City",
                category="business",
                search_results=[],
                api_key="test-key",
                provider="unknown",
            )

    @responses.activate
    def test_invalid_status_normalized_to_not_sure(self):
        responses.add(
            responses.POST,
            "https://api.groq.com/openai/v1/chat/completions",
            json={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps({
                                "status": "maybe",
                                "website_url": None,
                                "reason": "Not sure.",
                            })
                        }
                    }
                ]
            },
            status=200,
        )
        result = _analyze_with_llm(
            business_name="Test",
            city_name="City",
            category="business",
            search_results=[],
            api_key="test-key",
            provider="groq",
        )
        assert result["status"] == "not_sure"
