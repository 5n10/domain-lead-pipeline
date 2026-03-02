"""Unit tests for web_search_verify (DDG HTML scraper) worker.

Tests _search_web(), _extract_business_website(), _is_directory_or_social(),
_domain_contains_name(), _build_search_queries(), and helper functions
with mocked HTTP responses. No database required.
"""
from __future__ import annotations

import responses

from domain_pipeline.workers.web_search_verify import (
    DIRECTORY_DOMAINS,
    _build_search_queries,
    _domain_contains_name,
    _extract_business_website,
    _get_domain_from_url,
    _is_directory_or_social,
    _is_root_url,
    _looks_like_article_url,
    _normalize_name,
    _name_words,
    _result_matches_business,
    _search_web,
)


class TestSearchWeb:
    """Tests for the _search_web() DDG HTML scraper."""

    @responses.activate
    def test_parses_html_results(self):
        html = """
        <html><body>
        <div class="result">
          <a class="result__a" href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com">
            Example Site
          </a>
          <a class="result__snippet">An example website.</a>
        </div>
        </body></html>
        """
        responses.add(
            responses.GET,
            "https://html.duckduckgo.com/html/",
            body=html,
            status=200,
        )
        results = _search_web("test query")
        assert len(results) == 1
        assert results[0]["title"] == "Example Site"
        assert results[0]["href"] == "https://example.com"
        assert results[0]["body"] == "An example website."

    @responses.activate
    def test_empty_page_returns_empty(self):
        responses.add(
            responses.GET,
            "https://html.duckduckgo.com/html/",
            body="<html><body></body></html>",
            status=200,
        )
        results = _search_web("test query")
        assert results == []

    @responses.activate
    def test_non_200_returns_empty(self):
        responses.add(
            responses.GET,
            "https://html.duckduckgo.com/html/",
            status=403,
        )
        results = _search_web("test query")
        assert results == []

    @responses.activate
    def test_429_retries_then_returns_empty(self):
        responses.add(responses.GET, "https://html.duckduckgo.com/html/", status=429)
        responses.add(responses.GET, "https://html.duckduckgo.com/html/", status=429)
        responses.add(responses.GET, "https://html.duckduckgo.com/html/", status=429)
        results = _search_web("test query")
        assert results == []

    @responses.activate
    def test_max_results_respected(self):
        divs = ""
        for i in range(20):
            divs += f"""
            <div class="result">
              <a class="result__a" href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fsite{i}.com">Site {i}</a>
              <a class="result__snippet">Snippet {i}</a>
            </div>
            """
        html = f"<html><body>{divs}</body></html>"
        responses.add(responses.GET, "https://html.duckduckgo.com/html/", body=html, status=200)
        results = _search_web("test query", max_results=5)
        assert len(results) == 5


class TestGetDomainFromUrl:
    def test_basic_url(self):
        assert _get_domain_from_url("https://example.com/path") == "example.com"

    def test_strips_www(self):
        assert _get_domain_from_url("https://www.example.com") == "example.com"

    def test_invalid_url_returns_empty(self):
        assert _get_domain_from_url("") == ""

    def test_subdomain_preserved(self):
        assert _get_domain_from_url("https://shop.example.com") == "shop.example.com"


class TestIsDirectoryOrSocial:
    def test_yelp_is_directory(self):
        assert _is_directory_or_social("https://www.yelp.com/biz/test") is True

    def test_facebook_is_directory(self):
        assert _is_directory_or_social("https://www.facebook.com/test") is True

    def test_real_website_not_directory(self):
        assert _is_directory_or_social("https://joespizza.com") is False

    def test_subdomain_of_directory(self):
        assert _is_directory_or_social("https://m.yelp.com/biz/test") is True

    def test_empty_url_treated_as_directory(self):
        assert _is_directory_or_social("") is True


class TestDomainContainsName:
    def test_full_name_in_domain(self):
        assert _domain_contains_name("sonidentistry.com", "Soni Dentistry") is True

    def test_domain_in_full_name(self):
        assert _domain_contains_name("mortonmotor.ca", "Morton Motors") is True

    def test_generic_word_not_matched(self):
        assert _domain_contains_name("yankeecandle.com", "Candle Night Personal Care") is False

    def test_two_word_match(self):
        assert _domain_contains_name("villagecobbler.ca", "The Village Cobbler") is True

    def test_empty_inputs(self):
        assert _domain_contains_name("", "Test") is False
        assert _domain_contains_name("test.com", "") is False


class TestBuildSearchQueries:
    def test_with_city(self):
        queries = _build_search_queries("Joe's Pizza", "New York")
        assert len(queries) >= 1
        assert any("New York" in q for q in queries)

    def test_without_city(self):
        queries = _build_search_queries("Joe's Pizza", None)
        assert len(queries) >= 1
        # At least the first query should contain the business name
        assert "Joe" in queries[0] or "joe" in queries[0].lower() or "pizza" in queries[0].lower()

    def test_no_duplicate_queries(self):
        queries = _build_search_queries("Simple Name", "City")
        assert len(queries) == len(set(queries))


class TestResultMatchesBusiness:
    def test_title_overlap_matches(self):
        result = {"title": "Morton Motors - Auto Repair in Toronto", "href": "https://mortonmotors.ca"}
        assert _result_matches_business(result, "Morton Motors") is True

    def test_domain_name_matches(self):
        result = {"title": "Welcome to Our Site", "href": "https://mortonmotors.ca"}
        assert _result_matches_business(result, "Morton Motors") is True

    def test_unrelated_result_no_match(self):
        result = {"title": "Best Restaurants in Toronto", "href": "https://blogto.com/restaurants"}
        assert _result_matches_business(result, "Morton Motors") is False


class TestLooksLikeArticleUrl:
    def test_homepage_not_article(self):
        assert _looks_like_article_url("https://example.com") is False

    def test_about_page_not_article(self):
        assert _looks_like_article_url("https://example.com/about") is False

    def test_date_based_path_is_article(self):
        assert _looks_like_article_url("https://blog.com/2025/01/24/my-post") is True

    def test_blog_path_is_article(self):
        assert _looks_like_article_url("https://example.com/blog/some-article") is True

    def test_deeply_nested_is_article(self):
        assert _looks_like_article_url("https://example.com/a/b/c/d/e") is True

    def test_long_hyphenated_slug_is_article(self):
        assert _looks_like_article_url("https://example.com/category/this-is-a-very-long-article-slug-name") is True


class TestIsRootUrl:
    def test_root_domain(self):
        assert _is_root_url("https://example.com") is True

    def test_about_page(self):
        assert _is_root_url("https://example.com/about") is True

    def test_deep_path_not_root(self):
        assert _is_root_url("https://example.com/a/b/c") is False


class TestExtractBusinessWebsite:
    def test_finds_domain_name_match(self):
        results = [
            {"href": "https://www.yelp.com/biz/test", "title": "Test - Yelp", "body": ""},
            {"href": "https://mortonmotors.ca", "title": "Morton Motors", "body": ""},
        ]
        website = _extract_business_website(results, "Morton Motors")
        assert website is not None
        assert "mortonmotors" in website.lower()

    def test_returns_none_for_only_directories(self):
        results = [
            {"href": "https://www.yelp.com/biz/test", "title": "Test Biz - Yelp", "body": ""},
            {"href": "https://www.facebook.com/testbiz", "title": "Test Biz - Facebook", "body": ""},
        ]
        website = _extract_business_website(results, "Test Biz")
        assert website is None

    def test_returns_root_url_for_deep_domain_match(self):
        results = [
            {
                "href": "https://mortonmotors.ca/blog/some-article-about-cars",
                "title": "Blog - Morton Motors",
                "body": "",
            },
        ]
        website = _extract_business_website(results, "Morton Motors")
        assert website is not None
        assert website.rstrip("/").endswith("mortonmotors.ca")

    def test_empty_results_returns_none(self):
        assert _extract_business_website([], "Test Business") is None

    def test_title_match_on_root_page(self):
        results = [
            {"href": "https://bestpizzanyc.com", "title": "Joe's Amazing Pizza Place", "body": ""},
        ]
        website = _extract_business_website(results, "Joe's Amazing Pizza")
        assert website is not None
