"""Unit tests for Foursquare enrichment & verification worker.

Tests FoursquareClient.search(), _is_good_match(), _build_search_query()
with mocked HTTP. No database required.
"""
from __future__ import annotations

import responses

from domain_pipeline.workers.foursquare import (
    FoursquareClient,
    _build_search_query,
    _is_good_match,
)


class TestFoursquareClientSearch:
    """Tests for FoursquareClient.search() HTTP client."""

    @responses.activate
    def test_happy_path_returns_first_result(self):
        responses.add(
            responses.GET,
            "https://api.foursquare.com/v3/places/search",
            json={
                "results": [
                    {
                        "fsq_id": "abc123",
                        "name": "Joe's Pizza",
                        "location": {"formatted_address": "123 Broadway"},
                        "tel": "+12125551234",
                        "website": "https://joespizza.com",
                    }
                ]
            },
            status=200,
        )
        client = FoursquareClient(api_key="test-key")
        result = client.search("Joe's Pizza")
        assert result is not None
        assert result["fsq_id"] == "abc123"
        assert client.calls_made == 1

    @responses.activate
    def test_no_results_returns_none(self):
        responses.add(
            responses.GET,
            "https://api.foursquare.com/v3/places/search",
            json={"results": []},
            status=200,
        )
        client = FoursquareClient(api_key="test-key")
        result = client.search("Unknown")
        assert result is None

    @responses.activate
    def test_server_error_returns_none(self):
        responses.add(
            responses.GET,
            "https://api.foursquare.com/v3/places/search",
            status=500,
        )
        client = FoursquareClient(api_key="test-key")
        result = client.search("test")
        assert result is None

    @responses.activate
    def test_includes_location_when_provided(self):
        responses.add(
            responses.GET,
            "https://api.foursquare.com/v3/places/search",
            json={"results": [{"fsq_id": "test", "name": "Test"}]},
            status=200,
        )
        client = FoursquareClient(api_key="test-key")
        client.search("test", lat=25.2, lon=55.3)
        assert "ll=25.2" in responses.calls[0].request.url

    @responses.activate
    def test_increments_calls_made(self):
        responses.add(
            responses.GET,
            "https://api.foursquare.com/v3/places/search",
            json={"results": []},
            status=200,
        )
        client = FoursquareClient(api_key="test-key")
        assert client.calls_made == 0
        client.search("test")
        assert client.calls_made == 1


class TestFoursquareIsGoodMatch:
    """Tests for _is_good_match()."""

    def test_matching_names(self, make_business):
        biz = make_business(name="Morton Motors")
        place = {"name": "Morton Motors Auto Repair"}
        assert _is_good_match(biz, place) is True

    def test_no_overlap(self, make_business):
        biz = make_business(name="Morton Motors")
        place = {"name": "City Bakery"}
        assert _is_good_match(biz, place) is False

    def test_empty_business_name(self, make_business):
        biz = make_business(name="")
        place = {"name": "Test"}
        assert _is_good_match(biz, place) is False

    def test_empty_place_name(self, make_business):
        biz = make_business(name="Test")
        place = {"name": ""}
        assert _is_good_match(biz, place) is False

    def test_stop_words_ignored(self, make_business):
        biz = make_business(name="The Village Cobbler")
        place = {"name": "Village Cobbler Shoe Repair"}
        assert _is_good_match(biz, place) is True


class TestFoursquareBuildSearchQuery:
    """Tests for _build_search_query()."""

    def test_name_and_city(self, make_business):
        biz = make_business(name="Test Biz")
        query = _build_search_query(biz, "Dubai")
        assert "Test Biz" in query
        assert "Dubai" in query

    def test_name_only(self, make_business):
        biz = make_business(name="Test Biz")
        query = _build_search_query(biz, None)
        assert "Test Biz" in query

    def test_address_preferred_over_city(self, make_business):
        biz = make_business(name="Test")
        biz.address = "456 Main St"
        query = _build_search_query(biz, "Toronto")
        assert "456 Main St" in query
        assert "Toronto" not in query

    def test_empty_name(self, make_business):
        biz = make_business(name="")
        query = _build_search_query(biz, None)
        assert query.strip() == ""
