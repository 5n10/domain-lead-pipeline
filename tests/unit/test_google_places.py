"""Unit tests for Google Places enrichment & verification worker.

Tests PlacesClient.text_search(), _build_search_query(), _is_good_match(),
_extract_contacts() with mocked HTTP. No database required.
"""
from __future__ import annotations

import responses

from domain_pipeline.workers.google_places import (
    PlacesClient,
    _build_search_query,
    _extract_contacts,
    _is_good_match,
)


class TestPlacesClientTextSearch:
    """Tests for PlacesClient.text_search() HTTP client."""

    @responses.activate
    def test_happy_path_returns_first_place(self):
        responses.add(
            responses.POST,
            "https://places.googleapis.com/v1/places:searchText",
            json={
                "places": [
                    {
                        "id": "ChIJ_test123",
                        "displayName": {"text": "Joe's Pizza"},
                        "formattedAddress": "123 Broadway, NY",
                        "websiteUri": "https://joespizza.com",
                        "nationalPhoneNumber": "+1 212-555-1234",
                    }
                ]
            },
            status=200,
        )
        client = PlacesClient(api_key="test-key")
        result = client.text_search("Joe's Pizza New York")
        assert result is not None
        assert result["id"] == "ChIJ_test123"
        assert client.calls_made == 1

    @responses.activate
    def test_no_places_returns_none(self):
        responses.add(
            responses.POST,
            "https://places.googleapis.com/v1/places:searchText",
            json={"places": []},
            status=200,
        )
        client = PlacesClient(api_key="test-key")
        result = client.text_search("Unknown Business")
        assert result is None

    @responses.activate
    def test_empty_response_returns_none(self):
        responses.add(
            responses.POST,
            "https://places.googleapis.com/v1/places:searchText",
            json={},
            status=200,
        )
        client = PlacesClient(api_key="test-key")
        result = client.text_search("test")
        assert result is None

    @responses.activate
    def test_server_error_returns_none(self):
        responses.add(
            responses.POST,
            "https://places.googleapis.com/v1/places:searchText",
            status=500,
        )
        client = PlacesClient(api_key="test-key")
        result = client.text_search("test")
        assert result is None

    @responses.activate
    def test_location_bias_included_when_coords_provided(self):
        responses.add(
            responses.POST,
            "https://places.googleapis.com/v1/places:searchText",
            json={"places": [{"id": "test"}]},
            status=200,
        )
        client = PlacesClient(api_key="test-key")
        client.text_search("test", location_lat=25.2, location_lon=55.3)
        body = responses.calls[0].request.body
        assert b"locationBias" in body

    @responses.activate
    def test_increments_calls_made(self):
        responses.add(
            responses.POST,
            "https://places.googleapis.com/v1/places:searchText",
            json={"places": []},
            status=200,
        )
        client = PlacesClient(api_key="test-key")
        assert client.calls_made == 0
        client.text_search("test1")
        assert client.calls_made == 1
        client.text_search("test2")
        assert client.calls_made == 2


class TestBuildSearchQuery:
    """Tests for _build_search_query()."""

    def test_name_and_city(self, make_business):
        biz = make_business(name="Joe's Pizza")
        assert "Joe's Pizza" in _build_search_query(biz, "New York")
        assert "New York" in _build_search_query(biz, "New York")

    def test_name_only(self, make_business):
        biz = make_business(name="Test Biz")
        query = _build_search_query(biz, None)
        assert "Test Biz" in query

    def test_empty_name_returns_empty(self, make_business):
        biz = make_business(name="")
        query = _build_search_query(biz, None)
        assert query.strip() == ""

    def test_address_preferred_over_city(self, make_business):
        biz = make_business(name="Test")
        biz.address = "123 Main St"
        query = _build_search_query(biz, "Toronto")
        assert "123 Main St" in query
        assert "Toronto" not in query


class TestIsGoodMatch:
    """Tests for _is_good_match()."""

    def test_matching_names(self, make_business):
        biz = make_business(name="Morton Motors")
        place = {"displayName": {"text": "Morton Motors Auto Repair"}}
        assert _is_good_match(biz, place) is True

    def test_no_overlap(self, make_business):
        biz = make_business(name="Morton Motors")
        place = {"displayName": {"text": "City Bakery"}}
        assert _is_good_match(biz, place) is False

    def test_empty_business_name(self, make_business):
        biz = make_business(name="")
        place = {"displayName": {"text": "Some Place"}}
        assert _is_good_match(biz, place) is False

    def test_empty_place_name(self, make_business):
        biz = make_business(name="Test Biz")
        place = {"displayName": {"text": ""}}
        assert _is_good_match(biz, place) is False

    def test_stop_words_ignored(self, make_business):
        biz = make_business(name="The Grand Hotel")
        place = {"displayName": {"text": "Grand Hotel and Spa"}}
        assert _is_good_match(biz, place) is True

    def test_partial_match_above_threshold(self, make_business):
        biz = make_business(name="Joe Pizza Place")
        place = {"displayName": {"text": "Joe Pizza"}}
        assert _is_good_match(biz, place) is True

    def test_missing_displayname_key(self, make_business):
        biz = make_business(name="Test")
        place = {}
        assert _is_good_match(biz, place) is False


class TestExtractContacts:
    """Tests for _extract_contacts()."""

    def test_full_data(self):
        place = {
            "id": "ChIJ_123",
            "googleMapsUri": "https://maps.google.com/123",
            "nationalPhoneNumber": "+1 555-1234",
            "websiteUri": "https://example.com",
            "rating": 4.5,
            "userRatingCount": 100,
            "displayName": {"text": "Test Biz"},
        }
        result = _extract_contacts(place)
        assert result["google_place_id"] == "ChIJ_123"
        assert result["phone"] == "+1 555-1234"
        assert result["website"] == "https://example.com"
        assert result["rating"] == 4.5
        assert result["review_count"] == 100

    def test_no_phone_or_website(self):
        place = {"id": "ChIJ_456", "displayName": {"text": "Test"}}
        result = _extract_contacts(place)
        assert result["phone"] is None
        assert result["website"] is None

    def test_prefers_national_phone(self):
        place = {
            "nationalPhoneNumber": "+1 555-1234",
            "internationalPhoneNumber": "+1 555-1234-000",
            "displayName": {"text": "Test"},
        }
        result = _extract_contacts(place)
        assert result["phone"] == "+1 555-1234"

    def test_falls_back_to_international_phone(self):
        place = {
            "internationalPhoneNumber": "+1 555-1234-000",
            "displayName": {"text": "Test"},
        }
        result = _extract_contacts(place)
        assert result["phone"] == "+1 555-1234-000"
