"""Unit tests for chain/franchise detection.

Tests _is_branded_chain() and _is_wikidata_chain() logic with mocked
Wikidata responses to avoid network calls.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from domain_pipeline.workers.business_leads import (
    _is_branded_chain,
    _is_wikidata_chain,
    _load_wikidata_chains,
)


@pytest.fixture(autouse=True)
def _clear_wikidata_cache():
    """Reset the module-level Wikidata cache before each test."""
    import domain_pipeline.workers.business_leads as bl
    original = bl._wikidata_chain_cache
    bl._wikidata_chain_cache = None
    yield
    bl._wikidata_chain_cache = original


class TestIsBrandedChain:
    def test_brand_wikidata_tag(self, make_business):
        biz = make_business(name="Starbucks", raw={"brand:wikidata": "Q37158"})
        assert _is_branded_chain(biz) is True

    def test_operator_wikidata_tag(self, make_business):
        biz = make_business(name="Some Franchise", raw={"operator:wikidata": "Q12345"})
        assert _is_branded_chain(biz) is True

    def test_brand_tag_alone(self, make_business):
        biz = make_business(name="Tim Hortons", raw={"brand": "Tim Hortons"})
        assert _is_branded_chain(biz) is True

    def test_no_brand_tags(self, make_business):
        with patch(
            "domain_pipeline.workers.business_leads._is_wikidata_chain",
            return_value=False,
        ):
            biz = make_business(name="Local Plumber", raw={})
            assert _is_branded_chain(biz) is False

    def test_none_raw(self, make_business):
        with patch(
            "domain_pipeline.workers.business_leads._is_wikidata_chain",
            return_value=False,
        ):
            biz = make_business(name="Local Biz", raw=None)
            assert _is_branded_chain(biz) is False

    def test_wikidata_match_triggers_chain(self, make_business):
        with patch(
            "domain_pipeline.workers.business_leads._load_wikidata_chains",
            return_value={"mcdonald's", "starbucks", "tim hortons"},
        ):
            biz = make_business(name="Tim Hortons #1234", raw={})
            assert _is_branded_chain(biz) is True

    def test_wikidata_no_match(self, make_business):
        with patch(
            "domain_pipeline.workers.business_leads._load_wikidata_chains",
            return_value={"mcdonald's", "starbucks"},
        ):
            biz = make_business(name="Local Bakery", raw={})
            assert _is_branded_chain(biz) is False


class TestIsWikidataChain:
    @patch(
        "domain_pipeline.workers.business_leads._load_wikidata_chains",
        return_value={"mcdonald's", "starbucks", "tim hortons", "subway"},
    )
    def test_exact_match(self, mock_load):
        assert _is_wikidata_chain("Starbucks") is True

    @patch(
        "domain_pipeline.workers.business_leads._load_wikidata_chains",
        return_value={"mcdonald's", "starbucks", "tim hortons", "subway"},
    )
    def test_substring_match(self, mock_load):
        # "Tim Hortons #1234" contains "tim hortons"
        assert _is_wikidata_chain("Tim Hortons #1234") is True

    @patch(
        "domain_pipeline.workers.business_leads._load_wikidata_chains",
        return_value={"mcdonald's", "starbucks"},
    )
    def test_no_match(self, mock_load):
        assert _is_wikidata_chain("Joe's Local Diner") is False

    @patch(
        "domain_pipeline.workers.business_leads._load_wikidata_chains",
        return_value=set(),
    )
    def test_empty_chain_set(self, mock_load):
        assert _is_wikidata_chain("Starbucks") is False

    def test_empty_name(self):
        assert _is_wikidata_chain("") is False

    def test_none_name(self):
        assert _is_wikidata_chain(None) is False

    @patch(
        "domain_pipeline.workers.business_leads._load_wikidata_chains",
        return_value={"ab"},  # Short chain name (< 4 chars)
    )
    def test_short_chain_name_not_substring_matched(self, mock_load):
        # Chain names < 4 chars are only exact matched, not substring matched
        assert _is_wikidata_chain("abcdef") is False
        # But exact match still works
        assert _is_wikidata_chain("ab") is True

    @patch(
        "domain_pipeline.workers.business_leads._load_wikidata_chains",
        return_value={"subway"},
    )
    def test_case_insensitive_match(self, mock_load):
        assert _is_wikidata_chain("SUBWAY") is True
        assert _is_wikidata_chain("Subway") is True


class TestLoadWikidataChains:
    @patch("domain_pipeline.workers.business_leads.requests.get")
    def test_successful_load(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "results": {
                "bindings": [
                    {"label": {"value": "McDonald's"}},
                    {"label": {"value": "Starbucks"}},
                    {"label": {"value": "Tim Hortons"}},
                ]
            }
        }
        chains = _load_wikidata_chains()
        assert "mcdonald's" in chains
        assert "starbucks" in chains
        assert "tim hortons" in chains
        assert len(chains) == 3

    @patch("domain_pipeline.workers.business_leads.requests.get")
    def test_wikidata_failure_returns_empty(self, mock_get):
        mock_get.side_effect = Exception("Network error")
        chains = _load_wikidata_chains()
        assert chains == set()

    @patch("domain_pipeline.workers.business_leads.requests.get")
    def test_wikidata_non_200_returns_empty(self, mock_get):
        mock_get.return_value.status_code = 500
        chains = _load_wikidata_chains()
        assert chains == set()
