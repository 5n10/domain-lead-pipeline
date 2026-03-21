"""Unit tests for compute_verification_confidence() and related scoring functions.

Tests the weighted confidence scoring system that determines lead quality
from multiple verification sources (domain_guess, searxng, llm, ddg, etc.).
"""
from __future__ import annotations

import pytest

from domain_pipeline.workers.business_leads import (
    compute_verification_confidence,
    compute_verification_count,
    compute_verification_weight,
    get_verification_sources,
)


# ---------------------------------------------------------------------------
# compute_verification_confidence
# ---------------------------------------------------------------------------


class TestComputeVerificationConfidence:
    def test_none_raw_returns_unverified(self):
        assert compute_verification_confidence(None) == "unverified"

    def test_empty_raw_returns_unverified(self):
        assert compute_verification_confidence({}) == "unverified"

    def test_no_verification_keys_returns_unverified(self):
        assert compute_verification_confidence({"some_key": True}) == "unverified"

    def test_domain_guess_no_match_plus_searxng_no_website_is_high(self):
        # 0.7 + 0.9 = 1.6 >= 1.5
        raw = {
            "domain_guess_verified": True,
            "domain_guess_result": "no_match",
            "searxng_verified": True,
            "searxng_result": "no_website",
        }
        assert compute_verification_confidence(raw) == "high"

    def test_domain_guess_no_match_alone_is_medium(self):
        # 0.7 >= 0.7
        raw = {
            "domain_guess_verified": True,
            "domain_guess_result": "no_match",
        }
        assert compute_verification_confidence(raw) == "medium"

    def test_domain_guess_no_match_plus_llm_not_sure_is_medium(self):
        # 0.7 + 0.2 = 0.9 >= 0.7 but < 1.5
        raw = {
            "domain_guess_verified": True,
            "domain_guess_result": "no_match",
            "llm_verified": True,
            "llm_verify_result": "not_sure",
        }
        assert compute_verification_confidence(raw) == "medium"

    def test_ddg_no_results_only_is_low(self):
        # 0.05 > 0 but < 0.7
        raw = {
            "ddg_verified": True,
            "ddg_verify_result": "no_results",
        }
        assert compute_verification_confidence(raw) == "low"

    def test_google_search_blocked_is_low(self):
        # 0.0 > 0 check — actually 0.0 still means verified
        raw = {
            "google_search_verified": True,
            "google_search_result": "blocked",
        }
        assert compute_verification_confidence(raw) == "low"

    def test_multiple_inconclusive_still_low(self):
        # ddg_no_results (0.05) + google_search_no_results (0.05) = 0.1
        raw = {
            "ddg_verified": True,
            "ddg_verify_result": "no_results",
            "google_search_verified": True,
            "google_search_result": "no_results",
        }
        assert compute_verification_confidence(raw) == "low"

    def test_three_sources_all_conclusive_is_high(self):
        # domain_guess no_match (0.7) + searxng no_website (0.9) + llm no_website (0.8) = 2.4
        raw = {
            "domain_guess_verified": True,
            "domain_guess_result": "no_match",
            "searxng_verified": True,
            "searxng_result": "no_website",
            "llm_verified": True,
            "llm_verify_result": "no_website",
        }
        assert compute_verification_confidence(raw) == "high"

    def test_google_places_no_website_plus_domain_guess_is_high(self):
        # google_places no_website (0.9) + domain_guess no_match (0.7) = 1.6
        raw = {
            "google_places_verified": True,
            "google_places_verify_result": "no_website",
            "domain_guess_verified": True,
            "domain_guess_result": "no_match",
        }
        assert compute_verification_confidence(raw) == "high"

    def test_has_website_results_contribute_to_weight(self):
        # If a source finds a website, it still contributes to confidence weight
        raw = {
            "searxng_verified": True,
            "searxng_result": "has_website",
        }
        # has_website weight is 1.0, so >= 0.7 → medium
        assert compute_verification_confidence(raw) == "medium"

    def test_domain_guess_no_candidates_is_low(self):
        # 0.1 < 0.7
        raw = {
            "domain_guess_verified": True,
            "domain_guess_result": "no_candidates",
        }
        assert compute_verification_confidence(raw) == "low"

    def test_foursquare_no_website_is_medium(self):
        # 0.7 >= 0.7
        raw = {
            "foursquare_verified": True,
            "foursquare_verify_result": "no_website",
        }
        assert compute_verification_confidence(raw) == "medium"

    def test_all_seven_sources_no_website_is_high(self):
        raw = {
            "domain_guess_verified": True,
            "domain_guess_result": "no_match",
            "searxng_verified": True,
            "searxng_result": "no_website",
            "llm_verified": True,
            "llm_verify_result": "no_website",
            "ddg_verified": True,
            "ddg_verify_result": "no_website",
            "google_search_verified": True,
            "google_search_result": "no_website",
            "google_places_verified": True,
            "google_places_verify_result": "no_website",
            "foursquare_verified": True,
            "foursquare_verify_result": "no_website",
        }
        assert compute_verification_confidence(raw) == "high"


# ---------------------------------------------------------------------------
# compute_verification_weight
# ---------------------------------------------------------------------------


class TestComputeVerificationWeight:
    def test_none_returns_zero(self):
        assert compute_verification_weight(None) == 0.0

    def test_empty_returns_zero(self):
        assert compute_verification_weight({}) == 0.0

    def test_single_source_weight(self):
        raw = {
            "domain_guess_verified": True,
            "domain_guess_result": "no_match",
        }
        assert compute_verification_weight(raw) == pytest.approx(0.7)

    def test_unknown_result_gets_default_weight(self):
        raw = {
            "domain_guess_verified": True,
            "domain_guess_result": "some_unknown_result",
        }
        assert compute_verification_weight(raw) == pytest.approx(0.1)

    def test_legacy_data_without_result_key(self):
        # If a verification key exists but no result key mapping
        raw = {"llm_verified": True}
        # Missing llm_verify_result → weights.get("", 0.1) → 0.1
        assert compute_verification_weight(raw) == pytest.approx(0.1)

    def test_additive_weights(self):
        raw = {
            "domain_guess_verified": True,
            "domain_guess_result": "no_match",  # 0.7
            "searxng_verified": True,
            "searxng_result": "no_website",  # 0.9
        }
        assert compute_verification_weight(raw) == pytest.approx(1.6)


# ---------------------------------------------------------------------------
# compute_verification_count / get_verification_sources
# ---------------------------------------------------------------------------


class TestVerificationCount:
    def test_none_returns_zero(self):
        assert compute_verification_count(None) == 0

    def test_empty_returns_zero(self):
        assert compute_verification_count({}) == 0

    def test_one_source(self):
        raw = {"domain_guess_verified": True}
        assert compute_verification_count(raw) == 1

    def test_three_sources(self):
        raw = {
            "domain_guess_verified": True,
            "searxng_verified": True,
            "llm_verified": True,
        }
        assert compute_verification_count(raw) == 3

    def test_false_values_not_counted(self):
        raw = {"domain_guess_verified": False}
        assert compute_verification_count(raw) == 0


class TestGetVerificationSources:
    def test_none_returns_empty(self):
        assert get_verification_sources(None) == []

    def test_returns_source_names(self):
        raw = {
            "domain_guess_verified": True,
            "searxng_verified": True,
        }
        sources = get_verification_sources(raw)
        assert "domain_guess" in sources
        assert "searxng" in sources
        assert len(sources) == 2
