"""Unit tests for business leads scoring worker.

Tests _name_looks_like_domain(), business_eligibility_filters(),
daily_platform_name(), _score_business(), and compute_verification_confidence()
edge cases. No database required.
"""
from __future__ import annotations

from datetime import date

import pytest

import domain_pipeline.workers.business_leads as bl_module
from domain_pipeline.workers.business_leads import (
    _name_looks_like_domain,
    _score_business,
    business_eligibility_filters,
    compute_verification_confidence,
    compute_verification_count,
    compute_verification_weight,
    daily_platform_name,
)


@pytest.fixture(autouse=True)
def _mock_wikidata_cache():
    """Prevent Wikidata HTTP calls by pre-setting the cache."""
    bl_module._wikidata_chain_cache = set()
    yield
    bl_module._wikidata_chain_cache = None


class TestNameLooksLikeDomain:
    """Tests for _name_looks_like_domain()."""

    def test_dot_com_suffix(self):
        assert _name_looks_like_domain("iRepair.com") is True

    def test_dot_ca_suffix(self):
        assert _name_looks_like_domain("SuperMart.ca") is True

    def test_dot_ae_suffix(self):
        assert _name_looks_like_domain("MyBiz.ae") is True

    def test_normal_name(self):
        assert _name_looks_like_domain("Joe's Pizza") is False

    def test_empty_string(self):
        assert _name_looks_like_domain("") is False

    def test_with_spaces_still_detects(self):
        assert _name_looks_like_domain("i Repair .com") is True

    def test_dot_io(self):
        assert _name_looks_like_domain("MyApp.io") is True


class TestComputeVerificationCount:
    """Tests for compute_verification_count()."""

    def test_no_raw(self):
        assert compute_verification_count(None) == 0

    def test_empty_raw(self):
        assert compute_verification_count({}) == 0

    def test_one_source(self):
        raw = {"llm_verified": True}
        assert compute_verification_count(raw) == 1

    def test_multiple_sources(self):
        raw = {"llm_verified": True, "ddg_verified": True, "searxng_verified": True}
        assert compute_verification_count(raw) == 3

    def test_false_values_not_counted(self):
        raw = {"llm_verified": False, "ddg_verified": True}
        assert compute_verification_count(raw) == 1


class TestComputeVerificationWeight:
    """Tests for compute_verification_weight()."""

    def test_no_raw(self):
        assert compute_verification_weight(None) == 0.0

    def test_empty_raw(self):
        assert compute_verification_weight({}) == 0.0

    def test_single_conclusive_source(self):
        raw = {"searxng_verified": True, "searxng_result": "no_website"}
        weight = compute_verification_weight(raw)
        assert weight == pytest.approx(0.9)

    def test_multiple_sources(self):
        raw = {
            "domain_guess_verified": True, "domain_guess_result": "no_match",
            "searxng_verified": True, "searxng_result": "no_website",
        }
        weight = compute_verification_weight(raw)
        assert weight == pytest.approx(0.7 + 0.9)

    def test_inconclusive_result_low_weight(self):
        raw = {"ddg_verified": True, "ddg_verify_result": "no_results"}
        weight = compute_verification_weight(raw)
        assert weight == pytest.approx(0.05)


class TestComputeVerificationConfidence:
    """Tests for compute_verification_confidence()."""

    def test_unverified(self):
        assert compute_verification_confidence(None) == "unverified"
        assert compute_verification_confidence({}) == "unverified"

    def test_high_confidence(self):
        raw = {
            "domain_guess_verified": True, "domain_guess_result": "no_match",
            "searxng_verified": True, "searxng_result": "no_website",
        }
        assert compute_verification_confidence(raw) == "high"

    def test_medium_confidence(self):
        raw = {"domain_guess_verified": True, "domain_guess_result": "no_match"}
        assert compute_verification_confidence(raw) == "medium"

    def test_low_confidence(self):
        raw = {"ddg_verified": True, "ddg_verify_result": "no_results"}
        assert compute_verification_confidence(raw) == "low"


class TestScoreBusiness:
    """Tests for _score_business() scoring logic."""

    def test_no_website_no_contacts_capped(self, make_business, empty_features):
        biz = make_business(name="Test Biz", website_url=None)
        score, reasons = _score_business(biz, empty_features)
        assert score <= 5.0
        assert reasons["disqualify_reason"] == "no_contacts"

    def test_no_website_with_phone_and_email(self, make_business, empty_features):
        biz = make_business(name="Test Biz", website_url=None)
        features = {**empty_features, "emails": {"info@test.com"}, "business_emails": {"info@test.com"}, "phones": {"+1234"}}
        score, reasons = _score_business(biz, features)
        assert score > 5.0

    def test_high_priority_category_bonus(self, make_business, empty_features):
        # Provide high-confidence verification so the unverified cap (35) does not flatten both scores
        verified_raw = {
            "domain_guess_verified": True, "domain_guess_result": "no_match",
            "searxng_verified": True, "searxng_result": "no_website",
        }
        biz_trades = make_business(name="Plumber Biz", category="trades", website_url=None, raw=verified_raw)
        biz_other = make_business(name="Other Biz", category="other_category", website_url=None, raw=verified_raw)
        features = {**empty_features, "emails": {"a@b.com"}, "business_emails": {"a@b.com"}, "phones": {"+1234"}}
        score_trades, _ = _score_business(biz_trades, features)
        score_other, _ = _score_business(biz_other, features)
        assert score_trades > score_other

    def test_branded_chain_disqualified(self, make_business, empty_features):
        biz = make_business(name="Tim Hortons", website_url=None, raw={"brand": "Tim Hortons"})
        features = {**empty_features, "phones": {"+1234"}}
        score, reasons = _score_business(biz, features)
        assert score == 0.0
        assert reasons["disqualify_reason"] == "branded_chain"

    def test_name_looks_like_domain_capped(self, make_business, empty_features):
        biz = make_business(name="iRepair.ca", website_url=None)
        features = {**empty_features, "emails": {"a@b.com"}, "business_emails": {"a@b.com"}, "phones": {"+1234"}}
        score, reasons = _score_business(biz, features)
        assert score <= 15.0

    def test_active_domain_disqualified(self, make_business, empty_features):
        biz = make_business(name="Test Biz", website_url=None)
        features = {**empty_features, "hosted_domains": {"test.com"}, "domains": {"test.com"}, "phones": {"+1234"}}
        score, reasons = _score_business(biz, features)
        assert score == 0.0
        assert reasons["disqualify_reason"] == "active_domain"

    def test_unknown_domain_capped(self, make_business, empty_features):
        biz = make_business(name="Test Biz", website_url=None)
        features = {**empty_features, "unknown_domains": {"test.com"}, "domains": {"test.com"}, "emails": {"a@b.com"}, "business_emails": {"a@b.com"}, "phones": {"+1234"}}
        score, _ = _score_business(biz, features)
        assert score <= 10.0

    def test_unverified_confidence_cap(self, make_business, empty_features):
        biz = make_business(name="Test Biz", website_url=None, raw=None)
        features = {**empty_features, "emails": {"a@b.com"}, "business_emails": {"a@b.com"}, "phones": {"+1234"}}
        score, _ = _score_business(biz, features)
        assert score <= 35.0

    def test_score_never_exceeds_100(self, make_business, empty_features):
        biz = make_business(
            name="Max Score Biz",
            category="trades",
            raw={
                "domain_guess_verified": True,
                "domain_guess_result": "no_match",
                "searxng_verified": True,
                "searxng_result": "no_website",
            },
        )
        features = {
            **empty_features,
            "emails": {"info@max.com"},
            "business_emails": {"info@max.com"},
            "phones": {"+1234"},
        }
        score, _ = _score_business(biz, features)
        assert score <= 100.0


class TestBusinessEligibilityFilters:
    """Tests for business_eligibility_filters() which builds SQL filter lists."""

    def test_returns_list(self):
        """Should always return a list."""
        result = business_eligibility_filters(
            require_contact=False,
            require_unhosted_domain=False,
            require_domain_qualification=False,
        )
        assert isinstance(result, list)

    def test_require_contact_adds_filter(self):
        """Requiring contact should add at least one filter."""
        without = business_eligibility_filters(
            require_contact=False,
            require_unhosted_domain=False,
            require_domain_qualification=False,
            exclude_hosted_email_domain=False,
        )
        with_contact = business_eligibility_filters(
            require_contact=True,
            require_unhosted_domain=False,
            require_domain_qualification=False,
            exclude_hosted_email_domain=False,
        )
        assert len(with_contact) > len(without)

    def test_require_domain_qualification_adds_filters(self):
        """Requiring domain qualification should add more filters."""
        without = business_eligibility_filters(
            require_contact=False,
            require_unhosted_domain=False,
            require_domain_qualification=False,
            exclude_hosted_email_domain=False,
        )
        with_qual = business_eligibility_filters(
            require_contact=False,
            require_unhosted_domain=False,
            require_domain_qualification=True,
            exclude_hosted_email_domain=False,
        )
        assert len(with_qual) > len(without)

    def test_exclude_hosted_adds_filter(self):
        """exclude_hosted_email_domain=True should add a filter."""
        without = business_eligibility_filters(
            require_contact=False,
            require_unhosted_domain=False,
            require_domain_qualification=False,
            exclude_hosted_email_domain=False,
        )
        with_excl = business_eligibility_filters(
            require_contact=False,
            require_unhosted_domain=False,
            require_domain_qualification=False,
            exclude_hosted_email_domain=True,
        )
        assert len(with_excl) > len(without)

    def test_all_flags_false_returns_empty_or_minimal(self):
        """All flags false should return a minimal (possibly empty) filter list."""
        result = business_eligibility_filters(
            require_contact=False,
            require_unhosted_domain=False,
            require_domain_qualification=False,
            exclude_hosted_email_domain=False,
        )
        assert isinstance(result, list)
        assert len(result) == 0


class TestDailyPlatformName:
    """Tests for daily_platform_name() date-based platform names."""

    def test_specific_date(self):
        result = daily_platform_name(for_date=date(2025, 6, 15))
        assert result == "daily_20250615"

    def test_custom_prefix(self):
        result = daily_platform_name(for_date=date(2025, 1, 1), prefix="weekly")
        assert result == "weekly_20250101"

    def test_default_prefix_is_daily(self):
        result = daily_platform_name(for_date=date(2025, 3, 10))
        assert result.startswith("daily_")

    def test_leading_zeros(self):
        result = daily_platform_name(for_date=date(2025, 1, 5))
        assert result == "daily_20250105"

    def test_end_of_year(self):
        result = daily_platform_name(for_date=date(2025, 12, 31))
        assert result == "daily_20251231"


class TestComputeVerificationConfidenceEdgeCases:
    """Additional edge case tests for compute_verification_confidence()."""

    def test_false_verified_key_is_unverified(self):
        """A verification key set to False should count as unverified."""
        raw = {"domain_guess_verified": False}
        assert compute_verification_confidence(raw) == "unverified"

    def test_mixed_true_and_false_keys(self):
        """Only True verification keys should contribute to confidence."""
        raw = {
            "domain_guess_verified": True,
            "domain_guess_result": "no_match",
            "searxng_verified": False,
        }
        # Only domain_guess contributes: 0.7 -> medium
        assert compute_verification_confidence(raw) == "medium"
