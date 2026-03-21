"""Unit tests for _score_business() and related scoring logic.

Tests lead scoring weights, disqualification rules, quality caps,
and verification confidence caps.
"""
from __future__ import annotations



from domain_pipeline.workers.business_leads import (
    _name_looks_like_domain,
    _score_business,
)


class TestScoreBusiness:
    """Tests for _score_business() using detached Business objects."""

    def test_no_website_no_contacts_capped_at_5(self, make_business, empty_features):
        biz = make_business(name="Solo Biz", category="trades")
        score, reasons = _score_business(biz, empty_features)
        assert score <= 5.0
        assert reasons["disqualify_reason"] == "no_contacts"

    def test_no_website_with_phone_scores_correctly(self, make_business, empty_features):
        biz = make_business(name="Plumber Joe", category="trades")
        features = {**empty_features, "phones": {"+971500000001"}}
        score, reasons = _score_business(biz, features)
        # no_website (25) + phone (15) + high_priority_category (20) = 60
        # But capped by verification confidence (unverified → 35)
        assert score == 35.0

    def test_no_website_with_business_email_and_phone(self, make_business, empty_features):
        biz = make_business(
            name="ABC Plumbing",
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
            "emails": {"info@abcplumbing.com"},
            "business_emails": {"info@abcplumbing.com"},
            "phones": {"+971500000001"},
        }
        score, reasons = _score_business(biz, features)
        # no_website (25) + business_email (20) + phone (15) + high_priority (20) = 80
        # high confidence → no cap
        assert score == 80.0

    def test_high_priority_category_bonus(self, make_business, empty_features):
        biz = make_business(
            name="Joe Contractor",
            category="contractors",
            raw={
                "domain_guess_verified": True,
                "domain_guess_result": "no_match",
                "searxng_verified": True,
                "searxng_result": "no_website",
            },
        )
        features = {**empty_features, "phones": {"+1234"}}
        score, _ = _score_business(biz, features)
        # 25 + 15 + 20 = 60, high confidence
        assert score == 60.0

    def test_medium_priority_category_bonus(self, make_business, empty_features):
        biz = make_business(
            name="Nice Retail",
            category="retail",
            raw={
                "domain_guess_verified": True,
                "domain_guess_result": "no_match",
                "searxng_verified": True,
                "searxng_result": "no_website",
            },
        )
        features = {**empty_features, "phones": {"+1234"}}
        score, _ = _score_business(biz, features)
        # 25 + 15 + 10 = 50
        assert score == 50.0

    def test_any_category_bonus(self, make_business, empty_features):
        biz = make_business(
            name="Misc Biz",
            category="other_stuff",
            raw={
                "domain_guess_verified": True,
                "domain_guess_result": "no_match",
                "searxng_verified": True,
                "searxng_result": "no_website",
            },
        )
        features = {**empty_features, "phones": {"+1234"}}
        score, _ = _score_business(biz, features)
        # 25 + 15 + 5 = 45
        assert score == 45.0

    def test_free_email_gives_lower_bonus(self, make_business, empty_features):
        biz = make_business(
            name="Gmail User",
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
            "emails": {"user@gmail.com"},
            "free_emails": {"user@gmail.com"},
        }
        score, _ = _score_business(biz, features)
        # 25 + 5 (any_email, not business_email) + 20 = 50
        assert score == 50.0

    def test_branded_chain_disqualified(self, make_business, empty_features):
        biz = make_business(name="Tim Hortons", raw={"brand": "Tim Hortons"})
        features = {**empty_features, "phones": {"+1234"}}
        score, reasons = _score_business(biz, features)
        assert score == 0.0
        assert reasons["disqualify_reason"] == "branded_chain"

    def test_brand_wikidata_disqualifies(self, make_business, empty_features):
        biz = make_business(name="Starbucks", raw={"brand:wikidata": "Q37158"})
        features = {**empty_features, "phones": {"+1234"}}
        score, reasons = _score_business(biz, features)
        assert score == 0.0
        assert reasons["disqualify_reason"] == "branded_chain"

    def test_active_domain_disqualifies(self, make_business, empty_features):
        biz = make_business(name="Active Domain Biz")
        features = {
            **empty_features,
            "hosted_domains": {"activebiz.com"},
            "domains": {"activebiz.com"},
        }
        score, reasons = _score_business(biz, features)
        assert score == 0.0
        assert reasons["disqualify_reason"] == "active_domain"

    def test_parked_domain_disqualifies(self, make_business, empty_features):
        biz = make_business(name="Parked Biz")
        features = {
            **empty_features,
            "parked_domains": {"parkedbiz.com"},
            "domains": {"parkedbiz.com"},
        }
        score, reasons = _score_business(biz, features)
        assert score == 0.0
        assert reasons["disqualify_reason"] == "active_domain"

    def test_registered_domain_disqualifies(self, make_business, empty_features):
        biz = make_business(name="Registered Biz")
        features = {
            **empty_features,
            "registered_domains": {"registered.com"},
            "domains": {"registered.com"},
        }
        score, reasons = _score_business(biz, features)
        assert score == 0.0
        assert reasons["disqualify_reason"] == "active_domain"

    def test_unknown_domain_caps_at_10(self, make_business, empty_features):
        biz = make_business(name="Unknown Biz", category="trades")
        features = {
            **empty_features,
            "phones": {"+1234"},
            "unknown_domains": {"unknown.com"},
            "domains": {"unknown.com"},
        }
        score, _ = _score_business(biz, features)
        assert score <= 10.0

    def test_name_looks_like_domain_caps_at_15(self, make_business, empty_features):
        biz = make_business(name="iRepair.ca", category="trades")
        features = {**empty_features, "phones": {"+1234"}}
        score, reasons = _score_business(biz, features)
        assert score <= 15.0
        assert reasons["disqualify_reason"] == "name_is_domain"

    def test_unverified_capped_at_35(self, make_business, empty_features):
        biz = make_business(name="Unverified Biz", category="trades")
        features = {
            **empty_features,
            "emails": {"info@unverified.com"},
            "business_emails": {"info@unverified.com"},
            "phones": {"+1234"},
        }
        score, _ = _score_business(biz, features)
        # Raw score would be 80, but unverified caps at 35
        assert score == 35.0

    def test_low_confidence_capped_at_50(self, make_business, empty_features):
        biz = make_business(
            name="Low Confidence Biz",
            category="trades",
            raw={
                "ddg_verified": True,
                "ddg_verify_result": "no_results",
            },
        )
        features = {
            **empty_features,
            "emails": {"info@lowconf.com"},
            "business_emails": {"info@lowconf.com"},
            "phones": {"+1234"},
        }
        score, _ = _score_business(biz, features)
        # Raw would be 80, low confidence caps at 50
        assert score == 50.0

    def test_medium_confidence_no_cap(self, make_business, empty_features):
        biz = make_business(
            name="Medium Confidence Biz",
            category="trades",
            raw={
                "domain_guess_verified": True,
                "domain_guess_result": "no_match",
            },
        )
        features = {
            **empty_features,
            "emails": {"info@medconf.com"},
            "business_emails": {"info@medconf.com"},
            "phones": {"+1234"},
        }
        score, _ = _score_business(biz, features)
        # 25 + 20 + 15 + 20 = 80, medium confidence → no cap
        assert score == 80.0

    def test_has_website_still_scores_zero_base(self, make_business, empty_features):
        biz = make_business(
            name="Has Website Biz",
            website_url="https://example.com",
            category="trades",
            raw={
                "domain_guess_verified": True,
                "domain_guess_result": "no_match",
                "searxng_verified": True,
                "searxng_result": "no_website",
            },
        )
        features = {**empty_features, "phones": {"+1234"}}
        score, _ = _score_business(biz, features)
        # Has website → no SCORE_NO_WEBSITE, but still gets phone + category
        # 0 + 15 + 20 = 35
        assert score == 35.0

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


class TestNameLooksLikeDomain:
    def test_empty_name(self):
        assert _name_looks_like_domain("") is False

    def test_none_name(self):
        assert _name_looks_like_domain(None) is False

    def test_domain_like_name(self):
        assert _name_looks_like_domain("iRepair.ca") is True

    def test_domain_like_with_space(self):
        assert _name_looks_like_domain("super mart.com") is True

    def test_normal_business_name(self):
        assert _name_looks_like_domain("Joe's Plumbing") is False

    def test_ae_tld(self):
        assert _name_looks_like_domain("company.ae") is True

    def test_io_tld(self):
        assert _name_looks_like_domain("startup.io") is True
