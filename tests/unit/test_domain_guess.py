"""Unit tests for domain guess candidate generation.

Tests _clean_business_name(), _generate_candidates(), _singular_plural_variants(),
and COUNTRY_TLDS mapping. These are pure functions with no DB or network dependency.
"""
from __future__ import annotations

import pytest

from domain_pipeline.workers.domain_guess import (
    COUNTRY_TLDS,
    DEFAULT_TLDS,
    ENTITY_SUFFIXES,
    STRIP_ALWAYS,
    _clean_business_name,
    _generate_candidates,
    _singular_plural_variants,
)


class TestCleanBusinessName:
    def test_empty_name(self):
        assert _clean_business_name("") == []

    def test_none_name(self):
        assert _clean_business_name(None) == []

    def test_strips_entity_suffixes(self):
        words = _clean_business_name("Acme Corp LLC")
        assert "llc" not in words
        assert "corp" not in words

    def test_strips_stop_words_by_default(self):
        words = _clean_business_name("The Village Cobbler Services")
        assert "the" not in words
        assert "services" not in words
        assert "village" in words
        assert "cobbler" in words

    def test_keep_articles_mode(self):
        words = _clean_business_name("The Village Cobbler", keep_articles=True)
        assert "the" in words
        assert "village" in words
        assert "cobbler" in words

    def test_keep_articles_still_strips_generic_words(self):
        words = _clean_business_name("The Village Services", keep_articles=True)
        assert "the" in words
        assert "village" in words
        assert "services" not in words

    def test_keep_category_mode(self):
        words = _clean_business_name("Dima Laundry LLC", keep_category=True)
        assert "dima" in words
        assert "laundry" in words
        assert "llc" not in words

    def test_ampersand_replaced_with_and(self):
        words = _clean_business_name("Curry & Co.")
        assert "and" not in words  # "and" is an article, stripped by default
        words_with_articles = _clean_business_name("Curry & Co.", keep_articles=True)
        assert "and" in words_with_articles

    def test_special_chars_removed(self):
        words = _clean_business_name("Joe's #1 Plumbing!")
        assert all(w.isalnum() for w in words)

    def test_single_char_words_filtered(self):
        words = _clean_business_name("A B C Plumbing")
        # Single chars should be filtered (len >= 2 requirement)
        assert all(len(w) >= 2 for w in words)

    def test_lowercased(self):
        words = _clean_business_name("GTA Heating")
        assert all(w == w.lower() for w in words)

    def test_arabic_articles_stripped_by_default(self):
        words = _clean_business_name("Al Haramain Trading")
        assert "al" not in words
        assert "trading" not in words

    def test_arabic_articles_kept_when_requested(self):
        words = _clean_business_name("Al Haramain", keep_articles=True)
        assert "al" in words


class TestSingularPluralVariants:
    def test_plural_s_to_singular(self):
        variants = _singular_plural_variants("mortonmotors")
        assert "mortonmotors" in variants
        assert "mortonmotor" in variants

    def test_singular_to_plural(self):
        variants = _singular_plural_variants("mortonmotor")
        assert "mortonmotor" in variants
        assert "mortonmotors" in variants

    def test_ies_to_y(self):
        variants = _singular_plural_variants("deliveries")
        assert "deliveries" in variants
        assert "delivery" in variants

    def test_ses_to_s(self):
        variants = _singular_plural_variants("buses")
        assert "buses" in variants
        assert "bus" in variants

    def test_xes_to_x(self):
        variants = _singular_plural_variants("boxes")
        assert "boxes" in variants
        assert "box" in variants

    def test_no_variant_for_y_ending(self):
        variants = _singular_plural_variants("dentistry")
        assert len(variants) == 1
        assert "dentistry" in variants

    def test_sh_gets_es(self):
        variants = _singular_plural_variants("brush")
        assert "brush" in variants
        assert "brushes" in variants

    def test_short_base_no_variants(self):
        variants = _singular_plural_variants("abcd")
        assert variants == ["abcd"]

    def test_ss_ending_not_truncated(self):
        variants = _singular_plural_variants("glass")
        assert "glass" in variants
        # Should NOT truncate the trailing 's' for words ending in 'ss'
        assert "glas" not in variants


class TestGenerateCandidates:
    def test_empty_name_returns_empty(self):
        assert _generate_candidates("", None) == []

    def test_basic_two_word_name(self):
        candidates = _generate_candidates("GTA Heating", "CA")
        # Should include gtaheating.ca, gtaheating.com, gta-heating.ca, etc.
        domains = set(candidates)
        assert "gtaheating.ca" in domains or "gtaheating.com" in domains

    def test_uses_country_tlds(self):
        candidates = _generate_candidates("Test Business", "AE")
        tld_set = {c.split(".")[-1] for c in candidates}
        assert "ae" in tld_set or "com" in tld_set

    def test_always_includes_com(self):
        candidates = _generate_candidates("Test Business", "AE")
        com_domains = [c for c in candidates if c.endswith(".com")]
        assert len(com_domains) > 0

    def test_default_tlds_for_unknown_country(self):
        candidates = _generate_candidates("Test Business", "ZZ")
        # Should fall back to DEFAULT_TLDS (.com, .net)
        tlds_used = {c[c.rindex("."):] for c in candidates}
        assert ".com" in tlds_used

    def test_hyphenated_variants_generated(self):
        candidates = _generate_candidates("GTA Heating Cooling", "CA")
        domains = set(candidates)
        # Should include hyphenated variant
        has_hyphen = any("-" in c for c in candidates)
        assert has_hyphen

    def test_the_article_variants(self):
        candidates = _generate_candidates("The Village Cobbler", "CA")
        domains = set(candidates)
        # Should include both with and without "the"
        has_village_cobbler = any("villagecobbler" in c for c in candidates)
        has_the_village = any("thevillagecobbler" in c for c in candidates)
        assert has_village_cobbler
        assert has_the_village

    def test_category_word_variants(self):
        candidates = _generate_candidates("Dima Laundry", "AE")
        domains = set(candidates)
        # Track 3 should generate "dimalaundry" even though "laundry" is a stop word
        has_dimalaundry = any("dimalaundry" in c for c in candidates)
        assert has_dimalaundry

    def test_singular_plural_in_candidates(self):
        candidates = _generate_candidates("Morton Motors", "CA")
        domains = set(candidates)
        has_motors = any("mortonmotors" in c for c in candidates)
        has_motor = any("mortonmotor." in c for c in candidates)
        assert has_motors
        assert has_motor

    def test_acronym_handling(self):
        candidates = _generate_candidates("GTA Heating", "CA")
        # "GTA" is all uppercase → should generate gta + remaining words
        has_gta = any(c.startswith("gta") for c in candidates)
        assert has_gta

    def test_candidates_not_too_long(self):
        candidates = _generate_candidates("Very Long Business Name With Many Words Here", "CA")
        for c in candidates:
            # Domain part (without TLD) should be <= 40 chars
            base = c[:c.rindex(".")]
            assert len(base) <= 40

    def test_candidates_not_too_short(self):
        candidates = _generate_candidates("Normal Name", "CA")
        for c in candidates:
            base = c[:c.rindex(".")]
            assert len(base) >= 3


class TestCountryTLDs:
    def test_ae_tlds(self):
        assert ".ae" in COUNTRY_TLDS["AE"]
        assert ".com" in COUNTRY_TLDS["AE"]

    def test_ca_tlds(self):
        assert ".ca" in COUNTRY_TLDS["CA"]
        assert ".com" in COUNTRY_TLDS["CA"]

    def test_us_tlds(self):
        assert ".com" in COUNTRY_TLDS["US"]
        assert ".us" in COUNTRY_TLDS["US"]

    def test_gb_has_couk(self):
        assert ".co.uk" in COUNTRY_TLDS["GB"]

    def test_default_tlds(self):
        assert ".com" in DEFAULT_TLDS
        assert ".net" in DEFAULT_TLDS


class TestEntitySuffixes:
    def test_common_suffixes_present(self):
        for suffix in ["llc", "ltd", "inc", "corp", "co"]:
            assert suffix in ENTITY_SUFFIXES

    def test_uae_suffixes_present(self):
        for suffix in ["fzc", "fze", "fz", "dmcc"]:
            assert suffix in ENTITY_SUFFIXES
