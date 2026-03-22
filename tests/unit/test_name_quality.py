"""Unit tests for domain_guess name quality filter."""
from __future__ import annotations

from domain_pipeline.workers.domain_guess import (
    GENERIC_SINGLE_WORDS,
    is_name_quality_for_domain_guess,
)


class TestIsNameQualityForDomainGuess:
    # --- Should pass (specific enough for domain guess) ---

    def test_multi_word_name(self):
        assert is_name_quality_for_domain_guess("Sunrise Bakery") is True

    def test_two_word_specific(self):
        assert is_name_quality_for_domain_guess("Mario's Pizza") is True

    def test_unique_single_word(self):
        assert is_name_quality_for_domain_guess("Starbucks") is True

    def test_long_name(self):
        assert is_name_quality_for_domain_guess("The Grand Old Bakery of Amsterdam") is True

    def test_name_with_numbers(self):
        assert is_name_quality_for_domain_guess("Studio 54") is True

    def test_generic_word_in_multi_word(self):
        """Generic words are fine when combined with other words."""
        assert is_name_quality_for_domain_guess("Golden Salon") is True

    # --- Should fail (too generic) ---

    def test_single_word_salon(self):
        assert is_name_quality_for_domain_guess("Salon") is False

    def test_single_word_bakery(self):
        assert is_name_quality_for_domain_guess("Bakery") is False

    def test_single_word_restaurant(self):
        assert is_name_quality_for_domain_guess("Restaurant") is False

    def test_single_word_case_insensitive(self):
        assert is_name_quality_for_domain_guess("GARAGE") is False

    def test_empty_string(self):
        assert is_name_quality_for_domain_guess("") is False

    def test_none(self):
        assert is_name_quality_for_domain_guess(None) is False

    def test_whitespace_only(self):
        assert is_name_quality_for_domain_guess("   ") is False

    def test_very_short_name(self):
        """Single character or two characters are too short."""
        assert is_name_quality_for_domain_guess("A") is False

    def test_two_char_name(self):
        assert is_name_quality_for_domain_guess("AB") is False

    def test_entity_suffix_only(self):
        """Name that's just entity suffixes like 'Ltd' or 'Inc' has no meaningful words."""
        assert is_name_quality_for_domain_guess("Ltd") is False

    # --- Edge cases ---

    def test_generic_word_with_entity_suffix(self):
        """'Salon LLC' — 'LLC' is stripped, leaving just 'Salon' which is generic."""
        assert is_name_quality_for_domain_guess("Salon LLC") is False

    def test_all_generic_words_covered(self):
        """Every word in GENERIC_SINGLE_WORDS should be rejected when alone."""
        for word in GENERIC_SINGLE_WORDS:
            assert is_name_quality_for_domain_guess(word) is False, f"'{word}' should be rejected"
