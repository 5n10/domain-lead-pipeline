"""Unit tests for domain_pipeline.workers.dedup module."""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch


from domain_pipeline.workers.dedup import (
    PROXIMITY_THRESHOLD_M,
    haversine_distance,
    normalize_name,
    _pick_primary,
    mark_duplicates,
    dismiss_duplicate,
    duplicate_count,
)


# ---------- haversine_distance ----------

class TestHaversineDistance:
    def test_same_point_is_zero(self):
        assert haversine_distance(52.0, 4.0, 52.0, 4.0) == 0.0

    def test_known_distance(self):
        # Amsterdam Central to Dam Square: roughly 800m
        dist = haversine_distance(52.3791, 4.9003, 52.3730, 4.8932)
        assert 600 < dist < 1000

    def test_very_close_points(self):
        # Points ~50m apart
        dist = haversine_distance(52.37, 4.89, 52.3704, 4.89)
        assert dist < PROXIMITY_THRESHOLD_M

    def test_far_apart_cities(self):
        # Amsterdam to Rotterdam: ~60km
        dist = haversine_distance(52.3676, 4.9041, 51.9225, 4.4792)
        assert dist > 50_000

    def test_antipodal_points(self):
        # Roughly halfway around the world
        dist = haversine_distance(0, 0, 0, 180)
        assert dist > 19_000_000


# ---------- normalize_name ----------

class TestNormalizeName:
    def test_lowercases(self):
        assert normalize_name("ACME Corp") == "acme corp"

    def test_strips_punctuation(self):
        assert normalize_name("Joe's Café & Bar") == "joes café bar"

    def test_collapses_whitespace(self):
        assert normalize_name("  Multi   Space  ") == "multi space"

    def test_empty_string(self):
        assert normalize_name("") == ""


# ---------- _pick_primary ----------

class TestPickPrimary:
    def test_higher_score_wins(self):
        a = {"id": "a", "lead_score": 80, "created_at": "2024-01-01"}
        b = {"id": "b", "lead_score": 60, "created_at": "2024-01-01"}
        primary, dup = _pick_primary(a, b)
        assert primary["id"] == "a"
        assert dup["id"] == "b"

    def test_tie_earlier_created_wins(self):
        a = {"id": "a", "lead_score": 50, "created_at": "2024-06-01"}
        b = {"id": "b", "lead_score": 50, "created_at": "2024-01-01"}
        primary, dup = _pick_primary(a, b)
        assert primary["id"] == "b"

    def test_none_scores_treated_as_zero(self):
        a = {"id": "a", "lead_score": None, "created_at": "2024-01-01"}
        b = {"id": "b", "lead_score": 10, "created_at": "2024-01-01"}
        primary, dup = _pick_primary(a, b)
        assert primary["id"] == "b"


# ---------- mark_duplicates ----------

class TestMarkDuplicates:
    @patch("domain_pipeline.workers.dedup.session_scope")
    def test_marks_pairs(self, mock_scope):
        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.rowcount = 1

        pairs = [
            {"primary_id": uuid.uuid4(), "duplicate_id": uuid.uuid4(), "reason": "test"},
            {"primary_id": uuid.uuid4(), "duplicate_id": uuid.uuid4(), "reason": "test2"},
        ]
        count = mark_duplicates(pairs)
        assert count == 2

    def test_empty_pairs(self):
        assert mark_duplicates([]) == 0


# ---------- dismiss_duplicate ----------

class TestDismissDuplicate:
    @patch("domain_pipeline.workers.dedup.session_scope")
    def test_dismisses(self, mock_scope):
        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.rowcount = 1

        assert dismiss_duplicate(uuid.uuid4()) is True

    @patch("domain_pipeline.workers.dedup.session_scope")
    def test_not_found(self, mock_scope):
        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.rowcount = 0

        assert dismiss_duplicate(uuid.uuid4()) is False


# ---------- duplicate_count ----------

class TestDuplicateCount:
    @patch("domain_pipeline.workers.dedup.session_scope")
    def test_returns_count(self, mock_scope):
        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.execute.return_value.scalar.return_value = 15

        assert duplicate_count() == 15
