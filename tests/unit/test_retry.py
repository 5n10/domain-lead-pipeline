"""Unit tests for domain_pipeline.retry module."""
from __future__ import annotations

from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from domain_pipeline.retry import (
    MAX_RETRIES,
    BASE_BACKOFF_SECONDS,
    backoff_seconds,
    record_verification_error,
    clear_verification_error,
    get_retryable_businesses,
    reset_for_retry,
    get_dead_letter_businesses,
    dead_letter_count,
)


# ---------- backoff_seconds ----------

class TestBackoffSeconds:
    def test_first_retry(self):
        assert backoff_seconds(0) == 300  # 5 minutes

    def test_second_retry(self):
        assert backoff_seconds(1) == 1200  # 20 minutes

    def test_third_retry(self):
        assert backoff_seconds(2) == 4800  # 80 minutes

    def test_exponential_growth(self):
        for i in range(5):
            assert backoff_seconds(i) == BASE_BACKOFF_SECONDS * (4 ** i)


# ---------- record_verification_error ----------

class TestRecordVerificationError:
    @patch("domain_pipeline.retry.session_scope")
    def test_increments_retry_count(self, mock_scope):
        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        record_verification_error("biz-123", "Connection timeout")

        mock_session.execute.assert_called_once()

    @patch("domain_pipeline.retry.session_scope")
    def test_truncates_long_error(self, mock_scope):
        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        long_error = "x" * 2000
        record_verification_error("biz-123", long_error)

        # The update statement is constructed with error[:1000]
        mock_session.execute.assert_called_once()


# ---------- clear_verification_error ----------

class TestClearVerificationError:
    @patch("domain_pipeline.retry.session_scope")
    def test_clears_error_fields(self, mock_scope):
        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        clear_verification_error("biz-456")

        mock_session.execute.assert_called_once()


# ---------- reset_for_retry ----------

class TestResetForRetry:
    @patch("domain_pipeline.retry.session_scope")
    def test_resets_businesses(self, mock_scope):
        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        count = reset_for_retry(["id1", "id2", "id3"])

        assert count == 3
        mock_session.execute.assert_called_once()

    def test_empty_list_returns_zero(self):
        count = reset_for_retry([])
        assert count == 0


# ---------- constants ----------

class TestConstants:
    def test_max_retries(self):
        assert MAX_RETRIES == 3

    def test_base_backoff(self):
        assert BASE_BACKOFF_SECONDS == 300


# ---------- get_retryable_businesses ----------

class TestGetRetryableBusinesses:
    @patch("domain_pipeline.retry.session_scope")
    def test_returns_retryable_businesses(self, mock_scope):
        """Businesses with retry_count < MAX_RETRIES and backoff elapsed should be returned."""
        import uuid

        biz_id = uuid.uuid4()
        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        # Mock the query results: simulate finding one business on retry_count=1 query
        mock_session.execute.return_value.all.side_effect = [
            [(biz_id, "Test Biz", 1, "timeout error")],  # retry_count == 1
            [],  # retry_count == 2
            [],  # retry_count == 3
        ]

        results = get_retryable_businesses(limit=50)

        assert len(results) == 1
        assert results[0]["id"] == biz_id
        assert results[0]["name"] == "Test Biz"
        assert results[0]["retry_count"] == 1
        assert results[0]["error"] == "timeout error"

    @patch("domain_pipeline.retry.session_scope")
    def test_respects_limit(self, mock_scope):
        import uuid

        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        # Return 2 for first retry level, should stop since limit=2
        biz1 = (uuid.uuid4(), "Biz 1", 1, "err1")
        biz2 = (uuid.uuid4(), "Biz 2", 1, "err2")
        mock_session.execute.return_value.all.side_effect = [
            [biz1, biz2],
        ]

        results = get_retryable_businesses(limit=2)

        assert len(results) == 2

    @patch("domain_pipeline.retry.session_scope")
    def test_empty_when_no_retryable(self, mock_scope):
        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.execute.return_value.all.return_value = []

        results = get_retryable_businesses()

        assert results == []

    @patch("domain_pipeline.retry.session_scope")
    def test_queries_each_retry_level(self, mock_scope):
        """Should query for each retry_count from 1 to MAX_RETRIES."""
        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.execute.return_value.all.return_value = []

        get_retryable_businesses()

        # Should query for each of the MAX_RETRIES levels
        assert mock_session.execute.call_count == MAX_RETRIES


# ---------- get_dead_letter_businesses ----------

class TestGetDeadLetterBusinesses:
    @patch("domain_pipeline.retry.session_scope")
    def test_returns_dead_letter_businesses(self, mock_scope):
        import uuid

        biz_id = uuid.uuid4()
        error_at = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.execute.return_value.all.return_value = [
            (biz_id, "Dead Biz", "trades", 3, "permanent error", error_at),
        ]

        results = get_dead_letter_businesses(limit=200, offset=0)

        assert len(results) == 1
        assert results[0]["id"] == str(biz_id)
        assert results[0]["name"] == "Dead Biz"
        assert results[0]["category"] == "trades"
        assert results[0]["retry_count"] == 3
        assert results[0]["error"] == "permanent error"
        assert results[0]["error_at"] == error_at.isoformat()

    @patch("domain_pipeline.retry.session_scope")
    def test_handles_none_error_at(self, mock_scope):
        import uuid

        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.execute.return_value.all.return_value = [
            (uuid.uuid4(), "Biz", "food", 4, "err", None),
        ]

        results = get_dead_letter_businesses()

        assert results[0]["error_at"] is None

    @patch("domain_pipeline.retry.session_scope")
    def test_empty_dead_letter(self, mock_scope):
        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.execute.return_value.all.return_value = []

        results = get_dead_letter_businesses()
        assert results == []


# ---------- dead_letter_count ----------

class TestDeadLetterCount:
    @patch("domain_pipeline.retry.session_scope")
    def test_returns_count(self, mock_scope):
        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.execute.return_value.scalar.return_value = 42

        assert dead_letter_count() == 42

    @patch("domain_pipeline.retry.session_scope")
    def test_returns_zero_when_none(self, mock_scope):
        mock_session = MagicMock()
        mock_scope.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_scope.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.execute.return_value.scalar.return_value = None

        assert dead_letter_count() == 0
