"""Unit tests for business domain sync worker.

Tests _parse_cursor(), _make_cursor() as pure functions.
No database required.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from domain_pipeline.workers.business_domain_sync import (
    _make_cursor,
    _parse_cursor,
)


class TestParseCursor:
    """Tests for _parse_cursor()."""

    def test_valid_cursor(self):
        ts = datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        uid = uuid.uuid4()
        cursor = f"{ts.isoformat()}|{uid}"
        parsed_ts, parsed_id = _parse_cursor(cursor)
        assert parsed_ts == ts
        assert parsed_id == uid

    def test_none_returns_none_tuple(self):
        ts, uid = _parse_cursor(None)
        assert ts is None
        assert uid is None

    def test_empty_string_returns_none_tuple(self):
        ts, uid = _parse_cursor("")
        assert ts is None
        assert uid is None

    def test_invalid_format_returns_none_tuple(self):
        ts, uid = _parse_cursor("not-a-valid-cursor")
        assert ts is None
        assert uid is None

    def test_invalid_uuid_returns_none_tuple(self):
        ts, uid = _parse_cursor("2025-01-15T10:30:00+00:00|not-a-uuid")
        assert ts is None
        assert uid is None


class TestMakeCursor:
    """Tests for _make_cursor()."""

    def test_roundtrip(self):
        ts = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        uid = uuid.uuid4()
        cursor = _make_cursor(ts, uid)
        parsed_ts, parsed_id = _parse_cursor(cursor)
        assert parsed_ts == ts
        assert parsed_id == uid

    def test_format(self):
        ts = datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        uid = uuid.UUID("12345678-1234-5678-1234-567812345678")
        cursor = _make_cursor(ts, uid)
        assert "|" in cursor
        assert "12345678-1234-5678-1234-567812345678" in cursor

    def test_contains_pipe_separator(self):
        """Cursor string should use pipe as separator between timestamp and UUID."""
        ts = datetime(2025, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
        uid = uuid.uuid4()
        cursor = _make_cursor(ts, uid)
        parts = cursor.split("|")
        assert len(parts) == 2

    def test_multiple_roundtrips_independent(self):
        """Multiple cursors should independently roundtrip."""
        ts1 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        uid1 = uuid.uuid4()
        uid2 = uuid.uuid4()
        c1 = _make_cursor(ts1, uid1)
        c2 = _make_cursor(ts2, uid2)
        p1_ts, p1_id = _parse_cursor(c1)
        p2_ts, p2_id = _parse_cursor(c2)
        assert p1_ts == ts1
        assert p1_id == uid1
        assert p2_ts == ts2
        assert p2_id == uid2
