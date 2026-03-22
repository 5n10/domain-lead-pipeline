"""Unit tests for domain_pipeline.reporting module."""
from __future__ import annotations

import json
from unittest.mock import patch, MagicMock

import pytest

from domain_pipeline.reporting import (
    _delta,
    _load_previous_report,
    _save_report_state,
    generate_daily_report,
)


# ---------- _delta (pure function) ----------


class TestDelta:
    def test_positive_delta(self):
        assert _delta(100, 80) == "+20"

    def test_negative_delta(self):
        assert _delta(50, 70) == "-20"

    def test_zero_delta(self):
        assert _delta(100, 100) == "0"

    def test_previous_none(self):
        assert _delta(100, None) == "N/A"

    def test_large_positive(self):
        assert _delta(10000, 0) == "+10000"

    def test_large_negative(self):
        assert _delta(0, 5000) == "-5000"


# ---------- _load_previous_report ----------


class TestLoadPreviousReport:
    @patch("domain_pipeline.reporting.REPORT_STATE_FILE")
    def test_returns_data_when_file_exists(self, mock_path):
        mock_path.exists.return_value = True
        mock_path.read_text.return_value = json.dumps({"businesses_total": 100})

        result = _load_previous_report()
        assert result == {"businesses_total": 100}

    @patch("domain_pipeline.reporting.REPORT_STATE_FILE")
    def test_returns_none_when_file_missing(self, mock_path):
        mock_path.exists.return_value = False

        result = _load_previous_report()
        assert result is None

    @patch("domain_pipeline.reporting.REPORT_STATE_FILE")
    def test_returns_none_on_read_error(self, mock_path):
        mock_path.exists.return_value = True
        mock_path.read_text.side_effect = IOError("disk error")

        result = _load_previous_report()
        assert result is None

    @patch("domain_pipeline.reporting.REPORT_STATE_FILE")
    def test_returns_none_on_invalid_json(self, mock_path):
        mock_path.exists.return_value = True
        mock_path.read_text.return_value = "not valid json"

        result = _load_previous_report()
        assert result is None


# ---------- _save_report_state ----------


class TestSaveReportState:
    @patch("domain_pipeline.reporting.REPORT_STATE_FILE")
    def test_writes_correct_keys(self, mock_path):
        mock_parent = MagicMock()
        mock_path.parent = mock_parent

        metrics = {
            "businesses": {"total": 100, "no_website": 40, "scored": 60},
            "business_exports": {"total": 25},
            "confidence_distribution": {"high": 15, "medium": 10},
            "verification": {"any_source": 50},
        }

        _save_report_state(metrics)

        mock_parent.mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_path.write_text.assert_called_once()
        written = json.loads(mock_path.write_text.call_args[0][0])
        assert written["businesses_total"] == 100
        assert written["no_website"] == 40
        assert written["scored"] == 60
        assert written["exports_total"] == 25
        assert written["high_confidence"] == 15
        assert written["medium_confidence"] == 10
        assert written["any_verified"] == 50
        assert "timestamp" in written

    @patch("domain_pipeline.reporting.REPORT_STATE_FILE")
    def test_handles_write_error_gracefully(self, mock_path):
        mock_path.parent = MagicMock()
        mock_path.write_text.side_effect = IOError("disk full")

        metrics = {
            "businesses": {"total": 0, "no_website": 0, "scored": 0},
            "business_exports": {"total": 0},
            "confidence_distribution": {"high": 0, "medium": 0},
            "verification": {"any_source": 0},
        }

        # Should not raise
        _save_report_state(metrics)


# ---------- generate_daily_report ----------


def _sample_metrics():
    """Return a realistic metrics dict for testing."""
    return {
        "businesses": {
            "total": 500,
            "no_website": 200,
            "scored": 300,
            "no_website_scored": 150,
            "no_website_unscored": 50,
        },
        "domains": {"active": 100, "parked": 50},
        "contacts": {"total": 80, "scored": 60, "unscored": 20},
        "exports": {"total": 10, "queued": 2},
        "business_exports": {"total": 25, "queued": 5},
        "verification": {"ddg": 100, "llm": 50, "any_source": 120},
        "verification_details": {
            "ddg_conclusive": 80,
            "ddg_no_results": 20,
            "llm_conclusive": 40,
            "llm_not_sure": 10,
            "searxng_conclusive": 0,
            "searxng_no_results": 0,
        },
        "confidence_distribution": {"high": 60, "medium": 30, "low": 10, "unverified": 100},
        "queue": {},
        "recent_jobs": [],
    }


class TestGenerateDailyReport:
    @patch("domain_pipeline.reporting._save_report_state")
    @patch("domain_pipeline.reporting.send_notification", return_value=True)
    @patch("domain_pipeline.reporting._load_previous_report", return_value=None)
    @patch("domain_pipeline.reporting.collect_metrics")
    def test_first_run_no_previous(self, mock_metrics, mock_load, mock_notify, mock_save):
        """First run with no previous report should use N/A deltas."""
        mock_metrics.return_value = _sample_metrics()

        report = generate_daily_report()

        assert report["notification_sent"] is True
        assert report["deltas"]["businesses"] == "N/A"
        assert report["deltas"]["scored"] == "N/A"
        assert report["metrics_snapshot"]["businesses_total"] == 500
        mock_save.assert_called_once()

    @patch("domain_pipeline.reporting._save_report_state")
    @patch("domain_pipeline.reporting.send_notification", return_value=True)
    @patch("domain_pipeline.reporting._load_previous_report")
    @patch("domain_pipeline.reporting.collect_metrics")
    def test_with_previous_report_computes_deltas(self, mock_metrics, mock_load, mock_notify, mock_save):
        """When previous report exists, deltas are computed correctly."""
        mock_metrics.return_value = _sample_metrics()
        mock_load.return_value = {
            "businesses_total": 450,
            "no_website": 180,
            "scored": 280,
            "exports_total": 20,
            "high_confidence": 50,
            "any_verified": 100,
        }

        report = generate_daily_report()

        assert report["deltas"]["businesses"] == "+50"
        assert report["deltas"]["no_website"] == "+20"
        assert report["deltas"]["scored"] == "+20"
        assert report["deltas"]["exports"] == "+5"
        assert report["deltas"]["high_confidence"] == "+10"
        assert report["deltas"]["verified"] == "+20"

    @patch("domain_pipeline.reporting._save_report_state")
    @patch("domain_pipeline.reporting.send_notification", return_value=False)
    @patch("domain_pipeline.reporting._load_previous_report", return_value=None)
    @patch("domain_pipeline.reporting.collect_metrics")
    def test_notification_failure_reflected(self, mock_metrics, mock_load, mock_notify, mock_save):
        """Report should reflect notification_sent=False when notifications fail."""
        mock_metrics.return_value = _sample_metrics()

        report = generate_daily_report()

        assert report["notification_sent"] is False

    @patch("domain_pipeline.reporting._save_report_state")
    @patch("domain_pipeline.reporting.send_notification", return_value=True)
    @patch("domain_pipeline.reporting._load_previous_report", return_value=None)
    @patch("domain_pipeline.reporting.collect_metrics")
    def test_report_has_timestamp(self, mock_metrics, mock_load, mock_notify, mock_save):
        mock_metrics.return_value = _sample_metrics()

        report = generate_daily_report()

        assert "timestamp" in report
        assert report["timestamp"] is not None

    @patch("domain_pipeline.reporting._save_report_state")
    @patch("domain_pipeline.reporting.send_notification", return_value=True)
    @patch("domain_pipeline.reporting._load_previous_report", return_value=None)
    @patch("domain_pipeline.reporting.collect_metrics")
    def test_report_metrics_snapshot_keys(self, mock_metrics, mock_load, mock_notify, mock_save):
        mock_metrics.return_value = _sample_metrics()

        report = generate_daily_report()

        snap = report["metrics_snapshot"]
        assert "businesses_total" in snap
        assert "no_website" in snap
        assert "scored" in snap
        assert "verified" in snap
        assert "high_confidence" in snap
        assert "medium_confidence" in snap
        assert "exports_total" in snap

    @patch("domain_pipeline.reporting._save_report_state")
    @patch("domain_pipeline.reporting.send_notification", return_value=True)
    @patch("domain_pipeline.reporting._load_previous_report")
    @patch("domain_pipeline.reporting.collect_metrics")
    def test_negative_delta(self, mock_metrics, mock_load, mock_notify, mock_save):
        """Decreases are reflected correctly."""
        mock_metrics.return_value = _sample_metrics()
        mock_load.return_value = {
            "businesses_total": 600,
            "no_website": 250,
            "scored": 350,
            "exports_total": 30,
            "high_confidence": 80,
            "any_verified": 150,
        }

        report = generate_daily_report()

        assert report["deltas"]["businesses"] == "-100"
        assert report["deltas"]["scored"] == "-50"

    @patch("domain_pipeline.reporting._save_report_state")
    @patch("domain_pipeline.reporting.send_notification", return_value=True)
    @patch("domain_pipeline.reporting._load_previous_report")
    @patch("domain_pipeline.reporting.collect_metrics")
    def test_notification_message_content(self, mock_metrics, mock_load, mock_notify, mock_save):
        mock_metrics.return_value = _sample_metrics()
        mock_load.return_value = None

        generate_daily_report()

        call_kwargs = mock_notify.call_args[1]
        assert call_kwargs["title"] == "Daily Pipeline Report"
        assert "Businesses: 500" in call_kwargs["message"]
        assert "Scored: 300" in call_kwargs["message"]
