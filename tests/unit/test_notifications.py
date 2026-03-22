"""Unit tests for domain_pipeline.notifications module."""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest
import requests
import responses

from domain_pipeline.notifications import (
    _send_ntfy,
    _send_slack,
    notify_error,
    notify_new_leads,
    notify_pipeline_complete,
    send_notification,
)


# ---------- Helper: create a mock config ----------


def _make_config(
    ntfy_topic: str | None = "test-topic",
    ntfy_server: str = "https://ntfy.sh",
    slack_webhook_url: str | None = None,
):
    """Return a mock config with the given notification settings."""
    cfg = MagicMock()
    cfg.ntfy_topic = ntfy_topic
    cfg.ntfy_server = ntfy_server
    cfg.slack_webhook_url = slack_webhook_url
    return cfg


# ---------- _send_ntfy ----------


class TestSendNtfy:
    @responses.activate
    @patch("domain_pipeline.notifications.load_config")
    def test_happy_path(self, mock_load_config):
        """Successful ntfy notification returns True."""
        mock_load_config.return_value = _make_config(ntfy_topic="my-topic")
        responses.add(
            responses.POST,
            "https://ntfy.sh/my-topic",
            status=200,
        )

        result = _send_ntfy("Title", "Body", "default", None)
        assert result is True
        assert len(responses.calls) == 1
        assert responses.calls[0].request.headers["Title"] == "Title"
        assert responses.calls[0].request.headers["Priority"] == "default"

    @responses.activate
    @patch("domain_pipeline.notifications.load_config")
    def test_with_tags(self, mock_load_config):
        """Tags are sent as comma-separated header."""
        mock_load_config.return_value = _make_config(ntfy_topic="t")
        responses.add(responses.POST, "https://ntfy.sh/t", status=200)

        _send_ntfy("T", "B", "high", ["fire", "warning"])
        assert responses.calls[0].request.headers["Tags"] == "fire,warning"

    @patch("domain_pipeline.notifications.load_config")
    def test_no_ntfy_topic_returns_false(self, mock_load_config):
        """Missing ntfy_topic means notification is skipped."""
        mock_load_config.return_value = _make_config(ntfy_topic=None)
        result = _send_ntfy("Title", "Body", "default", None)
        assert result is False

    @responses.activate
    @patch("domain_pipeline.notifications.load_config")
    def test_http_error_returns_false(self, mock_load_config):
        """Non-OK HTTP response returns False."""
        mock_load_config.return_value = _make_config(ntfy_topic="t")
        responses.add(responses.POST, "https://ntfy.sh/t", status=500, body="error")

        result = _send_ntfy("Title", "Body", "default", None)
        assert result is False

    @patch("domain_pipeline.notifications.load_config")
    @patch("domain_pipeline.notifications.requests.post", side_effect=requests.RequestException("connection error"))
    def test_request_exception_returns_false(self, mock_post, mock_load_config):
        """Network exception returns False."""
        mock_load_config.return_value = _make_config(ntfy_topic="t")
        result = _send_ntfy("Title", "Body", "default", None)
        assert result is False

    @responses.activate
    @patch("domain_pipeline.notifications.load_config")
    def test_custom_server(self, mock_load_config):
        """Custom ntfy server URL is used correctly."""
        mock_load_config.return_value = _make_config(
            ntfy_topic="t", ntfy_server="https://custom.ntfy.example.com/"
        )
        responses.add(
            responses.POST,
            "https://custom.ntfy.example.com/t",
            status=200,
        )
        result = _send_ntfy("T", "B", "default", None)
        assert result is True

    @responses.activate
    @patch("domain_pipeline.notifications.load_config")
    def test_message_encoded_as_utf8(self, mock_load_config):
        mock_load_config.return_value = _make_config(ntfy_topic="t")
        responses.add(responses.POST, "https://ntfy.sh/t", status=200)

        _send_ntfy("T", "Hello World", "default", None)
        assert responses.calls[0].request.body == b"Hello World"


# ---------- _send_slack ----------


class TestSendSlack:
    @responses.activate
    @patch("domain_pipeline.notifications.load_config")
    def test_happy_path(self, mock_load_config):
        """Successful Slack notification returns True."""
        webhook = "https://hooks.slack.com/services/T/B/xxx"
        mock_load_config.return_value = _make_config(slack_webhook_url=webhook)
        responses.add(responses.POST, webhook, status=200)

        result = _send_slack("Title", "Message")
        assert result is True
        assert len(responses.calls) == 1

    @patch("domain_pipeline.notifications.load_config")
    def test_no_webhook_returns_false(self, mock_load_config):
        """Missing slack_webhook_url means notification is skipped."""
        mock_load_config.return_value = _make_config(slack_webhook_url=None)
        result = _send_slack("Title", "Message")
        assert result is False

    @responses.activate
    @patch("domain_pipeline.notifications.load_config")
    def test_http_error_returns_false(self, mock_load_config):
        webhook = "https://hooks.slack.com/services/T/B/xxx"
        mock_load_config.return_value = _make_config(slack_webhook_url=webhook)
        responses.add(responses.POST, webhook, status=403, body="invalid_token")

        result = _send_slack("Title", "Message")
        assert result is False

    @patch("domain_pipeline.notifications.load_config")
    @patch("domain_pipeline.notifications.requests.post", side_effect=requests.RequestException("timeout"))
    def test_request_exception_returns_false(self, mock_post, mock_load_config):
        mock_load_config.return_value = _make_config(
            slack_webhook_url="https://hooks.slack.com/xxx"
        )
        result = _send_slack("Title", "Message")
        assert result is False


# ---------- send_notification ----------


class TestSendNotification:
    @patch("domain_pipeline.notifications._send_slack", return_value=False)
    @patch("domain_pipeline.notifications._send_ntfy", return_value=True)
    def test_returns_true_when_ntfy_succeeds(self, mock_ntfy, mock_slack):
        result = send_notification("T", "M")
        assert result is True

    @patch("domain_pipeline.notifications._send_slack", return_value=True)
    @patch("domain_pipeline.notifications._send_ntfy", return_value=False)
    def test_returns_true_when_slack_succeeds(self, mock_ntfy, mock_slack):
        result = send_notification("T", "M")
        assert result is True

    @patch("domain_pipeline.notifications._send_slack", return_value=True)
    @patch("domain_pipeline.notifications._send_ntfy", return_value=True)
    def test_returns_true_when_both_succeed(self, mock_ntfy, mock_slack):
        result = send_notification("T", "M")
        assert result is True

    @patch("domain_pipeline.notifications._send_slack", return_value=False)
    @patch("domain_pipeline.notifications._send_ntfy", return_value=False)
    def test_returns_false_when_both_fail(self, mock_ntfy, mock_slack):
        result = send_notification("T", "M")
        assert result is False

    @patch("domain_pipeline.notifications._send_slack", return_value=False)
    @patch("domain_pipeline.notifications._send_ntfy", return_value=True)
    def test_passes_all_params(self, mock_ntfy, mock_slack):
        send_notification("Title", "Body", priority="high", tags=["fire"])
        mock_ntfy.assert_called_once_with("Title", "Body", "high", ["fire"])
        mock_slack.assert_called_once_with("Title", "Body")


# ---------- notify_pipeline_complete ----------


class TestNotifyPipelineComplete:
    @patch("domain_pipeline.notifications.send_notification", return_value=True)
    def test_happy_path(self, mock_send):
        result_dict = {
            "imported": 50,
            "business_scored": 30,
            "websites_found": 10,
            "ddg_websites_found": 5,
            "business_export_path": "/tmp/export.csv",
        }
        result = notify_pipeline_complete(result_dict)
        assert result is True
        mock_send.assert_called_once()
        call_kwargs = mock_send.call_args
        assert call_kwargs[1]["title"] == "Pipeline Complete"
        assert "50" in call_kwargs[1]["message"]
        assert "30" in call_kwargs[1]["message"]

    @patch("domain_pipeline.notifications.send_notification", return_value=True)
    def test_missing_keys_default_to_zero(self, mock_send):
        result = notify_pipeline_complete({})
        assert result is True
        msg = mock_send.call_args[1]["message"]
        assert "Imported: 0" in msg


# ---------- notify_new_leads ----------


class TestNotifyNewLeads:
    @patch("domain_pipeline.notifications.send_notification", return_value=True)
    def test_with_export_path(self, mock_send):
        result = notify_new_leads(25, export_path="/tmp/leads.csv")
        assert result is True
        call_kwargs = mock_send.call_args[1]
        assert "25" in call_kwargs["title"]
        assert "/tmp/leads.csv" in call_kwargs["message"]
        assert call_kwargs["priority"] == "high"  # >= 10 leads

    @patch("domain_pipeline.notifications.send_notification", return_value=True)
    def test_without_export_path(self, mock_send):
        notify_new_leads(3)
        call_kwargs = mock_send.call_args[1]
        assert "File:" not in call_kwargs["message"]
        assert call_kwargs["priority"] == "default"  # < 10 leads

    @patch("domain_pipeline.notifications.send_notification", return_value=True)
    def test_high_priority_at_threshold(self, mock_send):
        notify_new_leads(10)
        assert mock_send.call_args[1]["priority"] == "high"

    @patch("domain_pipeline.notifications.send_notification", return_value=True)
    def test_default_priority_below_threshold(self, mock_send):
        notify_new_leads(9)
        assert mock_send.call_args[1]["priority"] == "default"


# ---------- notify_error ----------


class TestNotifyError:
    @patch("domain_pipeline.notifications.send_notification", return_value=True)
    def test_sends_error_notification(self, mock_send):
        result = notify_error("import_job", "Connection refused")
        assert result is True
        call_kwargs = mock_send.call_args[1]
        assert "import_job" in call_kwargs["title"]
        assert call_kwargs["message"] == "Connection refused"
        assert call_kwargs["priority"] == "high"

    @patch("domain_pipeline.notifications.send_notification", return_value=True)
    def test_truncates_long_error(self, mock_send):
        long_error = "x" * 1000
        notify_error("job", long_error)
        msg = mock_send.call_args[1]["message"]
        assert len(msg) <= 500
