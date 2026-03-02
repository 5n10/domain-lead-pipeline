"""Unit tests for RDAP check worker.

Tests detect_parked(), extract_registrar(), dns_check(), RdapClient.fetch(),
and PARKED_KEYWORDS / PARKED_HOST_HINTS constants as pure/helper functions.
No database or network required.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import responses

from domain_pipeline.workers.rdap_check import (
    PARKED_HOST_HINTS,
    PARKED_KEYWORDS,
    RdapClient,
    detect_parked,
    dns_check,
    extract_registrar,
)


class TestDetectParked:
    """Tests for detect_parked() — parked domain detection."""

    def test_parked_keyword_in_body(self):
        body = "<html><body>This domain is for sale. Contact us.</body></html>"
        assert detect_parked(body, None, []) is True

    def test_sedo_parking_in_body(self):
        body = "<html><body>Welcome to Sedo parking page</body></html>"
        assert detect_parked(body, None, []) is True

    def test_normal_website_not_parked(self):
        body = "<html><body>Welcome to our business website.</body></html>"
        assert detect_parked(body, None, []) is False

    def test_parked_host_in_final_url(self):
        assert detect_parked(None, "https://sedoparking.com/domain.com", []) is True

    def test_parked_host_in_cname(self):
        assert detect_parked(None, None, ["parkingcrew.net."]) is True

    def test_bodis_in_cname(self):
        assert detect_parked(None, None, ["bodis.com."]) is True

    def test_normal_cname_not_parked(self):
        assert detect_parked(None, None, ["cloudflare.com."]) is False

    def test_no_data_not_parked(self):
        assert detect_parked(None, None, []) is False

    def test_empty_body_not_parked(self):
        assert detect_parked("", None, []) is False

    def test_buy_this_domain_parked(self):
        body = "Buy this domain now for only $999"
        assert detect_parked(body, None, []) is True

    def test_afternic_redirect(self):
        assert detect_parked(None, "https://www.afternic.com/domain/test.com", []) is True

    def test_dan_com_in_final_url(self):
        assert detect_parked(None, "https://dan.com/buy-domain/test.com", []) is True


class TestExtractRegistrar:
    """Tests for extract_registrar() — RDAP registrar extraction."""

    def test_extracts_registrar_name(self):
        rdap_data = {
            "entities": [
                {
                    "roles": ["registrar"],
                    "vcardArray": [
                        "vcard",
                        [
                            ["version", {}, "text", "4.0"],
                            ["fn", {}, "text", "GoDaddy.com, LLC"],
                        ],
                    ],
                }
            ]
        }
        assert extract_registrar(rdap_data) == "GoDaddy.com, LLC"

    def test_no_registrar_entity(self):
        rdap_data = {
            "entities": [
                {"roles": ["registrant"], "vcardArray": ["vcard", [["fn", {}, "text", "John Doe"]]]},
            ]
        }
        assert extract_registrar(rdap_data) is None

    def test_empty_entities(self):
        assert extract_registrar({"entities": []}) is None

    def test_none_input(self):
        assert extract_registrar(None) is None

    def test_no_entities_key(self):
        assert extract_registrar({}) is None

    def test_no_vcard_array(self):
        rdap_data = {"entities": [{"roles": ["registrar"]}]}
        assert extract_registrar(rdap_data) is None

    def test_vcard_without_fn(self):
        rdap_data = {
            "entities": [
                {
                    "roles": ["registrar"],
                    "vcardArray": [
                        "vcard",
                        [["version", {}, "text", "4.0"]],
                    ],
                }
            ]
        }
        assert extract_registrar(rdap_data) is None


class TestDnsCheck:
    """Tests for dns_check() with mocked dns.resolver.Resolver."""

    @patch("domain_pipeline.workers.rdap_check._query_records")
    def test_result_structure_with_a_record(self, mock_query):
        """dns_check returns proper structure when A record exists."""
        def side_effect(domain, rtype, timeout):
            if rtype == "A" and not domain.startswith("www."):
                return True, ["1.2.3.4"], None
            return False, [], None

        mock_query.side_effect = side_effect
        result = dns_check("example.com", timeout=5, check_www=False)
        assert result["has_a"] is True
        assert result["has_mx"] is False
        assert result["has_ns"] is False
        assert "cname_targets" in result
        assert isinstance(result["dns_errors"], list)

    @patch("domain_pipeline.workers.rdap_check._query_records")
    def test_all_records_missing(self, mock_query):
        """dns_check returns all-False when no records exist."""
        mock_query.return_value = (False, [], None)
        result = dns_check("nonexistent.example", timeout=5, check_www=False)
        assert result["has_a"] is False
        assert result["has_aaaa"] is False
        assert result["has_cname"] is False
        assert result["has_mx"] is False
        assert result["has_ns"] is False
        assert result["dns_error"] is False

    @patch("domain_pipeline.workers.rdap_check._query_records")
    def test_timeout_records_dns_error(self, mock_query):
        """dns_check reports dns_error when resolution errors occur."""
        mock_query.return_value = (False, [], "Timeout")
        result = dns_check("slow.example", timeout=5, check_www=False)
        assert result["has_a"] is False
        assert result["dns_error"] is True
        assert len(result["dns_errors"]) > 0

    @patch("domain_pipeline.workers.rdap_check._query_records")
    def test_www_check_adds_www_records(self, mock_query):
        """dns_check checks www subdomain when check_www=True."""
        call_domains = []

        def side_effect(domain, rtype, timeout):
            call_domains.append(domain)
            return False, [], None

        mock_query.side_effect = side_effect
        dns_check("example.com", timeout=5, check_www=True)
        # With check_www: apex (A,AAAA,CNAME,MX,NS) + www (A,AAAA,CNAME) = 8 calls
        assert len(call_domains) == 8
        www_calls = [d for d in call_domains if d.startswith("www.")]
        assert len(www_calls) == 3

    @patch("domain_pipeline.workers.rdap_check._query_records")
    def test_cname_targets_collected(self, mock_query):
        """dns_check collects CNAME targets into result."""
        def side_effect(domain, rtype, timeout):
            if rtype == "CNAME" and not domain.startswith("www."):
                return True, ["cdn.example.net"], None
            return False, [], None

        mock_query.side_effect = side_effect
        result = dns_check("example.com", timeout=5, check_www=False)
        assert result["has_cname"] is True
        assert "cdn.example.net" in result["cname_targets"]


class TestRdapClientFetch:
    """Tests for RdapClient.fetch() with mocked HTTP responses."""

    @responses.activate
    @patch("domain_pipeline.workers.rdap_check.load_config")
    def test_successful_fetch_200(self, mock_config):
        """fetch() returns parsed JSON and status code on 200."""
        mock_config.return_value = MagicMock(
            rdap_base_url="https://rdap.org/domain/",
            http_user_agent="test-agent",
            http_timeout=10,
        )
        responses.add(
            responses.GET,
            "https://rdap.org/domain/example.com",
            json={"handle": "example.com", "entities": []},
            status=200,
        )
        client = RdapClient()
        data, status = client.fetch("example.com")
        assert status == 200
        assert data is not None
        assert data["handle"] == "example.com"

    @responses.activate
    @patch("domain_pipeline.workers.rdap_check.load_config")
    def test_not_found_404(self, mock_config):
        """fetch() returns (None, 404) when domain not in RDAP."""
        mock_config.return_value = MagicMock(
            rdap_base_url="https://rdap.org/domain/",
            http_user_agent="test-agent",
            http_timeout=10,
        )
        responses.add(
            responses.GET,
            "https://rdap.org/domain/unknown.example",
            json={"errorCode": 404},
            status=404,
        )
        client = RdapClient()
        data, status = client.fetch("unknown.example")
        assert status == 404
        assert data is None

    @responses.activate
    @patch("domain_pipeline.workers.rdap_check.load_config")
    def test_server_error(self, mock_config):
        """fetch() returns (None, status) on server errors."""
        mock_config.return_value = MagicMock(
            rdap_base_url="https://rdap.org/domain/",
            http_user_agent="test-agent",
            http_timeout=10,
        )
        responses.add(
            responses.GET,
            "https://rdap.org/domain/error.example",
            json={"error": "Internal Server Error"},
            status=500,
        )
        client = RdapClient()
        data, status = client.fetch("error.example")
        assert status == 500
        assert data is None

    @responses.activate
    @patch("domain_pipeline.workers.rdap_check.load_config")
    def test_connection_error_returns_none_none(self, mock_config):
        """fetch() returns (None, None) on connection errors."""
        import requests

        mock_config.return_value = MagicMock(
            rdap_base_url="https://rdap.org/domain/",
            http_user_agent="test-agent",
            http_timeout=10,
        )
        responses.add(
            responses.GET,
            "https://rdap.org/domain/timeout.example",
            body=requests.ConnectionError("Connection refused"),
        )
        client = RdapClient()
        data, status = client.fetch("timeout.example")
        assert data is None
        assert status is None

    @responses.activate
    @patch("domain_pipeline.workers.rdap_check.load_config")
    def test_invalid_json_returns_none(self, mock_config):
        """fetch() returns (None, status) when response is not valid JSON."""
        mock_config.return_value = MagicMock(
            rdap_base_url="https://rdap.org/domain/",
            http_user_agent="test-agent",
            http_timeout=10,
        )
        responses.add(
            responses.GET,
            "https://rdap.org/domain/badjson.example",
            body="not json at all",
            status=200,
            content_type="text/plain",
        )
        client = RdapClient()
        data, status = client.fetch("badjson.example")
        assert data is None
        assert status == 200


class TestParkedConstants:
    """Tests for PARKED_KEYWORDS and PARKED_HOST_HINTS constants."""

    def test_parked_keywords_is_nonempty_list(self):
        """PARKED_KEYWORDS should be a non-empty list of strings."""
        assert isinstance(PARKED_KEYWORDS, list)
        assert len(PARKED_KEYWORDS) > 0
        assert all(isinstance(kw, str) for kw in PARKED_KEYWORDS)

    def test_parked_host_hints_is_nonempty_list(self):
        """PARKED_HOST_HINTS should be a non-empty list of strings."""
        assert isinstance(PARKED_HOST_HINTS, list)
        assert len(PARKED_HOST_HINTS) > 0
        assert all(isinstance(h, str) for h in PARKED_HOST_HINTS)

    def test_known_keywords_present(self):
        """Specific important parked keywords should be present."""
        assert "domain for sale" in PARKED_KEYWORDS
        assert "sedo" in PARKED_KEYWORDS
        assert "bodis" in PARKED_KEYWORDS

    def test_known_hosts_present(self):
        """Specific important host hints should be present."""
        assert "sedoparking" in PARKED_HOST_HINTS
        assert "parkingcrew" in PARKED_HOST_HINTS

    def test_all_keywords_lowercase(self):
        """All keywords should be lowercase for consistent matching."""
        for kw in PARKED_KEYWORDS:
            assert kw == kw.lower(), f"Keyword '{kw}' is not lowercase"
