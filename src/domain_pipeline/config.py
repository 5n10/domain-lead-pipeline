from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    database_url: str
    batch_size: int
    dns_timeout: int
    dns_check_www: bool
    http_timeout: int
    http_user_agent: str
    tcp_probe_enabled: bool
    tcp_probe_timeout: int
    tcp_probe_ports: tuple[int, ...]
    auto_runner_enabled: bool
    auto_runner_interval_seconds: int
    mutation_api_key: Optional[str]
    mutation_localhost_bypass: bool
    auto_daily_target_enabled: bool
    daily_target_count: int
    daily_target_min_score: float
    daily_target_platform_prefix: str
    daily_target_require_contact: bool
    daily_target_require_domain_qualification: bool
    daily_target_require_unhosted_domain: bool
    daily_target_allow_recycle: bool
    rdap_base_url: str
    overpass_endpoint: str
    overpass_timeout: int
    export_dir: str

    whoisxml_api_key: Optional[str]
    domaintools_api_key: Optional[str]
    hunter_api_key: Optional[str]
    apollo_api_key: Optional[str]
    instantly_api_key: Optional[str]
    lemlist_api_key: Optional[str]
    google_places_api_key: Optional[str]


def load_config() -> Config:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")

    return Config(
        database_url=database_url,
        batch_size=int(os.getenv("BATCH_SIZE", "100")),
        dns_timeout=int(os.getenv("DNS_TIMEOUT", "5")),
        dns_check_www=os.getenv("DNS_CHECK_WWW", "true").strip().lower() in {"1", "true", "yes", "on"},
        http_timeout=int(os.getenv("HTTP_TIMEOUT", "10")),
        http_user_agent=os.getenv("HTTP_USER_AGENT", "domain-lead-pipeline/0.1"),
        tcp_probe_enabled=os.getenv("TCP_PROBE_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"},
        tcp_probe_timeout=int(os.getenv("TCP_PROBE_TIMEOUT", "3")),
        tcp_probe_ports=tuple(
            int(port.strip())
            for port in os.getenv("TCP_PROBE_PORTS", "80,443").split(",")
            if port.strip()
        ),
        auto_runner_enabled=os.getenv("AUTO_RUNNER_ENABLED", "false").strip().lower()
        in {"1", "true", "yes", "on"},
        auto_runner_interval_seconds=max(int(os.getenv("AUTO_RUNNER_INTERVAL_SECONDS", "900")), 30),
        mutation_api_key=(os.getenv("MUTATION_API_KEY") or "").strip() or None,
        mutation_localhost_bypass=os.getenv("MUTATION_LOCALHOST_BYPASS", "true").strip().lower()
        in {"1", "true", "yes", "on"},
        auto_daily_target_enabled=os.getenv("AUTO_DAILY_TARGET_ENABLED", "false").strip().lower()
        in {"1", "true", "yes", "on"},
        daily_target_count=int(os.getenv("DAILY_TARGET_COUNT", "100")),
        daily_target_min_score=float(os.getenv("DAILY_TARGET_MIN_SCORE", "40")),
        daily_target_platform_prefix=os.getenv("DAILY_TARGET_PLATFORM_PREFIX", "daily"),
        daily_target_require_contact=os.getenv("DAILY_TARGET_REQUIRE_CONTACT", "true").strip().lower()
        in {"1", "true", "yes", "on"},
        daily_target_require_domain_qualification=os.getenv(
            "DAILY_TARGET_REQUIRE_DOMAIN_QUALIFICATION", "false"
        ).strip().lower()
        in {"1", "true", "yes", "on"},
        daily_target_require_unhosted_domain=os.getenv(
            "DAILY_TARGET_REQUIRE_UNHOSTED_DOMAIN", "false"
        ).strip().lower()
        in {"1", "true", "yes", "on"},
        daily_target_allow_recycle=os.getenv(
            "DAILY_TARGET_ALLOW_RECYCLE", "true"
        ).strip().lower()
        in {"1", "true", "yes", "on"},
        rdap_base_url=os.getenv("RDAP_BASE_URL", "https://rdap.org/domain/"),
        overpass_endpoint=os.getenv("OVERPASS_ENDPOINT", "https://overpass-api.de/api/interpreter"),
        overpass_timeout=int(os.getenv("OVERPASS_TIMEOUT", "180")),
        export_dir=os.getenv("EXPORT_DIR", "./exports"),
        whoisxml_api_key=os.getenv("WHOISXML_API_KEY") or None,
        domaintools_api_key=os.getenv("DOMAINTOOLS_API_KEY") or None,
        hunter_api_key=os.getenv("HUNTER_API_KEY") or None,
        apollo_api_key=os.getenv("APOLLO_API_KEY") or None,
        instantly_api_key=os.getenv("INSTANTLY_API_KEY") or None,
        lemlist_api_key=os.getenv("LEMLIST_API_KEY") or None,
        google_places_api_key=os.getenv("GOOGLE_PLACES_API_KEY") or None,
    )
