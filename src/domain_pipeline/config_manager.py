"""
Configuration Management System

Loads configuration from environment variables with sensible defaults.
ConfigSchema reads directly from env vars — no database, no Redis, no connection leaks.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Optional
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


def _env_bool(key: str, default: str = "false") -> bool:
    return os.getenv(key, default).strip().lower() in ("1", "true", "yes", "on")


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid integer for %s=%r, using default %d", key, raw, default)
        return default


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid float for %s=%r, using default %s", key, raw, default)
        return default


def _env_str(key: str, default: str = "") -> Optional[str]:
    val = (os.getenv(key) or "").strip()
    return val if val else (default if default else None)


@dataclass
class ConfigSchema:
    """Complete configuration schema — all fields loaded from environment variables."""

    # Database
    database_url: str = field(default_factory=lambda: os.getenv("DATABASE_URL", "postgresql://localhost:5432/domain_leads"))
    batch_size: int = field(default_factory=lambda: _env_int("BATCH_SIZE", 100))

    # DNS / HTTP / TCP processing
    dns_timeout: int = field(default_factory=lambda: _env_int("DNS_TIMEOUT", 5))
    dns_check_www: bool = field(default_factory=lambda: _env_bool("DNS_CHECK_WWW", "true"))
    http_timeout: int = field(default_factory=lambda: _env_int("HTTP_TIMEOUT", 10))
    http_user_agent: str = field(default_factory=lambda: os.getenv("HTTP_USER_AGENT", "domain-lead-pipeline/0.1"))
    tcp_probe_enabled: bool = field(default_factory=lambda: _env_bool("TCP_PROBE_ENABLED", "false"))
    tcp_probe_timeout: int = field(default_factory=lambda: _env_int("TCP_PROBE_TIMEOUT", 3))
    tcp_probe_ports: tuple = field(default_factory=lambda: tuple(
        int(p.strip()) for p in os.getenv("TCP_PROBE_PORTS", "80,443").split(",") if p.strip().isdigit()
    ))

    # Mutation API auth
    mutation_api_key: Optional[str] = field(default_factory=lambda: _env_str("MUTATION_API_KEY"))
    mutation_localhost_bypass: bool = field(default_factory=lambda: _env_bool("MUTATION_LOCALHOST_BYPASS", "true"))

    # Automation
    auto_runner_enabled: bool = field(default_factory=lambda: _env_bool("AUTO_RUNNER_ENABLED", "false"))
    auto_runner_interval_seconds: int = field(default_factory=lambda: max(_env_int("AUTO_RUNNER_INTERVAL_SECONDS", 900), 30))
    auto_daily_target_enabled: bool = field(default_factory=lambda: _env_bool("AUTO_DAILY_TARGET_ENABLED", "true"))
    daily_target_count: int = field(default_factory=lambda: _env_int("DAILY_TARGET_COUNT", 100))
    daily_target_min_score: float = field(default_factory=lambda: _env_float("DAILY_TARGET_MIN_SCORE", 40.0))
    daily_target_platform_prefix: str = field(default_factory=lambda: os.getenv("DAILY_TARGET_PLATFORM_PREFIX", "daily"))
    daily_target_require_contact: bool = field(default_factory=lambda: _env_bool("DAILY_TARGET_REQUIRE_CONTACT", "true"))
    daily_target_require_domain_qualification: bool = field(default_factory=lambda: _env_bool("DAILY_TARGET_REQUIRE_DOMAIN_QUALIFICATION", "false"))
    daily_target_require_unhosted_domain: bool = field(default_factory=lambda: _env_bool("DAILY_TARGET_REQUIRE_UNHOSTED_DOMAIN", "false"))
    daily_target_allow_recycle: bool = field(default_factory=lambda: _env_bool("DAILY_TARGET_ALLOW_RECYCLE", "true"))

    # RDAP / Overpass
    rdap_base_url: str = field(default_factory=lambda: os.getenv("RDAP_BASE_URL", "https://rdap.org/domain/"))
    overpass_endpoint: str = field(default_factory=lambda: os.getenv("OVERPASS_ENDPOINT", "https://overpass-api.de/api/interpreter"))
    overpass_timeout: int = field(default_factory=lambda: _env_int("OVERPASS_TIMEOUT", 180))

    # Export
    export_dir: str = field(default_factory=lambda: os.getenv("EXPORT_DIR", "./exports"))

    # --- API keys (third-party services) ---
    google_places_api_key: Optional[str] = field(default_factory=lambda: _env_str("GOOGLE_PLACES_API_KEY"))
    foursquare_api_key: Optional[str] = field(default_factory=lambda: _env_str("FOURSQUARE_API_KEY"))
    openrouter_api_key: Optional[str] = field(default_factory=lambda: _env_str("OPENROUTER_API_KEY"))
    gemini_api_key: Optional[str] = field(default_factory=lambda: _env_str("GEMINI_API_KEY"))
    groq_api_key: Optional[str] = field(default_factory=lambda: _env_str("GROQ_API_KEY"))
    whoisxml_api_key: Optional[str] = field(default_factory=lambda: _env_str("WHOISXML_API_KEY"))
    domaintools_api_key: Optional[str] = field(default_factory=lambda: _env_str("DOMAINTOOLS_API_KEY"))
    hunter_api_key: Optional[str] = field(default_factory=lambda: _env_str("HUNTER_API_KEY"))
    apollo_api_key: Optional[str] = field(default_factory=lambda: _env_str("APOLLO_API_KEY"))
    instantly_api_key: Optional[str] = field(default_factory=lambda: _env_str("INSTANTLY_API_KEY"))
    lemlist_api_key: Optional[str] = field(default_factory=lambda: _env_str("LEMLIST_API_KEY"))

    # Notifications (ntfy.sh)
    ntfy_topic: Optional[str] = field(default_factory=lambda: _env_str("NTFY_TOPIC"))
    ntfy_server: str = field(default_factory=lambda: os.getenv("NTFY_SERVER", "https://ntfy.sh"))

    # Google Sheets export
    google_sheets_credentials_file: Optional[str] = field(default_factory=lambda: _env_str("GOOGLE_SHEETS_CREDENTIALS_FILE"))
    google_sheets_spreadsheet_id: Optional[str] = field(default_factory=lambda: _env_str("GOOGLE_SHEETS_SPREADSHEET_ID"))


# Module-level cached config instance (avoids re-reading env vars on every call)
_cached_config: Optional[ConfigSchema] = None


def load_config() -> ConfigSchema:
    """Load configuration from environment variables.

    Returns a cached ConfigSchema instance. The first call creates the
    dataclass (reading env vars); subsequent calls return the same object.
    """
    global _cached_config
    if _cached_config is None:
        _cached_config = ConfigSchema()
    return _cached_config


def reload_config() -> ConfigSchema:
    """Force-reload configuration from environment variables."""
    global _cached_config
    _cached_config = ConfigSchema()
    return _cached_config
