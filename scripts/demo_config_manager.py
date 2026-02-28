#!/usr/bin/env python3
"""
Demo script for the configuration management system.

Shows how the ConfigSchema dataclass loads from environment variables
with sensible defaults.
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from domain_pipeline.config_manager import load_config, reload_config


def demo_configuration_system():
    print("=== Domain Lead Pipeline - Configuration Demo ===\n")

    # Load initial configuration
    config = load_config()
    print("1. Current configuration (from env vars / defaults):")
    print(f"   - Database URL: {config.database_url}")
    print(f"   - Batch Size: {config.batch_size}")
    print(f"   - DNS Timeout: {config.dns_timeout}")
    print(f"   - HTTP Timeout: {config.http_timeout}")
    print(f"   - Auto Runner Enabled: {config.auto_runner_enabled}")
    print(f"   - Auto Runner Interval: {config.auto_runner_interval_seconds}s")
    print(f"   - Daily Target Count: {config.daily_target_count}")
    print(f"   - Export Dir: {config.export_dir}")
    print()

    # Show API key status
    print("2. API key status:")
    keys = [
        ("Google Places", config.google_places_api_key),
        ("Foursquare", config.foursquare_api_key),
        ("OpenRouter", config.openrouter_api_key),
        ("Gemini", config.gemini_api_key),
        ("Groq", config.groq_api_key),
        ("Hunter", config.hunter_api_key),
        ("ntfy topic", config.ntfy_topic),
    ]
    for name, val in keys:
        status = "configured" if val else "not set"
        print(f"   - {name}: {status}")
    print()

    # Show reload capability
    print("3. Configuration can be reloaded at runtime via reload_config()")
    print("   Set env vars and call reload_config() to pick up changes.")
    print()

    print("Configuration demo completed!")


if __name__ == "__main__":
    demo_configuration_system()
