from __future__ import annotations

import pytest
from domain_pipeline.domain_utils import normalize_domain

@pytest.mark.parametrize("raw, expected", [
    # Basic happy paths
    ("example.com", "example.com"),
    ("  EXAMPLE.com  ", "example.com"),
    ("http://example.com", "example.com"),
    ("https://www.example.com/path?query=1", "example.com"),
    ("user@example.com", "example.com"),
    ("www.example.com", "example.com"),
    ("example.com.", "example.com"),
    ("http://example.com:8080", "example.com"),

    # Null/Empty/Whitespace
    (None, None),
    ("", None),
    ("   ", None),

    # Invalid domains
    ("invalid", None),
    ("not a domain", None),
    ("http://", None),

    # Complex URLs with user info
    ("https://user:pass@example.com", "example.com"),
    ("https://user@example.com", "example.com"),
    ("https://user:pass@example.com:8080/path", "example.com"),

    # Email-like/SSH-like strings (without scheme)
    ("git@github.com:user/repo.git", "github.com"),
    ("admin@internal.server", "internal.server"),

    # Path variants
    ("example.com/some/path", "example.com"),
    ("http://example.com/some/path", "example.com"),

    # Cases with trailing dots or spaces
    ("  www.example.com.  ", "example.com"),

    # Port handling in non-URL strings
    ("example.com:8080", "example.com"),
    ("example.com:8080/path", "example.com"),
])
def test_normalize_domain(raw, expected):
    assert normalize_domain(raw) == expected
