from __future__ import annotations

from typing import Optional
from urllib.parse import urlparse

PUBLIC_EMAIL_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "yahoo.com",
    "yahoo.co.uk",
    "yahoo.ae",
    "hotmail.com",
    "outlook.com",
    "live.com",
    "msn.com",
    "icloud.com",
    "me.com",
    "aol.com",
    "protonmail.com",
    "pm.me",
    "mail.com",
    "gmx.com",
    "zoho.com",
    "yandex.com",
}
PUBLIC_EMAIL_DOMAIN_PREFIXES = (
    "gmail.",
    "googlemail.",
    "yahoo.",
    "hotmail.",
    "outlook.",
    "live.",
    "icloud.",
    "aol.",
    "protonmail.",
    "yandex.",
    "gmx.",
    "zoho.",
)


def normalize_domain(raw: str) -> Optional[str]:
    if not raw:
        return None

    value = raw.strip().lower()
    if not value:
        return None

    if "@" in value and "://" not in value:
        value = value.split("@", 1)[1]

    if "://" in value:
        parsed = urlparse(value)
        host = parsed.netloc
    else:
        host = value.split("/")[0]

    host = host.strip().lower().rstrip(".")
    if host.startswith("www."):
        host = host[4:]

    if ":" in host:
        host = host.split(":", 1)[0]

    # Basic domain sanity check to avoid storing malformed values.
    if "." not in host:
        return None
    if any(ch.isspace() for ch in host):
        return None

    return host or None


def extract_domain_from_email(email: str) -> Optional[str]:
    if not email or "@" not in email:
        return None
    return normalize_domain(email)


def is_public_email_domain(domain: str) -> bool:
    if not domain:
        return False
    candidate = domain.strip().lower()
    if candidate in PUBLIC_EMAIL_DOMAINS:
        return True
    return any(candidate.startswith(prefix) for prefix in PUBLIC_EMAIL_DOMAIN_PREFIXES)
