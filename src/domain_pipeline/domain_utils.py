from __future__ import annotations

from typing import Optional
from urllib.parse import urlparse

PUBLIC_EMAIL_DOMAINS = {
    # Major free email providers
    "gmail.com",
    "googlemail.com",
    "yahoo.com",
    "yahoo.co.uk",
    "yahoo.ae",
    "ymail.com",
    "rocketmail.com",
    "hotmail.com",
    "outlook.com",
    "live.com",
    "msn.com",
    "icloud.com",
    "me.com",
    "mac.com",
    "aol.com",
    "protonmail.com",
    "proton.me",
    "pm.me",
    "mail.com",
    "email.com",
    "gmx.com",
    "gmx.net",
    "zoho.com",
    "yandex.com",
    "tutanota.com",
    "tuta.io",
    "fastmail.com",
    "hushmail.com",
    "mailinator.com",
    "guerrillamail.com",
    "sharklasers.com",
    # ISP / telecom email providers (shared domains, not business websites)
    "rogers.com",
    "bellnet.ca",
    "bell.net",
    "sympatico.ca",
    "cogeco.ca",
    "shaw.ca",
    "telus.net",
    "videotron.ca",
    "sasktel.net",
    "eastlink.ca",
    "comcast.net",
    "sbcglobal.net",
    "att.net",
    "verizon.net",
    "cox.net",
    "charter.net",
    "spectrum.net",
    "centurylink.net",
    "frontier.com",
    "windstream.net",
    "earthlink.net",
    "optonline.net",
    "btinternet.com",
    "virginmedia.com",
    "sky.com",
    "talktalk.net",
    "ntlworld.com",
    "emirates.net.ae",
    "eim.ae",
    "etisalat.ae",
    "du.ae",
    "qatar.net.qa",
    "ooredoo.qa",
    "nyu.edu",
    "gamil.com",  # Common misspelling of gmail
}
PUBLIC_EMAIL_DOMAIN_PREFIXES = (
    "gmail.",
    "googlemail.",
    "yahoo.",
    "ymail.",
    "rocketmail.",
    "hotmail.",
    "outlook.",
    "live.",
    "msn.",
    "icloud.",
    "aol.",
    "protonmail.",
    "proton.",
    "yandex.",
    "gmx.",
    "zoho.",
    "tutanota.",
    "fastmail.",
    "mail.ru",
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
