from __future__ import annotations

import re
from typing import Any, Iterable

# Some OSM exports contain bidi markers around phone numbers (e.g. U+2066 .. U+2069).
_BIDI_MARKS = dict.fromkeys(map(ord, "\u2066\u2067\u2068\u2069\u200e\u200f"), None)

_INVALID_VALUES = {
    "-",
    "n/a",
    "na",
    "none",
    "null",
    "unknown",
    "0",
}

# Common OSM tag keys for phone/email contact details.
_PHONE_KEYS = {
    "phone",
    "contact:phone",
    "mobile",
    "contact:mobile",
    "telephone",
    "contact:telephone",
    "tel",
    "contact:tel",
    "whatsapp",
    "contact:whatsapp",
}
_EMAIL_KEYS = {
    "email",
    "contact:email",
}

# Phone fields are often "a;b", "a, b", or "a / b" and occasionally "a: b".
_PHONE_SPLIT_RE = re.compile(r"(?:\s*/\s*)|(?:\s*[,;:\n]\s*)|\s+or\s+", re.IGNORECASE)
_EMAIL_SPLIT_RE = re.compile(r"[,\s;\n]+")


def _strip_bidi(value: str) -> str:
    return value.translate(_BIDI_MARKS)


def _clean_value(value: Any) -> str | None:
    if value is None:
        return None
    text = _strip_bidi(str(value)).strip()
    if not text:
        return None
    if text.lower() in _INVALID_VALUES:
        return None
    return text


def _collect_tag_values(tags: dict[str, Any], key_predicate) -> Iterable[str]:
    for key, value in tags.items():
        if not value:
            continue
        normalized_key = str(key).strip().lower()
        if key_predicate(normalized_key):
            cleaned = _clean_value(value)
            if cleaned:
                yield cleaned


def _is_phone_key(normalized_key: str) -> bool:
    if normalized_key in _PHONE_KEYS:
        return True
    if normalized_key.startswith("contact:"):
        parts = normalized_key.split(":")
        return len(parts) >= 2 and parts[1] in {"phone", "mobile", "telephone", "tel", "whatsapp"}
    if normalized_key.startswith("phone:"):
        return True
    if normalized_key.startswith("mobile:"):
        return True
    if normalized_key.startswith("telephone:"):
        return True
    return False


def _is_email_key(normalized_key: str) -> bool:
    if normalized_key in _EMAIL_KEYS:
        return True
    if normalized_key.startswith("contact:"):
        parts = normalized_key.split(":")
        return len(parts) >= 2 and parts[1] == "email"
    if normalized_key.startswith("email:"):
        return True
    return False


def _split_and_clean(value: str, splitter: re.Pattern[str]) -> list[str]:
    parts = []
    for raw in splitter.split(value):
        cleaned = _clean_value(raw)
        if not cleaned:
            continue
        parts.append(cleaned)
    return parts


def extract_osm_contacts(tags: dict[str, Any]) -> list[tuple[str, str]]:
    """Extract (contact_type, value) pairs from an OSM tags dict.

    The lead pipeline primarily considers phone/email as "contact" because those are
    outreach-ready. We normalize common tag variants and split multiple values.
    """

    contacts: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    phone_values = list(_collect_tag_values(tags, _is_phone_key))
    for phone_value in phone_values:
        # Strip common URI prefixes if present.
        cleaned = phone_value
        if cleaned.lower().startswith("tel:"):
            cleaned = cleaned[4:].strip()
        for part in _split_and_clean(cleaned, _PHONE_SPLIT_RE):
            pair = ("phone", part)
            if pair not in seen:
                contacts.append(pair)
                seen.add(pair)

    email_values = list(_collect_tag_values(tags, _is_email_key))
    for email_value in email_values:
        cleaned = email_value
        if cleaned.lower().startswith("mailto:"):
            cleaned = cleaned[7:].strip()
        for part in _split_and_clean(cleaned, _EMAIL_SPLIT_RE):
            normalized = part.strip().lower().strip(";,")
            if "@" not in normalized:
                continue
            pair = ("email", normalized)
            if pair not in seen:
                contacts.append(pair)
                seen.add(pair)

    return contacts

