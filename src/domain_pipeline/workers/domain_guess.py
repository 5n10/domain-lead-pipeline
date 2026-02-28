"""Domain Guess Verification — FREE, no API key, fast.

Generates candidate domain names from business names and checks via HTTP
HEAD requests whether they resolve to a live (non-parked) website.

Performance: ~500 businesses/minute using parallel HTTP checks.
Should run BEFORE DDG/Google verification to reduce their workload.
"""

from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import httpx
from sqlalchemy import or_, not_, select

from ..db import session_scope
from ..jobs import complete_job, fail_job, start_job
from ..models import Business, City

logger = logging.getLogger(__name__)

JOB_NAME = "domain_guess_verify"

# ---------------------------------------------------------------------------
# Country → TLD mapping
# ---------------------------------------------------------------------------

COUNTRY_TLDS: dict[str, list[str]] = {
    "AE": [".ae", ".com", ".net"],
    "CA": [".ca", ".com", ".net"],
    "QA": [".qa", ".com", ".net"],
    "US": [".com", ".us", ".net", ".org"],
    "GB": [".co.uk", ".com", ".net", ".org"],
    "UK": [".co.uk", ".com", ".net", ".org"],
    "AU": [".com.au", ".com", ".net"],
    "IN": [".in", ".com", ".net"],
    "SA": [".sa", ".com", ".net"],
    "KW": [".kw", ".com", ".net"],
    "BH": [".bh", ".com", ".net"],
    "OM": [".om", ".com", ".net"],
    "JO": [".jo", ".com", ".net"],
    "LB": [".lb", ".com", ".net"],
    "EG": [".eg", ".com", ".net"],
    "PK": [".pk", ".com", ".net"],
}
DEFAULT_TLDS = [".com", ".net"]

# ---------------------------------------------------------------------------
# Name cleaning
# ---------------------------------------------------------------------------

# Articles / prepositions — sometimes part of brand domain names (e.g. thevillagecobbler.ca)
ARTICLE_WORDS = {
    "the", "a", "an", "and", "&", "of", "in", "at", "to", "for", "by", "on",
    # Arabic articles
    "al", "el",
    # French
    "le", "la", "les", "de", "du", "des", "et",
}

# Business entity suffixes — ALWAYS stripped (never part of domain names)
ENTITY_SUFFIXES = {
    "llc", "ltd", "inc", "corp", "co", "company", "plc", "fzc", "fze", "fz",
    "est", "wll", "spc", "dmcc", "pllc", "lp", "llp",
}

# Words ALWAYS stripped from domain candidates (entity suffixes, generic business words)
STRIP_ALWAYS = {
    *ENTITY_SUFFIXES,
    # Generic business words (often dropped from domain names)
    "services", "service", "solutions", "solution", "group", "enterprise",
    "enterprises", "trading", "general", "international", "global", "center",
    "centre", "shop", "store", "mart", "market", "plaza", "mall",
    # Trade/industry words
    "ladies", "gents", "mens", "womens", "children", "kids",
    "textiles", "textile", "tailoring", "tailor", "upholstery",
    "materials", "supplies", "supply", "equipment", "parts",
    "maintenance", "repair", "repairs", "installation", "installations",
    "cleaning", "laundry", "salon", "spa", "beauty", "barber",
    "restaurant", "cafe", "cafeteria", "bakery", "grill", "kitchen",
    "pharmacy", "medical", "dental", "clinic", "hospital",
    "electrical", "electric", "electronics", "electronic",
    "lighting", "lights", "light", "plumbing", "heating", "cooling",
    "furniture", "furnishing", "furnishings", "flooring",
    "printing", "print", "graphics", "graphic", "design",
    "photography", "photo", "photos", "video", "media",
    "fitness", "gym", "wellness", "yoga",
    "travel", "tours", "tourism", "transport", "transportation",
    "logistics", "shipping", "cargo", "freight",
    "school", "academy", "institute", "university", "college",
    "consultants", "consulting", "consultant", "advisory",
    "management", "properties", "property", "real", "estate",
    "contracting", "construction", "building", "builders",
}

# Combined set for backward compatibility
STOP_WORDS = ARTICLE_WORDS | STRIP_ALWAYS


def _clean_business_name(
    name: str,
    keep_articles: bool = False,
    keep_category: bool = False,
) -> list[str]:
    """Clean business name and return list of significant words.

    Args:
        name: Raw business name.
        keep_articles: If True, keep article words (the, a, an, al, etc.)
                       for brand-domain generation. If False, strip all stop words.
        keep_category: If True, only strip entity suffixes (LLC, Ltd, Inc).
                       Keeps ALL other words including category words like
                       "laundry", "dental", "salon" — useful for generating
                       candidates like "dimalaundry.com".
    """
    if not name:
        return []
    clean = name.lower().strip()
    # Replace "&" with "and" before cleaning (many domains use "and")
    # e.g. "Curry & Co." → "curry and co"
    clean = clean.replace("&", " and ")
    # Remove anything that's not alphanumeric or space
    clean = re.sub(r"[^a-z0-9\s]", " ", clean)
    if keep_category:
        remove_set = ENTITY_SUFFIXES  # Only strip LLC/Ltd/Inc etc.
    elif keep_articles:
        remove_set = STRIP_ALWAYS     # Strip generic words, keep articles
    else:
        remove_set = STOP_WORDS       # Strip everything
    words = [w for w in clean.split() if w and w not in remove_set and len(w) >= 2]
    return words


def _singular_plural_variants(base: str) -> list[str]:
    """Generate singular and plural variants of a domain base.

    'mortonmotors' → ['mortonmotors', 'mortonmotor']
    'mortonmotor'  → ['mortonmotor', 'mortonmotors']
    'gaynedenture' → ['gaynedenture', 'gaynedentures']
    'dentistry'    → ['dentistry'] (no variant — 'dentistrys' is not useful)
    """
    variants = [base]
    if len(base) <= 4:
        return variants

    if base.endswith("ies") and len(base) > 5:
        # deliveries → delivery
        variants.append(base[:-3] + "y")
    elif base.endswith("ses") or base.endswith("xes") or base.endswith("zes"):
        # buses → bus, boxes → box
        variants.append(base[:-2])
    elif base.endswith("s") and not base.endswith("ss"):
        # motors → motor, shoes → shoe
        variants.append(base[:-1])
    elif base.endswith(("y", "ry", "ty", "cy", "gy", "ny")):
        # dentistry, beauty, pharmacy → skip (adding 's' makes nonsense)
        pass
    elif base.endswith(("sh", "ch", "x", "z")):
        # brush → brushes
        variants.append(base + "es")
    else:
        # motor → motors
        variants.append(base + "s")

    return variants


def _generate_candidates(name: str, country: str | None) -> list[str]:
    """Generate candidate domain names from a business name.

    Uses triple-track generation:
    1. WITHOUT articles or category words (stripped) — core brand words
    2. WITH articles (the, a, al) but WITHOUT category words — brand + article
    3. WITH all words except entity suffixes — full name including category

    This ensures:
    - "The Village Cobbler" → thevillagecobbler.ca AND villagecobbler.ca
    - "Dima Laundry" → dima.com AND dimalaundry.com
    - "Morton Motors" → mortonmotors.com AND mortonmotor.com (singular)
    """
    words_no_articles = _clean_business_name(name, keep_articles=False)
    words_with_articles = _clean_business_name(name, keep_articles=True)
    words_all = _clean_business_name(name, keep_category=True)

    if not words_no_articles and not words_with_articles and not words_all:
        return []

    tlds = COUNTRY_TLDS.get(country or "", DEFAULT_TLDS)
    # Always include .com
    if ".com" not in tlds:
        tlds = list(tlds) + [".com"]

    bases: set[str] = set()

    # -----------------------------------------------------------------
    # Track 1 & 2: Core brand words (with and without articles)
    # -----------------------------------------------------------------
    for words in [words_no_articles, words_with_articles]:
        if not words:
            continue

        # Full name joined: "gtaheatingcooling" or "thevillagecobbler"
        full_joined = "".join(words)
        if 4 <= len(full_joined) <= 40:
            bases.add(full_joined)

        # First word only — VERY restrictive to avoid false positives.
        # "colborne.com" for "Colborne Street United Church" (redirects to
        # a bakery robotics company) → false positive.
        # Only generate first-word candidates when:
        # - It's the sole word (brand name like "Etihad"), OR
        # - It's VERY distinctive (10+ chars) AND name has <= 2 words.
        # For 3+ word names, the first word alone is too generic.
        if len(words) == 1 and len(words[0]) >= 4:
            bases.add(words[0])
        elif len(words) == 2 and len(words[0]) >= 10:
            bases.add(words[0])  # very distinctive (e.g. "haramain")

        # First two words joined: "gtaheating"
        if len(words) >= 2:
            two_joined = words[0] + words[1]
            if 4 <= len(two_joined) <= 30:
                bases.add(two_joined)

        # First three words joined: "gtaheatingcooling" (might differ from full)
        if len(words) >= 3:
            three_joined = words[0] + words[1] + words[2]
            if 5 <= len(three_joined) <= 35:
                bases.add(three_joined)

        # Hyphenated: "gta-heating-cooling"
        if len(words) >= 2:
            hyphenated = "-".join(words[:4])  # max 4 words hyphenated
            if len(hyphenated) <= 40:
                bases.add(hyphenated)

        # Hyphenated first two: "gta-heating"
        if len(words) >= 2:
            bases.add(f"{words[0]}-{words[1]}")

    # -----------------------------------------------------------------
    # Track 3: Full name with category words (only strip entity suffixes)
    # e.g. "Dima Laundry" → words_all = ["dima", "laundry"]
    #      → generates "dimalaundry" which Track 1 misses
    # -----------------------------------------------------------------
    if words_all and words_all != words_no_articles and words_all != words_with_articles:
        full_all = "".join(words_all)
        if 5 <= len(full_all) <= 40:
            bases.add(full_all)
        if len(words_all) >= 2:
            two_all = words_all[0] + words_all[1]
            if 4 <= len(two_all) <= 30:
                bases.add(two_all)
            hyphenated_all = "-".join(words_all[:4])
            if len(hyphenated_all) <= 40:
                bases.add(hyphenated_all)

    # -----------------------------------------------------------------
    # Track 4: Raw name — strip NOTHING except punctuation.
    # Catches "curryandco" from "Curry & Co." where Track 3 strips "co".
    # Only generates the full-joined variant (not subsets) to limit noise.
    # -----------------------------------------------------------------
    raw_clean = name.lower().strip().replace("&", "and")
    raw_clean = re.sub(r"[^a-z0-9\s]", " ", raw_clean)
    raw_words = [w for w in raw_clean.split() if w and len(w) >= 2]
    if raw_words and len(raw_words) >= 2:
        raw_joined = "".join(raw_words)
        if 6 <= len(raw_joined) <= 35 and raw_joined not in bases:
            bases.add(raw_joined)

    # Handle acronyms in original name (e.g., "GTA" in "GTA Heating")
    original_words = name.split()
    for i, w in enumerate(original_words):
        if w.isupper() and 2 <= len(w) <= 5:
            remaining = _clean_business_name(" ".join(original_words[i + 1:]))
            if remaining:
                bases.add(w.lower() + "".join(remaining))
                bases.add(w.lower() + remaining[0])  # acronym + first word

    # Arabic transliteration variants: try dropping trailing vowel patterns
    for base in list(bases):
        if len(base) > 6:
            # alharamain → alharaman (drop trailing 'i'/'in')
            if base.endswith("ain"):
                bases.add(base[:-2])  # drop 'in' → 'a'
                bases.add(base[:-1])  # drop 'n' → 'ai'
            elif base.endswith("een"):
                bases.add(base[:-2])
            # Try without 'al'/'al-'/'el'/'el-' prefix too
            for prefix in ("al-", "el-", "al", "el"):
                if base.startswith(prefix) and len(base) > len(prefix) + 3:
                    stripped = base[len(prefix):]
                    if stripped and not stripped.startswith("-"):
                        bases.add(stripped)

    # -----------------------------------------------------------------
    # Singular/plural variants for ALL bases
    # "mortonmotors" → also try "mortonmotor"
    # "mortonmotor"  → also try "mortonmotors"
    # -----------------------------------------------------------------
    expanded_bases: set[str] = set()
    for base in bases:
        # Skip hyphenated bases for s/p variants (apply to last segment)
        if "-" in base:
            expanded_bases.add(base)
            # Also try s/p on last hyphenated segment
            parts = base.split("-")
            for variant in _singular_plural_variants(parts[-1]):
                expanded_bases.add("-".join(parts[:-1] + [variant]))
        else:
            for variant in _singular_plural_variants(base):
                expanded_bases.add(variant)

    # Remove very short or very long candidates
    expanded_bases = {b for b in expanded_bases if 3 <= len(b) <= 40}

    candidates = []
    seen = set()
    for base in sorted(expanded_bases, key=len, reverse=True):  # prefer longer names first
        for tld in tlds:
            domain = base + tld
            if domain not in seen:
                seen.add(domain)
                candidates.append(domain)

    return candidates


# ---------------------------------------------------------------------------
# HTTP checking
# ---------------------------------------------------------------------------

# Realistic browser user-agent (bot UAs get blocked by many servers)
_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

PARKED_INDICATORS = [
    "domain is for sale",
    "buy this domain",
    "parked free",
    "parked by",
    "this domain may be for sale",
    "godaddy.com/domain",
    "sedoparking",
    "hugedomains",
    "dan.com",
    "afternic",
    "namecheap.com/domains",
    "domain parking",
    "this webpage was generated by the domain owner",
    "is for sale",
    "is available for purchase",
    "bodis.com",
    # Broader "for sale" patterns (e.g. "mortons.net for sale")
    "for sale</title>",
    "for sale |",
    "for sale -",
    # JavaScript redirect to lander pages (domain parking)
    'href="/lander"',
    "window.location.href=\"/lander\"",
    # Registrar holding pages
    "domain has been registered",
    "this domain is registered",
    "sav.com",
    "porkbun.com",
    # Coming soon / under construction / launching soon
    "coming soon</title>",
    "coming soon |",
    "under construction</title>",
    "site coming soon",
    "website coming soon",
    "launching soon",
    # Email/domain hosting services (not a business website)
    "hover realnames",
    "realnames",
    "a more meaningful email",
    "namecheap.com",
    "squarespace.com/domain",
    # Domain selling/marketplace pages
    "premium domain",
    "high value domain",
    "domain names for sale",
    "domain name for sale",
    "domain marketplace",
    "domain auction",
    "domain portfolio",
    "category-defining",
    "category defining",
    "brandable domain",
    "exact match domain",
    # Registrar holding/default pages
    "domain registered at",
    "domain default page",
    "default web site page",
    "this site is under construction",
    "this account has been suspended",
    "web hosting by",
    "cpanel",
    "plesk default page",
    "welcome to nginx",
    "apache2 default page",
    "it works!",  # Apache default
    "test page for the apache",
    "congratulations! your new host",
]

# Minimum content length for a real business website.
# Pages under this threshold are almost certainly empty/placeholder/parking.
MIN_REAL_PAGE_BYTES = 500


def _check_domain(domain: str, timeout: float = 3.0) -> tuple[str, bool, int | None]:
    """Check if a domain responds to HTTP. Returns (domain, alive, status_code)."""
    for scheme in ["https", "http"]:
        url = f"{scheme}://{domain}"
        try:
            with httpx.Client(
                follow_redirects=True,
                timeout=(2.0, timeout),  # (connect, read/total)
                verify=False,
                headers={"User-Agent": _BROWSER_UA},
            ) as client:
                resp = client.head(url)
                if 200 <= resp.status_code < 400:
                    return (domain, True, resp.status_code)
                # 403/405 = server is alive, just blocking HEAD
                if resp.status_code in (403, 405):
                    return (domain, True, resp.status_code)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout,
                httpx.TooManyRedirects, httpx.RemoteProtocolError):
            continue
        except Exception:
            continue
    return (domain, False, None)


def _domains_related(original_domain: str, final_domain: str) -> bool:
    """Check if two domains are related (same business).

    Returns True if they share a base name, False if they're completely different.
    e.g. dima.com → ddv.de = unrelated (False)
         loveyourshoes.ca → www.loveyourshoes.ca = related (True)
         brand.com → brand.co.uk = related (True)
         thechildren.com → montrealchildrenshospital.ca = unrelated (False)

    NOTE: We intentionally do NOT do substring-chunk matching (e.g. 8-char
    chunks). "children" (8 chars) is shared between "thechildren" and
    "montrealchildrenshospital" but they are completely different businesses.
    Common English words like "children", "national", "american" cause false
    matches. Only exact base match and containment are reliable.
    """
    if not original_domain or not final_domain:
        return True  # can't tell, assume related

    # Normalize: strip www. and get base domain parts
    for prefix in ("www.",):
        if original_domain.startswith(prefix):
            original_domain = original_domain[len(prefix):]
        if final_domain.startswith(prefix):
            final_domain = final_domain[len(prefix):]

    # Extract base name (before first dot)
    orig_base = original_domain.split(".")[0].lower().replace("-", "")
    final_base = final_domain.split(".")[0].lower().replace("-", "")

    if not orig_base or not final_base:
        return True

    # Same base name → related
    if orig_base == final_base:
        return True

    # One contains the other → related, BUT only if the shorter base is
    # long enough (10+ chars) relative to the longer one. Short bases
    # create false matches: "colborne" (8 chars) in "colbornefoodbotics" (18)
    # are completely different businesses.
    shorter, longer = (orig_base, final_base) if len(orig_base) <= len(final_base) else (final_base, orig_base)
    if shorter in longer:
        # If the shorter base is at least 60% of the longer one's length,
        # they're probably related (e.g. "mortonmotor" vs "mortonmotors")
        if len(shorter) >= len(longer) * 0.6:
            return True
        # If shorter is a prefix and >= 10 chars, likely related
        # (e.g. "indianroti" in "indianrotihouse")
        if longer.startswith(shorter) and len(shorter) >= 10:
            return True

    return False


def _fetch_page(url: str) -> tuple[int, str, str, str]:
    """Fetch a page and return (status_code, body_text, final_url, title).

    Returns (0, "", "", "") on any error.
    """
    try:
        from urllib.parse import urlparse

        with httpx.Client(
            follow_redirects=True,
            timeout=(2.0, 5.0),
            verify=False,
            headers={"User-Agent": _BROWSER_UA},
        ) as client:
            resp = client.get(url)
            body = resp.text
            final_url = str(resp.url)
            # Extract title
            import re as _re
            m = _re.search(r"<title[^>]*>(.*?)</title>", body[:5000], _re.IGNORECASE | _re.DOTALL)
            title = m.group(1).strip()[:200].lower() if m else ""
            return (resp.status_code, body, final_url, title)
    except Exception:
        return (0, "", "", "")


def _word_in_text(word: str, text: str) -> bool:
    """Check if a word appears as a whole word in text (not a substring).

    Uses word-boundary regex to avoid false matches like:
    - "spa" matching in "space" or "spacing"
    - "cao" matching in "caoni"
    - "nam" matching in "name" or "dynamic"

    Also checks singular/plural variants so "motors" matches "motor" and
    vice versa. Business name "Morton Motors" should match a page with
    "Morton Motor" (the same business using singular form).
    """
    # Check the word itself
    if re.search(r"\b" + re.escape(word) + r"\b", text):
        return True
    # Check singular/plural variants
    for variant in _singular_plural_variants(word):
        if variant != word and re.search(r"\b" + re.escape(variant) + r"\b", text):
            return True
    return False


def _is_valid_business_site(
    url: str,
    business_name: str,
    status_code: int,
    body: str,
    final_url: str,
    title: str,
) -> bool:
    """Validate that a live domain is a real business website for this business.

    Checks:
    1. Not parked/for-sale/empty/default-page
    2. Not redirected to unrelated domain
    3. Page content is relevant to the business (whole-word matching)

    This is the critical false-positive filter.

    Content relevance uses WHOLE-WORD matching (regex \\b) to avoid:
    - "spa" matching "space" on hosting pages
    - "cao" matching "caoni" in URLs
    - Short word substrings causing false positives

    Also checks if the domain base name appears in page content (even as
    a joined token in URLs/JS config). Wix, Squarespace, and other SPA
    sites have the domain in their configuration long before the visible
    text content — e.g. "indianrotihouse" appears in Wix config at char
    4228 while the separate words only appear at char 101000+.

    When the domain redirected to a different host, content matching is
    STRICTER (requires more word matches) because the redirect itself is
    a signal of potential mismatch.
    """
    from urllib.parse import urlparse

    if status_code != 200:
        return False

    # Detect redirect to different host
    original_host = urlparse(url).hostname or ""
    final_host = urlparse(final_url).hostname or ""
    # Normalize: strip www. for comparison
    orig_host_norm = original_host.removeprefix("www.")
    final_host_norm = final_host.removeprefix("www.")
    is_redirected = (
        orig_host_norm != final_host_norm
        and original_host != ""
        and final_host != ""
    )

    if is_redirected and not _domains_related(original_host, final_host):
        logger.debug("Reject %s → redirected to unrelated %s", url, final_host)
        return False

    body_len = len(body)

    # Too small to be a real website
    if body_len < MIN_REAL_PAGE_BYTES:
        return False

    body_lower = body[:8000].lower()

    # Check parking indicators
    if any(indicator in body_lower for indicator in PARKED_INDICATORS):
        return False

    # ---------------------------------------------------------------
    # Content relevance check — does the page relate to this business?
    # ---------------------------------------------------------------
    # Extract significant words from business name
    name_clean = business_name.lower().strip()
    name_clean = name_clean.replace("&", " and ")
    name_clean = re.sub(r"[^a-z0-9\s]", " ", name_clean)

    # Common words that don't help identify a specific business
    CONTENT_STOP_WORDS = {
        "the", "and", "for", "from", "with", "that", "this",
        "our", "your", "all", "new", "one", "two",
    }

    # Geographic/common words that appear on many pages and don't
    # meaningfully identify a specific business. If ALL matching words
    # are in this set, the match is probably coincidental.
    # e.g. "College Street Medical Laboratories" → "college" + "street"
    # match on any "College Street" page, but "medical" and "laboratories"
    # (the distinctive words) don't match → false positive.
    GENERIC_LOCATION_WORDS = {
        # Geographic/address words
        "street", "avenue", "road", "drive", "boulevard", "lane", "place",
        "way", "court", "circle", "terrace", "crescent", "square",
        "north", "south", "east", "west", "central", "upper", "lower",
        "college", "park", "lake", "hill", "mountain", "river", "bay",
        "city", "town", "village", "downtown", "midtown", "uptown",
        "first", "second", "third", "main", "high", "grand",
        "new", "old", "big", "little", "great", "royal", "golden",
        "green", "blue", "red", "white", "black",
        "national", "international", "global", "general", "universal",
        # Generic business/industry prefixes that appear on ANY website
        # in that industry and don't identify a specific business.
        # "B-K Auto Service" → "auto" matches any auto site.
        "auto", "car", "home", "food", "tech", "pro", "express",
        "quick", "fast", "best", "top", "prime", "elite", "premium",
    }

    name_words = {
        w for w in name_clean.split()
        if w and len(w) >= 3 and w not in CONTENT_STOP_WORDS
    }

    if not name_words:
        return True  # Can't check, allow it

    # Build check_text from multiple sources (wider window for JS-heavy sites):
    # - <title> tag
    # - meta description and og:title (reliable even on SPAs)
    # - first 5000 chars of body (up from 3000 — catches Wix/React sites)
    head_section = body_lower[:5000]
    meta_parts = []
    for pattern in [
        r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\']+)',
        r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*name=["\']description["\']',
        r'<meta[^>]*property=["\']og:title["\'][^>]*content=["\']([^"\']+)',
        r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*property=["\']og:title["\']',
        r'<meta[^>]*property=["\']og:site_name["\'][^>]*content=["\']([^"\']+)',
        r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*property=["\']og:site_name["\']',
    ]:
        m = re.search(pattern, head_section, re.IGNORECASE)
        if m:
            meta_parts.append(m.group(1))

    check_text = title + " " + " ".join(meta_parts) + " " + body_lower[:5000]

    # ----- Detect title-is-domain-name pages (near-parked) -----
    # If the page title is just the domain name itself (e.g. "etihads.net"),
    # AND the body is small (< 10KB), it's almost certainly parked.
    # Larger pages (like monkey-sushi.com with 80K body) may have a domain
    # title but be real websites with JS-rendered content.
    domain_host = original_host.removeprefix("www.")
    domain_base = domain_host.split(".")[0].lower().replace("-", "")
    title_stripped = title.strip().lower().replace(" ", "").replace("-", "")
    is_title_just_domain = title_stripped and (
        title_stripped == domain_host.lower().replace("-", "")
        or title_stripped == domain_base
        or title_stripped == domain_host.lower().replace(".", "").replace("-", "")
    )
    if is_title_just_domain:
        # When the page title is just the domain name, it's likely parked or
        # a placeholder. Small pages are obviously parked; large pages (SPA
        # frameworks) may also be placeholders with JS boilerplate.
        if body_len < 10000:
            logger.debug("Reject %s — title is domain name + small page", url, title)
            return False
        # Even for large pages: the title only contains the domain name, not
        # the business name. Any word match in the title is just a domain echo.
        # We must NOT count title matches as evidence for single-word names.
        # Set a flag so the single-word check below requires body match instead.
        title_is_domain_echo = True
    else:
        title_is_domain_echo = False

    # ----- Domain base name in page content (supportive evidence) -----
    # If the domain's base name (e.g. "indianrotihouse") appears in the
    # page's own HTML/JS/config, it's supportive evidence that this site
    # belongs to the business.
    #
    # BUT: every website contains its own domain in URLs, canonical links,
    # script src, etc. So short domain bases (< 12 chars) will ALWAYS match
    # and cause false positives:
    #   - "eminail.com" → "eminail" (7 chars) found in own <link> tags
    #   - "drgeetas.com" → "drgeetas" (8 chars) found in own URLs
    #   - "universals.com" → "universals" (10 chars) found in own content
    #
    # Only trust the domain-base check for VERY long bases (12+ chars) that
    # are distinctive enough to be a brand name, not a coincidence.
    # e.g. "indianrotihouse" (15 chars) is clearly the business name.
    #
    # NOTE: We set a FLAG instead of returning True. The domain-base match
    # alone is NOT sufficient — we still must run the title filter at the end.
    # Without this, "electricallightings.com" (19 chars) passes because the
    # domain base is found in its own page, but the title says "Ace Hardware
    # Shop" — clearly a different business. The title filter catches this.
    domain_base_match = len(domain_base) >= 12 and domain_base in check_text

    # Count how many significant business-name words appear as WHOLE WORDS
    matching_words = {w for w in name_words if _word_in_text(w, check_text)}

    # Distinctive = longer words (5+ chars) that are less likely to match randomly
    distinctive_matches = {w for w in matching_words if len(w) >= 5}

    # Filter out generic business/category words from STRIP_ALWAYS.
    # These words (auto, service, repair, dental, etc.) appear on ANY website
    # in that industry, so matching them doesn't identify a SPECIFIC business.
    # "B-K Auto Service" → matching {"auto", "service"} → both are generic →
    # auto-services.ca could be ANY auto service, not specifically B-K.
    brand_matches = matching_words - STRIP_ALWAYS - GENERIC_LOCATION_WORDS
    brand_distinctive = {w for w in brand_matches if len(w) >= 5}

    # ----- Content matching rules -----
    # When domain_base_match is True, we already have strong evidence from
    # the domain base appearing in page content (e.g. "indianrotihouse" found
    # in Wix config). We skip the detailed word-matching rejections but STILL
    # run the title filter below — because a domain base match alone doesn't
    # prove the site belongs to THIS business:
    #   - "electricallightings.com" (19 chars) → domain base found in page
    #     BUT title says "Ace Hardware Shop" → NOT the lighting company.
    #
    # When domain_base_match is False, apply all word-matching rules as usual.

    if not domain_base_match:
        # Quick reject: if ALL matching words are generic business/category words
        # (from STRIP_ALWAYS or GENERIC_LOCATION_WORDS), the match is coincidental.
        # e.g. "B-K Auto Service" → {"auto", "service"} match on auto-services.ca,
        # but those words appear on ANY auto service website. The distinctive part
        # "B-K" was stripped and doesn't match → this is not B-K's website.
        if matching_words and not brand_matches:
            logger.debug(
                "Reject %s for '%s' — all matching words are generic (%s), no brand-specific match",
                url, business_name, matching_words,
            )
            return False

        if len(name_words) >= 3:
            if not matching_words:
                logger.debug(
                    "Reject %s for '%s' — no name words in page (had: %s)",
                    url, business_name, name_words,
                )
                return False

            # If ALL matching words are generic/geographic/business-category,
            # the match is probably coincidental. e.g. "College Street Medical
            # Labs" → "college" + "street" match on any College St. website.
            # Also exclude STRIP_ALWAYS words (trading, company, service, etc.)
            # which are generic business terms, not brand identifiers.
            non_generic_matches = matching_words - GENERIC_LOCATION_WORDS - STRIP_ALWAYS
            if not non_generic_matches:
                logger.debug(
                    "Reject %s for '%s' — only generic/location/category word matches (%s)",
                    url, business_name, matching_words,
                )
                return False

            # For 3+ word names, require PROPORTIONAL matches. Only matching
            # 1 out of 3+ distinctive words is usually coincidental:
            #   "In Style Furniture Gallery" → "style" alone matches InStyle
            #   Magazine. That's 1/3 words = likely a name collision.
            non_generic_distinctive = {w for w in non_generic_matches if len(w) >= 5}
            very_distinctive = {w for w in non_generic_matches if len(w) >= 7}

            # For names with 4+ significant words, matching only 1 word is
            # too weak regardless of word length. A 5-word name with 1/5
            # match (20%) is almost certainly coincidental:
            #   "St. Gabriel Medical Centre Walk-In Clinic" →
            #   only "gabriel" (7 chars, common saint name) matches on a
            #   Catholic church page → false positive.
            # For 3-word names, allow a single very distinctive word (7+
            # chars) as sufficient evidence (33% match rate).
            if len(name_words) >= 4:
                if len(non_generic_matches) < 2:
                    logger.debug(
                        "Reject %s for '%s' — only %d non-generic match(es) for %d-word name (%s), need 2+",
                        url, business_name, len(non_generic_matches), len(name_words),
                        non_generic_matches,
                    )
                    return False
            elif len(non_generic_matches) < 2 and not very_distinctive:
                logger.debug(
                    "Reject %s for '%s' — only %d non-generic match(es) for %d-word name (%s)",
                    url, business_name, len(non_generic_matches), len(name_words),
                    non_generic_matches,
                )
                return False

            # Redirect extra: for 3+ word names on a redirect, require even
            # more evidence — at least 2 matches total AND 1 distinctive.
            if is_redirected:
                if len(matching_words) < 2 or not non_generic_distinctive:
                    logger.debug(
                        "Reject %s for '%s' — redirect + insufficient evidence (%s)",
                        url, business_name, matching_words,
                    )
                    return False

        elif len(name_words) == 2:
            if not matching_words:
                logger.debug(
                    "Reject %s for '%s' — 0 of 2 words match",
                    url, business_name,
                )
                return False
            # For 2-word names, require at least 1 DISTINCTIVE match (5+ chars).
            # Short words (< 5 chars) are too common and cause name collisions:
            #   "Lila Cafe" → only "lila" (4 chars) matches on lila.ae which
            #   is "Lac De Lila" (different business).
            if not distinctive_matches:
                logger.debug(
                    "Reject %s for '%s' — only short-word matches (%s), need 5+ char",
                    url, business_name, matching_words,
                )
                return False
            # If BOTH name words are distinctive (5+ chars), require BOTH to
            # match. This catches name collisions like:
            #   "Dr. Geeta Shukla" → {"geeta", "shukla"} → drgeetas.com has
            #   "geeta" but not "shukla" → different doctor.
            #   "Universal Pharmacy" → {"universal", "pharmacy"} → only
            #   "universal" matches (common word) → not a pharmacy.
            all_distinctive = {w for w in name_words if len(w) >= 5}
            if len(all_distinctive) == 2 and len(distinctive_matches) < 2:
                logger.debug(
                    "Reject %s for '%s' — both words distinctive but only %d/2 match (%s)",
                    url, business_name, len(distinctive_matches), distinctive_matches,
                )
                return False
        else:
            # Single-word name: very high false-positive risk.
            # e.g. "Al Zowar" → name_words = {"zowar"} → zowar.net is a
            # DIFFERENT restaurant in Jordan, not the Dubai retail shop.
            # Require the word in the TITLE (strongest signal), not just body.
            if not matching_words:
                logger.debug(
                    "Reject %s for '%s' — word not in page",
                    url, business_name,
                )
                return False
            # For single-word names, also require the word appears in the
            # <title> tag specifically (not just body). Page title is the
            # strongest indicator of what the site is actually about.
            # EXCEPTION: if the title IS the domain name, the title match is
            # meaningless (domain echo). Require a body match in meta tags instead.
            the_word = list(name_words)[0]
            if title_is_domain_echo:
                # Title is just the domain name — word appears in title only
                # because of domain echo, not because the page is about this biz.
                # Require the word in meta description or og:title instead.
                meta_text = " ".join(meta_parts)
                if not _word_in_text(the_word, meta_text):
                    logger.debug(
                        "Reject %s for '%s' — single word, title is domain echo, not in meta ('%s')",
                        url, business_name, meta_text[:80],
                    )
                    return False
            elif not _word_in_text(the_word, title):
                logger.debug(
                    "Reject %s for '%s' — single word name not in page title ('%s')",
                    url, business_name, title[:80],
                )
                return False

    # ----- Final filter: title verification for multi-word names -----
    # The page title is the strongest signal of what a site is actually about.
    # If the content matches (body has the right words) but the TITLE doesn't
    # contain any brand-specific word, it's likely a name collision:
    #   - "Trans Tech" (Dubai) → trans-tech.net has "CDL Training" in title
    #   - "Tesla Power" (Dubai) → tesla-power.net has "Diesel Generator" in title
    #   - "Electrical Lighting" → has "Ace Hardware Shop" in title
    #
    # Only apply when title is non-empty (SPAs may have empty titles) and
    # name has 2+ words (single-word names already checked title above).
    if title and len(name_words) >= 2:
        # Check if any brand word (not in STRIP_ALWAYS/GENERIC) appears in title
        brand_in_title = any(
            _word_in_text(w, title)
            for w in brand_matches
        )
        if not brand_in_title:
            # Also check if ANY matching word is in the title (less strict)
            any_in_title = any(
                _word_in_text(w, title)
                for w in matching_words
            )
            if not any_in_title:
                logger.debug(
                    "Reject %s for '%s' — no name words in page title ('%s')",
                    url, business_name, title[:80],
                )
                return False
            # If only generic/STRIP_ALWAYS words are in the title, also reject
            # e.g. "Electrical Lighting" → "lighting" in title but it's generic
            matching_in_title = {
                w for w in matching_words if _word_in_text(w, title)
            }
            brand_in_title_set = matching_in_title - STRIP_ALWAYS - GENERIC_LOCATION_WORDS
            if not brand_in_title_set:
                logger.debug(
                    "Reject %s for '%s' — only generic words in title (%s)",
                    url, business_name, matching_in_title,
                )
                return False

    return True


def _domain_base_length(domain: str) -> int:
    """Get the base (pre-TLD) length of a domain for priority sorting."""
    return len(domain.split(".")[0])


def _check_candidates(
    candidates: list[str],
    business_name: str,
    max_workers: int = 15,
    timeout: float = 3.0,
) -> str | None:
    """Check multiple candidate domains in parallel.

    Returns the best (most specific, longest base name) live, non-parked,
    content-relevant URL for this business.
    """
    if not candidates:
        return None

    live_domains: list[tuple[str, int | None]] = []

    with ThreadPoolExecutor(max_workers=min(max_workers, len(candidates))) as pool:
        futures = {pool.submit(_check_domain, c, timeout): c for c in candidates}
        for future in as_completed(futures):
            try:
                domain, alive, status = future.result()
                if alive:
                    live_domains.append((domain, status))
            except Exception:
                continue

    # Sort by specificity: longer base name = more specific = higher priority.
    # "mortonmotor.com" (11-char base) beats "morton.com" (6-char base).
    live_domains.sort(key=lambda x: _domain_base_length(x[0]), reverse=True)

    # Check live domains: parking + content relevance in priority order
    for domain, status in live_domains:
        url = f"https://{domain}"
        code, body, final_url, title = _fetch_page(url)
        if _is_valid_business_site(url, business_name, code, body, final_url, title):
            return url

    # If HTTPS all failed, try HTTP variants
    for domain, status in live_domains:
        url = f"http://{domain}"
        code, body, final_url, title = _fetch_page(url)
        if _is_valid_business_site(url, business_name, code, body, final_url, title):
            return url

    return None


def _process_one_business(
    biz_name: str,
    biz_country: str | None,
    max_workers: int = 10,
) -> tuple[str | None, int, str]:
    """Process a single business: generate candidates, check domains.

    Returns (found_url, candidates_checked, result_key).
    Runs entirely outside the DB session so it's safe for threads.
    """
    candidates = _generate_candidates(biz_name, biz_country)
    if not candidates:
        return (None, 0, "no_candidates")
    found_url = _check_candidates(candidates, business_name=biz_name, max_workers=max_workers)
    result_key = "has_website" if found_url else "no_match"
    return (found_url, len(candidates), result_key)


# ---------------------------------------------------------------------------
# Main batch function
# ---------------------------------------------------------------------------

def run_batch(
    limit: int | None = None,
    min_score: float = 0.0,
    scope: str | None = None,
    max_workers: int = 10,
    business_parallelism: int = 15,
) -> dict:
    """Guess domains from business names and check if they resolve.

    For each business with lead_score >= min_score and no website_url:
    1. Generate candidate domains from the business name + country TLD
    2. HTTP HEAD check candidates in parallel
    3. If a domain responds (not parked), set business.website_url

    FREE — no API key, no rate limits.

    Args:
        limit: Max businesses to check. Default 100.
        min_score: Only check businesses scoring at or above this.
        scope: Job scope tag.
        max_workers: Parallel HTTP workers per business (for candidate checks).
        business_parallelism: Number of businesses to process concurrently.

    Returns:
        Dict with processing stats.
    """
    effective_limit = limit if limit is not None else 1000

    with session_scope() as session:
        run = start_job(session, JOB_NAME, scope=scope)
        try:
            stmt = (
                select(Business, City)
                .outerjoin(City, Business.city_id == City.id)
                .where(Business.name.isnot(None))
                .where(Business.name != "")
                .where(or_(Business.website_url.is_(None), Business.website_url == ""))
                .where(
                    or_(
                        Business.raw.is_(None),
                        not_(Business.raw.has_key("domain_guess_verified")),
                    )
                )
                .order_by(Business.lead_score.desc().nullslast(), Business.created_at)
            )
            if min_score > 0:
                stmt = stmt.where(Business.lead_score >= min_score)
            stmt = stmt.limit(effective_limit)

            rows = session.execute(stmt).all()

            if not rows:
                complete_job(session, run, processed_count=0, details={
                    "processed": 0, "websites_found": 0, "candidates_checked": 0,
                })
                return {"processed": 0, "websites_found": 0, "candidates_checked": 0}

            # Prepare business data for parallel processing (outside DB session)
            biz_data = []
            for business, city in rows:
                biz_data.append({
                    "id": business.id,
                    "name": business.name,
                    "country": city.country if city else None,
                })

            start_time = time.time()

            # Process businesses in parallel — HTTP checks happen outside DB session
            results_map: dict = {}  # biz_id -> (found_url, candidates_checked, result_key)
            with ThreadPoolExecutor(max_workers=business_parallelism) as pool:
                future_to_id = {
                    pool.submit(
                        _process_one_business,
                        bd["name"],
                        bd["country"],
                        max_workers,
                    ): bd["id"]
                    for bd in biz_data
                }
                for future in as_completed(future_to_id):
                    biz_id = future_to_id[future]
                    try:
                        results_map[biz_id] = future.result()
                    except Exception as exc:
                        logger.warning("Domain guess error for business %s: %s", biz_id, exc)
                        results_map[biz_id] = (None, 0, "error")

            # Apply results back to DB (sequential, fast)
            processed = 0
            websites_found = 0
            total_candidates_checked = 0

            for business, city in rows:
                found_url, candidates_checked, result_key = results_map.get(
                    business.id, (None, 0, "error")
                )
                total_candidates_checked += candidates_checked

                raw = dict(business.raw or {})
                raw["domain_guess_verified"] = True
                raw["domain_guess_result"] = result_key
                raw["domain_guess_candidates_checked"] = candidates_checked

                if found_url:
                    business.website_url = found_url
                    raw["domain_guess_website"] = found_url
                    websites_found += 1
                    logger.info(
                        "Domain guess found website for '%s': %s",
                        business.name, found_url,
                    )

                business.raw = raw
                business.scored_at = None
                processed += 1

                # Flush every 50 rows to keep transactions short
                if processed % 50 == 0:
                    session.flush()

            elapsed = time.time() - start_time
            result = {
                "processed": processed,
                "websites_found": websites_found,
                "candidates_checked": total_candidates_checked,
                "elapsed_seconds": round(elapsed, 1),
                "rate_per_minute": round((processed / elapsed) * 60, 1) if elapsed > 0 else 0,
            }

            complete_job(session, run, processed_count=processed, details=result)
            logger.info(
                "Domain guess complete: %d processed, %d websites found in %.1fs (%.0f/min)",
                processed, websites_found, elapsed,
                result["rate_per_minute"],
            )
            return result

        except Exception as exc:
            fail_job(session, run, error=str(exc))
            raise
