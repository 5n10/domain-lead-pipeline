"""Web search website verification worker.

Searches DuckDuckGo for each lead candidate to verify whether the business
truly lacks a website. If search results contain a real business website
(not a directory listing like Yelp/Facebook), sets business.website_url
so the business gets disqualified from leads on next rescore.

Completely FREE — no API key, no registration required.
Uses DDG's HTML lite endpoint directly (the duckduckgo_search library is broken).
"""
from __future__ import annotations

import logging
import re
import time
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse

from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

import requests as http_requests
from bs4 import BeautifulSoup
from sqlalchemy import not_, or_, select

from ..db import session_scope
from ..jobs import complete_job, fail_job, start_job
from ..models import Business, City

logger = logging.getLogger(__name__)

JOB_NAME = "web_search_verify_websites"

# Domains that are business directories or social media — NOT real business websites.
# If a search result points here, the business doesn't necessarily own this URL.
DIRECTORY_DOMAINS = {
    # Social media
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "linkedin.com", "tiktok.com", "youtube.com", "pinterest.com",
    "threads.net",
    # Business directories
    "yelp.com", "yelp.ca", "yelp.ae",
    "yellowpages.com", "yellowpages.ca", "yellowpages.ae",
    "tripadvisor.com", "tripadvisor.ca", "tripadvisor.ae",
    "bbb.org",
    "trustpilot.com",
    "glassdoor.com",
    "indeed.com",
    "mapquest.com",
    "foursquare.com",
    "zomato.com",
    "talabat.com",
    "deliveroo.com", "deliveroo.ae",
    "ubereats.com",
    "doordash.com",
    "grubhub.com",
    "justeat.com",
    # Maps & navigation
    "google.com", "google.ca", "google.ae",
    "maps.google.com",
    "apple.com",
    "bing.com",
    # General directories & aggregators
    "crunchbase.com",
    "bloomberg.com",
    "reuters.com",
    "wikipedia.org",
    "wikidata.org",
    "openstreetmap.org",
    "manta.com",
    "dnb.com",
    "hoovers.com",
    "kompass.com",
    "chamberofcommerce.com",
    # UAE-specific directories
    "bayut.com",
    "propertyfinder.ae",
    "dubizzle.com",
    "yallacompare.com",
    "connectuae.com",
    "yellowpages-uae.com",
    # Canada-specific directories
    "canada411.ca",
    "canadapages.com",
    "pagesjaunes.ca",
    # Booking & reservations
    "booking.com",
    "airbnb.com",
    "expedia.com",
    "hotels.com",
    "agoda.com",
    # Food/restaurant aggregators
    "menulog.com.au",
    "eat.ch",
    "lieferando.de",
    # UAE/Middle East directories
    "bizuum.com",
    "dubaitradersonline.com",
    "2gis.ae", "2gis.com",
    "connectuae.ae",
    "uaecontact.com",
    "dubaibizfinder.com",
    "finduslocal.com",
    "thedubaimall.com",
    "visitdubai.com",
    "aiwa.ae",
    "bestrestaurantdubaii.com",
    # Maps & navigation (additional)
    "mapy.com",
    "yango.com",
    "waze.com",
    "here.com",
    "mapcarta.com",
    # Food delivery platforms
    "noon.com", "food.noon.com",
    "careem.com",
    "hungerstation.com",
    "toters.com",
    "skipcart.com",
    "instacart.com",
    "postmates.com",
    # Hotel/travel aggregators
    "hotelscombined.com", "hotelscombined.co.uk",
    "trivago.com", "trivago.ae", "trivago.ca",
    "kayak.com", "kayak.ae",
    "priceline.com",
    "fiji.travel",
    # Review/blog platforms
    "blogspot.com",
    "wordpress.com",
    "medium.com",
    "tumblr.com",
    "reddit.com",
    "quora.com",
    # Business listing aggregators
    "qdexx.com",
    "cylex.com", "cylex.ca",
    "brownbook.net",
    "hotfrog.com", "hotfrog.ca",
    "shopintoronto.com",
    "carsandcars.ca",
    "n49.com",
    "ourbis.ca",
    "411.ca", "411.info",
    "canpages.ca",
    "mysask411.com",
    "infobel.com",
    # Chinese/foreign language aggregators
    "zhihu.com",
    "baidu.com",
    "jingyan.baidu.com",
    "zhidao.baidu.com",
    # Booking/appointment platforms
    "fresha.com",
    "vagaro.com",
    "booksy.com",
    "mindbodyonline.com",
    "schedulicity.com",
    # More directories & listing sites
    "neardaddy.com",
    "smokepipeshops.com",
    "city-data.com",
    "investinganswers.com",
    "newmouth.com",
    "backindo.com",
    "finduslocal.com",
    # Reference / encyclopedia
    "britannica.com",
    "merriam-webster.com",
    "dictionary.com",
    "howstuffworks.com",
    # Food / recipe sites
    "foodnetwork.com",
    "eatingwell.com",
    "allrecipes.com",
    # Government / institutional (never a small biz website)
    "worldbank.org",
    "un.org",
    "who.int",
    # Tech / Q&A sites
    "stackoverflow.com",
    "stackexchange.com",
    "github.com",
    # E-commerce marketplaces
    "amazon.com", "amazon.ca", "amazon.ae",
    "ebay.com", "ebay.ca",
    "walmart.com", "walmart.ca",
    "alibaba.com",
    "etsy.com",
}

# Common public email domains — search results from these aren't business websites
PUBLIC_EMAIL_DOMAINS_QUICK = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "aol.com", "icloud.com", "mail.com", "protonmail.com",
}


def _get_domain_from_url(url: str) -> str:
    """Extract the root domain from a URL."""
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        # Strip www. prefix
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def _is_directory_or_social(url: str) -> bool:
    """Check if a URL points to a directory, social media, or aggregator."""
    domain = _get_domain_from_url(url)
    if not domain:
        return True  # Can't parse = skip
    # Check exact match and parent domain match
    for dir_domain in DIRECTORY_DOMAINS:
        if domain == dir_domain or domain.endswith("." + dir_domain):
            return True
    return False


def _normalize_name(name: str) -> str:
    """Normalize a business name for comparison."""
    # Remove common suffixes and punctuation
    clean = name.lower().strip()
    clean = re.sub(r"[''`]s?\b", "", clean)  # Remove possessives
    clean = re.sub(r"[^a-z0-9\s]", " ", clean)  # Keep only letters/numbers
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def _name_words(name: str) -> set[str]:
    """Get significant words from a name (skip stop words)."""
    stop_words = {"the", "a", "an", "and", "of", "in", "at", "to", "for", "by", "le", "la", "les", "de", "du", "al"}
    words = set(_normalize_name(name).split())
    return words - stop_words


# Words too generic to confirm a domain belongs to a specific business.
# "candle" in yankeecandle.com does NOT mean it belongs to "Candle Night Personal Care".
# But "morton" in mortonmotor.com DOES mean it belongs to "Morton Motors".
GENERIC_BUSINESS_TERMS = {
    # Business types
    "fashion", "beauty", "salon", "cafe", "restaurant", "food", "market",
    "store", "shop", "mart", "auto", "dental", "medical", "health",
    "service", "services", "trading", "general", "kitchen", "grill",
    "pizza", "coffee", "hotel", "travel", "tour", "tours", "fitness",
    "clinic", "pharmacy", "mobile", "computer", "digital", "tech",
    "media", "print", "photo", "electric", "electronics", "sign",
    "care", "personal", "super", "plus", "express", "premium",
    "classic", "modern", "barber", "nails", "spa", "studio", "lab",
    "consulting", "realty", "properties", "rentals", "cleaning",
    "repair", "repairs", "parts", "supplies", "wholesale", "retail",
    "packaging", "logistics", "shipping", "delivery", "transport",
    "construction", "building", "plumbing", "roofing", "flooring",
    "catering", "bakery", "grocery", "laundry", "tailor", "jewellery",
    "jewelry", "optical", "dental", "dentist", "doctor", "lawyer",
    # Common descriptors
    "candle", "light", "night", "star", "gold", "silver", "royal",
    "grand", "golden", "smart", "fresh", "clean", "bright", "paradise",
    "diamond", "crystal", "pearl", "ruby", "jade", "emerald",
    # Geographic / location terms
    "island", "park", "garden", "urban", "village", "city",
    "center", "centre", "global", "international", "world",
    "pacific", "atlantic", "northern", "southern", "eastern", "western",
    # Country / region names commonly used in business names
    "belgium", "germany", "france", "italy", "turkey", "jordan",
    "lebanon", "morocco", "america", "canada", "brazil", "mexico",
    "thailand", "vietnam", "malaysia", "indonesia", "africa",
    "europe", "kingdom", "dynasty", "empire",
    # Common English words used as business names
    "chapter", "element", "essence", "fusion", "cascade", "pioneer",
    "horizon", "phoenix", "genesis", "vintage", "premier", "prestige",
    "supreme", "triumph", "liberty", "fortune", "destiny", "miracle",
    # Other generic
    "best", "first", "great", "good", "quality", "standard",
}


def _domain_contains_name(domain: str, business_name: str) -> bool:
    """Check if a domain likely belongs to this business based on name match.

    Strict matching to avoid false positives:
    - Full name substring match (strong signal)
    - Multi-word match (2+ words from name in domain)
    - Single-word match ONLY if word is distinctive (not generic) and 7+ chars

    e.g. "sonidentistry.com" matches "Soni Dentistry" (full name in domain)
         "mortonmotor.com"   matches "Morton Motors"  (domain in full name)
         "torontodentureservices.ca" matches "Gayne Denture Clinic" (distinctive word)
    but  "yankeecandle.com" does NOT match "Candle Night Personal Care" (generic word)
         "dubai-fashions.com" does NOT match "Al Riyan Fashion" (generic word)
    """
    if not domain or not business_name:
        return False

    # Remove TLD and hyphens from domain for comparison
    domain_base = domain.split(".")[0].lower().replace("-", "")
    name_clean = re.sub(r"[^a-z0-9]", "", business_name.lower())

    # Strong match: full cleaned name is substring of domain
    # e.g. "sonidentistry" in "sonidentistry" or "villagecobbler" in "thevillagecobbler"
    # Require 7+ chars to avoid short-name ambiguity (e.g. "asiana" in "flyasiana")
    if len(name_clean) >= 7 and name_clean in domain_base:
        return True

    # Strong match: domain base is substring of full cleaned name
    # e.g. "mortonmotor" in "mortonmotors"
    # Require 6+ chars AND significant overlap (>=65% of name length) to avoid
    # partial matches like "sunnyside" (9) in "sunnysidedental" (15) = 60%
    if len(domain_base) >= 6 and domain_base in name_clean:
        overlap_ratio = len(domain_base) / max(len(name_clean), 1)
        if overlap_ratio >= 0.65:
            return True

    # Word-level matching — count how many name words appear in the domain
    words = _name_words(business_name)
    matching_words = [w for w in words if len(w) >= 4 and w in domain_base]

    # 2+ words match → strong signal even with generic words
    if len(matching_words) >= 2:
        return True

    # 1 word match → only if it's distinctive (not generic) AND long enough
    if len(matching_words) == 1:
        word = matching_words[0]
        if word not in GENERIC_BUSINESS_TERMS and len(word) >= 7:
            return True

    return False


def _build_search_queries(biz_name: str, city_name: str | None) -> list[str]:
    """Generate multiple search queries with decreasing specificity.

    Instead of only trying exact-match quoted full name, tries:
    1. Full name without quotes + city (broadest match)
    2. Shortened name (2-3 longest significant words) + city
    3. Full name in quotes + city (most specific, original behavior)
    """
    queries = []

    # Query 1: Full name WITHOUT quotes + city (broadest match)
    if city_name:
        queries.append(f"{biz_name} {city_name}")
    else:
        queries.append(biz_name)

    # Query 2: Shortened name — keep 2-3 longest significant words
    words = sorted(_name_words(biz_name), key=len, reverse=True)
    if len(words) >= 2:
        short_words = words[:3]
        short_name = " ".join(short_words)
        if city_name:
            q = f'"{short_name}" {city_name}'
        else:
            q = f'"{short_name}"'
        if q not in queries:
            queries.append(q)

    # Query 3: Full name in quotes + city (most specific, may miss)
    if city_name:
        q = f'"{biz_name}" {city_name}'
    else:
        q = f'"{biz_name}"'
    if q not in queries:
        queries.append(q)

    return queries


def _result_matches_business(
    result: dict,
    business_name: str,
) -> bool:
    """Check if a search result likely belongs to this business.

    Uses title matching and domain-name correlation.
    """
    title = (result.get("title") or "").lower()
    href = result.get("href") or ""
    domain = _get_domain_from_url(href)

    biz_words = _name_words(business_name)
    if not biz_words:
        return False

    # Check title word overlap
    title_words = _name_words(title)
    if title_words:
        overlap = biz_words & title_words
        if len(overlap) >= max(1, len(biz_words) * 0.5):
            return True

    # Check if domain contains business name
    if _domain_contains_name(domain, business_name):
        return True

    return False


def _looks_like_article_url(url: str) -> bool:
    """Heuristic: does this URL look like a blog post or news article?

    Articles typically have date-based paths, long slugs, or blog indicators.
    Business homepages are usually short: example.com, example.com/about, etc.
    """
    try:
        parsed = urlparse(url)
        path = parsed.path.strip("/")

        # No path at all → likely a homepage
        if not path:
            return False

        # Date-based paths: /2025/10/24/... or /2025-01-24-...
        if re.search(r"\d{4}[/-]\d{2}[/-]\d{2}", path):
            return True

        # Common blog/article path indicators
        article_indicators = [
            "/blog/", "/article/", "/news/", "/post/",
            "/story/", "/review/", "/supplier", "/archives/",
            "/magazine/", "/press/", "/media/", "/column/",
        ]
        path_lower = f"/{path.lower()}/"
        if any(ind in path_lower for ind in article_indicators):
            return True

        segments = [s for s in path.split("/") if s]

        # Many segments (>=4) → deeply nested content page
        if len(segments) >= 4:
            return True

        # Long hyphenated slug in ANY segment → article/blog slug pattern
        # Business pages: /about, /services, /contact (short, few hyphens)
        # Article pages: /since-1979-al-afadhils-has-been-serving-lucknowi-delicacies-in-uae/
        for segment in segments:
            hyphen_count = segment.count("-")
            if hyphen_count >= 5:
                return True
            # Also catch very long segments (60+ chars) — article slugs tend to be verbose
            if len(segment) >= 60 and hyphen_count >= 3:
                return True

        # 3 segments with long last slug → likely article path
        # e.g. /category/subcategory/long-article-slug-name
        if len(segments) == 3 and len(segments[-1]) >= 30:
            return True

        return False
    except Exception:
        return False


def _is_root_url(url: str) -> bool:
    """Check if URL is a root/homepage (no path or very short path like /about)."""
    try:
        parsed = urlparse(url)
        path = parsed.path.strip("/")
        if not path:
            return True
        segments = [s for s in path.split("/") if s]
        # Allow 1 short segment like /about, /services, /contact, /en, /home
        if len(segments) == 1 and len(segments[0]) <= 20:
            return True
        return False
    except Exception:
        return False


def _extract_business_website(
    results: list[dict],
    business_name: str,
) -> Optional[str]:
    """Analyze search results to find the business's actual website.

    STRICT matching to minimize false positives (<1% target):

    Pass 1 (STRONG): Domain contains business name → accept.
        Uses tight matching: full name substring or 2+ word match or
        single distinctive (non-generic, 7+ char) word match.
        Catches: sonidentistry.com, mortonmotor.com, thevillagecobbler.ca,
                 torontodentureservices.ca

    Pass 2 (VERY STRICT): Title matches AND URL is root/homepage.
        Only accepts root pages (domain.com or domain.com/about) where
        the title has strong word overlap AND at least 2 words match.
        Rejects: any deep URL, any article, any page with weak overlap.
    """
    # Pass 1: Domain-name match (strongest signal)
    for result in results:
        href = result.get("href") or ""
        if not href:
            continue
        if _is_directory_or_social(href):
            continue
        domain = _get_domain_from_url(href)
        if domain in PUBLIC_EMAIL_DOMAINS_QUICK:
            continue

        if _domain_contains_name(domain, business_name):
            # If URL is a deep/article page, return the root domain instead
            # e.g. packaging-gateway.com/news/article → packaging-gateway.com
            if _is_root_url(href):
                return href
            else:
                parsed = urlparse(href)
                return f"{parsed.scheme}://{parsed.hostname}/"

    # Pass 2: STRICT title match + must be a root/homepage URL
    biz_words = _name_words(business_name)
    if len(biz_words) < 2:
        # Single-word business names are too ambiguous for title matching
        return None

    for result in results:
        href = result.get("href") or ""
        if not href:
            continue
        if _is_directory_or_social(href):
            continue
        domain = _get_domain_from_url(href)
        if domain in PUBLIC_EMAIL_DOMAINS_QUICK:
            continue

        # Must be a root/homepage URL — no deep pages, no articles
        if not _is_root_url(href):
            continue

        # Title must have strong word overlap (2+ matching words, >=60% overlap)
        title = (result.get("title") or "").lower()
        title_words = _name_words(title)
        if not title_words:
            continue
        overlap = biz_words & title_words
        if len(overlap) >= 2 and len(overlap) >= len(biz_words) * 0.6:
            return href

    return None


_SEARCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}


def return_empty_on_error(retry_state):
    return []

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=3, min=3, max=15),
    retry=retry_if_exception_type(http_requests.RequestException),
    retry_error_callback=return_empty_on_error
)
def _search_web(query: str, max_results: int = 10) -> list[dict]:
    """Search via DuckDuckGo HTML lite endpoint.

    Returns list of dicts with 'title', 'href', 'body' keys.
    Uses direct HTML scraping because the duckduckgo_search library (v8.1.1)
    is broken and returns 0 results for all queries.
    """
    resp = http_requests.get(
        "https://html.duckduckgo.com/html/",
        params={"q": query},
        headers=_SEARCH_HEADERS,
        timeout=(5, 10),  # (connect_timeout, read_timeout) — connect must fail fast
    )
    if resp.status_code == 429:
        resp.raise_for_status()

    if resp.status_code != 200:
        logger.warning("DDG returned status %d for '%s'", resp.status_code, query)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    results = []
    for div in soup.find_all("div", class_="result")[:max_results]:
        link = div.find("a", class_="result__a")
        snippet = div.find("a", class_="result__snippet")
        if not link:
            continue
        # Extract actual URL from DDG redirect wrapper
        raw_href = link.get("href", "")
        parsed = urlparse(raw_href)
        qs = parse_qs(parsed.query)
        actual_url = unquote(qs.get("uddg", [raw_href])[0])
        results.append({
            "title": link.get_text(strip=True),
            "href": actual_url,
            "body": snippet.get_text(strip=True) if snippet else "",
        })
    return results


def run_batch(
    limit: Optional[int] = None,
    min_score: float = 30.0,
    scope: Optional[str] = None,
) -> dict:
    """Verify whether potential leads have websites via DuckDuckGo search.

    For each business with lead_score >= min_score and no website_url:
    1. Search DuckDuckGo for "business_name city"
    2. Analyze results — filter out directories/social media
    3. If a real business website is found, set business.website_url
    4. Track result in business.raw["ddg_verified"]

    This is the FREE alternative to Google Places verify_websites().
    No API key needed. No registration.

    Args:
        limit: Max businesses to verify. None = 100 default, 0 = unlimited.
        min_score: Only verify businesses scoring at or above this threshold.
        scope: Job scope tag.

    Returns:
        Dict with processing stats.
    """
    from ..config import load_config

    config = load_config()

    batch_size = 100  # Conservative default for DDG
    if limit is not None and limit > 0:
        batch_size = limit
    elif limit is not None and limit <= 0:
        batch_size = None

    with session_scope() as session:
        run = start_job(session, JOB_NAME, scope=scope)

        try:
            # Find leads that haven't been DDG-verified yet
            stmt = (
                select(Business, City)
                .outerjoin(City, Business.city_id == City.id)
                .where(Business.name.isnot(None))
                .where(Business.name != "")
                .where(or_(Business.website_url.is_(None), Business.website_url == ""))
                .where(Business.lead_score >= min_score)
                .where(
                    or_(
                        Business.raw.is_(None),
                        not_(Business.raw.has_key("ddg_verified")),
                    )
                )
                .order_by(Business.lead_score.desc(), Business.created_at)
            )

            if batch_size is not None:
                stmt = stmt.limit(batch_size)

            rows = session.execute(stmt).all()

            if not rows:
                complete_job(session, run, processed_count=0, details={
                    "min_score": min_score,
                    "websites_found": 0,
                    "no_website_confirmed": 0,
                    "inconclusive": 0,
                    "errors": 0,
                })
                return {
                    "processed": 0,
                    "websites_found": 0,
                    "no_website_confirmed": 0,
                    "inconclusive": 0,
                    "errors": 0,
                }

            processed = 0
            websites_found = 0
            no_website_confirmed = 0
            inconclusive = 0
            errors = 0

            for business, city in rows:
                city_name = city.name if city else None
                biz_name = (business.name or "").strip()
                if not biz_name:
                    processed += 1
                    continue

                # Build multiple search queries (broad → specific)
                search_queries = _build_search_queries(biz_name, city_name)

                # Try each query until we get results
                results = []
                query = search_queries[0]  # Track which query was used
                for q in search_queries:
                    results = _search_web(q, max_results=10)
                    query = q
                    if results:
                        break
                    time.sleep(1.0)  # Brief delay between query attempts

                raw = dict(business.raw) if business.raw else {}

                if not results:
                    # No results — inconclusive (NOT a confirmation of no website)
                    raw["ddg_verified"] = True
                    raw["ddg_verify_result"] = "no_results"
                    raw["ddg_search_query"] = query
                    business.raw = raw
                    business.scored_at = None
                    inconclusive += 1
                    processed += 1
                    time.sleep(1.5)
                    continue

                # Analyze results
                website = _extract_business_website(results, biz_name)

                if website:
                    # Found a real website — disqualify this lead
                    business.website_url = website
                    raw["ddg_verified"] = True
                    raw["ddg_verify_result"] = "has_website"
                    raw["ddg_website"] = website
                    raw["ddg_search_query"] = query
                    raw["ddg_result_count"] = len(results)
                    business.raw = raw
                    websites_found += 1
                    logger.debug(
                        "DDG found website for '%s': %s", biz_name, website,
                    )
                else:
                    # No business website in results — genuine lead candidate
                    raw["ddg_verified"] = True
                    raw["ddg_verify_result"] = "no_website"
                    raw["ddg_search_query"] = query
                    raw["ddg_result_count"] = len(results)
                    business.raw = raw
                    no_website_confirmed += 1

                business.scored_at = None
                processed += 1

                if processed % 50 == 0:
                    session.flush()
                    logger.info(
                        "DDG verification progress: %d/%d processed, "
                        "%d have websites, %d confirmed no website",
                        processed, len(rows), websites_found, no_website_confirmed,
                    )

                # DuckDuckGo rate limit — be conservative
                time.sleep(1.5)

            details = {
                "min_score": min_score,
                "websites_found": websites_found,
                "no_website_confirmed": no_website_confirmed,
                "inconclusive": inconclusive,
                "errors": errors,
            }
            complete_job(session, run, processed_count=processed, details=details)

            return {
                "processed": processed,
                "websites_found": websites_found,
                "no_website_confirmed": no_website_confirmed,
                "inconclusive": inconclusive,
                "errors": errors,
            }

        except Exception as exc:
            fail_job(session, run, error=str(exc))
            raise
