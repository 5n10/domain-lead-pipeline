from __future__ import annotations

from pathlib import Path
from typing import Optional

from .workers.business_leads import export_business_leads, score_businesses
from .workers.business_domain_sync import run_batch as sync_business_domains
from .workers.email_crawler import run_batch as run_role_email_enrichment
from .workers.export_contacts import export_csv
from .workers.lead_scoring import run_batch as run_lead_scoring
from .workers.osm_import import import_osm, load_areas, load_categories, resolve_free_text_area
from .workers.rdap_check import run_batch as run_rdap_checks
from .workers.google_places import run_batch as run_google_places_enrich, verify_websites
from .workers.web_search_verify import run_batch as run_ddg_verify
from .workers.llm_verify import run_batch as run_llm_verify
from .workers.foursquare import run_batch as run_foursquare_enrich, verify_websites as verify_websites_foursquare
from .workers.domain_guess import run_batch as run_domain_guess
from .workers.google_search_verify import run_batch as run_google_search_verify
from .notifications import notify_pipeline_complete, notify_error


def maybe_import_businesses(
    area_key: Optional[str],
    categories_arg: str,
    areas_file: str,
    categories_file: str,
) -> int:
    if not area_key:
        return 0

    areas = load_areas(Path(areas_file))

    if area_key in areas:
        selected_area = areas[area_key]
    else:
        # Free-text fallback: geocode via Nominatim
        selected_area = resolve_free_text_area(area_key)

    categories = load_categories(Path(categories_file))
    if categories_arg == "all":
        selected = list(categories.values())
    else:
        keys = [key.strip() for key in categories_arg.split(",") if key.strip()]
        missing = [key for key in keys if key not in categories]
        if missing:
            raise ValueError(f"Unknown categories: {', '.join(missing)}")
        selected = [categories[key] for key in keys]

    return import_osm(selected_area, selected)


def run_once(
    area: Optional[str] = None,
    categories: str = "all",
    areas_file: str = "config/areas.json",
    categories_file: str = "config/categories.json",
    sync_limit: Optional[int] = None,
    rdap_limit: Optional[int] = None,
    rdap_statuses: Optional[list[str]] = None,
    email_limit: Optional[int] = None,
    score_limit: Optional[int] = None,
    platform: str = "csv",
    min_score: Optional[float] = None,
    business_score_limit: Optional[int] = None,
    business_platform: str = "csv_business",
    business_min_score: Optional[float] = None,
    business_require_unhosted_domain: bool = False,
    business_require_contact: bool = True,
    business_require_domain_qualification: bool = True,
) -> dict:
    imported = maybe_import_businesses(area, categories, areas_file, categories_file)
    synced = sync_business_domains(limit=sync_limit, scope=area, reset_cursor=False)
    # Disable auto_rescore in RDAP since run_once already calls score_businesses below
    rdap_processed = run_rdap_checks(limit=rdap_limit, scope=area, statuses=rdap_statuses, auto_rescore=False)
    email_processed = run_role_email_enrichment(limit=email_limit, scope=area)
    # Google Places enrichment — adds phone numbers for businesses without contacts.
    # Only runs if GOOGLE_PLACES_API_KEY is set; silently skips otherwise.
    places_result = run_google_places_enrich(limit=200, priority="no_contacts")
    # Foursquare enrichment — alternative/supplementary to Google Places.
    # Only runs if FOURSQUARE_API_KEY is set; silently skips otherwise.
    foursquare_result = run_foursquare_enrich(limit=200, priority="no_contacts")
    scored = run_lead_scoring(limit=score_limit, force_rescore=False)
    export_path = export_csv(platform, min_score=min_score)
    business_scored = score_businesses(limit=business_score_limit, scope=area, force_rescore=False)
    # === VERIFICATION PIPELINE (5 layers, fast → slow, free → paid) ===
    #
    # Layer 1: Domain Guess — FREE, ~500/min, no API key
    # Generates candidate domains from business names and checks HTTP HEAD.
    # Catches obvious cases (e.g. "The Village Cobbler" → thevillagecobbler.ca).
    domain_guess_result = run_domain_guess(limit=200, min_score=0.0)
    #
    # Layer 2: DDG Search — FREE, ~40/min, no API key (FIXED: now uses HTML scraper)
    # Searches DuckDuckGo for each lead to verify web presence.
    ddg_result = run_ddg_verify(limit=50, min_score=30.0)
    #
    # Layer 3: Google Search — FREE, ~15/min, no API key (NEW verification stage)
    # Additional search engine layer for cross-verification with DDG.
    google_search_result = run_google_search_verify(limit=30, min_score=30.0)
    #
    # Layer 4: LLM Verification — requires API key
    llm_verify_result = run_llm_verify(limit=50, min_score=30.0)
    #
    # Layer 5: Google Places API — requires GOOGLE_PLACES_API_KEY
    # Silently returns 0 if API key not set.
    verify_result = verify_websites(limit=200, min_score=30.0)
    #
    # Layer 6: Foursquare API — requires FOURSQUARE_API_KEY
    # Supplementary verification, silently returns 0 if key not set.
    fsq_verify_result = verify_websites_foursquare(limit=200, min_score=30.0)
    #
    # Rescore after verification — confidence caps now apply
    websites_discovered = (
        domain_guess_result.get("websites_found", 0)
        + ddg_result.get("websites_found", 0)
        + google_search_result.get("websites_found", 0)
        + llm_verify_result.get("websites_found", 0)
        + verify_result.get("websites_found", 0)
        + fsq_verify_result.get("websites_found", 0)
    )
    any_verified = (
        domain_guess_result.get("processed", 0)
        + ddg_result.get("processed", 0)
        + google_search_result.get("processed", 0)
        + llm_verify_result.get("processed", 0)
        + verify_result.get("processed", 0)
        + fsq_verify_result.get("processed", 0)
    )
    # Rescore whenever verification ran — both to disqualify businesses with found websites
    # AND to update confidence caps for businesses confirmed no-website (unverified→medium/high)
    if any_verified > 0:
        business_scored += score_businesses(limit=None, force_rescore=websites_discovered > 0)
    business_export_path = export_business_leads(
        platform=business_platform,
        min_score=business_min_score,
        require_unhosted_domain=business_require_unhosted_domain,
        require_contact=business_require_contact,
        require_domain_qualification=business_require_domain_qualification,
    )

    result = {
        "imported": imported,
        "synced": synced,
        "rdap_processed": rdap_processed,
        "email_processed": email_processed,
        "places_enriched": places_result.get("enriched", 0),
        "places_phones_added": places_result.get("phones_added", 0),
        "foursquare_enriched": foursquare_result.get("enriched", 0),
        "foursquare_phones_added": foursquare_result.get("phones_added", 0),
        "domain_guess_processed": domain_guess_result.get("processed", 0),
        "domain_guess_websites_found": domain_guess_result.get("websites_found", 0),
        "websites_verified": verify_result.get("processed", 0),
        "websites_found": verify_result.get("websites_found", 0),
        "no_website_confirmed": verify_result.get("no_website_confirmed", 0),
        "ddg_verified": ddg_result.get("processed", 0),
        "ddg_websites_found": ddg_result.get("websites_found", 0),
        "google_search_verified": google_search_result.get("processed", 0),
        "google_search_websites_found": google_search_result.get("websites_found", 0),
        "llm_verified": llm_verify_result.get("processed", 0),
        "llm_websites_found": llm_verify_result.get("websites_found", 0),
        "scored": scored,
        "export_path": str(export_path) if export_path else None,
        "business_scored": business_scored,
        "business_export_path": str(business_export_path) if business_export_path else None,
    }

    try:
        notify_pipeline_complete(result)
    except Exception:
        pass  # Notifications are best-effort

    return result
