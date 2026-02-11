from __future__ import annotations

from pathlib import Path
from typing import Optional

from .workers.business_leads import export_business_leads, score_businesses
from .workers.business_domain_sync import run_batch as sync_business_domains
from .workers.email_crawler import run_batch as run_role_email_enrichment
from .workers.export_contacts import export_csv
from .workers.lead_scoring import run_batch as run_lead_scoring
from .workers.osm_import import import_osm, load_areas, load_categories
from .workers.rdap_check import run_batch as run_rdap_checks


def maybe_import_businesses(
    area_key: Optional[str],
    categories_arg: str,
    areas_file: str,
    categories_file: str,
) -> int:
    if not area_key:
        return 0

    areas = load_areas(Path(areas_file))
    if area_key not in areas:
        raise ValueError(f"Unknown area: {area_key}")

    categories = load_categories(Path(categories_file))
    if categories_arg == "all":
        selected = list(categories.values())
    else:
        keys = [key.strip() for key in categories_arg.split(",") if key.strip()]
        missing = [key for key in keys if key not in categories]
        if missing:
            raise ValueError(f"Unknown categories: {', '.join(missing)}")
        selected = [categories[key] for key in keys]

    return import_osm(areas[area_key], selected)


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
    rdap_processed = run_rdap_checks(limit=rdap_limit, scope=area, statuses=rdap_statuses)
    email_processed = run_role_email_enrichment(limit=email_limit, scope=area)
    scored = run_lead_scoring(limit=score_limit, force_rescore=False)
    export_path = export_csv(platform, min_score=min_score)
    business_scored = score_businesses(limit=business_score_limit, scope=area, force_rescore=False)
    business_export_path = export_business_leads(
        platform=business_platform,
        min_score=business_min_score,
        require_unhosted_domain=business_require_unhosted_domain,
        require_contact=business_require_contact,
        require_domain_qualification=business_require_domain_qualification,
    )

    return {
        "imported": imported,
        "synced": synced,
        "rdap_processed": rdap_processed,
        "email_processed": email_processed,
        "scored": scored,
        "export_path": str(export_path) if export_path else None,
        "business_scored": business_scored,
        "business_export_path": str(business_export_path) if business_export_path else None,
    }
