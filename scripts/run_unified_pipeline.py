from __future__ import annotations

import argparse

from domain_pipeline.pipeline import run_once


def main() -> None:
    parser = argparse.ArgumentParser(description="Run unified business-to-outreach pipeline")
    parser.add_argument("--area", default=None, help="Optional area key to import from OSM before syncing domains")
    parser.add_argument("--categories", default="all", help="Category keys for OSM import")
    parser.add_argument("--areas-file", default="config/areas.json")
    parser.add_argument("--categories-file", default="config/categories.json")
    parser.add_argument("--sync-limit", type=int, default=None)
    parser.add_argument("--rdap-limit", type=int, default=None)
    parser.add_argument(
        "--rdap-statuses",
        default="new,skipped,rdap_error,dns_error",
        help="Comma-separated domain statuses for RDAP check",
    )
    parser.add_argument("--email-limit", type=int, default=None)
    parser.add_argument("--score-limit", type=int, default=None)
    parser.add_argument("--min-score", type=float, default=None)
    parser.add_argument("--platform", default="csv")
    parser.add_argument("--business-score-limit", type=int, default=None)
    parser.add_argument("--business-platform", default="csv_business")
    parser.add_argument("--business-min-score", type=float, default=None)
    parser.add_argument("--business-require-unhosted-domain", action="store_true")
    parser.add_argument("--business-allow-no-contact", action="store_true")
    parser.add_argument("--business-allow-unqualified-domain", action="store_true")
    args = parser.parse_args()

    result = run_once(
        area=args.area,
        categories=args.categories,
        areas_file=args.areas_file,
        categories_file=args.categories_file,
        sync_limit=args.sync_limit,
        rdap_limit=args.rdap_limit,
        rdap_statuses=[item.strip() for item in args.rdap_statuses.split(",") if item.strip()],
        email_limit=args.email_limit,
        score_limit=args.score_limit,
        platform=args.platform,
        min_score=args.min_score,
        business_score_limit=args.business_score_limit,
        business_platform=args.business_platform,
        business_min_score=args.business_min_score,
        business_require_unhosted_domain=args.business_require_unhosted_domain,
        business_require_contact=not args.business_allow_no_contact,
        business_require_domain_qualification=not args.business_allow_unqualified_domain,
    )

    print(f"Imported businesses: {result['imported']}")
    print(
        f"Synced businesses: {result['synced']['processed']}, "
        f"new domains: {result['synced']['domains_inserted']}, "
        f"new links: {result['synced']['links_inserted']}"
    )
    print(f"RDAP checks processed: {result['rdap_processed']}")
    print(f"Role-email enrichment processed: {result['email_processed']}")
    print(f"Leads scored: {result['scored']}")
    print(f"Export: {result['export_path'] if result['export_path'] else 'no rows exported'}")
    print(f"Business leads scored: {result['business_scored']}")
    print(
        "Business export: "
        f"{result['business_export_path'] if result['business_export_path'] else 'no rows exported'}"
    )


if __name__ == "__main__":
    main()
