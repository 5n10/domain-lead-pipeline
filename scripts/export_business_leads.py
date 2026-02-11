from __future__ import annotations

import argparse

from domain_pipeline.workers.business_leads import export_business_leads


def main() -> None:
    parser = argparse.ArgumentParser(description="Export scored business leads to CSV")
    parser.add_argument("--platform", default="csv_business", help="Export label for tracking")
    parser.add_argument("--min-score", type=float, default=None, help="Optional minimum lead score")
    parser.add_argument("--limit", type=int, default=None, help="Optional max rows to evaluate")
    parser.add_argument(
        "--require-unhosted-domain",
        action="store_true",
        help="Only include businesses with at least one linked unhosted/non-parked domain signal",
    )
    parser.add_argument(
        "--allow-no-contact",
        action="store_true",
        help="Allow export of businesses even when no phone/email is present",
    )
    parser.add_argument(
        "--allow-unqualified-domain",
        action="store_true",
        help="Allow export without verified-unhosted/unregistered domain qualification",
    )
    args = parser.parse_args()

    path = export_business_leads(
        platform=args.platform,
        min_score=args.min_score,
        limit=args.limit,
        require_contact=not args.allow_no_contact,
        require_unhosted_domain=args.require_unhosted_domain,
        require_domain_qualification=not args.allow_unqualified_domain,
    )
    if path is None:
        print("No business leads to export")
    else:
        print(f"Exported CSV to {path}")


if __name__ == "__main__":
    main()
