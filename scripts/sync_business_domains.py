from __future__ import annotations

import argparse

from domain_pipeline.workers.business_domain_sync import run_batch


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync business websites/emails into domains and links")
    parser.add_argument("--limit", type=int, default=None, help="Max businesses to process in this run")
    parser.add_argument("--scope", default=None, help="Optional scope key for checkpoint partitioning")
    parser.add_argument("--reset-cursor", action="store_true", help="Reset cursor for this scope before processing")
    args = parser.parse_args()

    result = run_batch(limit=args.limit, scope=args.scope, reset_cursor=args.reset_cursor)
    print(
        "Processed {processed} businesses, inserted {domains_inserted} domains, "
        "inserted {links_inserted} links".format(**result)
    )


if __name__ == "__main__":
    main()
