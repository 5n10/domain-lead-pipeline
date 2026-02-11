from __future__ import annotations

import argparse

from domain_pipeline.workers.rdap_check import run_batch


def main() -> None:
    parser = argparse.ArgumentParser(description="Run RDAP/DNS/HTTP checks")
    parser.add_argument("--limit", type=int, default=None, help="Max domains to process")
    parser.add_argument("--scope", default=None, help="Optional scope key for job tracking")
    parser.add_argument(
        "--statuses",
        default="new",
        help="Comma-separated domain statuses to process (default: new). Example: new,skipped,rdap_error",
    )
    args = parser.parse_args()

    statuses = [item.strip() for item in args.statuses.split(",") if item.strip()]
    count = run_batch(limit=args.limit, scope=args.scope, statuses=statuses)
    print(f"Processed {count} domains")


if __name__ == "__main__":
    main()
