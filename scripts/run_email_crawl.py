from __future__ import annotations

import argparse

from domain_pipeline.workers.email_crawler import run_batch


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate role-based emails for domains with MX")
    parser.add_argument("--limit", type=int, default=None, help="Max domains to process")
    parser.add_argument("--scope", default=None, help="Optional scope key for job tracking")
    args = parser.parse_args()

    count = run_batch(limit=args.limit, scope=args.scope)
    print(f"Processed {count} domains")


if __name__ == "__main__":
    main()
