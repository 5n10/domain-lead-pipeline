from __future__ import annotations

import argparse

from domain_pipeline.workers.business_leads import score_businesses


def main() -> None:
    parser = argparse.ArgumentParser(description="Score business leads from local business data")
    parser.add_argument("--limit", type=int, default=None, help="Max businesses to score")
    parser.add_argument("--scope", default=None, help="Optional scope key for job tracking")
    parser.add_argument("--force-rescore", action="store_true", help="Rescore businesses even if already scored")
    args = parser.parse_args()

    count = score_businesses(limit=args.limit, scope=args.scope, force_rescore=args.force_rescore)
    print(f"Scored {count} businesses")


if __name__ == "__main__":
    main()
