from __future__ import annotations

import argparse

from domain_pipeline.workers.lead_scoring import run_batch


def main() -> None:
    parser = argparse.ArgumentParser(description="Score contact leads")
    parser.add_argument("--limit", type=int, default=None, help="Max contacts to score")
    parser.add_argument("--force-rescore", action="store_true", help="Rescore contacts even if already scored")
    args = parser.parse_args()

    count = run_batch(limit=args.limit, force_rescore=args.force_rescore)
    print(f"Scored {count} contacts")


if __name__ == "__main__":
    main()
