from __future__ import annotations

import argparse
from domain_pipeline.workers.export_contacts import export_csv


def main() -> None:
    parser = argparse.ArgumentParser(description="Export contacts to CSV")
    parser.add_argument("--platform", default="csv", help="Export label for tracking")
    parser.add_argument("--min-score", type=float, default=None, help="Optional minimum lead score")
    args = parser.parse_args()

    path = export_csv(args.platform, min_score=args.min_score)
    if path is None:
        print("No contacts to export")
    else:
        print(f"Exported CSV to {path}")


if __name__ == "__main__":
    main()
