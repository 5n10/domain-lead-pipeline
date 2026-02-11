from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from domain_pipeline.metrics import collect_metrics
from domain_pipeline.pipeline import run_once


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run unified pipeline on an interval")
    parser.add_argument("--interval-seconds", type=int, default=900, help="Seconds between runs")
    parser.add_argument("--max-runs", type=int, default=None, help="Stop after N runs")
    parser.add_argument("--once", action="store_true", help="Run once and exit")

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
    parser.add_argument("--metrics-out", default=None, help="Optional path to persist latest metrics JSON")
    args = parser.parse_args()

    run_count = 0
    interval_seconds = max(args.interval_seconds, 1)

    while True:
        started = _utc_now()
        print(f"[{started}] Pipeline run started")

        try:
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
            metrics = collect_metrics()
            finished = _utc_now()

            print(
                f"[{finished}] Pipeline run finished: "
                f"imported={result['imported']} "
                f"synced={result['synced']['processed']} "
                f"rdap={result['rdap_processed']} "
                f"emails={result['email_processed']} "
                f"scored={result['scored']} "
                f"export={result['export_path'] or 'none'} "
                f"business_scored={result['business_scored']} "
                f"business_export={result['business_export_path'] or 'none'}"
            )

            if args.metrics_out:
                out_path = Path(args.metrics_out)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
                print(f"[{finished}] Metrics written: {out_path}")

        except Exception as exc:
            print(f"[{_utc_now()}] Pipeline run failed: {exc}")

        run_count += 1
        if args.once:
            break
        if args.max_runs is not None and run_count >= args.max_runs:
            break

        print(f"[{_utc_now()}] Sleeping {interval_seconds}s")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
