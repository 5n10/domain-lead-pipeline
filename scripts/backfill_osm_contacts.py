from __future__ import annotations

import argparse

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from domain_pipeline.db import session_scope
from domain_pipeline.models import Business, BusinessContact
from domain_pipeline.workers.osm_contacts import extract_osm_contacts


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill phone/email BusinessContact rows from stored OSM tags")
    parser.add_argument("--limit", type=int, default=None, help="Max businesses to inspect (default: all)")
    parser.add_argument("--batch-size", type=int, default=500, help="Businesses per batch (default: 500)")
    parser.add_argument("--dry-run", action="store_true", help="Compute counts but do not write to the DB")
    args = parser.parse_args()

    limit = None if args.limit is None else max(args.limit, 0)
    batch_size = max(args.batch_size, 1)

    inspected = 0
    proposed = 0
    inserted = 0

    with session_scope() as session:
        offset = 0
        while True:
            remaining = None if limit is None else max(limit - inspected, 0)
            if remaining is not None and remaining <= 0:
                break

            this_batch = batch_size if remaining is None else min(batch_size, remaining)

            rows = (
                session.execute(
                    select(Business.id, Business.raw)
                    .where(Business.source == "osm")
                    .order_by(Business.created_at, Business.id)
                    .offset(offset)
                    .limit(this_batch)
                )
                .all()
            )
            if not rows:
                break

            to_insert: list[dict] = []
            for business_id, raw_tags in rows:
                tags = raw_tags or {}
                for contact_type, value in extract_osm_contacts(tags):
                    proposed += 1
                    to_insert.append(
                        {
                            "business_id": business_id,
                            "contact_type": contact_type,
                            "value": value,
                            "source": "osm",
                        }
                    )

            if to_insert and not args.dry_run:
                result = session.execute(
                    insert(BusinessContact)
                    .values(to_insert)
                    .on_conflict_do_nothing(constraint="business_contacts_business_type_value_uidx")
                )
                inserted += int(result.rowcount or 0)

            inspected += len(rows)
            offset += len(rows)

    print(
        f"inspected={inspected} proposed={proposed} inserted={inserted} dry_run={bool(args.dry_run)} batch_size={batch_size}"
    )


if __name__ == "__main__":
    main()

