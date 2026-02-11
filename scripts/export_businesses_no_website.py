from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path

from sqlalchemy import or_, select

from domain_pipeline.config import load_config
from domain_pipeline.db import session_scope
from domain_pipeline.models import Business, BusinessContact, City


def export_no_website() -> Path | None:
    config = load_config()
    export_dir = Path(config.export_dir)
    export_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = export_dir / f"businesses_no_website_{timestamp}.csv"

    with session_scope() as session:
        stmt = (
            select(Business, City)
            .outerjoin(City, Business.city_id == City.id)
            .where(or_(Business.website_url.is_(None), Business.website_url == ""))
        )
        rows = session.execute(stmt).all()
        if not rows:
            return None

        business_ids = [business.id for business, _ in rows]
        contact_rows = (
            session.execute(
                select(BusinessContact.business_id, BusinessContact.contact_type, BusinessContact.value)
                .where(BusinessContact.business_id.in_(business_ids))
            )
            .all()
        )
        contacts_by_business: dict = {}
        for business_id, contact_type, value in contact_rows:
            if not value:
                continue
            entry = contacts_by_business.setdefault(business_id, {"phone": [], "email": []})
            if contact_type in entry:
                entry[contact_type].append(value)

        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow([
                "name",
                "category",
                "address",
                "city",
                "country",
                "phone",
                "email",
                "lat",
                "lon",
                "source",
                "source_id",
            ])

            for business, city in rows:
                contact_entry = contacts_by_business.get(business.id, {})
                phones = contact_entry.get("phone", [])
                emails = contact_entry.get("email", [])

                writer.writerow([
                    business.name or "",
                    business.category or "",
                    business.address or "",
                    city.name if city else "",
                    city.country if city else "",
                    ";".join(phones),
                    ";".join(emails),
                    business.lat or "",
                    business.lon or "",
                    business.source,
                    business.source_id,
                ])

    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Export businesses without websites")
    parser.parse_args()

    path = export_no_website()
    if path is None:
        print("No businesses without websites")
    else:
        print(f"Exported CSV to {path}")


if __name__ == "__main__":
    main()
