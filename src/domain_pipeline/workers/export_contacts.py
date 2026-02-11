from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import select

from ..config import load_config
from ..db import session_scope
from ..jobs import complete_job, fail_job, start_job
from ..models import Contact, Domain, Organization, OutreachExport


def export_csv(platform: str, min_score: Optional[float] = None) -> Optional[Path]:
    config = load_config()
    export_dir = Path(config.export_dir)
    export_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = export_dir / f"contacts_{platform}_{timestamp}.csv"

    with session_scope() as session:
        run = start_job(session, "export_contacts", scope=platform, details={"min_score": min_score})
        try:
            stmt = (
                select(Contact, Organization, Domain)
                .join(Organization, Contact.org_id == Organization.id)
                .join(Domain, Organization.domain_id == Domain.id)
                .where(Domain.status == "enriched")
            )
            if min_score is not None:
                stmt = stmt.where(Contact.lead_score.isnot(None)).where(Contact.lead_score >= min_score)
            rows = session.execute(stmt).all()

            if not rows:
                complete_job(session, run, processed_count=0)
                return None

            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow([
                    "domain",
                    "organization",
                    "email",
                    "first_name",
                    "last_name",
                    "title",
                    "source",
                    "lead_score",
                ])
                written_rows = 0

                for contact, org, domain in rows:
                    existing_export = session.execute(
                        select(OutreachExport.id)
                        .where(OutreachExport.contact_id == contact.id)
                        .where(OutreachExport.platform == platform)
                    ).first()

                    if existing_export:
                        continue

                    writer.writerow([
                        domain.domain,
                        org.name or "",
                        contact.email or "",
                        contact.first_name or "",
                        contact.last_name or "",
                        contact.title or "",
                        contact.source or "",
                        float(contact.lead_score) if contact.lead_score is not None else "",
                    ])

                    session.add(
                        OutreachExport(
                            contact_id=contact.id,
                            platform=platform,
                            status="queued",
                        )
                    )
                    written_rows += 1

                session.flush()

            if written_rows == 0:
                path.unlink(missing_ok=True)
                complete_job(session, run, processed_count=0)
                return None

            complete_job(session, run, processed_count=written_rows, details={"min_score": min_score})
        except Exception as exc:
            fail_job(session, run, error=str(exc), details={"min_score": min_score})
            raise

    return path
