"""Google Sheets export worker.

Exports leads directly to a shared Google Sheet. Completely FREE, unlimited.
Requires a Google Cloud service account (one-time setup).

Setup:
1. Create a GCP project: https://console.cloud.google.com
2. Enable Google Sheets API
3. Create a Service Account and download JSON key
4. Share your spreadsheet with the service account email
5. Set GOOGLE_SHEETS_CREDENTIALS_FILE and GOOGLE_SHEETS_SPREADSHEET_ID in .env
"""
from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import exists, func, not_, or_, select
from sqlalchemy.orm import Session

from ..config import load_config
from ..db import session_scope
from ..jobs import complete_job, fail_job, start_job
from ..models import (
    Business,
    BusinessContact,
    BusinessOutreachExport,
    City,
)
from .business_leads import (
    business_eligibility_filters,
    load_business_features,
)

logger = logging.getLogger(__name__)

JOB_NAME = "google_sheets_export"


def _build_sheets_client(credentials_file: str):
    """Build Google Sheets API client using service account credentials."""
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
    except ImportError:
        logger.error(
            "Google API libraries not installed. Run: "
            "pip install google-auth google-api-python-client"
        )
        return None

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
    return build("sheets", "v4", credentials=creds)


def export_to_sheets(
    min_score: Optional[float] = None,
    limit: Optional[int] = None,
    require_contact: bool = True,
    require_unhosted_domain: bool = False,
    require_domain_qualification: bool = True,
    exclude_hosted_email_domain: bool = True,
    sheet_name: str = "Leads",
) -> dict:
    """Export leads to Google Sheets.

    Clears existing sheet data and writes fresh lead data.
    Uses the same query logic as export_business_leads().

    Returns:
        Dict with rows_written, spreadsheet_id, or error.
    """
    config = load_config()

    if not config.google_sheets_credentials_file:
        return {"error": "GOOGLE_SHEETS_CREDENTIALS_FILE not configured", "rows_written": 0}
    if not config.google_sheets_spreadsheet_id:
        return {"error": "GOOGLE_SHEETS_SPREADSHEET_ID not configured", "rows_written": 0}

    service = _build_sheets_client(config.google_sheets_credentials_file)
    if not service:
        return {"error": "Failed to build Google Sheets client (missing dependencies?)", "rows_written": 0}

    spreadsheet_id = config.google_sheets_spreadsheet_id

    with session_scope() as session:
        run = start_job(session, JOB_NAME)

        try:
            # Build query — same filters as CSV export
            filters = [
                Business.lead_score.isnot(None),
                or_(Business.website_url.is_(None), Business.website_url == ""),
            ]
            if min_score is not None:
                filters.append(Business.lead_score >= min_score)
            filters.extend(
                business_eligibility_filters(
                    require_contact=require_contact,
                    require_unhosted_domain=require_unhosted_domain,
                    require_domain_qualification=require_domain_qualification,
                    exclude_hosted_email_domain=exclude_hosted_email_domain,
                )
            )

            stmt = (
                select(Business, City)
                .outerjoin(City, Business.city_id == City.id)
                .order_by(Business.lead_score.desc(), Business.created_at)
            )
            for f in filters:
                stmt = stmt.where(f)
            if limit:
                stmt = stmt.limit(limit)

            rows = session.execute(stmt).all()
            business_ids = [b.id for b, _ in rows]
            features = load_business_features(session, business_ids)

            # Build header and data rows
            header = [
                "Name", "Category", "Address", "City", "Country",
                "Lead Score", "Emails", "Business Emails", "Phones",
                "Domains", "Hosted Domains", "Registered Domains",
            ]

            data_rows = [header]
            for business, city_row in rows:
                feat = features[business.id]
                data_rows.append([
                    business.name or "",
                    business.category or "",
                    business.address or "",
                    city_row.name if city_row else "",
                    city_row.country if city_row else "",
                    float(business.lead_score) if business.lead_score is not None else 0,
                    ", ".join(sorted(feat["emails"])),
                    ", ".join(sorted(feat["business_emails"])),
                    ", ".join(sorted(feat["phones"])),
                    ", ".join(sorted(feat["domains"])),
                    ", ".join(sorted(feat["hosted_domains"])),
                    ", ".join(sorted(feat["registered_domains"])),
                ])

            # Clear and write to sheet
            sheets = service.spreadsheets()

            # Clear existing data
            sheets.values().clear(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A:Z",
            ).execute()

            # Write new data
            result = sheets.values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="RAW",
                body={"values": data_rows},
            ).execute()

            rows_written = len(data_rows) - 1  # Subtract header
            sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

            complete_job(session, run, processed_count=rows_written, details={
                "spreadsheet_id": spreadsheet_id,
                "sheet_url": sheet_url,
            })

            return {
                "rows_written": rows_written,
                "spreadsheet_id": spreadsheet_id,
                "sheet_url": sheet_url,
            }

        except Exception as exc:
            fail_job(session, run, error=str(exc))
            raise
