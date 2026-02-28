"""Check current eligibility and processing status."""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import select, func, or_
from domain_pipeline.db import session_scope
from domain_pipeline.models import Business

with session_scope() as session:
    # Leads with score >= 30 and no website
    stmt = select(func.count(Business.id)).where(
        Business.lead_score >= 30.0, 
        or_(Business.website_url.is_(None), Business.website_url == "")
    )
    total_eligible = session.execute(stmt).scalar()
    
    # Processed by Domain Guess
    stmt_dg = stmt.where(Business.raw.has_key("domain_guess_verified"))
    processed_dg = session.execute(stmt_dg).scalar()
    
    # Processed by DDG
    stmt_ddg = stmt.where(Business.raw.has_key("ddg_verified"))
    processed_ddg = session.execute(stmt_ddg).scalar()

    print(f"Total Eligible Leads (Score>=30, No Website): {total_eligible}")
    print(f"Processed by Domain Guess: {processed_dg}")
    print(f"Processed by DDG: {processed_ddg}")
    print(f"Remaining for DDG: {total_eligible - processed_ddg}")
