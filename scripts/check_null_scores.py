"""Check null score leads."""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import select, func, or_
from domain_pipeline.db import session_scope
from domain_pipeline.models import Business

with session_scope() as session:
    stmt = select(func.count(Business.id)).where(
        Business.lead_score.is_(None), 
        or_(Business.website_url.is_(None), Business.website_url == "")
    )
    count = session.execute(stmt).scalar()
    print(f"Leads with NULL score and NO website: {count}")
