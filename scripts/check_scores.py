"""Check square distribution."""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import select, func
from domain_pipeline.db import session_scope
from domain_pipeline.models import Business

with session_scope() as session:
    stmt = select(func.round(Business.lead_score / 10) * 10, func.count()).group_by(func.round(Business.lead_score / 10) * 10).order_by(func.round(Business.lead_score / 10) * 10)
    results = session.execute(stmt).all()
    print("Score Distribution:")
    for score, count in results:
        print(f"  {score}: {count}")
