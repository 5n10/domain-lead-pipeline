"""Check verification result values."""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import select, func
from domain_pipeline.db import session_scope
from domain_pipeline.models import Business

with session_scope() as session:
    print("=== Domain Guess Results ===")
    stmt = select(Business.raw["domain_guess_result"].astext, func.count()).where(Business.raw.has_key("domain_guess_verified")).group_by(Business.raw["domain_guess_result"].astext)
    results = session.execute(stmt).all()
    for val, count in results:
        print(f"  {val}: {count}")

    print("\n=== DDG Results ===")
    stmt_ddg = select(Business.raw["ddg_verify_result"].astext, func.count()).where(Business.raw.has_key("ddg_verified")).group_by(Business.raw["ddg_verify_result"].astext)
    results_ddg = session.execute(stmt_ddg).all()
    for val, count in results_ddg:
        print(f"  {val}: {count}")
