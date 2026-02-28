"""Check current verification counts."""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import select, func
from domain_pipeline.db import session_scope
from domain_pipeline.models import Business

with session_scope() as session:
    def get_count(vkey):
        return session.execute(
            select(func.count(Business.id)).where(Business.raw.has_key(vkey))
        ).scalar()

    print(f"Counts at {sys.argv[1] if len(sys.argv) > 1 else 'now'}:")
    for vkey in ["domain_guess_verified", "ddg_verified", "google_search_verified", "llm_verified", "google_places_verified"]:
        print(f"  {vkey}: {get_count(vkey)}")
