"""Check for any DDG or other source verification hits."""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import select, or_, func
from domain_pipeline.db import session_scope
from domain_pipeline.models import Business
from domain_pipeline.workers.business_leads import _RESULT_KEY_MAP

with session_scope() as session:
    print("=== Verification counts in DB ===")
    for vkey in ["ddg_verified", "llm_verified", "google_search_verified", "google_places_verified", "foursquare_verified"]:
        count = session.execute(
            select(func.count(Business.id)).where(Business.raw.has_key(vkey))
        ).scalar()
        print(f"  {vkey}: {count}")

    # For DDG, check the result values
    rkey = _RESULT_KEY_MAP.get("ddg_verified")
    if rkey:
        print(f"\n=== DDG Result Values ===")
        results = session.execute(
            select(Business.raw[rkey].astext, func.count()).where(Business.raw.has_key("ddg_verified")).group_by(Business.raw[rkey].astext)
        ).all()
        for val, count in results:
            print(f"  {val}: {count}")

    # For Google Search, check the result values
    rkey_gs = _RESULT_KEY_MAP.get("google_search_verified")
    if rkey_gs:
        print(f"\n=== Google Search Result Values ===")
        results = session.execute(
            select(Business.raw[rkey_gs].astext, func.count()).where(Business.raw.has_key("google_search_verified")).group_by(Business.raw[rkey_gs].astext)
        ).all()
        for val, count in results:
            print(f"  {val}: {count}")
