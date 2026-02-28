"""Check for recently updated verifications or scores."""
from __future__ import annotations
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import select, func, or_
from domain_pipeline.db import session_scope
from domain_pipeline.models import Business

with session_scope() as session:
    now = datetime.now(timezone.utc)
    ten_mins_ago = now - timedelta(minutes=10)
    
    # 1. Count scored in last 10 mins
    scored_count = session.execute(
        select(func.count(Business.id)).where(Business.scored_at >= ten_mins_ago)
    ).scalar()
    
    # 2. Count any verification keys added in last 10 mins (we check for existence of keys)
    # This is harder to check for "recently added" without a last_verified_at, 
    # but we can check forBusinesses where scored_at is recent and they HAVE verification keys.
    v_keys = ["domain_guess_verified", "ddg_verified", "google_search_verified", "llm_verified", "google_places_verified"]
    v_clauses = [Business.raw.has_key(k) for k in v_keys]
    
    verified_and_scored = session.execute(
        select(func.count(Business.id)).where(Business.scored_at >= ten_mins_ago, or_(*v_clauses))
    ).scalar()

    print(f"Stats for last 10 minutes:")
    print(f"  Businesses rescored: {scored_count}")
    print(f"  Businesses with verifications rescored: {verified_and_scored}")
    
    # 3. Check for any business that was recently processed by a worker (scored_at is None)
    pending_rescore = session.execute(
        select(func.count(Business.id)).where(Business.scored_at.is_(None), or_(*v_clauses))
    ).scalar()
    print(f"  Businesses pending rescore (verification completed): {pending_rescore}")
