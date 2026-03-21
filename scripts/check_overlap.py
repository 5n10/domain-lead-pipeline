"""Check top leads for multiple verification sources."""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import select, or_, func
from domain_pipeline.db import session_scope
from domain_pipeline.models import Business
from domain_pipeline.workers.business_leads import compute_verification_confidence, _CONCLUSIVE_RESULTS, _RESULT_KEY_MAP

with session_scope() as session:
    # Get top 100 leads (no website)
    stmt = (
        select(Business)
        .where(or_(Business.website_url.is_(None), Business.website_url == ""))
        .where(Business.lead_score.isnot(None))
        .order_by(Business.lead_score.desc())
        .limit(100)
    )
    leads = session.execute(stmt).scalars().all()
    
    print(f"=== Top 100 leads verification status ===")
    counts = {"high": 0, "medium": 0, "low": 0, "unverified": 0}
    sources_count = {}
    
    for b in leads:
        conf = compute_verification_confidence(b.raw)
        counts[conf] += 1
        
        # Count conclusive sources
        concl_count = 0
        sources = []
        if b.raw:
            for vkey, rkey in _RESULT_KEY_MAP.items():
                if b.raw.get(vkey):
                    val = b.raw.get(rkey)
                    sources.append(f"{vkey}:{val}")
                    if val in _CONCLUSIVE_RESULTS:
                        concl_count += 1
        
        sources_count[concl_count] = sources_count.get(concl_count, 0) + 1
        
        if concl_count >= 1:
            bid_str = str(b.id)[:8]
            print(f"ID={bid_str} Score={b.lead_score:.1f} Conf={conf} Sources({concl_count})={sources}")

    print("\n=== Summary ===")
    print(f"Confidence Distribution (Top 100): {counts}")
    print(f"Conclusive Sources Count (Top 100): {sources_count}")

    # Check total counts across ALL leads
    print("\n=== All Lead Counts ===")
    all_concl_counts = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
    
    # We only care about those with at least one verification key to save time
    stmt_all = (
        select(Business.raw)
        .where(or_(Business.website_url.is_(None), Business.website_url == ""))
        .where(Business.raw.isnot(None))
    )
    # Sampling for speed if DB is large, but let's try top 5000 first
    raws = session.execute(stmt_all.limit(5000)).scalars().all()
    for raw in raws:
        concl = 0
        for vkey, rkey in _RESULT_KEY_MAP.items():
            if raw.get(vkey) and raw.get(rkey) in _CONCLUSIVE_RESULTS:
                concl += 1
        all_concl_counts[concl] = all_concl_counts.get(concl, 0) + 1
    
    print(f"Conclusive Sources Count (Sample 5000): {all_concl_counts}")
