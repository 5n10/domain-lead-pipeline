"""Check recent job runs."""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import select, desc
from domain_pipeline.db import session_scope
from domain_pipeline.models import JobRun

with session_scope() as session:
    stmt = select(JobRun).order_by(desc(JobRun.started_at)).limit(15)
    runs = session.execute(stmt).scalars().all()
    
    print(f"{'Job Name':<25} {'Status':<10} {'Started At':<30} {'Processed':<10}")
    print("-" * 75)
    for run in runs:
        print(f"{run.job_name:<25} {run.status:<10} {str(run.started_at):<30} {run.processed_count or 0:<10}")
