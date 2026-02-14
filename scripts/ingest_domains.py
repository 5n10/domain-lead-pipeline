from __future__ import annotations

import argparse
import csv
from pathlib import Path

from sqlalchemy.dialects.postgresql import insert

from domain_pipeline.db import session_scope
from domain_pipeline.domain_utils import normalize_domain
from domain_pipeline.models import Domain


def _has_header(sample: str) -> bool:
    try:
        return csv.Sniffer().has_header(sample)
    except csv.Error:
        return False


def read_domains(path: Path) -> list[str]:
    domains: list[str] = []
    with path.open(newline="", encoding="utf-8") as handle:
        sample = handle.read(4096)
        handle.seek(0)
        has_header = _has_header(sample)
        reader = csv.reader(handle)
        for idx, row in enumerate(reader):
            # Skip empty rows or rows with no columns
            if not row:
                continue
            if idx == 0 and has_header:
                continue
            domain = normalize_domain(row[0])
            if domain:
                domains.append(domain)
    return domains


def ingest(domains: list[str]) -> int:
    if not domains:
        return 0

    values = [{"domain": d} for d in domains]
    with session_scope() as session:
        stmt = insert(Domain).values(values).on_conflict_do_nothing(index_elements=["domain"])
        result = session.execute(stmt)
        return result.rowcount or 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest domains into Postgres")
    parser.add_argument("--file", required=True, help="CSV file with domains in first column")
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if not path.is_file():
        raise ValueError(f"Path is not a file: {path}")
    
    domains = read_domains(path)
    inserted = ingest(domains)
    print(f"Inserted {inserted} domains")


if __name__ == "__main__":
    main()
