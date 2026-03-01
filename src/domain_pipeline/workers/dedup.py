"""Duplicate detection for businesses.

Detects duplicates using:
1. Name + city + geographic proximity (haversine, 100m threshold)
2. Shared phone numbers or email addresses

Duplicates are linked via the `duplicate_of` FK on Business.
The business with the higher lead_score (or earlier created_at) is kept as primary.
"""
from __future__ import annotations

import logging
import math
import re
from collections import defaultdict
from typing import Optional

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.orm import Session

from ..db import session_scope
from ..models import Business, BusinessContact

logger = logging.getLogger(__name__)

# Haversine distance threshold in meters
PROXIMITY_THRESHOLD_M = 100.0

# Earth radius in meters
EARTH_RADIUS_M = 6_371_000.0


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the Haversine distance between two points in meters."""
    lat1_r, lon1_r = math.radians(lat1), math.radians(lon1)
    lat2_r, lon2_r = math.radians(lat2), math.radians(lon2)

    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    return EARTH_RADIUS_M * c


def normalize_name(name: str) -> str:
    """Normalize business name for comparison: lowercase, strip punctuation, collapse whitespace."""
    name = name.lower().strip()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name)
    return name


def _pick_primary(biz_a: dict, biz_b: dict) -> tuple[dict, dict]:
    """Return (primary, duplicate). Higher lead_score wins; ties go to earlier creation."""
    score_a = biz_a.get("lead_score") or 0
    score_b = biz_b.get("lead_score") or 0
    if score_a > score_b:
        return biz_a, biz_b
    elif score_b > score_a:
        return biz_b, biz_a
    # Tie: earlier created_at is primary
    if (biz_a.get("created_at") or "") <= (biz_b.get("created_at") or ""):
        return biz_a, biz_b
    return biz_b, biz_a


def find_name_proximity_duplicates(limit: int = 1000) -> list[dict]:
    """Find businesses with the same normalized name, city, and within PROXIMITY_THRESHOLD_M.

    Returns list of dicts: {primary_id, duplicate_id, reason, distance_m}.
    """
    with session_scope() as session:
        # Get businesses with lat/lon and city, not already marked as duplicates
        businesses = session.execute(
            select(
                Business.id,
                Business.name,
                Business.city_id,
                Business.lat,
                Business.lon,
                Business.lead_score,
                Business.created_at,
            )
            .where(
                Business.name.isnot(None),
                Business.lat.isnot(None),
                Business.lon.isnot(None),
                Business.city_id.isnot(None),
                Business.duplicate_of.is_(None),
            )
            .order_by(Business.city_id, Business.name)
            .limit(limit)
        ).all()

    # Group by city_id + normalized name
    groups = defaultdict(list)
    for biz_id, name, city_id, lat, lon, score, created in businesses:
        norm = normalize_name(name)
        groups[(city_id, norm)].append({
            "id": biz_id,
            "name": name,
            "lat": float(lat),
            "lon": float(lon),
            "lead_score": float(score) if score else 0,
            "created_at": created.isoformat() if created else "",
        })

    duplicates = []
    for (city_id, norm_name), biz_list in groups.items():
        if len(biz_list) < 2:
            continue
        # Compare all pairs
        for i in range(len(biz_list)):
            for j in range(i + 1, len(biz_list)):
                dist = haversine_distance(
                    biz_list[i]["lat"], biz_list[i]["lon"],
                    biz_list[j]["lat"], biz_list[j]["lon"],
                )
                if dist <= PROXIMITY_THRESHOLD_M:
                    primary, dup = _pick_primary(biz_list[i], biz_list[j])
                    duplicates.append({
                        "primary_id": primary["id"],
                        "duplicate_id": dup["id"],
                        "reason": f"name_proximity:{dist:.0f}m",
                        "distance_m": round(dist, 1),
                    })

    return duplicates


def find_contact_duplicates(limit: int = 1000) -> list[dict]:
    """Find businesses that share phone numbers or email addresses.

    Returns list of dicts: {primary_id, duplicate_id, reason, shared_value}.
    """
    with session_scope() as session:
        # Get contacts grouped by value, finding cases where the same
        # phone or email belongs to multiple businesses
        shared_contacts = session.execute(
            select(
                BusinessContact.value,
                BusinessContact.contact_type,
                func.array_agg(BusinessContact.business_id.distinct()),
            )
            .where(
                BusinessContact.contact_type.in_(["phone", "email"]),
            )
            .group_by(BusinessContact.value, BusinessContact.contact_type)
            .having(func.count(BusinessContact.business_id.distinct()) > 1)
            .limit(limit)
        ).all()

    if not shared_contacts:
        return []

    # For each shared contact, look up business scores to determine primary
    all_biz_ids = set()
    for value, ctype, biz_ids in shared_contacts:
        all_biz_ids.update(biz_ids)

    with session_scope() as session:
        biz_data = {}
        rows = session.execute(
            select(Business.id, Business.lead_score, Business.created_at, Business.duplicate_of)
            .where(Business.id.in_(list(all_biz_ids)))
        ).all()
        for biz_id, score, created, dup_of in rows:
            biz_data[biz_id] = {
                "id": biz_id,
                "lead_score": float(score) if score else 0,
                "created_at": created.isoformat() if created else "",
                "already_duplicate": dup_of is not None,
            }

    duplicates = []
    for value, ctype, biz_ids in shared_contacts:
        # Filter out already-marked duplicates
        eligible = [biz_data[bid] for bid in biz_ids if bid in biz_data and not biz_data[bid]["already_duplicate"]]
        if len(eligible) < 2:
            continue

        # Sort by score desc, then created_at asc
        eligible.sort(key=lambda x: (-x["lead_score"], x["created_at"]))
        primary = eligible[0]
        for dup in eligible[1:]:
            duplicates.append({
                "primary_id": primary["id"],
                "duplicate_id": dup["id"],
                "reason": f"shared_{ctype}:{value}",
                "shared_value": value,
            })

    return duplicates


def mark_duplicates(pairs: list[dict]) -> int:
    """Mark businesses as duplicates in the database.

    Args:
        pairs: List of dicts with primary_id, duplicate_id, reason.

    Returns count of businesses marked.
    """
    if not pairs:
        return 0

    marked = 0
    with session_scope() as session:
        for pair in pairs:
            result = session.execute(
                update(Business)
                .where(
                    Business.id == pair["duplicate_id"],
                    Business.duplicate_of.is_(None),  # Don't overwrite existing marking
                )
                .values(
                    duplicate_of=pair["primary_id"],
                    duplicate_reason=pair["reason"],
                )
            )
            marked += result.rowcount

    return marked


def run_dedup(limit: int = 1000) -> dict:
    """Run full deduplication pass.

    Finds name+proximity duplicates and contact duplicates,
    then marks them in the database.
    """
    name_dups = find_name_proximity_duplicates(limit=limit)
    contact_dups = find_contact_duplicates(limit=limit)

    all_pairs = name_dups + contact_dups

    # Deduplicate the pairs (same duplicate_id might appear multiple times)
    seen_dup_ids = set()
    unique_pairs = []
    for pair in all_pairs:
        if pair["duplicate_id"] not in seen_dup_ids:
            seen_dup_ids.add(pair["duplicate_id"])
            unique_pairs.append(pair)

    marked = mark_duplicates(unique_pairs)

    logger.info(
        "Dedup: found %d name+proximity, %d contact duplicates, marked %d",
        len(name_dups), len(contact_dups), marked,
    )

    return {
        "name_proximity_found": len(name_dups),
        "contact_found": len(contact_dups),
        "total_unique": len(unique_pairs),
        "marked": marked,
    }


def get_duplicate_clusters(limit: int = 100, offset: int = 0) -> list[dict]:
    """Get duplicate clusters for the dashboard.

    Returns list of clusters, each with primary business and its duplicates.
    """
    with session_scope() as session:
        # Get primary businesses that have duplicates
        primary_ids = session.execute(
            select(Business.duplicate_of)
            .where(Business.duplicate_of.isnot(None))
            .group_by(Business.duplicate_of)
            .order_by(func.count(Business.id).desc())
            .limit(limit)
            .offset(offset)
        ).scalars().all()

        if not primary_ids:
            return []

        clusters = []
        for pid in primary_ids:
            primary = session.execute(
                select(Business.id, Business.name, Business.category, Business.address, Business.lead_score)
                .where(Business.id == pid)
            ).first()

            if not primary:
                continue

            dupes = session.execute(
                select(
                    Business.id, Business.name, Business.category, Business.address,
                    Business.lead_score, Business.duplicate_reason,
                )
                .where(Business.duplicate_of == pid)
            ).all()

            clusters.append({
                "primary": {
                    "id": str(primary[0]),
                    "name": primary[1],
                    "category": primary[2],
                    "address": primary[3],
                    "lead_score": float(primary[4]) if primary[4] else None,
                },
                "duplicates": [
                    {
                        "id": str(d[0]),
                        "name": d[1],
                        "category": d[2],
                        "address": d[3],
                        "lead_score": float(d[4]) if d[4] else None,
                        "reason": d[5],
                    }
                    for d in dupes
                ],
            })

        return clusters


def dismiss_duplicate(business_id) -> bool:
    """Clear the duplicate_of marking for a business (dismiss as not a duplicate)."""
    with session_scope() as session:
        result = session.execute(
            update(Business)
            .where(Business.id == business_id)
            .values(duplicate_of=None, duplicate_reason=None)
        )
        return result.rowcount > 0


def duplicate_count() -> int:
    """Count businesses marked as duplicates."""
    with session_scope() as session:
        return int(
            session.execute(
                select(func.count(Business.id))
                .where(Business.duplicate_of.isnot(None))
            ).scalar() or 0
        )
