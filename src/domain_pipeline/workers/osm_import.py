from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import requests
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from ..config import load_config
from ..db import session_scope
from ..models import Business, BusinessContact, City
from .osm_contacts import extract_osm_contacts


@dataclass
class AreaConfig:
    key: str
    name: str
    country: Optional[str]
    region: Optional[str]
    area_tags: dict[str, str]
    bbox: Optional[dict[str, float]]


@dataclass
class CategoryFilter:
    category: str
    tags: dict[str, str]


@dataclass
class CategoryConfig:
    key: str
    label: str
    filters: list[CategoryFilter]


def load_areas(path: Path) -> dict[str, AreaConfig]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    areas: dict[str, AreaConfig] = {}
    for key, value in raw.items():
        areas[key] = AreaConfig(
            key=key,
            name=value["name"],
            country=value.get("country"),
            region=value.get("region"),
            area_tags=value["area_tags"],
            bbox=value.get("bbox"),
        )
    return areas


def load_categories(path: Path) -> dict[str, CategoryConfig]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    categories: dict[str, CategoryConfig] = {}
    for key, value in raw.items():
        filters = [CategoryFilter(category=entry["category"], tags=entry["tags"]) for entry in value["filters"]]
        categories[key] = CategoryConfig(key=key, label=value.get("label", key), filters=filters)
    return categories


def build_area_clause(area_tags: dict[str, str]) -> str:
    parts = [f'"{k}"="{v}"' for k, v in area_tags.items()]
    return "[" + "][".join(parts) + "]"


def build_filter_clause(tags: dict[str, str]) -> str:
    parts = []
    for key, value in tags.items():
        if value in ("*", None):
            parts.append(f'"{key}"')
        else:
            parts.append(f'"{key}"="{value}"')
    return "[" + "][".join(parts) + "]"


def build_query(
    area: AreaConfig,
    filters: list[CategoryFilter],
    timeout: int,
    element_types: list[str],
    bbox_override: Optional[dict[str, float]] = None,
) -> str:
    lines = [f"[out:json][timeout:{timeout}];", "("]
    if bbox_override:
        bbox = bbox_override
        search_area = f"({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']})"
    elif area.bbox:
        bbox = area.bbox
        search_area = f"({bbox['min_lat']},{bbox['min_lon']},{bbox['max_lat']},{bbox['max_lon']})"
    else:
        area_clause = build_area_clause(area.area_tags)
        lines.insert(1, f"area{area_clause}->.searchArea;")
        search_area = "(area.searchArea)"

    for filt in filters:
        for element_type in element_types:
            lines.append(f"  {element_type}[\"name\"]{build_filter_clause(filt.tags)}{search_area};")

    lines.append(");")
    lines.append("out center tags;")
    return "\n".join(lines)


def extract_address(tags: dict[str, Any]) -> Optional[str]:
    if "addr:full" in tags:
        return tags.get("addr:full")

    parts = [
        tags.get("addr:housenumber"),
        tags.get("addr:street"),
        tags.get("addr:city"),
        tags.get("addr:postcode"),
        tags.get("addr:country"),
    ]
    combined = ", ".join([part for part in parts if part])
    return combined or None


def extract_contacts(tags: dict[str, Any]) -> list[tuple[str, str]]:
    # Backwards-compatible wrapper (kept for clarity within this module).
    return extract_osm_contacts(tags)


def extract_website(tags: dict[str, Any]) -> Optional[str]:
    for key in ("website", "contact:website", "url"):
        if tags.get(key):
            return tags[key]
    return None


def element_location(element: dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    if "lat" in element and "lon" in element:
        return element["lat"], element["lon"]
    center = element.get("center")
    if center:
        return center.get("lat"), center.get("lon")
    return None, None


def get_or_create_city(session, area: AreaConfig) -> City:
    city = (
        session.execute(
            select(City).where(City.name == area.name).where(City.country == area.country)
        )
        .scalars()
        .first()
    )
    if city:
        return city

    city = City(name=area.name, country=area.country, region=area.region)
    session.add(city)
    session.flush()
    return city


def match_category(filters: list[CategoryFilter], tags: dict[str, Any]) -> Optional[str]:
    for filt in filters:
        match = True
        for key, value in filt.tags.items():
            if value in ("*", None):
                if key not in tags:
                    match = False
                    break
            elif tags.get(key) != value:
                match = False
                break
        if match:
            if filt.category.startswith("any_"):
                return None
            return filt.category
    return None


def classify_business(tags: dict[str, Any]) -> str:
    craft = tags.get("craft")
    if craft:
        return "trades"

    if tags.get("office") == "construction_company" or tags.get("company") == "construction":
        return "contractors"

    amenity = tags.get("amenity")
    if amenity in {
        "restaurant",
        "cafe",
        "fast_food",
        "food_court",
        "bar",
        "pub",
    }:
        return "food"
    if amenity in {"clinic", "hospital", "doctors", "dentist", "pharmacy"} or tags.get("healthcare"):
        return "health"
    if amenity in {"school", "college", "university", "kindergarten"}:
        return "education"
    if amenity in {"bank", "bureau_de_change", "atm"}:
        return "finance"
    if amenity in {"place_of_worship"}:
        return "religious"
    if amenity in {"fuel", "car_wash", "car_rental", "car_repair"}:
        return "auto"

    if tags.get("shop"):
        return "retail"
    if tags.get("tourism"):
        return "hospitality"
    if tags.get("leisure"):
        return "recreation"
    if tags.get("office"):
        return "professional_services"
    if tags.get("industrial"):
        return "industrial"

    return "other"


def chunked(items: list[CategoryFilter], size: int) -> list[list[CategoryFilter]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def import_osm(area: AreaConfig, categories: list[CategoryConfig]) -> int:
    config = load_config()
    session = requests.Session()
    session.headers.update({"User-Agent": config.http_user_agent})

    endpoints_env = os.getenv("OVERPASS_ENDPOINTS")
    if endpoints_env:
        endpoints = [endpoint.strip() for endpoint in endpoints_env.split(",") if endpoint.strip()]
    else:
        endpoints = [config.overpass_endpoint]

    filters: list[CategoryFilter] = []
    for category in categories:
        filters.extend(category.filters)

    inserted = 0
    chunk_size = int(os.getenv("OVERPASS_FILTER_CHUNK", "3"))
    element_types_env = os.getenv("OVERPASS_ELEMENT_TYPES", "nwr")
    element_types = [entry.strip() for entry in element_types_env.split(",") if entry.strip()]
    retry_limit = int(os.getenv("OVERPASS_RETRIES", "3"))
    retry_delay = int(os.getenv("OVERPASS_RETRY_DELAY", "5"))
    split = int(os.getenv("OVERPASS_BBOX_SPLIT", "1"))
    if area.bbox and split > 1:
        min_lat = area.bbox["min_lat"]
        min_lon = area.bbox["min_lon"]
        max_lat = area.bbox["max_lat"]
        max_lon = area.bbox["max_lon"]
        lat_step = (max_lat - min_lat) / split
        lon_step = (max_lon - min_lon) / split
        bbox_list = []
        for i in range(split):
            for j in range(split):
                bbox_list.append(
                    {
                        "min_lat": min_lat + i * lat_step,
                        "min_lon": min_lon + j * lon_step,
                        "max_lat": min_lat + (i + 1) * lat_step,
                        "max_lon": min_lon + (j + 1) * lon_step,
                    }
                )
    else:
        bbox_list = [None]

    for bbox in bbox_list:
        for filt_chunk in chunked(filters, chunk_size):
            query = build_query(area, filt_chunk, config.overpass_timeout, element_types, bbox_override=bbox)

            data = None
            last_error = None
            for endpoint in endpoints:
                for attempt in range(1, retry_limit + 1):
                    try:
                        resp = session.post(endpoint, data=query.encode("utf-8"), timeout=config.overpass_timeout)
                    except requests.RequestException as exc:
                        last_error = exc
                        time.sleep(retry_delay)
                        continue

                    if resp.status_code in (429, 504):
                        last_error = RuntimeError(f"Overpass {endpoint} returned {resp.status_code}")
                        time.sleep(retry_delay)
                        continue

                    if resp.status_code != 200:
                        last_error = RuntimeError(f"Overpass {endpoint} returned {resp.status_code}")
                        break

                    try:
                        data = resp.json()
                        break
                    except ValueError:
                        snippet = resp.text[:200].replace("\n", " ")
                        last_error = RuntimeError(f"Non-JSON response from {endpoint}: {snippet}")
                        time.sleep(retry_delay)
                        continue

                if data is not None:
                    break

            if data is None:
                raise RuntimeError(f"Overpass failed for area {area.name}: {last_error}")

            elements = data.get("elements", [])
            if not elements:
                continue

            with session_scope() as db:
                city = get_or_create_city(db, area)

                for element in elements:
                    tags = element.get("tags", {})
                    source_id = f"{element.get('type')}/{element.get('id')}"

                    existing = (
                        db.execute(
                            select(Business.id).where(Business.source == "osm").where(Business.source_id == source_id)
                        )
                        .scalars()
                        .first()
                    )
                    if existing:
                        continue

                    lat, lon = element_location(element)
                    category = match_category(filters, tags) or classify_business(tags)
                    website = extract_website(tags)
                    address = extract_address(tags)

                    business = Business(
                        source="osm",
                        source_id=source_id,
                        name=tags.get("name"),
                        category=category,
                        website_url=website,
                        address=address,
                        lat=lat,
                        lon=lon,
                        raw=tags,
                        city_id=city.id,
                    )
                    db.add(business)
                    db.flush()

                    contacts = extract_contacts(tags)
                    for contact_type, value in contacts:
                        db.add(
                            BusinessContact(
                                business_id=business.id,
                                contact_type=contact_type,
                                value=value,
                                source="osm",
                            )
                        )

                    inserted += 1

            time.sleep(int(os.getenv("OVERPASS_SLEEP", "1")))

    return inserted
