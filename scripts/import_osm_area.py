from __future__ import annotations

import argparse
from pathlib import Path

from domain_pipeline.workers.osm_import import import_osm, load_areas, load_categories


def main() -> None:
    parser = argparse.ArgumentParser(description="Import businesses from OSM/Overpass")
    parser.add_argument("--area", default="uae", help="Area key from config/areas.json")
    parser.add_argument(
        "--categories",
        default="all",
        help="Comma-separated category keys from config/categories.json (or 'all')",
    )
    parser.add_argument("--areas-file", default="config/areas.json")
    parser.add_argument("--categories-file", default="config/categories.json")
    args = parser.parse_args()

    areas = load_areas(Path(args.areas_file))
    if args.area not in areas:
        raise SystemExit(f"Unknown area: {args.area}")

    categories = load_categories(Path(args.categories_file))
    if args.categories == "all":
        selected = list(categories.values())
    else:
        keys = [key.strip() for key in args.categories.split(",") if key.strip()]
        missing = [key for key in keys if key not in categories]
        if missing:
            raise SystemExit(f"Unknown categories: {', '.join(missing)}")
        selected = [categories[key] for key in keys]

    inserted = import_osm(areas[args.area], selected)
    print(f"Inserted {inserted} businesses from OSM")


if __name__ == "__main__":
    main()
