#!/usr/bin/env python3
"""Migration: add and populate city_name_en and zone_name_en columns.

Run once against an existing DB. Safe to re-run — uses ALTER TABLE IF NOT EXISTS
semantics via exception catch, then updates any NULL rows.

Usage:
    python3 scripts/enrich_english_names.py
    python3 scripts/enrich_english_names.py --db db/pikud_v1.db
"""

import argparse
import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pikud import _apply_english_names, DB_DIR


def _add_columns_if_missing(conn: sqlite3.Connection) -> None:
    existing_cities = {r[1] for r in conn.execute("PRAGMA table_info(cities)").fetchall()}
    existing_zones = {r[1] for r in conn.execute("PRAGMA table_info(zones)").fetchall()}

    if "city_name_en" not in existing_cities:
        conn.execute("ALTER TABLE cities ADD COLUMN city_name_en TEXT")
        print("  Added column: cities.city_name_en")
    if "zone_name_en" not in existing_zones:
        conn.execute("ALTER TABLE zones ADD COLUMN zone_name_en TEXT")
        print("  Added column: zones.zone_name_en")
    conn.commit()


def migrate(db_path: str) -> None:
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DB not found: {db_path}")

    print(f"Migrating: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    _add_columns_if_missing(conn)
    city_count, zone_count = _apply_english_names(conn)
    print(f"  Updated: {city_count} cities, {zone_count} zones with English names")

    conn.close()
    print("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Add English name columns to cities and zones tables")
    parser.add_argument(
        "--db",
        default=os.path.join(DB_DIR, "current"),
        help="Path to SQLite DB file (default: db/current)",
    )
    args = parser.parse_args()
    migrate(db_path=args.db)


if __name__ == "__main__":
    main()
