"""Proactive data contracts for the Pikud HaOref pipeline.

Unlike the reactive validation in pikud.py (which checks after the fact),
data contracts declare invariants upfront and enforce them at build time.
Pre-build contracts run before build_database() to catch bad input.
Post-build contracts run after to catch pipeline logic errors.

Usage in pikud.py:
    from data_contracts import check_pre_build, check_post_build, ContractViolation

    check_pre_build(ver_dir)          # before build_database()
    build_database(ver_dir, db_path, version)
    check_post_build(db_path)         # after build_database()
"""

import csv
import os
import sqlite3


class ContractViolation(Exception):
    """Raised when a data contract is violated."""

    def __init__(self, violations: list[str]):
        self.violations = violations
        super().__init__(f"{len(violations)} contract violation(s):\n" + "\n".join(f"  - {v}" for v in violations))


# ============================================================
# PRE-BUILD CONTRACTS (run on CSV input before DB is created)
# ============================================================


def check_pre_build(ver_dir: str) -> None:
    """Validate CSV inputs before building the database.

    Raises ContractViolation if any check fails.
    """
    violations = []
    csv_files = sorted(f for f in os.listdir(ver_dir) if f.startswith("alerts_") and f.endswith(".csv"))

    if not csv_files:
        raise ContractViolation(["No CSV files found in " + ver_dir])

    required_columns = {"msg_id", "datetime_utc", "raw_text", "views"}

    all_msg_ids = set()
    for csv_file in csv_files:
        filepath = os.path.join(ver_dir, csv_file)

        with open(filepath, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            # Contract: CSVs have required columns
            if reader.fieldnames:
                missing = required_columns - set(reader.fieldnames)
                if missing:
                    violations.append(f"{csv_file}: missing required columns: {missing}")

            for row in reader:
                mid = row.get("msg_id", "")
                if not mid or not mid.strip().isdigit():
                    violations.append(f"{csv_file}: non-numeric msg_id: '{mid}'")
                    continue

                mid_int = int(mid)
                # Contract: no duplicate msg_ids across all CSVs
                if mid_int in all_msg_ids:
                    violations.append(f"{csv_file}: duplicate msg_id {mid_int}")
                all_msg_ids.add(mid_int)

                # Contract: raw_text is not empty
                if not row.get("raw_text", "").strip():
                    violations.append(f"{csv_file}: msg_id {mid_int} has empty raw_text")

    if violations:
        raise ContractViolation(violations)


# ============================================================
# POST-BUILD CONTRACTS (run on the built database)
# ============================================================


def check_post_build(db_path: str) -> None:
    """Validate database integrity after building.

    Raises ContractViolation if any check fails.
    """
    violations = []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        # Contract: no orphaned alert_details (FK integrity)
        orphan_msg = conn.execute(
            "SELECT COUNT(*) FROM alert_details WHERE msg_id NOT IN (SELECT msg_id FROM messages)"
        ).fetchone()[0]
        if orphan_msg:
            violations.append(f"Orphaned alert_details: {orphan_msg} rows reference non-existent msg_id")

        orphan_zone = conn.execute(
            "SELECT COUNT(*) FROM alert_details WHERE zone_id NOT IN (SELECT zone_id FROM zones)"
        ).fetchone()[0]
        if orphan_zone:
            violations.append(f"Orphaned alert_details: {orphan_zone} rows reference non-existent zone_id")

        orphan_city = conn.execute(
            "SELECT COUNT(*) FROM alert_details WHERE city_id NOT IN (SELECT city_id FROM cities)"
        ).fetchone()[0]
        if orphan_city:
            violations.append(f"Orphaned alert_details: {orphan_city} rows reference non-existent city_id")

        # Contract: alert messages should have at least one alert_detail
        alerts_without_details = conn.execute(
            "SELECT COUNT(*) FROM messages m "
            "WHERE m.message_type = 'alert' "
            "AND NOT EXISTS ("
            "  SELECT 1 FROM alert_details ad WHERE ad.msg_id = m.msg_id"
            ")"
        ).fetchone()[0]
        if alerts_without_details:
            violations.append(
                f"Alert completeness: {alerts_without_details} messages with "
                f"message_type='alert' have zero alert_details rows"
            )

        # Contract: alert_date should be parseable (d/M/yyyy) for alert messages
        bad_dates = conn.execute(
            "SELECT COUNT(*) FROM messages "
            "WHERE message_type = 'alert' "
            "AND alert_date IS NOT NULL "
            "AND alert_date NOT GLOB '[0-9]*/[0-9]*/[0-9][0-9][0-9][0-9]'"
        ).fetchone()[0]
        if bad_dates:
            violations.append(
                f"Date format: {bad_dates} alert messages have unparseable alert_date (expected d/M/yyyy)"
            )

        # Contract: no NULL zone_id or city_id in alert_details
        null_zones = conn.execute("SELECT COUNT(*) FROM alert_details WHERE zone_id IS NULL").fetchone()[0]
        if null_zones:
            violations.append(f"Null zone_id: {null_zones} alert_details rows have NULL zone_id")

        null_cities = conn.execute("SELECT COUNT(*) FROM alert_details WHERE city_id IS NULL").fetchone()[0]
        if null_cities:
            violations.append(f"Null city_id: {null_cities} alert_details rows have NULL city_id")

        # Contract: every city referenced in alert_details must have an English name.
        # A missing translation means a new city appeared (new Pikud HaOref location or
        # a parser artifact) and was not covered by the translation dictionaries.
        missing_city_en = conn.execute(
            "SELECT c.city_name "
            "FROM cities c "
            "WHERE c.city_name_en IS NULL "
            "AND EXISTS (SELECT 1 FROM alert_details ad WHERE ad.city_id = c.city_id) "
            "ORDER BY c.city_name"
        ).fetchall()
        if missing_city_en:
            names = [r[0] for r in missing_city_en]
            violations.append(
                f"Missing English translation for {len(names)} cities used in alerts "
                f"(add to city_translations_manual.py): {names[:10]}"
                + (" ..." if len(names) > 10 else "")
            )

        # Contract: every zone must have an English name.
        missing_zone_en = conn.execute(
            "SELECT zone_name FROM zones WHERE zone_name_en IS NULL ORDER BY zone_name"
        ).fetchall()
        if missing_zone_en:
            names = [r[0] for r in missing_zone_en]
            violations.append(
                f"Missing English translation for {len(names)} zones "
                f"(add to _ZONE_MANUAL_EN in pikud.py): {names}"
            )

        # Contract: message_type is one of the known types
        known_types = {
            "alert",
            "event_ended",
            "heads_up",
            "update",
            "instructions",
            "intercept_report",
            "shelter_status",
            "general",
            "other",
        }
        unknown_types = conn.execute(
            "SELECT DISTINCT message_type FROM messages WHERE message_type NOT IN ({})".format(
                ",".join("?" for _ in known_types)
            ),
            list(known_types),
        ).fetchall()
        if unknown_types:
            types = [r[0] for r in unknown_types]
            violations.append(f"Unknown message_type values: {types}")

    finally:
        conn.close()

    if violations:
        raise ContractViolation(violations)
