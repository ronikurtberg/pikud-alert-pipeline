"""Tests for database integrity, schema, and data quality.

Requires a populated database. Run 'make fetch' first.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from conftest import requires_db

pytestmark = requires_db


class TestSchema:
    """Verify the database schema is correct."""

    def test_tables_exist(self, real_db):
        tables = [
            r[0] for r in real_db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
        ]
        assert "messages" in tables
        assert "alert_details" in tables
        assert "cities" in tables
        assert "zones" in tables
        assert "db_info" in tables

    def test_views_exist(self, real_db):
        views = [r[0] for r in real_db.execute("SELECT name FROM sqlite_master WHERE type='view'").fetchall()]
        assert "v_alerts_full" in views
        assert "v_city_alert_counts" in views

    def test_indexes_exist(self, real_db):
        indexes = [
            r[0]
            for r in real_db.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        ]
        assert "idx_messages_alert_filter" in indexes
        assert "idx_ad_msg_city_zone" in indexes
        assert "idx_messages_type_time" in indexes

    def test_wal_mode(self, real_db):
        mode = real_db.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"

    def test_messages_columns(self, real_db):
        cols = [r[1] for r in real_db.execute("PRAGMA table_info(messages)").fetchall()]
        expected = [
            "msg_id",
            "datetime_utc",
            "datetime_israel",
            "alert_date",
            "alert_time_local",
            "message_type",
            "alert_type",
            "is_drill",
            "raw_text",
            "views",
        ]
        for c in expected:
            assert c in cols, f"Missing column: {c}"


class TestDataIntegrity:
    """Verify data quality and referential integrity."""

    def test_no_null_message_type(self, real_db):
        count = real_db.execute("SELECT COUNT(*) FROM messages WHERE message_type IS NULL").fetchone()[0]
        assert count == 0

    def test_no_orphaned_alert_details(self, real_db):
        orphans = real_db.execute(
            "SELECT COUNT(*) FROM alert_details WHERE msg_id NOT IN (SELECT msg_id FROM messages)"
        ).fetchone()[0]
        assert orphans == 0

    def test_all_cities_referenced(self, real_db):
        """Every city_id in alert_details should exist in cities table."""
        bad = real_db.execute(
            "SELECT COUNT(*) FROM alert_details WHERE city_id NOT IN (SELECT city_id FROM cities)"
        ).fetchone()[0]
        assert bad == 0

    def test_all_zones_referenced(self, real_db):
        """Every zone_id in alert_details should exist in zones table."""
        bad = real_db.execute(
            "SELECT COUNT(*) FROM alert_details WHERE zone_id IS NOT NULL AND zone_id NOT IN (SELECT zone_id FROM zones)"
        ).fetchone()[0]
        assert bad == 0

    def test_no_duplicate_msg_ids(self, real_db):
        dupes = real_db.execute("SELECT msg_id, COUNT(*) c FROM messages GROUP BY msg_id HAVING c > 1").fetchall()
        assert len(dupes) == 0

    def test_msg_ids_are_positive(self, real_db):
        bad = real_db.execute("SELECT COUNT(*) FROM messages WHERE msg_id <= 0").fetchone()[0]
        assert bad == 0

    def test_datetime_israel_not_null_for_alerts(self, real_db):
        bad = real_db.execute(
            "SELECT COUNT(*) FROM messages WHERE message_type='alert' AND datetime_israel IS NULL"
        ).fetchone()[0]
        assert bad == 0

    def test_is_drill_is_boolean(self, real_db):
        bad = real_db.execute("SELECT COUNT(*) FROM messages WHERE is_drill NOT IN (0, 1)").fetchone()[0]
        assert bad == 0


class TestDataCounts:
    """Verify expected data volumes and distributions."""

    def test_message_count_reasonable(self, real_db):
        count = real_db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        assert count > 20000

    def test_alert_details_more_than_messages(self, real_db):
        msgs = real_db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        details = real_db.execute("SELECT COUNT(*) FROM alert_details").fetchone()[0]
        assert details > msgs * 5  # avg ~14 details per alert message

    def test_all_message_types_present(self, real_db):
        types = [r[0] for r in real_db.execute("SELECT DISTINCT message_type FROM messages").fetchall()]
        for expected in ["alert", "event_ended", "heads_up", "update"]:
            assert expected in types

    def test_all_alert_types_present(self, real_db):
        types = [
            r[0]
            for r in real_db.execute("SELECT DISTINCT alert_type FROM messages WHERE alert_type IS NOT NULL").fetchall()
        ]
        assert "rockets" in types
        assert "aircraft" in types

    def test_zones_count(self, real_db):
        count = real_db.execute("SELECT COUNT(*) FROM zones").fetchone()[0]
        assert count >= 36

    def test_cities_count(self, real_db):
        count = real_db.execute("SELECT COUNT(*) FROM cities").fetchone()[0]
        assert count >= 1900  # ~1,998 after parser fix (combined entries split)

    def test_drills_are_rare(self, real_db):
        total = real_db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        drills = real_db.execute("SELECT COUNT(*) FROM messages WHERE is_drill=1").fetchone()[0]
        assert drills < total * 0.01  # less than 1%

    def test_datetime_ordering(self, real_db):
        """msg_ids should generally increase with time."""
        row = real_db.execute("""
            SELECT COUNT(*) FROM (
                SELECT msg_id, datetime_utc,
                    LAG(datetime_utc) OVER (ORDER BY msg_id) as prev
                FROM messages
            ) WHERE prev IS NOT NULL AND datetime_utc < prev
        """).fetchone()[0]
        total = real_db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        assert row < total * 0.01  # less than 1% out of order


class TestCityCanonicalization:
    """Verify city name normalization (dash vs space variants unified)."""

    def test_canonical_names_set(self, real_db):
        """Cities with dash variants should have canonical_name set."""
        rows = real_db.execute(
            "SELECT city_name, canonical_name FROM cities WHERE canonical_name IS NOT NULL"
        ).fetchall()
        assert len(rows) >= 18  # 9 pairs = 18 rows

    def test_dash_maps_to_space(self, real_db):
        """Dash variant should map to space variant as canonical."""
        row = real_db.execute("SELECT canonical_name FROM cities WHERE city_name='אבו-גוש'").fetchone()
        assert row is not None
        assert row["canonical_name"] == "אבו גוש"

    def test_both_variants_same_canonical(self, real_db):
        dash = real_db.execute("SELECT canonical_name FROM cities WHERE city_name='בת-ים'").fetchone()
        space = real_db.execute("SELECT canonical_name FROM cities WHERE city_name='בת ים'").fetchone()
        assert dash["canonical_name"] == space["canonical_name"] == "בת ים"

    def test_top_cities_uses_canonical(self, client):
        """Top cities visualization should show canonical names (no duplicates)."""
        import json

        r = client.get("/api/viz/top_cities?limit=100")
        d = json.loads(r.data)
        names = [c["city_name"] for c in d]
        # No dash variants should appear — only canonical (space) versions
        for name in names:
            assert "-" not in name or name.count("-") == name.count(" - "), f"Dash variant leaked into display: {name}"


class TestCalculatedFields:
    """Verify calculated fields are computed correctly."""

    def test_datetime_israel_offset(self, real_db):
        """Israel time should be 2-3 hours ahead of UTC."""
        row = real_db.execute("""
            SELECT datetime_utc, datetime_israel FROM messages
            WHERE datetime_utc IS NOT NULL AND datetime_israel IS NOT NULL
            LIMIT 1
        """).fetchone()
        utc_h = int(row["datetime_utc"].split()[1].split(":")[0])
        il_h = int(row["datetime_israel"].split()[1].split(":")[0])
        diff = (il_h - utc_h) % 24
        assert diff in (2, 3)

    def test_alert_type_matches_text(self, real_db):
        """Spot check: rockets type should have רקטות in text."""
        rows = real_db.execute("SELECT raw_text FROM messages WHERE alert_type='rockets' LIMIT 10").fetchall()
        for r in rows:
            assert "רקטות" in r["raw_text"]

    def test_aircraft_type_matches_text(self, real_db):
        rows = real_db.execute("SELECT raw_text FROM messages WHERE alert_type='aircraft' LIMIT 10").fetchall()
        for r in rows:
            assert "כלי טיס" in r["raw_text"]
