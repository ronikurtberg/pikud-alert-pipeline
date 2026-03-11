"""Data integrity tests — verify data quality invariants that must hold across versions.

Requires a populated database. Run 'make fetch' first.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from conftest import requires_db

pytestmark = requires_db


class TestFilterOptionsIntegrity:
    """Filter options should return clean, deduplicated city names."""

    def test_no_dash_duplicates(self, client):
        """Dash variants (אבו-גוש) should not appear if space variant (אבו גוש) exists."""
        r = client.get("/api/filter_options")
        d = json.loads(r.data)
        cities = d["cities"]
        for city in cities:
            if "-" in city and " - " not in city:  # exclude "באר שבע - דרום" (real dash)
                space_variant = city.replace("-", " ")
                assert space_variant not in cities, (
                    f"Both '{city}' and '{space_variant}' in filter options — should be canonicalized"
                )

    def test_no_multi_city_concatenations(self, client):
        """No city name should be 5+ words (likely a parser concatenation bug)."""
        r = client.get("/api/filter_options")
        d = json.loads(r.data)
        for city in d["cities"]:
            assert city.count(" ") <= 3, f"Suspicious multi-word city: {city}"

    def test_cities_sorted(self, client):
        r = client.get("/api/filter_options")
        d = json.loads(r.data)
        assert d["cities"] == sorted(d["cities"])

    def test_all_alert_types_present(self, client):
        r = client.get("/api/filter_options")
        d = json.loads(r.data)
        for t in ["rockets", "aircraft"]:
            assert t in d["alert_types"]


class TestSQLLatencyEnhanced:
    """Test enhanced SQL latency tracking."""

    def test_summary_has_all_fields(self, client):
        # Generate some queries
        client.get("/api/stats")
        client.get("/api/viz/daily")
        client.get("/api/viz/top_cities")
        r = client.get("/api/pipeline/sql_latency")
        d = json.loads(r.data)
        assert "total" in d
        assert "percentiles" in d
        assert "avg_ms" in d
        assert "top_slow" in d
        assert "by_endpoint" in d
        assert "log_path" in d
        assert "total_rows" in d
        assert "total_bytes" in d

    def test_top_slow_has_row_info(self, client):
        client.get("/api/stats")
        r = client.get("/api/pipeline/sql_latency")
        d = json.loads(r.data)
        if d["top_slow"]:
            for q in d["top_slow"]:
                assert "avg_rows" in q
                assert "total_bytes" in q
                assert "calls" in q


class TestCityCanonicalInViz:
    """Verify canonical names used in all visualization endpoints."""

    def test_top_cities_no_dash_dupes(self, client):
        r = client.get("/api/viz/top_cities?limit=200")
        d = json.loads(r.data)
        names = [c["city_name"] for c in d]
        seen_canonical = set()
        for name in names:
            # If a name has a dash, its space variant shouldn't also appear
            canonical = name.replace("-", " ")
            assert canonical not in seen_canonical or canonical == name, (
                f"Duplicate canonical: {name} and space variant both in results"
            )
            seen_canonical.add(canonical)

    def test_drone_cities_canonical(self, client):
        r = client.get("/api/viz/drone_cities")
        d = json.loads(r.data)
        names = [c["city_name"] for c in d]
        for name in names:
            # No raw dash variants should appear
            if "-" in name and " - " not in name:
                space = name.replace("-", " ")
                assert space not in names


class TestEnglishTranslationCoverage:
    """Every city used in alerts and every zone must have an English name.

    If this test fails after a rebuild it means either:
    - A new city/location appeared in the Pikud HaOref data (add to city_translations_manual.py)
    - A parser bug produced a new artifact city (fix the parser first)
    - A new zone was added by Pikud HaOref (add to _ZONE_MANUAL_EN in pikud.py)
    """

    def test_all_alert_cities_have_english_name(self, db_conn):
        """Every city referenced in alert_details must have city_name_en populated."""
        rows = db_conn.execute(
            "SELECT c.city_name "
            "FROM cities c "
            "WHERE c.city_name_en IS NULL "
            "AND EXISTS (SELECT 1 FROM alert_details ad WHERE ad.city_id = c.city_id) "
            "ORDER BY c.city_name"
        ).fetchall()
        missing = [r[0] for r in rows]
        assert not missing, (
            f"{len(missing)} cities used in alerts are missing English translations. "
            f"Add them to dashboard_app/city_translations_manual.py: {missing}"
        )

    def test_all_zones_have_english_name(self, db_conn):
        """Every zone in the zones table must have zone_name_en populated."""
        rows = db_conn.execute(
            "SELECT zone_name FROM zones WHERE zone_name_en IS NULL ORDER BY zone_name"
        ).fetchall()
        missing = [r[0] for r in rows]
        assert not missing, (
            f"{len(missing)} zones are missing English translations. "
            f"Add them to _ZONE_MANUAL_EN in pikud.py: {missing}"
        )

    def test_no_cities_without_alerts_are_untranslated(self, db_conn):
        """Cities not in any alert (orphans) should also be translated — warn if not."""
        # This is a softer check: we report but don't hard-fail for unreferenced cities
        # because they may be parser artifacts that simply never fired an alert.
        rows = db_conn.execute(
            "SELECT COUNT(*) FROM cities WHERE city_name_en IS NULL"
        ).fetchone()
        total_untranslated = rows[0]
        # All should be zero after a clean rebuild with full translation coverage
        assert total_untranslated == 0, (
            f"{total_untranslated} cities (including unreferenced ones) lack English names. "
            f"Run: scripts/enrich_english_names.py then check for new parser artifacts."
        )


class TestDataVersionIntegrity:
    """Verify data version metadata is consistent."""

    def test_version_has_runs(self, client):
        r = client.get("/api/pipeline/versions")
        d = json.loads(r.data)
        assert len(d["versions"]) >= 1
        v = d["versions"][0]
        assert len(v["runs"]) >= 1
        assert v["total_messages"] > 0

    def test_runs_cover_all_messages(self, client):
        r = client.get("/api/pipeline/versions")
        d = json.loads(r.data)
        for v in d["versions"]:
            run_total = sum(r["message_count"] for r in v["runs"])
            assert run_total == v["total_messages"], (
                f"v{v['version']}: run total {run_total} != metadata {v['total_messages']}"
            )

    def test_db_message_count_matches(self, client):
        r = client.get("/api/pipeline/versions")
        d = json.loads(r.data)
        for v in d["versions"]:
            if v["db_exists"]:
                assert v["db_msg_count"] == v["total_messages"], (
                    f"v{v['version']}: DB has {v['db_msg_count']}, metadata says {v['total_messages']}"
                )
