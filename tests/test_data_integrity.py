"""Data integrity tests — verify data quality invariants that must hold across versions.

Requires a populated database. Run 'make fetch' first.
"""
import pytest
import json
import sys
import os

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
            if '-' in city and ' - ' not in city:  # exclude "באר שבע - דרום" (real dash)
                space_variant = city.replace('-', ' ')
                assert space_variant not in cities, \
                    f"Both '{city}' and '{space_variant}' in filter options — should be canonicalized"

    def test_no_multi_city_concatenations(self, client):
        """No city name should be 5+ words (likely a parser concatenation bug)."""
        r = client.get("/api/filter_options")
        d = json.loads(r.data)
        for city in d["cities"]:
            assert city.count(' ') <= 3, f"Suspicious multi-word city: {city}"

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
            canonical = name.replace('-', ' ')
            assert canonical not in seen_canonical or canonical == name, \
                f"Duplicate canonical: {name} and space variant both in results"
            seen_canonical.add(canonical)

    def test_drone_cities_canonical(self, client):
        r = client.get("/api/viz/drone_cities")
        d = json.loads(r.data)
        names = [c["city_name"] for c in d]
        for name in names:
            # No raw dash variants should appear
            if '-' in name and ' - ' not in name:
                space = name.replace('-', ' ')
                assert space not in names


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
            assert run_total == v["total_messages"], \
                f"v{v['version']}: run total {run_total} != metadata {v['total_messages']}"

    def test_db_message_count_matches(self, client):
        r = client.get("/api/pipeline/versions")
        d = json.loads(r.data)
        for v in d["versions"]:
            if v["db_exists"]:
                assert v["db_msg_count"] == v["total_messages"], \
                    f"v{v['version']}: DB has {v['db_msg_count']}, metadata says {v['total_messages']}"
