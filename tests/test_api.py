"""Tests for all dashboard API endpoints.

Tests in this file require a populated database. Run 'make fetch' first.
Without data, these tests are automatically skipped.
"""
import pytest
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from conftest import requires_db

pytestmark = requires_db  # Skip all tests in this file if no DB


class TestStatsAPI:
    def test_stats_returns_200(self, client):
        r = client.get("/api/stats")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert "total_messages" in d
        assert "total_alerts" in d
        assert "total_cities" in d
        assert "total_zones" in d
        assert d["total_messages"] > 0

    def test_stats_has_latest_alert_with_zones_cities(self, client):
        r = client.get("/api/stats")
        d = json.loads(r.data)
        la = d.get("latest_alert")
        assert la is not None
        assert "zones" in la
        assert "cities" in la
        assert "alert_type" in la

    def test_stats_has_version(self, client):
        r = client.get("/api/stats")
        d = json.loads(r.data)
        assert d["version"] is not None


class TestFilteredCounts:
    def test_unfiltered(self, client):
        r = client.get("/api/filtered_counts")
        d = json.loads(r.data)
        assert d["attack_events"] > 0
        assert d["city_alerts"] > 0
        assert d["zone_alerts"] > 0
        assert d["msg_alerts"] > 0
        assert d["first"] is not None
        assert d["last"] is not None

    def test_attack_events_less_than_messages(self, client):
        r = client.get("/api/filtered_counts")
        d = json.loads(r.data)
        assert d["attack_events"] < d["msg_alerts"]

    def test_city_alerts_more_than_messages(self, client):
        r = client.get("/api/filtered_counts")
        d = json.loads(r.data)
        assert d["city_alerts"] > d["msg_alerts"]

    def test_city_filter_scopes_correctly(self, client):
        """City filter should only count that city's alerts, not all cities in matching messages."""
        r = client.get("/api/filtered_counts?city=%D7%97%D7%A8%D7%91+%D7%9C%D7%90%D7%AA")  # חרב לאת
        d = json.loads(r.data)
        # City-level alerts should equal msg_alerts when filtering a single city
        assert d["city_alerts"] <= d["msg_alerts"] * 2  # reasonable bound
        assert d["city_alerts"] > 0

    def test_zone_filter(self, client):
        r = client.get("/api/filtered_counts?zone=%D7%90%D7%96%D7%95%D7%A8+%D7%A7%D7%95+%D7%94%D7%A2%D7%99%D7%9E%D7%95%D7%AA")
        d = json.loads(r.data)
        assert d["attack_events"] > 0

    def test_alert_type_filter(self, client):
        r = client.get("/api/filtered_counts?alert_type=aircraft")
        d = json.loads(r.data)
        assert d["attack_events"] > 0
        assert d["attack_events"] < json.loads(client.get("/api/filtered_counts").data)["attack_events"]

    def test_date_filter(self, client):
        r = client.get("/api/filtered_counts?date_from=2024-01-01&date_to=2024-12-31")
        d = json.loads(r.data)
        assert d["attack_events"] > 0

    def test_combined_city_and_date_filter(self, client):
        """Regression: city filter should persist when date filter is also active."""
        r = client.get("/api/filtered_counts?date_from=2024-01-01&city=%D7%A7%D7%A8%D7%99%D7%99%D7%AA+%D7%A9%D7%9E%D7%95%D7%A0%D7%94")
        d = json.loads(r.data)
        assert d["city_alerts"] > 0
        # City alerts should be reasonably scoped (not all cities in matching messages)
        assert d["city_alerts"] < 5000  # Kiryat Shmona alone shouldn't have 5K+

    def test_combined_zone_and_type_filter(self, client):
        r = client.get("/api/filtered_counts?alert_type=aircraft&zone=%D7%90%D7%96%D7%95%D7%A8+%D7%92%D7%9C%D7%99%D7%9C+%D7%A2%D7%9C%D7%99%D7%95%D7%9F")
        d = json.loads(r.data)
        assert d["attack_events"] > 0


class TestFilterOptions:
    def test_returns_all_types(self, client):
        r = client.get("/api/filter_options")
        d = json.loads(r.data)
        assert "cities" in d
        assert "zones" in d
        assert "alert_types" in d
        assert len(d["cities"]) > 1500  # ~1,591 clean cities (filtered from 2,156 raw)
        assert len(d["zones"]) > 30
        assert "rockets" in d["alert_types"]
        assert "aircraft" in d["alert_types"]


class TestVisualizationEndpoints:
    """Test all visualization endpoints return 200 with valid data."""

    ENDPOINTS = [
        "/api/viz/hourly", "/api/viz/daily", "/api/viz/top_cities",
        "/api/viz/zones", "/api/viz/monthly", "/api/viz/shelter_times",
        "/api/viz/dow", "/api/viz/escalation", "/api/viz/city_timeline",
        "/api/viz/alert_vs_ended", "/api/viz/safest_hours", "/api/viz/safest_10min",
        "/api/viz/city_safety_rank", "/api/viz/drone_cities",
        "/api/viz/city_zone_anomaly", "/api/viz/response_time",
        "/api/viz/multi_zone", "/api/viz/streaks", "/api/viz/calendar",
    ]

    @pytest.mark.parametrize("endpoint", ENDPOINTS)
    def test_viz_endpoint(self, client, endpoint):
        r = client.get(endpoint)
        assert r.status_code == 200
        d = json.loads(r.data)
        assert isinstance(d, list) or isinstance(d, dict)

    def test_viz_with_date_filter(self, client):
        r = client.get("/api/viz/daily?date_from=2024-01-01")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert isinstance(d, list)

    def test_top_cities_sort_asc(self, client):
        r = client.get("/api/viz/top_cities?limit=10&sort=asc")
        assert r.status_code == 200
        d = json.loads(r.data)
        counts = [c["alert_count"] for c in d]
        assert counts == sorted(counts), "ASC sort should return ascending order"

    def test_top_cities_sort_desc(self, client):
        r = client.get("/api/viz/top_cities?limit=10&sort=desc")
        assert r.status_code == 200
        d = json.loads(r.data)
        counts = [c["alert_count"] for c in d]
        assert counts == sorted(counts, reverse=True), "DESC sort should return descending order"


class TestHeadsUpCorrelation:
    def test_returns_valid_data(self, client):
        r = client.get("/api/viz/heads_up_correlation")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert d["total"] > 0
        assert d["hits"] > 0
        assert d["hit_rate_pct"] > 90  # should be ~99%
        assert "buckets" in d
        assert "monthly" in d

    def test_hit_rate_above_95(self, client):
        r = client.get("/api/viz/heads_up_correlation")
        d = json.loads(r.data)
        assert d["hit_rate_pct"] >= 95


class TestEventEndedAnalysis:
    def test_returns_valid_data(self, client):
        r = client.get("/api/viz/event_ended_analysis")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert d["total_alerts"] > 0
        assert "by_type" in d
        assert "monthly" in d

    def test_aircraft_has_high_clearance(self, client):
        r = client.get("/api/viz/event_ended_analysis")
        d = json.loads(r.data)
        aircraft = next((t for t in d["by_type"] if t["alert_type"] == "aircraft"), None)
        assert aircraft is not None
        assert aircraft["clear_pct"] > 90


class TestMetadataEndpoints:
    def test_stat_sql(self, client):
        r = client.get("/api/stat_sql")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert "attack_events" in d
        assert "sql" in d["attack_events"]
        assert "note" in d["attack_events"]

    def test_viz_meta(self, client):
        r = client.get("/api/viz/meta")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert len(d) >= 15
        for key, meta in d.items():
            assert "dimensions" in meta
            assert "measures" in meta
            assert "sql" in meta

    def test_transformations(self, client):
        r = client.get("/api/transformations")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert len(d) >= 8
        for t in d:
            assert "field" in t
            assert "formula" in t
            assert "purpose" in t


class TestDataJourney:
    def test_returns_diverse_examples(self, client):
        r = client.get("/api/data_journey/examples")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert len(d) >= 6
        types = {ex["type"] for ex in d}
        assert "single_city_rocket" in types
        assert "event_ended" in types
        assert "heads_up_message" in types

    def test_example_has_full_chain(self, client):
        r = client.get("/api/data_journey/examples")
        d = json.loads(r.data)
        for ex in d:
            assert "telegram" in ex
            assert "csv" in ex
            assert "db" in ex
            assert "counting" in ex
            assert "calculated_fields" in ex
            assert ex["telegram"]["raw_text"] is not None


class TestPipelineEndpoints:
    def test_versions(self, client):
        r = client.get("/api/pipeline/versions")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert "versions" in d
        assert len(d["versions"]) >= 1
        v = d["versions"][0]
        assert "runs" in v
        assert v["db_exists"] is True

    def test_validation_checks(self, client):
        checks = ["csv_files_exist", "no_duplicate_ids", "db_msg_count",
                   "no_orphan_details", "deltas_in_db", "db_version_match"]
        for check in checks:
            r = client.post(f"/api/pipeline/validate/{check}")
            assert r.status_code == 200
            d = json.loads(r.data)
            assert d["ok"] is True, f"{check} failed: {d.get('issues')}"

    def test_sample_check(self, client):
        r = client.post("/api/pipeline/sample_check")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert d["msg_id"] > 0
        assert d["csv_source"] is not None
        assert d["csv_row"] is not None
        assert d["db_record"]["raw_text"] is not None


class TestERDAndProfile:
    def test_erd(self, client):
        r = client.get("/api/viz/erd")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert "messages" in d["tables"]
        assert "alert_details" in d["tables"]
        assert d["tables"]["messages"]["count"] > 0

    def test_data_profile(self, client):
        r = client.get("/api/data_profile")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert d["cities_with_alerts"] > 0
        assert d["avg_cities_per_alert"] > 1


class TestAlertDrilldown:
    def test_drilldown_returns_alerts(self, client):
        r = client.get("/api/alerts/drilldown")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert d["total"] > 0
        assert len(d["alerts"]) > 0
        assert d["alerts"][0]["raw_text"] is not None

    def test_drilldown_newest_first(self, client):
        r = client.get("/api/alerts/drilldown?limit=10")
        d = json.loads(r.data)
        ids = [a["msg_id"] for a in d["alerts"]]
        assert ids == sorted(ids, reverse=True)

    def test_drilldown_with_city_filter(self, client):
        r = client.get("/api/alerts/drilldown?city=%D7%A7%D7%A8%D7%99%D7%99%D7%AA+%D7%A9%D7%9E%D7%95%D7%A0%D7%94&limit=5")
        d = json.loads(r.data)
        assert d["total"] > 0
        for a in d["alerts"]:
            city_names = [det["city"] for det in a["details"]]
            assert "קריית שמונה" in city_names

    def test_drilldown_has_details(self, client):
        r = client.get("/api/alerts/drilldown?limit=5")
        d = json.loads(r.data)
        for a in d["alerts"]:
            assert "details" in a
            assert len(a["details"]) > 0

    def test_drilldown_pagination(self, client):
        r1 = client.get("/api/alerts/drilldown?limit=5&offset=0")
        r2 = client.get("/api/alerts/drilldown?limit=5&offset=5")
        d1 = json.loads(r1.data)
        d2 = json.loads(r2.data)
        ids1 = {a["msg_id"] for a in d1["alerts"]}
        ids2 = {a["msg_id"] for a in d2["alerts"]}
        assert ids1.isdisjoint(ids2)  # no overlap


class TestSQLLatency:
    def test_latency_endpoint(self, client):
        # Generate some queries first
        client.get("/api/stats")
        client.get("/api/viz/daily")
        r = client.get("/api/pipeline/sql_latency")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert "percentiles" in d
        assert "top_slow" in d
        assert "log_path" in d


class TestSummary:
    """Tests for the mobile summary page and API."""

    def test_summary_page_renders(self, client):
        r = client.get("/summary")
        assert r.status_code == 200
        assert b"search" in r.data

    def test_summary_api_unfiltered(self, client):
        r = client.get("/api/summary")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert d["attack_events"] > 0
        assert d["total_cities"] > 1900  # ~1,998 after parser fix (was 2,156 with combined entries)
        assert d["latest"] is not None
        assert len(d["top_cities"]) == 10

    def test_summary_city_filter(self, client):
        r = client.get("/api/summary?city=%D7%A7%D7%A8%D7%99%D7%99%D7%AA+%D7%A9%D7%9E%D7%95%D7%A0%D7%94")
        d = json.loads(r.data)
        assert "city" in d
        assert d["city"]["name"] == "קריית שמונה"
        assert d["city"]["alerts"] > 0
        assert len(d["city"]["zones"]) > 0

    def test_summary_zone_filter(self, client):
        r = client.get("/api/summary?zone=%D7%90%D7%96%D7%95%D7%A8+%D7%A7%D7%95+%D7%94%D7%A2%D7%99%D7%9E%D7%95%D7%AA")
        d = json.loads(r.data)
        assert "zone" in d
        assert d["zone"]["alerts"] > 0
        assert d["zone"]["cities_affected"] > 0


class TestExport:
    """Test CSV data export functionality."""

    def test_full_export(self, client):
        import zipfile, io
        r = client.get("/api/export/full")
        assert r.status_code == 200
        assert r.content_type == "application/zip"
        z = zipfile.ZipFile(io.BytesIO(r.data))
        assert "with_calculated/messages.csv" in z.namelist()
        assert "with_calculated/manifest.json" in z.namelist()

    def test_raw_export(self, client):
        import zipfile, io
        r = client.get("/api/export/raw")
        assert r.status_code == 200
        z = zipfile.ZipFile(io.BytesIO(r.data))
        assert "raw_only/messages.csv" in z.namelist()
        # Verify calculated fields removed from CSV
        header = z.read("raw_only/messages.csv").decode().split("\n")[0]
        assert "datetime_israel" not in header
        assert "message_type" not in header
        assert "msg_id" in header

    def test_raw_manifest_has_tableau_formulas(self, client):
        import zipfile, io
        r = client.get("/api/export/raw")
        z = zipfile.ZipFile(io.BytesIO(r.data))
        manifest = json.loads(z.read("raw_only/manifest.json"))
        calc = manifest["tables"]["messages"].get("calculated_fields_to_add", {})
        assert "datetime_israel" in calc
        assert "tableau_formula" in calc["datetime_israel"]
        assert "message_type" in calc

    def test_full_manifest_has_relationships(self, client):
        import zipfile, io
        r = client.get("/api/export/full")
        z = zipfile.ZipFile(io.BytesIO(r.data))
        manifest = json.loads(z.read("with_calculated/manifest.json"))
        assert len(manifest["relationships"]) == 3
        assert manifest["tables"]["messages"]["row_count"] > 0
        assert "stream_name" in manifest["tables"]["messages"]

    def test_invalid_mode(self, client):
        r = client.get("/api/export/invalid")
        assert r.status_code == 400

    def test_full_export_all_tables(self, client):
        """Full export should contain all 4 tables as CSVs."""
        import zipfile, io
        r = client.get("/api/export/full")
        z = zipfile.ZipFile(io.BytesIO(r.data))
        for table in ["messages", "alert_details", "cities", "zones"]:
            assert f"with_calculated/{table}.csv" in z.namelist()

    def test_raw_export_all_tables(self, client):
        """Raw export should contain all 4 tables as CSVs."""
        import zipfile, io
        r = client.get("/api/export/raw")
        z = zipfile.ZipFile(io.BytesIO(r.data))
        for table in ["messages", "alert_details", "cities", "zones"]:
            assert f"raw_only/{table}.csv" in z.namelist()

    def test_full_export_has_calculated_fields(self, client):
        """Full export CSV should include calculated columns."""
        import zipfile, io
        r = client.get("/api/export/full")
        z = zipfile.ZipFile(io.BytesIO(r.data))
        header = z.read("with_calculated/messages.csv").decode().split("\n")[0]
        assert "message_type" in header
        assert "alert_type" in header
        assert "datetime_israel" in header
        assert "is_drill" in header

    def test_raw_export_cities_no_canonical(self, client):
        """Raw export should strip canonical_name from cities."""
        import zipfile, io
        r = client.get("/api/export/raw")
        z = zipfile.ZipFile(io.BytesIO(r.data))
        header = z.read("raw_only/cities.csv").decode().split("\n")[0]
        assert "canonical_name" not in header
        assert "city_id" in header
        assert "city_name" in header

    def test_manifest_cross_table_fields(self, client):
        """Manifest should include cross-table calculated fields."""
        import zipfile, io
        r = client.get("/api/export/raw")
        z = zipfile.ZipFile(io.BytesIO(r.data))
        manifest = json.loads(z.read("raw_only/manifest.json"))
        assert "cross_table_fields" in manifest
        names = [f["name"] for f in manifest["cross_table_fields"]]
        assert "City Display Name" in names
        assert "Is Real Alert" in names

    def test_manifest_has_tableau_next_names(self, client):
        """Raw manifest calculated fields should include Tableau Next field names."""
        import zipfile, io
        r = client.get("/api/export/raw")
        z = zipfile.ZipFile(io.BytesIO(r.data))
        manifest = json.loads(z.read("raw_only/manifest.json"))
        calc = manifest["tables"]["messages"]["calculated_fields_to_add"]
        for field_name, info in calc.items():
            assert "tableau_formula" in info
            assert "source_field" in info

    def test_export_csv_row_counts(self, client):
        """Export CSV row counts should match manifest row_count."""
        import zipfile, io, csv
        r = client.get("/api/export/full")
        z = zipfile.ZipFile(io.BytesIO(r.data))
        manifest = json.loads(z.read("with_calculated/manifest.json"))
        for table in ["messages", "alert_details", "cities", "zones"]:
            csv_data = z.read(f"with_calculated/{table}.csv").decode()
            reader = csv.reader(csv_data.strip().split("\n"))
            row_count = sum(1 for _ in reader) - 1  # minus header
            assert row_count == manifest["tables"][table]["row_count"]

    def test_export_primary_keys_marked(self, client):
        """Manifest should mark primary key fields."""
        import zipfile, io
        r = client.get("/api/export/full")
        z = zipfile.ZipFile(io.BytesIO(r.data))
        manifest = json.loads(z.read("with_calculated/manifest.json"))
        pk_map = {"messages": "msg_id", "alert_details": "id", "cities": "city_id", "zones": "zone_id"}
        for table, pk in pk_map.items():
            assert manifest["tables"][table]["fields"][pk].get("primary_key") is True


class TestTableauTab:
    """Test the Tableau Ready tab renders."""

    def test_tableau_tab_in_dashboard(self, client):
        """Dashboard should include Tableau Ready tab."""
        r = client.get("/")
        assert r.status_code == 200
        assert b"Tableau Ready" in r.data
        assert b"page-tableau" in r.data

    def test_tableau_tab_has_export_links(self, client):
        """Tableau tab should have download links."""
        r = client.get("/")
        assert b"/api/export/full" in r.data
        assert b"/api/export/raw" in r.data

    def test_tableau_tab_has_schema_info(self, client):
        """Tableau tab should show star schema documentation."""
        r = client.get("/")
        assert b"alert_details (FACT)" in r.data
        assert b"Star Schema" in r.data


class TestSQLQuery:
    def test_read_query(self, client):
        r = client.post("/api/query",
                        data=json.dumps({"sql": "SELECT COUNT(*) as c FROM messages"}),
                        content_type="application/json")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert d["count"] == 1
        assert d["rows"][0][0] > 0

    def test_write_blocked(self, client):
        r = client.post("/api/query",
                        data=json.dumps({"sql": "DELETE FROM messages"}),
                        content_type="application/json")
        assert r.status_code == 403
