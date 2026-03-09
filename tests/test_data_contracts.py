"""Tests for data_contracts.py — proactive data quality checks."""
import csv
import os
import sqlite3
import tempfile

import pytest

from data_contracts import (
    ContractViolation,
    check_post_build,
    check_pre_build,
)

# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def csv_dir(tmp_path):
    """Create a temp dir with a valid CSV file."""
    csv_file = tmp_path / "alerts_001.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["msg_id", "datetime_utc", "raw_text", "views"])
        writer.writerow(["1", "2026-03-01 12:00:00",
                         "🚨 ירי רקטות וטילים (1/3/2026) 12:00 **אזור דרום** אשדוד", "100"])
        writer.writerow(["2", "2026-03-01 12:05:00",
                         "🚨 האירוע הסתיים", "50"])
        writer.writerow(["3", "2026-03-01 13:00:00",
                         "🚨 ירי רקטות וטילים (1/3/2026) 13:00 **אזור צפון** חיפה", "200"])
    return tmp_path


@pytest.fixture
def good_db(tmp_path):
    """Create a valid SQLite DB matching the schema."""
    db_path = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE zones (zone_id INTEGER PRIMARY KEY, zone_name TEXT);
        CREATE TABLE cities (city_id INTEGER PRIMARY KEY, city_name TEXT, canonical_name TEXT);
        CREATE TABLE messages (
            msg_id INTEGER PRIMARY KEY, datetime_utc TEXT, datetime_israel TEXT,
            alert_date TEXT, alert_time_local TEXT, message_type TEXT NOT NULL,
            alert_type TEXT, is_drill INTEGER DEFAULT 0, raw_text TEXT, views TEXT
        );
        CREATE TABLE alert_details (
            id INTEGER PRIMARY KEY AUTOINCREMENT, msg_id INTEGER NOT NULL,
            zone_id INTEGER, city_id INTEGER, shelter_time TEXT
        );
    """)
    conn.execute("INSERT INTO zones VALUES (1, 'אזור דרום')")
    conn.execute("INSERT INTO zones VALUES (2, 'אזור צפון')")
    conn.execute("INSERT INTO cities VALUES (1, 'אשדוד', NULL)")
    conn.execute("INSERT INTO cities VALUES (2, 'חיפה', NULL)")
    conn.execute(
        "INSERT INTO messages VALUES (1, '2026-03-01 12:00:00', '2026-03-01 14:00:00', "
        "'1/3/2026', '12:00', 'alert', 'rockets', 0, 'ירי רקטות', '100')")
    conn.execute(
        "INSERT INTO messages VALUES (2, '2026-03-01 12:05:00', '2026-03-01 14:05:00', "
        "NULL, NULL, 'event_ended', NULL, 0, 'האירוע הסתיים', '50')")
    conn.execute(
        "INSERT INTO messages VALUES (3, '2026-03-01 13:00:00', '2026-03-01 15:00:00', "
        "'1/3/2026', '13:00', 'alert', 'rockets', 0, 'ירי רקטות', '200')")
    conn.execute("INSERT INTO alert_details (msg_id, zone_id, city_id, shelter_time) "
                 "VALUES (1, 1, 1, 'מיידי')")
    conn.execute("INSERT INTO alert_details (msg_id, zone_id, city_id, shelter_time) "
                 "VALUES (3, 2, 2, '15 שניות')")
    conn.commit()
    conn.close()
    return db_path


# ============================================================
# Pre-build contract tests
# ============================================================

class TestPreBuild:
    def test_valid_csvs_pass(self, csv_dir):
        """Valid CSV files should pass all pre-build checks."""
        check_pre_build(str(csv_dir))  # should not raise

    def test_no_csv_files_fails(self, tmp_path):
        """Empty directory should fail."""
        with pytest.raises(ContractViolation, match="No CSV files"):
            check_pre_build(str(tmp_path))

    def test_missing_columns_fails(self, tmp_path):
        """CSV missing required columns should fail."""
        csv_file = tmp_path / "alerts_001.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["msg_id", "some_other_field"])
            writer.writerow(["1", "test"])
        with pytest.raises(ContractViolation, match="missing required columns"):
            check_pre_build(str(tmp_path))

    def test_duplicate_msg_ids_fails(self, tmp_path):
        """Duplicate msg_ids across CSVs should fail."""
        for name in ["alerts_001.csv", "alerts_002.csv"]:
            csv_file = tmp_path / name
            with open(csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["msg_id", "datetime_utc", "raw_text", "views"])
                writer.writerow(["1", "2026-03-01 12:00:00", "some text", "10"])
        with pytest.raises(ContractViolation, match="duplicate msg_id 1"):
            check_pre_build(str(tmp_path))

    def test_empty_raw_text_fails(self, tmp_path):
        """Empty raw_text should fail."""
        csv_file = tmp_path / "alerts_001.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["msg_id", "datetime_utc", "raw_text", "views"])
            writer.writerow(["1", "2026-03-01 12:00:00", "", "10"])
        with pytest.raises(ContractViolation, match="empty raw_text"):
            check_pre_build(str(tmp_path))

    def test_non_numeric_msg_id_fails(self, tmp_path):
        """Non-numeric msg_id should fail."""
        csv_file = tmp_path / "alerts_001.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["msg_id", "datetime_utc", "raw_text", "views"])
            writer.writerow(["abc", "2026-03-01 12:00:00", "text", "10"])
        with pytest.raises(ContractViolation, match="non-numeric msg_id"):
            check_pre_build(str(tmp_path))

    def test_multiple_violations_collected(self, tmp_path):
        """Multiple violations should all be collected in one exception."""
        csv_file = tmp_path / "alerts_001.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["msg_id", "datetime_utc", "raw_text", "views"])
            writer.writerow(["abc", "2026-03-01 12:00:00", "", "10"])
            writer.writerow(["1", "2026-03-01 12:00:00", "", "10"])
        with pytest.raises(ContractViolation) as exc_info:
            check_pre_build(str(tmp_path))
        assert len(exc_info.value.violations) >= 2


# ============================================================
# Post-build contract tests
# ============================================================

class TestPostBuild:
    def test_valid_db_passes(self, good_db):
        """Valid database should pass all post-build checks."""
        check_post_build(good_db)  # should not raise

    def test_orphaned_alert_details_msg_id(self, good_db):
        """alert_details referencing non-existent msg_id should fail."""
        conn = sqlite3.connect(good_db)
        conn.execute("INSERT INTO alert_details (msg_id, zone_id, city_id) "
                     "VALUES (999, 1, 1)")
        conn.commit()
        conn.close()
        with pytest.raises(ContractViolation, match="non-existent msg_id"):
            check_post_build(good_db)

    def test_orphaned_alert_details_zone_id(self, good_db):
        """alert_details referencing non-existent zone_id should fail."""
        conn = sqlite3.connect(good_db)
        conn.execute("INSERT INTO alert_details (msg_id, zone_id, city_id) "
                     "VALUES (1, 999, 1)")
        conn.commit()
        conn.close()
        with pytest.raises(ContractViolation, match="non-existent zone_id"):
            check_post_build(good_db)

    def test_orphaned_alert_details_city_id(self, good_db):
        """alert_details referencing non-existent city_id should fail."""
        conn = sqlite3.connect(good_db)
        conn.execute("INSERT INTO alert_details (msg_id, zone_id, city_id) "
                     "VALUES (1, 1, 999)")
        conn.commit()
        conn.close()
        with pytest.raises(ContractViolation, match="non-existent city_id"):
            check_post_build(good_db)

    def test_alert_without_details_fails(self, good_db):
        """Alert message with no alert_details rows should fail."""
        conn = sqlite3.connect(good_db)
        conn.execute(
            "INSERT INTO messages VALUES (10, '2026-03-02 12:00:00', "
            "'2026-03-02 14:00:00', '2/3/2026', '12:00', 'alert', "
            "'rockets', 0, 'ירי רקטות', '100')")
        conn.commit()
        conn.close()
        with pytest.raises(ContractViolation, match="zero alert_details"):
            check_post_build(good_db)

    def test_bad_date_format_fails(self, good_db):
        """Unparseable alert_date should fail."""
        conn = sqlite3.connect(good_db)
        conn.execute(
            "INSERT INTO messages VALUES (10, '2026-03-02', "
            "'2026-03-02', 'not-a-date', '12:00', 'alert', "
            "'rockets', 0, 'ירי רקטות', '100')")
        conn.execute("INSERT INTO alert_details (msg_id, zone_id, city_id) "
                     "VALUES (10, 1, 1)")
        conn.commit()
        conn.close()
        with pytest.raises(ContractViolation, match="unparseable alert_date"):
            check_post_build(good_db)

    def test_null_zone_id_fails(self, good_db):
        """NULL zone_id in alert_details should fail."""
        conn = sqlite3.connect(good_db)
        conn.execute("INSERT INTO alert_details (msg_id, zone_id, city_id) "
                     "VALUES (1, NULL, 1)")
        conn.commit()
        conn.close()
        with pytest.raises(ContractViolation, match="NULL zone_id"):
            check_post_build(good_db)

    def test_null_city_id_fails(self, good_db):
        """NULL city_id in alert_details should fail."""
        conn = sqlite3.connect(good_db)
        conn.execute("INSERT INTO alert_details (msg_id, zone_id, city_id) "
                     "VALUES (1, 1, NULL)")
        conn.commit()
        conn.close()
        with pytest.raises(ContractViolation, match="NULL city_id"):
            check_post_build(good_db)

    def test_unknown_message_type_fails(self, good_db):
        """Unknown message_type should fail."""
        conn = sqlite3.connect(good_db)
        conn.execute(
            "INSERT INTO messages VALUES (10, '2026-03-02', "
            "'2026-03-02', NULL, NULL, 'bogus_type', "
            "NULL, 0, 'some text', '100')")
        conn.commit()
        conn.close()
        with pytest.raises(ContractViolation, match="Unknown message_type"):
            check_post_build(good_db)

    def test_event_ended_without_details_ok(self, good_db):
        """Non-alert messages without alert_details should pass."""
        # msg_id 2 is event_ended with no details — should be fine
        check_post_build(good_db)


class TestContractViolation:
    def test_stores_violations_list(self):
        """ContractViolation should store the list of violations."""
        v = ContractViolation(["error1", "error2"])
        assert len(v.violations) == 2
        assert "error1" in v.violations
        assert "2 contract violation(s)" in str(v)
