#!/usr/bin/env python3
"""
Pikud HaOref Alert Pipeline
============================

Main entry point for all operations:
  pikud.py delta          - Fetch new messages, validate, rebuild DB
  pikud.py full_refresh   - Full re-download into new data version, rebuild DB
  pikud.py rebuild_db     - Rebuild DB from existing CSVs (no fetch)
  pikud.py validate       - Run validation checks on current data + DB
  pikud.py status         - Show current state: versions, counts, last run

Data is organized by versions:
  data/
    v1/                   ← data version folder
      alerts_9_24251.csv  ← initial load
      alerts_24252_24300.csv ← delta 1
      metadata.json       ← tracks runs, last_msg_id
    v2/                   ← after full_refresh
      alerts_9_24500.csv
      metadata.json
    current -> v1         ← symlink to active version
  db/
    pikud_v1.db           ← DB built from v1 data
    pikud_v2.db
    current -> pikud_v1.db
"""

import argparse
import csv

from dotenv import load_dotenv

load_dotenv()
import json
import os
import re
import shutil
import sqlite3
import sys
from datetime import datetime, timezone

# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_DIR = os.path.join(BASE_DIR, "db")
SESSION_FILE = os.path.join(BASE_DIR, "pikud_session")
CHANNEL = "PikudHaOref_all"

API_ID = os.environ.get("TELEGRAM_API_ID", "")
API_HASH = os.environ.get("TELEGRAM_API_HASH", "")


# ============================================================
# VERSION MANAGEMENT
# ============================================================


def get_current_version():
    """Get the current active data version number."""
    link = os.path.join(DATA_DIR, "current")
    if os.path.islink(link):
        target = os.readlink(link)
        return target.replace("v", "")
    # Find highest existing version
    versions = (
        [d for d in os.listdir(DATA_DIR) if d.startswith("v") and d[1:].isdigit()] if os.path.exists(DATA_DIR) else []
    )
    if versions:
        return str(max(int(v[1:]) for v in versions))
    return None


def get_version_dir(version=None):
    """Get the data directory for a version (default: current)."""
    if version is None:
        version = get_current_version()
    if version is None:
        return None
    return os.path.join(DATA_DIR, f"v{version}")


def get_db_path(version=None):
    if version is None:
        version = get_current_version()
    return os.path.join(DB_DIR, f"pikud_v{version}.db")


def create_new_version():
    """Create a new data version folder and update symlink."""
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(DB_DIR, exist_ok=True)

    current = get_current_version()
    new_ver = str(int(current) + 1) if current else "1"
    new_dir = os.path.join(DATA_DIR, f"v{new_ver}")
    os.makedirs(new_dir, exist_ok=True)

    _update_symlink(DATA_DIR, "current", f"v{new_ver}")
    return new_ver, new_dir


def _update_symlink(parent, link_name, target):
    link_path = os.path.join(parent, link_name)
    if os.path.islink(link_path):
        os.remove(link_path)
    os.symlink(target, link_path)


def init_version():
    """Initialize version 1 if no versions exist. Migrate legacy data/ layout."""
    current = get_current_version()
    if current:
        return current, get_version_dir(current)

    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(DB_DIR, exist_ok=True)

    ver_dir = os.path.join(DATA_DIR, "v1")
    os.makedirs(ver_dir, exist_ok=True)

    # Migrate legacy files from data/ root into v1/
    for f in os.listdir(DATA_DIR):
        fpath = os.path.join(DATA_DIR, f)
        if os.path.isfile(fpath) and (f.endswith(".csv") or f == "metadata.json"):
            shutil.move(fpath, os.path.join(ver_dir, f))
            print(f"  Migrated {f} → v1/{f}")

    # Migrate legacy DB
    legacy_db = os.path.join(DATA_DIR, "pikud.db")
    if os.path.exists(legacy_db):
        shutil.move(legacy_db, os.path.join(DB_DIR, "pikud_v1.db"))
        print("  Migrated pikud.db → db/pikud_v1.db")

    _update_symlink(DATA_DIR, "current", "v1")
    _update_symlink(DB_DIR, "current", "pikud_v1.db")
    return "1", ver_dir


# ============================================================
# METADATA
# ============================================================


def load_metadata(ver_dir):
    meta_path = os.path.join(ver_dir, "metadata.json")
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"runs": [], "last_msg_id": 0, "total_messages": 0}


def save_metadata(ver_dir, metadata):
    meta_path = os.path.join(ver_dir, "metadata.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)


# ============================================================
# FETCH (TELEGRAM API)
# ============================================================


async def fetch_messages(min_id=0):
    """Fetch messages from Telegram API. Returns list of dicts."""
    from telethon import TelegramClient

    if not API_ID or not API_HASH:
        print("Error: Set TELEGRAM_API_ID and TELEGRAM_API_HASH environment variables.")
        return None

    client = TelegramClient(SESSION_FILE, int(API_ID), API_HASH)
    await client.start()

    if min_id:
        print(f"  Fetching messages after ID {min_id}...")
    else:
        print(f"  Fetching ALL messages from @{CHANNEL}...")

    rows = []
    count = 0
    async for msg in client.iter_messages(CHANNEL, min_id=min_id):
        if not msg.text:
            continue
        count += 1
        text = re.sub(r"\s+", " ", msg.text.strip())
        rows.append(
            {
                "msg_id": msg.id,
                "datetime_utc": msg.date.strftime("%Y-%m-%d %H:%M:%S") if msg.date else "",
                "raw_text": text,
                "views": msg.views or "",
            }
        )
        if count % 500 == 0:
            print(f"    {count} messages...")

    await client.disconnect()
    rows.sort(key=lambda r: r["msg_id"])
    return rows


def save_delta_csv(ver_dir, rows):
    """Save rows to a CSV named by ID range. Returns filename."""
    start_id = rows[0]["msg_id"]
    end_id = rows[-1]["msg_id"]
    filename = f"alerts_{start_id}_{end_id}.csv"
    filepath = os.path.join(ver_dir, filename)

    fieldnames = ["msg_id", "datetime_utc", "raw_text", "views"]
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  Saved {len(rows)} rows → {filename}")
    return filename


# ============================================================
# PARSING (used by build_db)
# ============================================================


def classify_message(text: str) -> tuple[str, str | None, bool]:
    is_drill = "תרגיל" in text
    if "האירוע הסתיים" in text:
        msg_type = "event_ended"
    elif "בדקות הקרובות" in text:
        msg_type = "heads_up"
    elif "ניתן לצאת" in text:
        msg_type = "can_leave_shelter"
    elif "מבזק" in text:
        msg_type = "flash"
    elif any(k in text for k in ["ירי רקטות", "חדירת כלי טיס", "רעידת אדמה", "חומרים מסוכנים", "חדירת מחבלים"]):
        msg_type = "alert"
    elif "עדכון" in text:
        msg_type = "update"
    elif any(k in text for k in ["שיגור", "יירוט"]):
        msg_type = "intercept_report"
    elif any(k in text for k in ["הנחיות", "מגבלות", "הגבלות"]):
        msg_type = "instructions"
    else:
        msg_type = "other"

    if "ירי רקטות" in text:
        alert_type = "rockets"
    elif "חדירת כלי טיס עוין" in text:
        alert_type = "aircraft"
    elif "רעידת אדמה" in text:
        alert_type = "earthquake"
    elif "חומרים מסוכנים" in text:
        alert_type = "hazmat"
    elif "חדירת מחבלים" in text or "מחבלים" in text:
        alert_type = "infiltration"
    elif "צונאמי" in text:
        alert_type = "tsunami"
    else:
        alert_type = None

    return msg_type, alert_type, is_drill


def extract_date_time(text: str) -> tuple[str | None, str | None]:
    m = re.search(r"[\[\(](\d{1,2}/\d{1,2}/\d{4})[\]\)]\s*(?:בשעה\s*)?(\d{1,2}:\d{2})", text)
    if m:
        return m.group(1), m.group(2)
    return None, None


def extract_zones_and_cities(text: str) -> list[tuple[str, list[tuple[str, str | None]]]]:
    results = []
    zone_splits = re.split(r"\*{2}(אזור\s+[^*]+?)\*{2}", text)
    i = 1
    while i < len(zone_splits) - 1:
        zone_name = zone_splits[i].strip()
        cities_text = zone_splits[i + 1] if i + 1 < len(zone_splits) else ""
        cities_text = re.split(r"היכנסו|חשד\s+לכניסת|להנחיות|לשאלות|כדי\s+שנהיה|בעקבות\s+כניסת", cities_text)[0]
        cities = parse_cities(cities_text)
        if cities:
            results.append((zone_name, cities))
        i += 2

    if not results and "**" not in text and "אזור" in text:
        known_zones = r"קו העימות|עוטף עזה|גליל עליון|לכיש|מערב לכיש|השפלה|דן|מערב הנגב|המפרץ|שרון|ירקון|מרכז הנגב|שומרון|גולן דרום|מרכז הגליל|שפלת יהודה|העמקים|יהודה|גולן צפון|דרום הנגב|ירושלים|מנשה|בקעת בית שאן|ואדי ערה|גליל תחתון|בקעה|דרום השפלה|ים המלח|הכרמל|ערבה|אילת|חפר|בית שמש"
        for m in re.finditer(rf"(אזור\s+(?:{known_zones}))\s+(.+?)(?=אזור\s|$)", text):
            zone_name = m.group(1).strip()
            cities = parse_cities(m.group(2).strip())
            if cities:
                results.append((zone_name, cities))

    return results


# Known multi-word city name prefixes (these should NOT be split on space)
_MULTI_WORD_PREFIXES = [
    "קריית",
    "כפר",
    "בית",
    "תל",
    "באר",
    "ראש",
    "גבעת",
    "מעלה",
    "מעלות",
    "שדה",
    "עין",
    "אבן",
    "גן",
    "נווה",
    "נאות",
    "רמת",
    "אור",
    "מצפה",
    "נוף",
    "אזור תעשייה",
    "פארק תעשיות",
    "מרכז אזורי",
    "קיבוץ",
    "מושב",
    "בני",
    "בן",
    "הר",
    "מגדל",
    "נחל",
    "צומת",
    "מעגן",
    "יד",
    "גבעות",
    "גשר",
    "עמק",
    "מעיין",
    "פנימיית",
    "מכללת",
    "שער",
]

# Known compound city names (4+ words) starting with multi-word prefixes.
# The prefix+1 rule in _split_space_separated_cities truncates these.
# Sorted longest-first for greedy matching.
_KNOWN_COMPOUND_CITIES = sorted(
    [
        # אזור תעשייה + multi-word suffix
        "אזור תעשייה אכזיב מילואות",
        "אזור תעשייה אלון התבור",
        "אזור תעשייה אפק ולב הארץ",
        "אזור תעשייה באר טוביה",
        "אזור תעשייה בני יהודה",
        "אזור תעשייה הדרומי אשקלון",
        "אזור תעשייה הר טוב",
        "אזור תעשייה חבל מודיעין",
        "אזור תעשייה חבל מודיעין שוהם",
        "אזור תעשייה חצור הגלילית",
        "אזור תעשייה יקנעם עילית",
        "אזור תעשייה כפר יונה",
        "אזור תעשייה מבוא כרמל",
        "אזור תעשייה מבואות הגלבוע",
        "אזור תעשייה מילואות צפון",
        "אזור תעשייה מישור אדומים",
        "אזור תעשייה ניר עציון",
        "אזור תעשייה עד הלום",
        "אזור תעשייה עידן הנגב",
        "אזור תעשייה עמק חפר",
        "אזור תעשייה צפוני אשקלון",
        "אזור תעשייה קדמת גליל",
        "אזור תעשייה קריית ביאליק",
        "אזור תעשייה קריית גת",
        "אזור תעשייה רמת דלתון",
        "אזור תעשייה שער בנימין",
        "אזור תעשייה שער נעמן",
        # מרכז אזורי + multi-word suffix
        "מרכז אזורי דרום השרון",
        "מרכז אזורי מבואות חרמון",
        "מרכז אזורי מרום גליל",
        "מרכז אזורי רמת כורזים",
        # פארק תעשיות + multi-word suffix
        "פארק תעשיות מגדל עוז",
    ],
    key=lambda x: len(x.split()),
    reverse=True,
)


def parse_cities(text: str) -> list[tuple[str, str | None]]:
    cities = []
    text = text.replace("**", "").strip()
    stop_words = [
        "היכנסו",
        "להנחיות",
        "לשאלות",
        "כדי",
        "בעקבות",
        "שהו",
        "ניתן",
        "באזורים",
        "בהמשך",
        "למשך",
        "השוהים",
        "על תושבי",
        "עליך",
        "למרחב",
        "המוגן",
        "דקות",
        "לפעול",
        "בהתאם",
        "המלאות",
        "התרעה",
    ]

    # Phase 1: Try splitting by shelter-time parentheses (most reliable)
    parts = re.split(r"\(([^)]*(?:שניות|דקה|דקות|מיידי)[^)]*)\)", text)
    has_shelter = len(parts) > 1
    has_comma = "," in text or "،" in text

    if has_shelter:
        # Shelter-time delimited: split each text block by comma
        for j in range(0, len(parts)):
            if j % 2 == 0:
                city_names = re.split(r"[,،]", parts[j])
                shelter_time = parts[j + 1].strip() if j + 1 < len(parts) else None
                for cn in city_names:
                    cn = cn.strip().strip("*").strip()
                    if cn and len(cn) > 1 and not any(k in cn for k in stop_words):
                        cities.append((cn, shelter_time))
    elif has_comma:
        # Comma-separated (no shelter times)
        for cn in re.split(r"[,،]", text):
            cn = cn.strip().strip("*").strip()
            if cn and len(cn) > 1 and not any(k in cn for k in stop_words):
                cities.append((cn, None))
    else:
        # Space-separated cities (common in aircraft alerts with no commas or shelter times)
        cities = _split_space_separated_cities(text, stop_words)

    return cities


def _split_space_separated_cities(text: str, stop_words: list[str]) -> list[tuple[str, str | None]]:
    """Split space-separated city names, respecting known multi-word prefixes."""
    words = text.split()
    cities = []
    i = 0
    while i < len(words):
        word = words[i].strip().strip("*").strip()
        if not word or word.isdigit() or any(k in word for k in stop_words):
            i += 1
            continue

        # Check if this word starts a known compound city name (longest match)
        matched_multi = False
        for known in _KNOWN_COMPOUND_CITIES:
            known_words = known.split()
            if i + len(known_words) <= len(words):
                candidate = " ".join(words[i : i + len(known_words)])
                if candidate == known:
                    city_name = known
                    i += len(known_words)
                    # Check for dash-suffix (e.g. "אזור תעשייה הר טוב - צרעה")
                    if i + 1 < len(words) and words[i] == "-":
                        city_name = f"{city_name} - {words[i + 1]}"
                        i += 2
                    cities.append((city_name, None))
                    matched_multi = True
                    break

        # Check if this word starts a known multi-word prefix
        if not matched_multi:
            for prefix in _MULTI_WORD_PREFIXES:
                prefix_words = prefix.split()
                if i + len(prefix_words) <= len(words):
                    candidate = " ".join(words[i : i + len(prefix_words)])
                    if candidate == prefix or candidate.startswith(prefix):
                        # Multi-word prefix: take prefix + 1 more word (e.g. "קריית שמונה", "בית שאן")
                        end = i + len(prefix_words)
                        if end < len(words) and not any(k in words[end] for k in stop_words):
                            city_name = " ".join(words[i : end + 1]).strip().strip("*").strip()
                            i = end + 1
                            # Check for dash-suffix (e.g. "אזור תעשייה נשר - רמלה")
                            if i + 1 < len(words) and words[i] == "-":
                                city_name = f"{city_name} - {words[i + 1]}"
                                i += 2
                            if city_name and len(city_name) > 1:
                                cities.append((city_name, None))
                            matched_multi = True
                            break
                        else:
                            city_name = candidate.strip().strip("*").strip()
                            if city_name and len(city_name) > 1:
                                cities.append((city_name, None))
                            i += len(prefix_words)
                            matched_multi = True
                            break

        if not matched_multi:
            # Single word city name — skip very short words (likely prepositions)
            if word and len(word) > 2 and not any(k in word for k in stop_words):
                # Check if next word is a dash-suffix (e.g. "חיפה - מערב")
                if i + 2 < len(words) and words[i + 1] == "-":
                    city_name = f"{word} - {words[i + 2]}"
                    cities.append((city_name, None))
                    i += 3
                else:
                    cities.append((word, None))
                    i += 1
            else:
                i += 1

    return cities


# ============================================================
# BUILD DATABASE
# ============================================================

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS db_info (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS zones (
    zone_id INTEGER PRIMARY KEY AUTOINCREMENT,
    zone_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS cities (
    city_id INTEGER PRIMARY KEY AUTOINCREMENT,
    city_name TEXT UNIQUE NOT NULL,
    canonical_name TEXT  -- normalized display name (NULL = city_name is canonical)
);

CREATE TABLE IF NOT EXISTS messages (
    msg_id INTEGER PRIMARY KEY,
    datetime_utc TEXT,
    datetime_israel TEXT,
    alert_date TEXT,
    alert_time_local TEXT,
    message_type TEXT NOT NULL,
    alert_type TEXT,
    is_drill INTEGER DEFAULT 0,
    raw_text TEXT,
    views TEXT
);

CREATE TABLE IF NOT EXISTS alert_details (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    msg_id INTEGER NOT NULL,
    zone_id INTEGER,
    city_id INTEGER,
    shelter_time TEXT,
    FOREIGN KEY (msg_id) REFERENCES messages(msg_id),
    FOREIGN KEY (zone_id) REFERENCES zones(zone_id),
    FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

-- Single-column indexes for simple lookups
CREATE INDEX IF NOT EXISTS idx_messages_datetime ON messages(datetime_utc);
CREATE INDEX IF NOT EXISTS idx_messages_datetime_il ON messages(datetime_israel);
CREATE INDEX IF NOT EXISTS idx_messages_type ON messages(message_type);
CREATE INDEX IF NOT EXISTS idx_messages_alert_type ON messages(alert_type);
CREATE INDEX IF NOT EXISTS idx_messages_drill ON messages(is_drill);
CREATE INDEX IF NOT EXISTS idx_alert_details_msg ON alert_details(msg_id);
CREATE INDEX IF NOT EXISTS idx_alert_details_city ON alert_details(city_id);
CREATE INDEX IF NOT EXISTS idx_alert_details_zone ON alert_details(zone_id);

-- Composite indexes for dashboard query patterns
CREATE INDEX IF NOT EXISTS idx_messages_alert_filter ON messages(message_type, is_drill, datetime_israel, alert_type);
CREATE INDEX IF NOT EXISTS idx_messages_alert_date ON messages(message_type, is_drill, datetime_israel);
CREATE INDEX IF NOT EXISTS idx_ad_msg_city_zone ON alert_details(msg_id, city_id, zone_id);
CREATE INDEX IF NOT EXISTS idx_ad_msg_zone ON alert_details(msg_id, zone_id);
CREATE INDEX IF NOT EXISTS idx_ad_city_msg ON alert_details(city_id, msg_id);
CREATE INDEX IF NOT EXISTS idx_messages_ended_time ON messages(message_type, datetime_utc) WHERE message_type='event_ended';
CREATE INDEX IF NOT EXISTS idx_messages_type_time ON messages(message_type, datetime_utc);

CREATE VIEW IF NOT EXISTS v_alerts_full AS
SELECT m.msg_id, m.datetime_utc, m.datetime_israel, m.alert_date, m.alert_time_local,
       m.message_type, m.alert_type, m.is_drill,
       z.zone_name, c.city_name, ad.shelter_time, m.views
FROM alert_details ad
JOIN messages m ON ad.msg_id = m.msg_id
LEFT JOIN zones z ON ad.zone_id = z.zone_id
LEFT JOIN cities c ON ad.city_id = c.city_id;

CREATE VIEW IF NOT EXISTS v_city_alert_counts AS
SELECT c.city_name, z.zone_name, COUNT(*) as alert_count,
       SUM(CASE WHEN m.alert_type = 'rockets' THEN 1 ELSE 0 END) as rocket_count,
       SUM(CASE WHEN m.alert_type = 'aircraft' THEN 1 ELSE 0 END) as aircraft_count,
       MIN(m.datetime_israel) as first_alert, MAX(m.datetime_israel) as last_alert
FROM alert_details ad
JOIN messages m ON ad.msg_id = m.msg_id
JOIN cities c ON ad.city_id = c.city_id
LEFT JOIN zones z ON ad.zone_id = z.zone_id
WHERE m.message_type = 'alert' AND m.is_drill = 0
GROUP BY c.city_name, z.zone_name;

CREATE VIEW IF NOT EXISTS v_hourly_distribution AS
SELECT CAST(strftime('%H', datetime_israel) AS INTEGER) as hour_israel,
       COUNT(*) as alert_count, alert_type
FROM messages WHERE message_type = 'alert' AND is_drill = 0
GROUP BY hour_israel, alert_type;

CREATE VIEW IF NOT EXISTS v_daily_counts AS
SELECT date(datetime_israel) as alert_date, COUNT(*) as total_messages,
       SUM(CASE WHEN message_type = 'alert' THEN 1 ELSE 0 END) as alerts,
       SUM(CASE WHEN message_type = 'event_ended' THEN 1 ELSE 0 END) as events_ended,
       SUM(CASE WHEN alert_type = 'rockets' THEN 1 ELSE 0 END) as rockets,
       SUM(CASE WHEN alert_type = 'aircraft' THEN 1 ELSE 0 END) as aircraft
FROM messages WHERE is_drill = 0 GROUP BY alert_date;
"""


def build_database(ver_dir: str, db_path: str, version: str) -> bool:
    """Build SQLite DB from all CSVs in a version directory."""
    csv_files = sorted(f for f in os.listdir(ver_dir) if f.startswith("alerts_") and f.endswith(".csv"))
    if not csv_files:
        print("  No CSV files found.")
        return False

    # Pre-build data contracts — fail fast before touching the DB
    from data_contracts import check_pre_build, check_post_build, ContractViolation
    try:
        check_pre_build(ver_dir)
        print("  Pre-build contracts: PASSED")
    except ContractViolation as e:
        print(f"  Pre-build contracts: FAILED\n{e}")
        return False

    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    conn.executescript(DB_SCHEMA)

    # Store build info
    metadata = load_metadata(ver_dir)
    build_info = {
        "data_version": version,
        "built_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "csv_files": csv_files,
        "source_metadata": json.dumps(metadata),
    }
    for k, v in build_info.items():
        conn.execute(
            "INSERT OR REPLACE INTO db_info (key, value) VALUES (?, ?)", (k, v if isinstance(v, str) else json.dumps(v))
        )

    zone_cache = {}
    city_cache = {}

    def get_zone_id(name):
        if name not in zone_cache:
            conn.execute("INSERT OR IGNORE INTO zones (zone_name) VALUES (?)", (name,))
            zone_cache[name] = conn.execute("SELECT zone_id FROM zones WHERE zone_name=?", (name,)).fetchone()[0]
        return zone_cache[name]

    def get_city_id(name):
        if name not in city_cache:
            conn.execute("INSERT OR IGNORE INTO cities (city_name) VALUES (?)", (name,))
            city_cache[name] = conn.execute("SELECT city_id FROM cities WHERE city_name=?", (name,)).fetchone()[0]
        return city_cache[name]

    total_msgs = 0
    total_details = 0

    for csv_file in csv_files:
        filepath = os.path.join(ver_dir, csv_file)
        print(f"  Processing {csv_file}...")

        with open(filepath, "r", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                msg_id = int(row["msg_id"])
                text = row["raw_text"]
                msg_type, alert_type, is_drill = classify_message(text)
                alert_date, alert_time = extract_date_time(text)

                # Compute Israel time (UTC+2 winter, UTC+3 summer — approximate with +2)
                utc_str = row["datetime_utc"]
                israel_str = None
                if utc_str:
                    try:
                        from datetime import datetime as _dt
                        from datetime import timedelta

                        utc_dt = _dt.strptime(utc_str, "%Y-%m-%d %H:%M:%S")
                        # Israel: UTC+2 (IST) Oct-Mar, UTC+3 (IDT) Mar-Oct. Approximate:
                        month = utc_dt.month
                        offset = 3 if 4 <= month <= 10 else 2
                        israel_dt = utc_dt + timedelta(hours=offset)
                        israel_str = israel_dt.strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        israel_str = utc_str

                conn.execute(
                    """INSERT OR REPLACE INTO messages
                       (msg_id, datetime_utc, datetime_israel, alert_date, alert_time_local, message_type, alert_type, is_drill, raw_text, views)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        msg_id,
                        utc_str,
                        israel_str,
                        alert_date,
                        alert_time,
                        msg_type,
                        alert_type,
                        1 if is_drill else 0,
                        text,
                        row.get("views", ""),
                    ),
                )
                total_msgs += 1

                for zone_name, cities in extract_zones_and_cities(text):
                    zone_id = get_zone_id(zone_name)
                    for city_name, shelter_time in cities:
                        city_id = get_city_id(city_name)
                        conn.execute(
                            "INSERT INTO alert_details (msg_id, zone_id, city_id, shelter_time) VALUES (?, ?, ?, ?)",
                            (msg_id, zone_id, city_id, shelter_time),
                        )
                        total_details += 1

                if total_msgs % 5000 == 0:
                    conn.commit()

    conn.commit()

    # Canonicalize city names: detect dash-vs-space variants, map to most common spelling
    canonical_count = 0
    all_cities = {r[0]: r[1] for r in conn.execute("SELECT city_id, city_name FROM cities").fetchall()}
    name_to_id = {v: k for k, v in all_cities.items()}
    for cid, cname in all_cities.items():
        if "-" in cname:
            space_variant = cname.replace("-", " ")
            if space_variant in name_to_id and space_variant != cname:
                # Both exist — pick the one with more alert_details as canonical
                dash_count = conn.execute("SELECT COUNT(*) FROM alert_details WHERE city_id=?", (cid,)).fetchone()[0]
                space_count = conn.execute(
                    "SELECT COUNT(*) FROM alert_details WHERE city_id=?", (name_to_id[space_variant],)
                ).fetchone()[0]
                canonical = space_variant if space_count >= dash_count else cname
                conn.execute("UPDATE cities SET canonical_name=? WHERE city_id=?", (canonical, cid))
                conn.execute(
                    "UPDATE cities SET canonical_name=? WHERE city_id=?", (canonical, name_to_id[space_variant])
                )
                canonical_count += 1
    if canonical_count:
        conn.commit()
        print(f"  Canonicalized: {canonical_count} city name pairs unified")

    # Update DB symlink
    _update_symlink(DB_DIR, "current", os.path.basename(db_path))

    print(f"  DB built: {total_msgs} messages, {total_details} alert details")
    print(f"  Zones: {len(zone_cache)}, Cities: {len(city_cache)}")
    conn.close()

    # Post-build data contracts — catch pipeline logic errors
    try:
        check_post_build(db_path)
        print("  Post-build contracts: PASSED")
    except ContractViolation as e:
        print(f"  Post-build contracts: FAILED\n{e}")
        return False

    return True


# ============================================================
# VALIDATION
# ============================================================


def validate(ver_dir: str, db_path: str) -> tuple[bool, list[str]]:
    """Run validation checks. Returns (ok: bool, issues: list[str])."""
    issues = []
    metadata = load_metadata(ver_dir)

    # 1. Check all CSVs referenced in metadata exist
    for run in metadata.get("runs", []):
        csv_path = os.path.join(ver_dir, run["filename"])
        if not os.path.exists(csv_path):
            issues.append(f"MISSING CSV: {run['filename']}")

    # 2. Count rows across all CSVs
    csv_files = sorted(f for f in os.listdir(ver_dir) if f.startswith("alerts_") and f.endswith(".csv"))
    total_csv_rows = 0
    all_msg_ids = set()
    for csv_file in csv_files:
        with open(os.path.join(ver_dir, csv_file), "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_csv_rows += 1
                mid = int(row["msg_id"])
                if mid in all_msg_ids:
                    issues.append(f"DUPLICATE msg_id {mid} in CSVs")
                all_msg_ids.add(mid)

    expected_total = metadata.get("total_messages", 0)
    if total_csv_rows != expected_total:
        issues.append(f"CSV row count mismatch: CSVs have {total_csv_rows}, metadata says {expected_total}")

    # 3. Check metadata.last_msg_id matches highest CSV msg_id
    if all_msg_ids:
        max_csv_id = max(all_msg_ids)
        meta_last = metadata.get("last_msg_id", 0)
        if max_csv_id != meta_last:
            issues.append(f"last_msg_id mismatch: max CSV id={max_csv_id}, metadata={meta_last}")

    # 4. Check ID ranges don't overlap between CSVs
    csv_ranges = []
    for run in metadata.get("runs", []):
        csv_ranges.append((run["start_msg_id"], run["end_msg_id"], run["filename"]))
    csv_ranges.sort()
    for j in range(1, len(csv_ranges)):
        if csv_ranges[j][0] <= csv_ranges[j - 1][1]:
            issues.append(
                f"OVERLAP: {csv_ranges[j - 1][2]} ends at {csv_ranges[j - 1][1]}, {csv_ranges[j][2]} starts at {csv_ranges[j][0]}"
            )

    # 5. Validate DB if it exists
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        db_msg_count = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        if db_msg_count != total_csv_rows:
            issues.append(f"DB message count mismatch: DB has {db_msg_count}, CSVs have {total_csv_rows}")

        # Check no orphaned alert_details
        orphans = conn.execute(
            "SELECT COUNT(*) FROM alert_details WHERE msg_id NOT IN (SELECT msg_id FROM messages)"
        ).fetchone()[0]
        if orphans:
            issues.append(f"DB has {orphans} orphaned alert_details rows")

        # Verify db_info references
        db_version = conn.execute("SELECT value FROM db_info WHERE key='data_version'").fetchone()
        if db_version:
            print(f"  DB built from data version: v{db_version[0]}")

        conn.close()
    else:
        issues.append(f"DB not found: {db_path}")

    # Report
    if issues:
        print(f"\n  VALIDATION FAILED - {len(issues)} issue(s):")
        for issue in issues:
            print(f"    ✗ {issue}")
    else:
        print("  VALIDATION PASSED")
        print(f"    CSV files: {len(csv_files)}")
        print(f"    Total messages: {total_csv_rows}")
        print(f"    Unique msg_ids: {len(all_msg_ids)}")
        print(f"    ID range: {min(all_msg_ids)} → {max(all_msg_ids)}")

    return len(issues) == 0, issues


# ============================================================
# COMMANDS
# ============================================================


def cmd_status():
    """Show current pipeline state."""
    print("=== PIKUD HAOREF PIPELINE STATUS ===\n")

    version = get_current_version()
    if not version:
        print("No data versions found. Run: pikud.py full_refresh")
        return

    ver_dir = get_version_dir(version)
    db_path = get_db_path(version)

    print(f"Data version: v{version} ({ver_dir})")
    print(f"Database: {db_path} ({'exists' if os.path.exists(db_path) else 'MISSING'})")

    metadata = load_metadata(ver_dir)
    print(f"Total messages: {metadata.get('total_messages', 0)}")
    print(f"Last msg_id: {metadata.get('last_msg_id', 0)}")
    print(f"Runs: {len(metadata.get('runs', []))}")

    if metadata.get("runs"):
        last_run = metadata["runs"][-1]
        print("\nLast run:")
        print(f"  File: {last_run['filename']}")
        print(f"  Fetched at: {last_run['fetched_at']}")
        print(f"  Messages: {last_run['message_count']}")
        print(f"  Range: {last_run['start_date']} → {last_run['end_date']}")

    # All versions
    if os.path.exists(DATA_DIR):
        versions = sorted(
            d for d in os.listdir(DATA_DIR) if d.startswith("v") and os.path.isdir(os.path.join(DATA_DIR, d))
        )
        if len(versions) > 1:
            print(f"\nAll versions: {', '.join(versions)} (current: v{version})")


def cmd_delta():
    """Incremental fetch + DB rebuild."""
    import asyncio

    print("=== DELTA RUN ===\n")

    version, ver_dir = init_version()
    db_path = get_db_path(version)
    metadata = load_metadata(ver_dir)
    last_msg_id = metadata["last_msg_id"]

    print(f"Data version: v{version}")
    print(f"Last msg_id: {last_msg_id}")

    # Fetch
    print("\n[1/3] FETCHING NEW MESSAGES...")
    rows = asyncio.run(fetch_messages(min_id=last_msg_id))
    if rows is None:
        return False
    if not rows:
        print("  No new messages. Database is up to date.")
        return True

    # Data integrity: delta size sanity check
    total_existing = metadata.get("total_messages", 0)
    delta_size = len(rows)
    if total_existing > 0 and delta_size > total_existing * 0.5:
        print(
            f"\n  ⚠️ LARGE DELTA WARNING: {delta_size} new messages is {delta_size / total_existing * 100:.0f}% of existing {total_existing}"
        )
        print("  This is unusually large. If unexpected, check Telegram channel for bulk changes.")
        print("  Proceeding anyway (use full_refresh if data structure changed)...")
    if delta_size > 0:
        # Check for ID regression (new IDs should be > last_msg_id)
        min_new_id = min(r["msg_id"] for r in rows)
        if min_new_id <= last_msg_id:
            print(f"\n  ✗ ID REGRESSION: new min ID {min_new_id} <= last_msg_id {last_msg_id}")
            print("  This indicates overlapping or duplicate data. Aborting.")
            return False

    # Save CSV
    print(f"\n[2/3] SAVING DELTA ({len(rows)} messages)...")
    filename = save_delta_csv(ver_dir, rows)
    start_id = rows[0]["msg_id"]
    end_id = rows[-1]["msg_id"]
    start_date = rows[0]["datetime_utc"]
    end_date = rows[-1]["datetime_utc"]

    run_info = {
        "filename": filename,
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "message_count": len(rows),
        "start_msg_id": start_id,
        "end_msg_id": end_id,
        "start_date": start_date,
        "end_date": end_date,
    }
    metadata["runs"].append(run_info)
    metadata["last_msg_id"] = end_id
    metadata["total_messages"] = sum(r["message_count"] for r in metadata["runs"])
    save_metadata(ver_dir, metadata)

    # Pre-build validation (CSV integrity before DB insert)
    print("\n[3/4] PRE-BUILD VALIDATION...")
    pre_ok, pre_issues = validate(ver_dir, db_path if os.path.exists(db_path) else "")
    if not pre_ok:
        # Only fail on CSV-level issues (not DB issues since we're about to rebuild)
        csv_issues = [i for i in pre_issues if "DB " not in i and "DB" not in i.split()[0] if i.split()]
        if csv_issues:
            print("  PRE-BUILD VALIDATION FAILED:")
            for issue in csv_issues:
                print(f"    ✗ {issue}")
            return False
        print("  CSV checks passed (DB will be rebuilt)")
    else:
        print("  All pre-build checks passed")

    # Rebuild DB
    print("\n[4/4] REBUILDING DATABASE...")
    build_database(ver_dir, db_path, version)

    # Post-build validation
    print("\n[VALIDATE]...")
    ok, _ = validate(ver_dir, db_path)

    print(f"\nDONE. Delta: {len(rows)} messages (IDs {start_id} → {end_id})")
    return ok


def cmd_full_refresh():
    """Full download into new data version + DB rebuild."""
    import asyncio

    print("=== FULL REFRESH ===\n")

    new_ver, ver_dir = create_new_version()
    db_path = get_db_path(new_ver)

    print(f"New data version: v{new_ver}")
    print(f"Data dir: {ver_dir}")

    # Fetch all
    print("\n[1/3] FETCHING ALL MESSAGES...")
    rows = asyncio.run(fetch_messages(min_id=0))
    if rows is None:
        return False
    if not rows:
        print("  Channel is empty.")
        return True

    # Save
    print(f"\n[2/3] SAVING ({len(rows)} messages)...")
    filename = save_delta_csv(ver_dir, rows)
    start_id = rows[0]["msg_id"]
    end_id = rows[-1]["msg_id"]

    metadata = {
        "runs": [
            {
                "filename": filename,
                "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "message_count": len(rows),
                "start_msg_id": start_id,
                "end_msg_id": end_id,
                "start_date": rows[0]["datetime_utc"],
                "end_date": rows[-1]["datetime_utc"],
            }
        ],
        "last_msg_id": end_id,
        "total_messages": len(rows),
    }
    save_metadata(ver_dir, metadata)

    # Build DB
    print("\n[3/3] BUILDING DATABASE...")
    build_database(ver_dir, db_path, new_ver)

    # Update DB symlink
    _update_symlink(DB_DIR, "current", os.path.basename(db_path))

    # Validate
    print("\n[VALIDATE]...")
    ok, _ = validate(ver_dir, db_path)

    print(f"\nDONE. v{new_ver}: {len(rows)} messages, DB at {db_path}")
    return ok


def cmd_rebuild_db():
    """Rebuild DB from existing CSVs without fetching."""
    print("=== REBUILD DATABASE ===\n")

    version, ver_dir = init_version()
    db_path = get_db_path(version)

    print(f"Data version: v{version}")
    print(f"Rebuilding from: {ver_dir}")

    build_database(ver_dir, db_path, version)

    print("\n[VALIDATE]...")
    ok, _ = validate(ver_dir, db_path)
    return ok


def cmd_validate():
    """Run validation only."""
    print("=== VALIDATE ===\n")

    version = get_current_version()
    if not version:
        print("No data versions found.")
        return False

    ver_dir = get_version_dir(version)
    db_path = get_db_path(version)
    print(f"Validating v{version}...")
    ok, _ = validate(ver_dir, db_path)
    return ok


# ============================================================
# MAIN
# ============================================================


def main():
    parser = argparse.ArgumentParser(
        description="Pikud HaOref Alert Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  delta          Fetch new messages, save delta CSV, rebuild DB
  full_refresh   Full re-download into new version, rebuild DB
  rebuild_db     Rebuild DB from existing CSVs (no fetch)
  validate       Run validation checks on current data + DB
  status         Show current state: versions, counts, last run
        """,
    )
    parser.add_argument("command", choices=["delta", "full_refresh", "rebuild_db", "validate", "status"])
    args = parser.parse_args()

    commands = {
        "status": cmd_status,
        "delta": cmd_delta,
        "full_refresh": cmd_full_refresh,
        "rebuild_db": cmd_rebuild_db,
        "validate": cmd_validate,
    }

    ok = commands[args.command]()
    if ok is False:
        sys.exit(1)


if __name__ == "__main__":
    main()
