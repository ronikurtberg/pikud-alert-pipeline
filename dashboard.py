#!/usr/bin/env python3
"""
Pikud HaOref Dashboard Server
==============================
Flask app serving interactive dashboard with:
- Filterable visualizations (date range, alert type, zone)
- ERD + data model statistics
- SQL query editor
- Pipeline controls with live SSE streaming
- Logs viewer

Architecture:
  dashboard.py          - Flask app + routes (entry point)
  dashboard_app/db.py   - Database connection, query helpers, SQL latency tracking
  dashboard_app/filters.py - Filter clause builders
  dashboard_app/metadata.py - STAT_SQL, VIZ_META, TRANSFORMATIONS constants
"""

import json
import logging
import os
import queue
import sqlite3
import subprocess
import time
import sys
import threading
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

from flask import Flask, Response, jsonify, render_template, request, stream_with_context

# Import from extracted modules
from dashboard_app.db import (
    get_db, get_shared_db, reset_shared_db, query_db, get_db_path,
    get_sql_stats, get_sql_summary, SQL_LOG_PATH,
    BASE_DIR, DB_DIR, DATA_DIR, LOGS_DIR,
)

app = Flask(__name__)
file_handler = logging.FileHandler(os.path.join(LOGS_DIR, "dashboard.log"))
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)

pipeline_lock = threading.Lock()
pipeline_running = False
pipeline_subscribers = []

# Request timing middleware
@app.before_request
def _start_timer():
    request._start_time = time.time()

@app.after_request
def _log_timing(response):
    if hasattr(request, '_start_time'):
        elapsed = (time.time() - request._start_time) * 1000
        if request.path.startswith('/api/'):
            app.logger.info(f"[TIMING] {request.method} {request.path} → {elapsed:.0f}ms")
            response.headers['X-Response-Time'] = f"{elapsed:.0f}ms"
    return response


# DB functions, filter builders, and metadata are imported from dashboard_app/
# (see imports at top of file)
from dashboard_app.filters import build_filter_clause, build_detail_filter_clause

# Canonical city name expression — uses canonical_name if set, falls back to city_name
CITY_DISPLAY = "COALESCE(c.canonical_name, c.city_name)"


def get_current_version():
    link = os.path.join(DATA_DIR, "current")
    if os.path.islink(link):
        return os.readlink(link).replace("v", "")
    return None


def load_metadata():
    ver = get_current_version()
    if not ver:
        return {}
    p = os.path.join(DATA_DIR, f"v{ver}", "metadata.json")
    if os.path.exists(p):
        with open(p) as f:
            return json.load(f)
    return {}


def resolve_dynamic_date(val):
    """Resolve dynamic date placeholders."""
    if val == "__LAST_7D__":
        from datetime import timedelta
        return (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    if val == "__LAST_30D__":
        from datetime import timedelta
        return (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    return val



# build_filter_clause and build_detail_filter_clause imported from dashboard_app.filters


def log_pipeline(level, msg):
    ts = datetime.now().strftime("%H:%M:%S")
    (app.logger.error if level == "error" else app.logger.info)(msg)
    for q in pipeline_subscribers[:]:
        try:
            q.put_nowait(json.dumps({"ts": ts, "level": level, "msg": msg}))
        except Exception:
            pass


# ============================================================
# PAGES
# ============================================================

@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/summary")
def summary_page():
    return render_template("summary.html")


@app.route("/api/summary")
def api_summary():
    """Lightweight summary data for the mobile page."""
    db = get_shared_db()
    if not db:
        return jsonify({"error": "No DB"}), 404

    city_filter = request.args.get("city")
    zone_filter = request.args.get("zone")

    # Date filter for summary queries
    date_from = request.args.get("date_from")
    df = f"AND datetime_israel >= '{date_from}'" if date_from else ""
    df_m = f"AND m.datetime_israel >= '{date_from}'" if date_from else ""

    # Base stats (filtered by date_from if provided)
    row = db.execute(f"""
        SELECT
            (SELECT COUNT(*) FROM messages WHERE message_type='alert' AND is_drill=0 {df}) as total_alerts,
            (SELECT COUNT(*) FROM alert_details ad JOIN messages m ON ad.msg_id=m.msg_id WHERE m.message_type='alert' AND m.is_drill=0 {df_m}) as total_city_alerts,
            (SELECT COUNT(DISTINCT c.city_id) FROM alert_details ad JOIN messages m ON ad.msg_id=m.msg_id JOIN cities c ON ad.city_id=c.city_id WHERE m.message_type='alert' AND m.is_drill=0 {df_m}) as total_cities,
            (SELECT COUNT(DISTINCT z.zone_id) FROM alert_details ad JOIN messages m ON ad.msg_id=m.msg_id JOIN zones z ON ad.zone_id=z.zone_id WHERE m.message_type='alert' AND m.is_drill=0 {df_m}) as total_zones,
            (SELECT MIN(datetime_israel) FROM messages WHERE message_type='alert' {df}) as first_date,
            (SELECT MAX(datetime_israel) FROM messages WHERE message_type='alert' {df}) as last_date
    """).fetchone()

    # Attack events (filtered)
    events = db.execute(f"""
        WITH t AS (
            SELECT datetime_utc, LAG(datetime_utc) OVER (ORDER BY datetime_utc) as prev
            FROM messages WHERE message_type='alert' AND is_drill=0 {df}
        )
        SELECT SUM(CASE WHEN prev IS NULL OR (julianday(datetime_utc)-julianday(prev))*24*60>2 THEN 1 ELSE 0 END)
        FROM t
    """).fetchone()[0] or 0

    # Latest alert
    latest = db.execute("""
        SELECT m.datetime_israel, m.alert_type,
               GROUP_CONCAT(DISTINCT z.zone_name) as zones,
               GROUP_CONCAT(DISTINCT c.city_name) as cities
        FROM messages m
        JOIN alert_details ad ON m.msg_id=ad.msg_id
        LEFT JOIN zones z ON ad.zone_id=z.zone_id
        LEFT JOIN cities c ON ad.city_id=c.city_id
        WHERE m.message_type='alert' AND m.is_drill=0
        GROUP BY m.msg_id ORDER BY m.msg_id DESC LIMIT 1
    """).fetchone()

    result = {
        "attack_events": events,
        "total_alerts": row["total_alerts"],
        "total_city_alerts": row["total_city_alerts"],
        "total_cities": row["total_cities"],
        "total_zones": row["total_zones"],
        "first_date": row["first_date"],
        "last_date": row["last_date"],
        "latest": {
            "time": latest["datetime_israel"],
            "type": latest["alert_type"],
            "zones": latest["zones"],
            "cities": latest["cities"],
        } if latest else None,
    }

    # If city/zone filter, add scoped data
    if city_filter:
        # Match both raw and canonical names, always apply date filter
        city_match = "(c.city_name=? OR c.canonical_name=?)"
        city_params = (city_filter, city_filter)
        city_data = db.execute(f"""
            SELECT COUNT(*) as alerts,
                   MIN(m.datetime_israel) as first_alert,
                   MAX(m.datetime_israel) as last_alert,
                   COUNT(DISTINCT date(m.datetime_israel)) as alert_days
            FROM alert_details ad
            JOIN messages m ON ad.msg_id=m.msg_id
            JOIN cities c ON ad.city_id=c.city_id
            WHERE m.message_type='alert' AND m.is_drill=0 AND {city_match} {df_m}
        """, city_params).fetchone()
        zone_for_city = db.execute(f"""
            SELECT DISTINCT z.zone_name FROM alert_details ad
            JOIN messages m ON ad.msg_id=m.msg_id
            JOIN cities c ON ad.city_id=c.city_id
            LEFT JOIN zones z ON ad.zone_id=z.zone_id
            WHERE m.message_type='alert' AND {city_match} {df_m} AND z.zone_name IS NOT NULL LIMIT 3
        """, city_params).fetchall()
        # Hourly distribution for this city
        hourly = [dict(r) for r in db.execute(f"""
            SELECT CAST(strftime('%H', m.datetime_israel) AS INTEGER) as hour, COUNT(*) as count
            FROM alert_details ad JOIN messages m ON ad.msg_id=m.msg_id
            JOIN cities c ON ad.city_id=c.city_id
            WHERE m.message_type='alert' AND m.is_drill=0 AND {city_match} {df_m}
            GROUP BY hour ORDER BY hour
        """, city_params).fetchall()]
        # Threat type breakdown for this city
        city_types = [dict(r) for r in db.execute(f"""
            SELECT m.alert_type, COUNT(*) as count
            FROM alert_details ad JOIN messages m ON ad.msg_id=m.msg_id
            JOIN cities c ON ad.city_id=c.city_id
            WHERE m.message_type='alert' AND m.is_drill=0 AND m.alert_type IS NOT NULL AND {city_match} {df_m}
            GROUP BY m.alert_type ORDER BY count DESC
        """, city_params).fetchall()]
        alert_days = city_data["alert_days"] or 1
        result["city"] = {
            "name": city_filter,
            "alerts": city_data["alerts"],
            "first": city_data["first_alert"],
            "last": city_data["last_alert"],
            "alert_days": alert_days,
            "avg_per_day": round(city_data["alerts"] / max(alert_days, 1), 1),
            "zones": [r["zone_name"] for r in zone_for_city],
            "hourly": hourly,
            "by_type": city_types,
            "busiest_hour": max(hourly, key=lambda h: h["count"])["hour"] if hourly else None,
        }

    if zone_filter:
        zone_data = db.execute("""
            SELECT COUNT(*) as alerts,
                   COUNT(DISTINCT c.city_id) as cities_affected,
                   MIN(m.datetime_israel) as first_alert,
                   MAX(m.datetime_israel) as last_alert
            FROM alert_details ad
            JOIN messages m ON ad.msg_id=m.msg_id
            JOIN zones z ON ad.zone_id=z.zone_id
            LEFT JOIN cities c ON ad.city_id=c.city_id
            WHERE m.message_type='alert' AND m.is_drill=0 AND z.zone_name=?
        """, (zone_filter,)).fetchone()
        result["zone"] = {
            "name": zone_filter,
            "alerts": zone_data["alerts"],
            "cities_affected": zone_data["cities_affected"],
            "first": zone_data["first_alert"],
            "last": zone_data["last_alert"],
        }

    date_filter = f"AND m.datetime_israel >= '{date_from}'" if date_from else ""

    # Top 10 cities (using canonical names)
    result["top_cities"] = [dict(r) for r in db.execute(f"""
        SELECT COALESCE(c.canonical_name, c.city_name) as city_name, COUNT(*) as alerts
        FROM alert_details ad
        JOIN messages m ON ad.msg_id=m.msg_id
        JOIN cities c ON ad.city_id=c.city_id
        WHERE m.message_type='alert' AND m.is_drill=0 {date_filter}
        GROUP BY COALESCE(c.canonical_name, c.city_name) ORDER BY alerts DESC LIMIT 10
    """).fetchall()]

    # Threat type breakdown
    date_filter_no_alias = f"AND datetime_israel >= '{date_from}'" if date_from else ""
    result["by_type"] = [dict(r) for r in db.execute(f"""
        SELECT alert_type, COUNT(*) as count
        FROM messages WHERE message_type='alert' AND is_drill=0 AND alert_type IS NOT NULL {date_filter_no_alias}
        GROUP BY alert_type ORDER BY count DESC
    """).fetchall()]

    # Top 5 zones
    result["top_zones"] = [dict(r) for r in db.execute(f"""
        SELECT z.zone_name, COUNT(DISTINCT ad.msg_id) as alerts
        FROM alert_details ad
        JOIN messages m ON ad.msg_id=m.msg_id
        JOIN zones z ON ad.zone_id=z.zone_id
        WHERE m.message_type='alert' AND m.is_drill=0 {date_filter}
        GROUP BY z.zone_name ORDER BY alerts DESC LIMIT 5
    """).fetchall()]

    # Busiest day
    busiest = db.execute(f"""
        SELECT date(datetime_israel) as date, COUNT(*) as count
        FROM messages WHERE message_type='alert' AND is_drill=0 {date_filter_no_alias}
        GROUP BY date ORDER BY count DESC LIMIT 1
    """).fetchone()
    result["busiest_day"] = {"date": busiest["date"], "count": busiest["count"]} if busiest else None

    # Last 5 alerts (brief)
    result["recent_alerts"] = [dict(r) for r in db.execute(f"""
        SELECT m.datetime_israel, m.alert_type,
               GROUP_CONCAT(DISTINCT z.zone_name) as zones,
               COUNT(DISTINCT c.city_id) as city_count
        FROM messages m
        JOIN alert_details ad ON m.msg_id=ad.msg_id
        LEFT JOIN zones z ON ad.zone_id=z.zone_id
        LEFT JOIN cities c ON ad.city_id=c.city_id
        WHERE m.message_type='alert' AND m.is_drill=0 {date_filter}
        GROUP BY m.msg_id ORDER BY m.msg_id DESC LIMIT 5
    """).fetchall()]

    return jsonify(result)


@app.route("/api/filter_options")
def api_filter_options():
    """Return distinct cities, zones, and alert types for dynamic filters.
    Cities are filtered to clean single-city entries (excludes multi-city parser artifacts)."""
    db = get_shared_db()
    if not db:
        return jsonify({"cities": [], "zones": [], "alert_types": []})
    # Only include cleanly-parsed cities, using canonical_name to deduplicate dash variants.
    # e.g., "אבו-גוש" and "אבו גוש" both map to canonical "אבו גוש" — show only canonical.
    clean_with_shelter = set(r[0] for r in db.execute("""
        SELECT DISTINCT COALESCE(c.canonical_name, c.city_name) FROM cities c
        JOIN alert_details ad ON c.city_id=ad.city_id
        WHERE ad.shelter_time IS NOT NULL
    """).fetchall())
    cities = sorted(name for name in clean_with_shelter if name.count(' ') <= 3)
    zones = [r[0] for r in db.execute(
        "SELECT DISTINCT zone_name FROM zones ORDER BY zone_name").fetchall()]
    alert_types = [r[0] for r in db.execute(
        "SELECT DISTINCT alert_type FROM messages WHERE alert_type IS NOT NULL ORDER BY alert_type").fetchall()]
    return jsonify({"cities": cities, "zones": zones, "alert_types": alert_types})


@app.route("/api/prefilters")
def api_prefilters():
    from config import PREFILTERS
    result = []
    for pf in PREFILTERS:
        r = dict(pf)
        r["date_from"] = resolve_dynamic_date(r["date_from"]) if r["date_from"] else None
        r["date_to"] = resolve_dynamic_date(r["date_to"]) if r["date_to"] else None
        result.append(r)
    return jsonify(result)


@app.route("/api/filtered_counts")
def api_filtered_counts():
    """Get alert counts with current filters — city-level and zone-level.
    When city/zone filter is active, counts are scoped to that city/zone's rows in alert_details."""
    filt, params = build_filter_clause()
    dfilt, dparams = build_detail_filter_clause()
    db = get_shared_db()
    if not db:
        return jsonify({"error": "No DB"}), 404

    has_detail_filter = bool(request.args.get("city") or request.args.get("zone"))

    # City-level alerts: when city/zone filter active, only count matching detail rows
    if has_detail_filter:
        detail_join = "JOIN cities c ON ad.city_id=c.city_id LEFT JOIN zones z ON ad.zone_id=z.zone_id"
        city_alerts = db.execute(f"""
            SELECT COUNT(*) FROM alert_details ad
            JOIN messages m ON ad.msg_id=m.msg_id {detail_join}
            WHERE m.message_type='alert' AND m.is_drill=0 {filt} {dfilt}
        """, params + dparams).fetchone()[0]
        zone_alerts = db.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT ad.msg_id, ad.zone_id FROM alert_details ad
                JOIN messages m ON ad.msg_id=m.msg_id {detail_join}
                WHERE m.message_type='alert' AND m.is_drill=0 {filt} {dfilt}
            )
        """, params + dparams).fetchone()[0]
    else:
        city_alerts = db.execute(f"""
            SELECT COUNT(*) FROM alert_details ad
            JOIN messages m ON ad.msg_id=m.msg_id
            WHERE m.message_type='alert' AND m.is_drill=0 {filt}
        """, params).fetchone()[0]
        zone_alerts = db.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT ad.msg_id, ad.zone_id FROM alert_details ad
                JOIN messages m ON ad.msg_id=m.msg_id
                WHERE m.message_type='alert' AND m.is_drill=0 {filt}
            )
        """, params).fetchone()[0]

    msg_alerts = db.execute(f"""
        SELECT COUNT(*) FROM messages m
        WHERE m.message_type='alert' AND m.is_drill=0 {filt}
    """, params).fetchone()[0]
    date_info = db.execute(f"""
        SELECT MIN(m.datetime_israel), MAX(m.datetime_israel) FROM messages m
        WHERE m.message_type='alert' AND m.is_drill=0 {filt}
    """, params).fetchone()

    # Attack events: group messages within 2-min gaps as one event
    attack_events = db.execute(f"""
        WITH msg_times AS (
            SELECT m.msg_id, m.datetime_utc,
                LAG(m.datetime_utc) OVER (ORDER BY m.datetime_utc) as prev_time
            FROM messages m WHERE m.message_type='alert' AND m.is_drill=0 {filt}
        ),
        gaps AS (
            SELECT msg_id,
                CASE WHEN prev_time IS NULL OR (julianday(datetime_utc)-julianday(prev_time))*24*60 > 2 THEN 1 ELSE 0 END as is_new_event
            FROM msg_times
        )
        SELECT SUM(is_new_event) FROM gaps
    """, params).fetchone()[0] or 0
    return jsonify({
        "attack_events": attack_events,
        "city_alerts": city_alerts,
        "zone_alerts": zone_alerts,
        "msg_alerts": msg_alerts,
        "first": date_info[0],
        "last": date_info[1],
    })


# ============================================================
# API: STATS
# ============================================================

@app.route("/api/stats")
def api_stats():
    db = get_shared_db()
    if not db:
        return jsonify({"error": "No database found"}), 404
    s = {}
    # Batch simple counts in one query
    counts = db.execute("""
        SELECT
            (SELECT COUNT(*) FROM messages) as total_messages,
            (SELECT COUNT(*) FROM messages WHERE message_type='alert' AND is_drill=0) as total_alerts,
            (SELECT COUNT(*) FROM cities) as total_cities,
            (SELECT COUNT(*) FROM zones) as total_zones,
            (SELECT COUNT(*) FROM alert_details) as total_details,
            (SELECT COUNT(*) FROM messages WHERE is_drill=1) as drills,
            (SELECT MIN(datetime_israel) FROM messages) as date_min,
            (SELECT MAX(datetime_israel) FROM messages) as date_max
    """).fetchone()
    s["total_messages"] = counts["total_messages"]
    s["total_alerts"] = counts["total_alerts"]
    s["total_cities"] = counts["total_cities"]
    s["total_zones"] = counts["total_zones"]
    s["total_details"] = counts["total_details"]
    s["drills"] = counts["drills"]
    s["date_range"] = {"min": counts["date_min"], "max": counts["date_max"]}
    s["message_types"] = [dict(r) for r in db.execute(
        "SELECT message_type, COUNT(*) as count FROM messages GROUP BY message_type ORDER BY count DESC").fetchall()]
    s["alert_types"] = [dict(r) for r in db.execute(
        "SELECT COALESCE(alert_type,'other') as alert_type, COUNT(*) as count FROM messages WHERE message_type='alert' AND is_drill=0 GROUP BY alert_type ORDER BY count DESC").fetchall()]
    s["version"] = get_current_version()
    meta = load_metadata()
    s["last_run"] = meta.get("runs", [{}])[-1] if meta.get("runs") else None
    s["run_count"] = len(meta.get("runs", []))
    s["total_fetched"] = meta.get("total_messages", 0)
    for row in db.execute("SELECT key, value FROM db_info"):
        s[f"db_{row['key']}"] = row["value"]
    s["yearly"] = [dict(r) for r in db.execute(
        "SELECT strftime('%Y',datetime_israel) as year, COUNT(*) as count FROM messages GROUP BY year ORDER BY year").fetchall()]
    s["busiest_day"] = dict(db.execute(
        "SELECT date(datetime_israel) as date, COUNT(*) as count FROM messages WHERE message_type='alert' AND is_drill=0 GROUP BY date ORDER BY count DESC LIMIT 1").fetchone())
    # Latest alert with zones and cities
    latest = db.execute("""
        SELECT m.msg_id, m.datetime_utc, m.datetime_israel, m.alert_type, m.raw_text,
               GROUP_CONCAT(DISTINCT z.zone_name) as zones,
               GROUP_CONCAT(DISTINCT c.city_name) as cities
        FROM messages m
        JOIN alert_details ad ON m.msg_id=ad.msg_id
        LEFT JOIN zones z ON ad.zone_id=z.zone_id
        LEFT JOIN cities c ON ad.city_id=c.city_id
        WHERE m.message_type='alert'
        GROUP BY m.msg_id ORDER BY m.msg_id DESC LIMIT 1
    """).fetchone()
    if latest:
        s["latest_alert"] = {
            "datetime_utc": latest["datetime_utc"],
            "datetime_israel": latest["datetime_israel"],
            "alert_type": latest["alert_type"],
            "text": latest["raw_text"][:200],
            "zones": latest["zones"],
            "cities": latest["cities"],
        }
    else:
        s["latest_alert"] = None
    last_msg = db.execute("SELECT datetime_utc, datetime_israel, message_type FROM messages ORDER BY msg_id DESC LIMIT 1").fetchone()
    s["latest_message"] = {"datetime_utc": last_msg["datetime_utc"], "datetime_israel": last_msg["datetime_israel"], "type": last_msg["message_type"]} if last_msg else None
    return jsonify(s)


# ============================================================
# API: FILTERED VISUALIZATIONS
# ============================================================

@app.route("/api/viz/hourly")
def api_viz_hourly():
    filt, params = build_filter_clause()
    rows = query_db(f"""
        SELECT CAST(strftime('%H', m.datetime_israel) AS INTEGER) as hour_israel,
               COUNT(*) as total,
               SUM(CASE WHEN m.alert_type='rockets' THEN 1 ELSE 0 END) as rockets,
               SUM(CASE WHEN m.alert_type='aircraft' THEN 1 ELSE 0 END) as aircraft
        FROM messages m WHERE m.message_type='alert' AND m.is_drill=0 {filt}
        GROUP BY hour_israel ORDER BY hour_israel
    """, params)
    return jsonify(rows)


@app.route("/api/viz/daily")
def api_viz_daily():
    filt, params = build_filter_clause()
    return jsonify(query_db(f"""
        SELECT date(m.datetime_israel) as date, COUNT(*) as total,
               SUM(CASE WHEN m.alert_type='rockets' THEN 1 ELSE 0 END) as rockets,
               SUM(CASE WHEN m.alert_type='aircraft' THEN 1 ELSE 0 END) as aircraft
        FROM messages m WHERE m.message_type='alert' AND m.is_drill=0 {filt}
        GROUP BY date ORDER BY date
    """, params))


@app.route("/api/viz/top_cities")
def api_viz_top_cities():
    limit = request.args.get("limit", 25, type=int)
    sort_order = "ASC" if request.args.get("sort") == "asc" else "DESC"
    filt, params = build_filter_clause()
    params.append(limit)
    return jsonify(query_db(f"""
        SELECT {CITY_DISPLAY} as city_name, COUNT(*) as alert_count,
               SUM(CASE WHEN m.alert_type='rockets' THEN 1 ELSE 0 END) as rocket_count,
               SUM(CASE WHEN m.alert_type='aircraft' THEN 1 ELSE 0 END) as aircraft_count
        FROM alert_details ad
        JOIN messages m ON ad.msg_id = m.msg_id
        JOIN cities c ON ad.city_id = c.city_id
        WHERE m.message_type='alert' AND m.is_drill=0 {filt}
        GROUP BY {CITY_DISPLAY} ORDER BY alert_count {sort_order} LIMIT ?
    """, params))


@app.route("/api/viz/zones")
def api_viz_zones():
    filt, params = build_filter_clause()
    return jsonify(query_db(f"""
        SELECT z.zone_name, COUNT(*) as alert_count,
               SUM(CASE WHEN m.alert_type='rockets' THEN 1 ELSE 0 END) as rockets,
               SUM(CASE WHEN m.alert_type='aircraft' THEN 1 ELSE 0 END) as aircraft
        FROM alert_details ad
        JOIN messages m ON ad.msg_id = m.msg_id
        JOIN zones z ON ad.zone_id = z.zone_id
        WHERE m.message_type='alert' AND m.is_drill=0 {filt}
        GROUP BY z.zone_name ORDER BY alert_count DESC
    """, params))


@app.route("/api/viz/monthly")
def api_viz_monthly():
    filt, params = build_filter_clause()
    return jsonify(query_db(f"""
        SELECT strftime('%Y-%m', m.datetime_israel) as month, COUNT(*) as total,
               SUM(CASE WHEN m.alert_type='rockets' THEN 1 ELSE 0 END) as rockets,
               SUM(CASE WHEN m.alert_type='aircraft' THEN 1 ELSE 0 END) as aircraft
        FROM messages m WHERE m.message_type='alert' AND m.is_drill=0 {filt}
        GROUP BY month ORDER BY month
    """, params))


@app.route("/api/viz/shelter_times")
def api_viz_shelter_times():
    filt, params = build_filter_clause()
    return jsonify(query_db(f"""
        SELECT ad.shelter_time, COUNT(*) as count
        FROM alert_details ad JOIN messages m ON ad.msg_id = m.msg_id
        WHERE m.message_type='alert' AND ad.shelter_time IS NOT NULL {filt}
        GROUP BY ad.shelter_time ORDER BY count DESC LIMIT 10
    """, params))


@app.route("/api/viz/dow")
def api_viz_dow():
    """Day of week distribution."""
    filt, params = build_filter_clause()
    return jsonify(query_db(f"""
        SELECT CAST(strftime('%w', m.datetime_israel) AS INTEGER) as dow,
               COUNT(*) as total
        FROM messages m WHERE m.message_type='alert' AND m.is_drill=0 {filt}
        GROUP BY dow ORDER BY dow
    """, params))


@app.route("/api/viz/escalation")
def api_viz_escalation():
    """Alerts per hour within the busiest days — escalation pattern."""
    filt, params = build_filter_clause()
    return jsonify(query_db(f"""
        SELECT date(m.datetime_israel) as date,
               CAST(strftime('%H', m.datetime_israel) AS INTEGER) as hour,
               COUNT(*) as count
        FROM messages m
        WHERE m.message_type='alert' AND m.is_drill=0 {filt}
          AND date(m.datetime_israel) IN (
            SELECT date(datetime_israel) FROM messages
            WHERE message_type='alert' AND is_drill=0
            GROUP BY date(datetime_israel) ORDER BY COUNT(*) DESC LIMIT 10
          )
        GROUP BY date, hour ORDER BY date, hour
    """, params))


@app.route("/api/viz/city_timeline")
def api_viz_city_timeline():
    """Top 10 cities monthly timeline."""
    filt, params = build_filter_clause()
    return jsonify(query_db(f"""
        SELECT {CITY_DISPLAY} as city_name, strftime('%Y-%m', m.datetime_israel) as month, COUNT(*) as count
        FROM alert_details ad
        JOIN messages m ON ad.msg_id = m.msg_id
        JOIN cities c ON ad.city_id = c.city_id
        WHERE m.message_type='alert' AND m.is_drill=0 {filt}
          AND {CITY_DISPLAY} IN (
            SELECT COALESCE(c2.canonical_name, c2.city_name) FROM alert_details ad2
            JOIN messages m2 ON ad2.msg_id=m2.msg_id
            JOIN cities c2 ON ad2.city_id=c2.city_id
            WHERE m2.message_type='alert' AND m2.is_drill=0
            GROUP BY COALESCE(c2.canonical_name, c2.city_name) ORDER BY COUNT(*) DESC LIMIT 8
          )
        GROUP BY {CITY_DISPLAY}, month ORDER BY month
    """, params))


@app.route("/api/viz/alert_vs_ended")
def api_viz_alert_vs_ended():
    """Alert messages vs event_ended messages over time."""
    filt, params = build_filter_clause()
    return jsonify(query_db(f"""
        SELECT date(m.datetime_israel) as date,
               SUM(CASE WHEN m.message_type='alert' THEN 1 ELSE 0 END) as alerts,
               SUM(CASE WHEN m.message_type='event_ended' THEN 1 ELSE 0 END) as ended,
               SUM(CASE WHEN m.message_type='heads_up' THEN 1 ELSE 0 END) as heads_up
        FROM messages m WHERE m.is_drill=0 {filt}
        GROUP BY date ORDER BY date
    """, params))


# ============================================================
# API: CREATIVE ANALYSES
# ============================================================

@app.route("/api/viz/safest_hours")
def api_viz_safest_hours():
    """Safest hours to shower — lowest alert probability by hour (Israel time)."""
    filt, params = build_filter_clause()
    rows = query_db(f"""
        SELECT CAST(strftime('%H', m.datetime_israel) AS INTEGER) as hour_israel,
               COUNT(*) as alerts
        FROM messages m WHERE m.message_type='alert' AND m.is_drill=0 {filt}
        GROUP BY hour_israel ORDER BY hour_israel
    """, params)
    total = sum(r["alerts"] for r in rows) or 1
    for r in rows:
        r["pct"] = round(100 * r["alerts"] / total, 1)
        r["risk"] = "LOW" if r["pct"] < 3 else "MED" if r["pct"] < 5 else "HIGH"
    return jsonify(rows)


@app.route("/api/viz/safest_10min")
def api_viz_safest_10min():
    """Find the safest 10-minute windows in the day.
    Counts alerts per 10-min bucket across all days, ranks by fewest alerts."""
    filt, params = build_filter_clause()
    # Get minute-level distribution using Israel time
    rows = query_db(f"""
        SELECT CAST(strftime('%H', m.datetime_israel) AS INTEGER) as h,
               CAST(strftime('%M', m.datetime_israel) AS INTEGER) / 10 as m10,
               COUNT(*) as alerts
        FROM messages m WHERE m.message_type='alert' AND m.is_drill=0 {filt}
        GROUP BY h, m10 ORDER BY h, m10
    """, params)
    # Build 144 10-min buckets (24h × 6)
    buckets = {}
    for h in range(24):
        for m in range(6):
            buckets[(h, m)] = 0
    for r in rows:
        buckets[(r["h"], r["m10"])] = r["alerts"]
    # Rank by fewest alerts
    result = []
    for (h, m), alerts in sorted(buckets.items(), key=lambda x: x[1]):
        label = f"{h:02d}:{m*10:02d}-{h:02d}:{m*10+9:02d}"
        result.append({"slot": label, "alerts": alerts, "h": h, "m10": m})
    return jsonify(result)


@app.route("/api/viz/city_safety_rank")
def api_viz_city_safety_rank():
    """City safety ranking — fewest alerts per city, only cities with any alert."""
    filt, params = build_filter_clause()
    return jsonify(query_db(f"""
        SELECT {CITY_DISPLAY} as city_name, z.zone_name, COUNT(*) as alerts,
               SUM(CASE WHEN m.alert_type='rockets' THEN 1 ELSE 0 END) as rockets,
               SUM(CASE WHEN m.alert_type='aircraft' THEN 1 ELSE 0 END) as drones,
               MIN(m.datetime_israel) as first, MAX(m.datetime_israel) as last
        FROM alert_details ad
        JOIN messages m ON ad.msg_id=m.msg_id
        JOIN cities c ON ad.city_id=c.city_id
        LEFT JOIN zones z ON ad.zone_id=z.zone_id
        WHERE m.message_type='alert' AND m.is_drill=0 {filt}
        GROUP BY {CITY_DISPLAY}, z.zone_name
        ORDER BY alerts DESC
    """, params))


@app.route("/api/viz/drone_cities")
def api_viz_drone_cities():
    """Cities with most drone/aircraft alerts."""
    filt, params = build_filter_clause()
    return jsonify(query_db(f"""
        SELECT {CITY_DISPLAY} as city_name, z.zone_name, COUNT(*) as drone_alerts
        FROM alert_details ad
        JOIN messages m ON ad.msg_id=m.msg_id
        JOIN cities c ON ad.city_id=c.city_id
        LEFT JOIN zones z ON ad.zone_id=z.zone_id
        WHERE m.message_type='alert' AND m.is_drill=0 AND m.alert_type='aircraft' {filt}
        GROUP BY {CITY_DISPLAY} ORDER BY drone_alerts DESC LIMIT 25
    """, params))


@app.route("/api/viz/city_zone_anomaly")
def api_viz_city_zone_anomaly():
    """City vs Zone anomaly — cities that are outliers vs their zone average."""
    filt, params = build_filter_clause()
    return jsonify(query_db(f"""
        WITH city_counts AS (
            SELECT c.city_name, z.zone_name, COUNT(*) as city_alerts
            FROM alert_details ad
            JOIN messages m ON ad.msg_id=m.msg_id
            JOIN cities c ON ad.city_id=c.city_id
            JOIN zones z ON ad.zone_id=z.zone_id
            WHERE m.message_type='alert' AND m.is_drill=0 {filt}
            GROUP BY c.city_name, z.zone_name
        ),
        zone_avg AS (
            SELECT zone_name, AVG(city_alerts) as avg_alerts, COUNT(*) as num_cities
            FROM city_counts GROUP BY zone_name
        )
        SELECT cc.city_name, cc.zone_name, cc.city_alerts,
               ROUND(za.avg_alerts,1) as zone_avg,
               ROUND(1.0*cc.city_alerts/za.avg_alerts, 1) as ratio,
               za.num_cities
        FROM city_counts cc
        JOIN zone_avg za ON cc.zone_name=za.zone_name
        WHERE za.num_cities >= 3
        ORDER BY ratio DESC
        LIMIT 50
    """, params))


# ============================================================
# API: NEW ADVANCED ANALYSES
# ============================================================

@app.route("/api/viz/response_time")
def api_viz_response_time():
    """Time from alert to event_ended — response/duration analysis.
    Pairs alerts with their closest following event_ended message using correlated subquery."""
    rows = query_db("""
        WITH paired AS (
            SELECT a.alert_type,
                   (SELECT MIN(e.datetime_utc) FROM messages e
                    WHERE e.message_type='event_ended'
                      AND e.datetime_utc > a.datetime_utc
                      AND e.datetime_utc <= datetime(a.datetime_utc, '+3 hours')
                   ) as ended_time,
                   a.datetime_utc as alert_time
            FROM messages a
            WHERE a.message_type='alert' AND a.is_drill=0 AND a.alert_type IS NOT NULL
        )
        SELECT alert_type,
               COUNT(*) as total,
               SUM(CASE WHEN ended_time IS NOT NULL THEN 1 ELSE 0 END) as with_clearance,
               ROUND(AVG(CASE WHEN ended_time IS NOT NULL
                   THEN (julianday(ended_time)-julianday(alert_time))*24*60 END),1) as avg_minutes,
               ROUND(MIN(CASE WHEN ended_time IS NOT NULL
                   THEN (julianday(ended_time)-julianday(alert_time))*24*60 END),1) as min_minutes,
               ROUND(MAX(CASE WHEN ended_time IS NOT NULL
                   THEN (julianday(ended_time)-julianday(alert_time))*24*60 END),1) as max_minutes
        FROM paired
        GROUP BY alert_type ORDER BY total DESC
    """)
    return jsonify(rows)


@app.route("/api/viz/multi_zone")
def api_viz_multi_zone():
    """Simultaneous multi-zone attacks — messages that hit the most zones at once."""
    filt, params = build_filter_clause()
    return jsonify(query_db(f"""
        SELECT m.msg_id, m.datetime_israel, m.alert_type,
               COUNT(DISTINCT ad.zone_id) as zone_count,
               COUNT(DISTINCT ad.city_id) as city_count,
               GROUP_CONCAT(DISTINCT z.zone_name) as zones
        FROM messages m
        JOIN alert_details ad ON m.msg_id = ad.msg_id
        JOIN zones z ON ad.zone_id = z.zone_id
        WHERE m.message_type='alert' AND m.is_drill=0 {filt}
        GROUP BY m.msg_id
        HAVING zone_count >= 3
        ORDER BY zone_count DESC, city_count DESC
        LIMIT 50
    """, params))


@app.route("/api/viz/streaks")
def api_viz_streaks():
    """Cities with longest consecutive-day alert streaks."""
    rows = query_db("""
        WITH city_days AS (
            SELECT DISTINCT COALESCE(c.canonical_name, c.city_name) as city_name, date(m.datetime_israel) as alert_date
            FROM alert_details ad
            JOIN messages m ON ad.msg_id = m.msg_id
            JOIN cities c ON ad.city_id = c.city_id
            WHERE m.message_type='alert' AND m.is_drill=0
        ),
        numbered AS (
            SELECT city_name, alert_date,
                   julianday(alert_date) - ROW_NUMBER() OVER (PARTITION BY city_name ORDER BY alert_date) as grp
            FROM city_days
        ),
        streaks AS (
            SELECT city_name, MIN(alert_date) as streak_start, MAX(alert_date) as streak_end,
                   COUNT(*) as streak_days
            FROM numbered GROUP BY city_name, grp
        )
        SELECT city_name, streak_days, streak_start, streak_end
        FROM streaks WHERE streak_days >= 5
        ORDER BY streak_days DESC LIMIT 30
    """)
    return jsonify(rows)


@app.route("/api/viz/calendar")
def api_viz_calendar():
    """Calendar heatmap data: daily alert count with weekday/week info."""
    filt, params = build_filter_clause()
    return jsonify(query_db(f"""
        SELECT date(m.datetime_israel) as date,
               CAST(strftime('%w', m.datetime_israel) AS INTEGER) as dow,
               CAST(strftime('%W', m.datetime_israel) AS INTEGER) as week,
               strftime('%Y', m.datetime_israel) as year,
               COUNT(*) as count
        FROM messages m WHERE m.message_type='alert' AND m.is_drill=0 {filt}
        GROUP BY date ORDER BY date
    """, params))


# ============================================================
# API: HEADS-UP & EVENT CORRELATION
# ============================================================

@app.route("/api/viz/heads_up_correlation")
def api_viz_heads_up_correlation():
    """Heads-up (התרעה מקדימה) → actual alert correlation within 15 minutes."""
    # Pre-fetch both sets and pair in Python (much faster than correlated subquery)
    db = get_shared_db()
    if not db:
        return jsonify({"error": "No DB"}), 404
    hu_rows = [dict(r) for r in db.execute(
        "SELECT msg_id, datetime_utc, datetime_israel FROM messages WHERE message_type='heads_up' ORDER BY datetime_utc"
    ).fetchall()]
    alert_times = [r[0] for r in db.execute(
        "SELECT datetime_utc FROM messages WHERE message_type='alert' AND is_drill=0 ORDER BY datetime_utc"
    ).fetchall()]
    import bisect
    rows = []
    for hu in hu_rows:
        hu_time = hu["datetime_utc"]
        # Binary search for first alert after this heads-up
        idx = bisect.bisect_right(alert_times, hu_time)
        alert_time = None
        if idx < len(alert_times):
            candidate = alert_times[idx]
            # Check within 15 minutes
            from datetime import datetime as _dt, timedelta
            hu_dt = _dt.strptime(hu_time, "%Y-%m-%d %H:%M:%S")
            al_dt = _dt.strptime(candidate, "%Y-%m-%d %H:%M:%S")
            if (al_dt - hu_dt).total_seconds() <= 900:  # 15 min
                alert_time = candidate
        minutes = round(((_dt.strptime(alert_time, "%Y-%m-%d %H:%M:%S") - hu_dt).total_seconds() / 60), 1) if alert_time else None
        rows.append({
            "msg_id": hu["msg_id"], "datetime_israel": hu["datetime_israel"],
            "followed_by_alert": 1 if alert_time else 0,
            "minutes_to_alert": minutes,
        })
    total = len(rows)
    hits = sum(1 for r in rows if r["followed_by_alert"])
    misses = total - hits
    avg_min = round(sum(r["minutes_to_alert"] for r in rows if r["minutes_to_alert"] is not None) / max(hits, 1), 1)
    # Time buckets
    buckets = {"0-1 min": 0, "1-2 min": 0, "2-5 min": 0, "5-10 min": 0, "10-15 min": 0, "No alert": 0}
    for r in rows:
        m = r["minutes_to_alert"]
        if m is None:
            buckets["No alert"] += 1
        elif m <= 1:
            buckets["0-1 min"] += 1
        elif m <= 2:
            buckets["1-2 min"] += 1
        elif m <= 5:
            buckets["2-5 min"] += 1
        elif m <= 10:
            buckets["5-10 min"] += 1
        else:
            buckets["10-15 min"] += 1
    # Monthly trend
    monthly = {}
    for r in rows:
        mo = r["datetime_israel"][:7] if r["datetime_israel"] else None
        if mo:
            if mo not in monthly:
                monthly[mo] = {"total": 0, "hits": 0}
            monthly[mo]["total"] += 1
            if r["followed_by_alert"]:
                monthly[mo]["hits"] += 1
    monthly_list = [{"month": k, "total": v["total"], "hits": v["hits"],
                     "hit_rate": round(100 * v["hits"] / v["total"], 1) if v["total"] else 0}
                    for k, v in sorted(monthly.items())]
    return jsonify({
        "total": total, "hits": hits, "misses": misses,
        "hit_rate_pct": round(100 * hits / max(total, 1), 1),
        "avg_minutes_to_alert": avg_min,
        "buckets": buckets,
        "monthly": monthly_list,
    })


@app.route("/api/viz/event_ended_analysis")
def api_viz_event_ended_analysis():
    """Event-ended timing analysis — per alert_type and monthly."""
    rows = query_db("""
        WITH paired AS (
            SELECT a.msg_id, a.alert_type, a.datetime_israel,
                (SELECT MIN(e.datetime_utc) FROM messages e
                 WHERE e.message_type='event_ended'
                   AND e.datetime_utc > a.datetime_utc
                   AND e.datetime_utc <= datetime(a.datetime_utc, '+3 hours')) as ended_time,
                a.datetime_utc as alert_time
            FROM messages a
            WHERE a.message_type='alert' AND a.is_drill=0 AND a.alert_type IS NOT NULL
        )
        SELECT alert_type, datetime_israel,
            CASE WHEN ended_time IS NOT NULL THEN 1 ELSE 0 END as has_clearance,
            CASE WHEN ended_time IS NOT NULL
                THEN ROUND((julianday(ended_time)-julianday(alert_time))*24*60, 1) ELSE NULL END as minutes_to_clear
        FROM paired
    """)
    # Per type summary
    by_type = {}
    for r in rows:
        t = r["alert_type"]
        if t not in by_type:
            by_type[t] = {"total": 0, "cleared": 0, "minutes": []}
        by_type[t]["total"] += 1
        if r["has_clearance"]:
            by_type[t]["cleared"] += 1
            by_type[t]["minutes"].append(r["minutes_to_clear"])
    type_summary = []
    for t, v in sorted(by_type.items(), key=lambda x: -x[1]["total"]):
        mins = v["minutes"]
        type_summary.append({
            "alert_type": t, "total": v["total"], "cleared": v["cleared"],
            "clear_pct": round(100 * v["cleared"] / v["total"], 1),
            "avg_min": round(sum(mins) / len(mins), 1) if mins else None,
            "median_min": round(sorted(mins)[len(mins) // 2], 1) if mins else None,
        })
    # Monthly trend of clearance rate
    monthly = {}
    for r in rows:
        mo = r["datetime_israel"][:7]
        if mo not in monthly:
            monthly[mo] = {"total": 0, "cleared": 0}
        monthly[mo]["total"] += 1
        if r["has_clearance"]:
            monthly[mo]["cleared"] += 1
    monthly_list = [{"month": k, "total": v["total"], "cleared": v["cleared"],
                     "clear_pct": round(100 * v["cleared"] / v["total"], 1)}
                    for k, v in sorted(monthly.items())]
    return jsonify({
        "by_type": type_summary,
        "monthly": monthly_list,
        "total_alerts": len(rows),
        "total_ended": sum(1 for r in rows if r["has_clearance"]),
    })


# ============================================================
# API: ERD + DATA MODEL
# ============================================================

@app.route("/api/viz/erd")
def api_viz_erd():
    db = get_shared_db()
    if not db:
        return jsonify({"error": "No DB"}), 404
    tables = {}
    for tbl in ["messages", "zones", "cities", "alert_details", "db_info"]:
        count = db.execute(f"SELECT COUNT(*) c FROM {tbl}").fetchone()["c"]
        cols = [dict(r) for r in db.execute(f"PRAGMA table_info({tbl})").fetchall()]
        tables[tbl] = {"count": count, "columns": cols}
    fks = {}
    for tbl in tables:
        fk_rows = [dict(r) for r in db.execute(f"PRAGMA foreign_key_list({tbl})").fetchall()]
        if fk_rows:
            fks[tbl] = fk_rows
    samples = {}
    for tbl in ["messages", "zones", "cities", "alert_details"]:
        samples[tbl] = [dict(r) for r in db.execute(f"SELECT * FROM {tbl} LIMIT 5").fetchall()]
    indexes = {}
    for tbl in tables:
        indexes[tbl] = [dict(r) for r in db.execute(f"PRAGMA index_list({tbl})").fetchall()]
    views = [dict(r) for r in db.execute("SELECT name, sql FROM sqlite_master WHERE type='view'").fetchall()]
    return jsonify({"tables": tables, "foreign_keys": fks, "samples": samples, "indexes": indexes, "views": views})


@app.route("/api/data_profile")
def api_data_profile():
    """Deep data profiling stats."""
    db = get_shared_db()
    if not db:
        return jsonify({"error": "No DB"}), 404
    p = {}
    p["messages_nulls"] = {}
    for col in ["datetime_utc", "alert_date", "alert_time_local", "message_type", "alert_type"]:
        p["messages_nulls"][col] = db.execute(f"SELECT COUNT(*) c FROM messages WHERE {col} IS NULL OR {col}=''").fetchone()["c"]
    p["distinct"] = {
        "message_types": db.execute("SELECT COUNT(DISTINCT message_type) c FROM messages").fetchone()["c"],
        "alert_types": db.execute("SELECT COUNT(DISTINCT alert_type) c FROM messages WHERE alert_type IS NOT NULL").fetchone()["c"],
        "zones": db.execute("SELECT COUNT(*) c FROM zones").fetchone()["c"],
        "cities": db.execute("SELECT COUNT(*) c FROM cities").fetchone()["c"],
        "dates": db.execute("SELECT COUNT(DISTINCT date(datetime_israel)) c FROM messages").fetchone()["c"],
    }
    p["cities_with_alerts"] = db.execute(
        "SELECT COUNT(DISTINCT city_id) c FROM alert_details ad JOIN messages m ON ad.msg_id=m.msg_id WHERE m.message_type='alert'").fetchone()["c"]
    p["avg_cities_per_alert"] = round(db.execute(
        "SELECT AVG(cnt) a FROM (SELECT COUNT(*) cnt FROM alert_details ad JOIN messages m ON ad.msg_id=m.msg_id WHERE m.message_type='alert' GROUP BY ad.msg_id)").fetchone()["a"], 1)
    db_path = get_db_path()
    p["db_size_mb"] = round(os.path.getsize(db_path) / 1024 / 1024, 1) if db_path else 0
    return jsonify(p)


# ============================================================
# API: SQL QUERY
# ============================================================

@app.route("/api/query", methods=["POST"])
def api_query():
    sql = request.json.get("sql", "").strip()
    if not sql:
        return jsonify({"error": "Empty query"}), 400
    first_word = sql.split()[0].upper() if sql.split() else ""
    if first_word in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "ATTACH"):
        return jsonify({"error": f"Write operations not allowed: {first_word}"}), 403
    try:
        db = get_db()
        if not db:
            return jsonify({"error": "No database"}), 404
        cur = db.execute(sql)
        columns = [desc[0] for desc in cur.description] if cur.description else []
        rows = [list(r) for r in cur.fetchmany(1000)]
        db.close()
        return jsonify({"columns": columns, "rows": rows, "count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


# ============================================================
# API: PIPELINE
# ============================================================

@app.route("/api/pipeline/<action>", methods=["POST"])
def api_pipeline(action):
    global pipeline_running
    valid = ["delta", "full_refresh", "rebuild_db", "validate", "status"]
    if action not in valid:
        return jsonify({"error": f"Invalid: {action}"}), 400
    with pipeline_lock:
        if pipeline_running and action != "status":
            return jsonify({"error": "Pipeline already running"}), 409
        if action != "status":
            pipeline_running = True
    if action == "status":
        r = subprocess.run([sys.executable, os.path.join(BASE_DIR, "pikud.py"), "status"],
                           capture_output=True, text=True, timeout=10)
        return jsonify({"output": r.stdout, "error": r.stderr, "returncode": r.returncode})

    def run():
        global pipeline_running
        log_file = os.path.join(LOGS_DIR, f"{action}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        log_pipeline("info", f"▶ Starting {action}...")
        try:
            proc = subprocess.Popen(
                [sys.executable, os.path.join(BASE_DIR, "pikud.py"), action],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                env=os.environ.copy(), cwd=BASE_DIR)
            with open(log_file, "w") as lf:
                for line in proc.stdout:
                    line = line.rstrip()
                    lf.write(line + "\n")
                    lf.flush()
                    log_pipeline("info", line)
            proc.wait()
            log_pipeline("success" if proc.returncode == 0 else "error",
                         f"{'✓' if proc.returncode==0 else '✗'} {action} {'completed' if proc.returncode==0 else 'failed'}")
            if proc.returncode == 0:
                reset_shared_db()
        except Exception as e:
            log_pipeline("error", f"✗ {action} exception: {e}")
        finally:
            with pipeline_lock:
                pipeline_running = False
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "action": action})


@app.route("/api/pipeline/stream")
def api_pipeline_stream():
    q = queue.Queue()
    pipeline_subscribers.append(q)
    def gen():
        try:
            while True:
                try:
                    yield f"data: {q.get(timeout=30)}\n\n"
                except queue.Empty:
                    yield f"data: {json.dumps({'type':'ping'})}\n\n"
        except GeneratorExit:
            pass
        finally:
            if q in pipeline_subscribers:
                pipeline_subscribers.remove(q)
    return Response(stream_with_context(gen()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/pipeline/running")
def api_pipeline_running():
    return jsonify({"running": pipeline_running})


@app.route("/api/refresh_db", methods=["POST"])
def api_refresh_db():
    """Force-reset the shared DB connection to pick up rebuilt/updated database."""
    reset_shared_db()
    db = get_shared_db()
    if not db:
        return jsonify({"ok": False, "error": "No DB after reset"})
    count = db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    return jsonify({"ok": True, "messages": count})


@app.route("/api/alerts/drilldown")
def api_alerts_drilldown():
    """Return filtered alerts in Telegram-like format (newest first) for drill-down view."""
    filt, params = build_filter_clause()
    dfilt, dparams = build_detail_filter_clause()
    db = get_shared_db()
    if not db:
        return jsonify({"error": "No DB"}), 404
    limit = request.args.get("limit", 100, type=int)
    offset = request.args.get("offset", 0, type=int)
    has_detail_filter = bool(request.args.get("city") or request.args.get("zone"))
    # Get messages newest first
    msgs = [dict(r) for r in db.execute(f"""
        SELECT m.msg_id, m.datetime_israel, m.alert_type, m.message_type, m.is_drill, m.raw_text
        FROM messages m
        WHERE m.message_type='alert' AND m.is_drill=0 {filt}
        ORDER BY m.msg_id DESC LIMIT ? OFFSET ?
    """, params + [limit, offset]).fetchall()]
    # Get details for these messages
    if msgs:
        msg_ids = [m["msg_id"] for m in msgs]
        placeholders = ",".join("?" * len(msg_ids))
        detail_sql = f"""
            SELECT ad.msg_id, z.zone_name, c.city_name, ad.shelter_time
            FROM alert_details ad
            LEFT JOIN zones z ON ad.zone_id=z.zone_id
            LEFT JOIN cities c ON ad.city_id=c.city_id
            WHERE ad.msg_id IN ({placeholders})
        """
        detail_params = msg_ids
        if has_detail_filter:
            if request.args.get("city"):
                detail_sql += " AND c.city_name = ?"
                detail_params.append(request.args["city"])
            if request.args.get("zone"):
                detail_sql += " AND z.zone_name = ?"
                detail_params.append(request.args["zone"])
        details = {}
        for r in db.execute(detail_sql, detail_params).fetchall():
            mid = r["msg_id"]
            if mid not in details:
                details[mid] = []
            details[mid].append({"zone": r["zone_name"], "city": r["city_name"], "shelter": r["shelter_time"]})
        for m in msgs:
            m["details"] = details.get(m["msg_id"], [])
    total = db.execute(f"""
        SELECT COUNT(*) FROM messages m WHERE m.message_type='alert' AND m.is_drill=0 {filt}
    """, params).fetchone()[0]
    return jsonify({"alerts": msgs, "total": total, "offset": offset, "limit": limit})


@app.route("/api/pipeline/sql_latency")
def api_pipeline_sql_latency():
    """SQL query latency statistics — computed from in-memory ring buffer."""
    return jsonify(get_sql_summary())


# ============================================================
# API: SQL METADATA FOR STATS
# ============================================================

STAT_SQL = {
    "attack_events": {
        "label": "Attack Events",
        "sql": "WITH msg_times AS (SELECT msg_id, datetime_utc, LAG(datetime_utc) OVER (ORDER BY datetime_utc) as prev_time FROM messages WHERE message_type='alert' AND is_drill=0), gaps AS (SELECT CASE WHEN prev_time IS NULL OR gap > 2min THEN 1 ELSE 0 END as is_new_event FROM msg_times) SELECT SUM(is_new_event)",
        "note": "Attack events: consecutive alert messages within 2 minutes of each other are grouped as one event. A barrage of 5 messages in 30 seconds = 1 attack event. More meaningful than raw message count.",
    },
    "msg_alerts": {
        "label": "Alert Messages",
        "sql": "SELECT COUNT(*) FROM messages m WHERE m.message_type='alert' AND m.is_drill=0",
        "note": "Each Telegram message with a siren activation = 1 count. One message may cover multiple cities in multiple zones. ~3.4 messages per attack event on average.",
    },
    "city_alerts": {
        "label": "City-Level Alerts",
        "sql": "SELECT COUNT(*) FROM alert_details ad JOIN messages m ON ad.msg_id=m.msg_id WHERE m.message_type='alert' AND m.is_drill=0",
        "note": "Each city mentioned in an alert message = 1 count. If one message alerts 5 cities, that's 5 city-alerts.",
    },
    "zone_alerts": {
        "label": "Zone-Level Alerts",
        "sql": "SELECT COUNT(*) FROM (SELECT DISTINCT ad.msg_id, ad.zone_id FROM alert_details ad JOIN messages m ON ad.msg_id=m.msg_id WHERE m.message_type='alert' AND m.is_drill=0)",
        "note": "Each unique zone per alert message = 1 count. Deduplicates multiple cities within same zone.",
    },
    "total_cities": {
        "label": "Cities",
        "sql": "SELECT COUNT(*) FROM cities",
        "note": "Total distinct city/settlement names extracted from all alerts.",
    },
    "total_zones": {
        "label": "Zones",
        "sql": "SELECT COUNT(*) FROM zones",
        "note": "Total distinct defense zones (e.g. 'גליל עליון', 'שפלת יהודה').",
    },
    "busiest_day": {
        "label": "Peak Day",
        "sql": "SELECT date(datetime_israel) as date, COUNT(*) as count FROM messages WHERE message_type='alert' AND is_drill=0 GROUP BY date ORDER BY count DESC LIMIT 1",
        "note": "Day with highest number of alert messages.",
    },
}

VIZ_META = {
    "hourly": {
        "title": "Hourly Risk Distribution",
        "dimensions": ["hour_israel (0-23)"],
        "measures": ["total alerts", "rockets", "aircraft"],
        "time": "Aggregated across all dates in filter",
        "sql": "SELECT CAST(strftime('%H', m.datetime_israel) AS INTEGER) as hour_israel, COUNT(*) as total, SUM(CASE WHEN m.alert_type='rockets' THEN 1 ELSE 0 END) as rockets, SUM(CASE WHEN m.alert_type='aircraft' THEN 1 ELSE 0 END) as aircraft FROM messages m WHERE m.message_type='alert' AND m.is_drill=0 GROUP BY hour_israel",
        "fields": ["messages.datetime_israel", "messages.alert_type", "messages.message_type", "messages.is_drill"],
    },
    "daily": {
        "title": "Daily Intensity",
        "dimensions": ["date (calendar day)"],
        "measures": ["total alerts", "rockets", "aircraft"],
        "time": "Each bar = one calendar day",
        "sql": "SELECT date(m.datetime_israel) as date, COUNT(*) as total, SUM(CASE WHEN m.alert_type='rockets' THEN 1 ELSE 0 END) as rockets FROM messages m WHERE m.message_type='alert' AND m.is_drill=0 GROUP BY date",
        "fields": ["messages.datetime_israel", "messages.alert_type"],
    },
    "top_cities": {
        "title": "Top Cities by Alert Count",
        "dimensions": ["city_name"],
        "measures": ["alert_count (city-level)", "rocket_count", "aircraft_count"],
        "time": "Aggregated across filter range",
        "sql": "SELECT c.city_name, COUNT(*) as alert_count FROM alert_details ad JOIN messages m ON ad.msg_id=m.msg_id JOIN cities c ON ad.city_id=c.city_id WHERE m.message_type='alert' AND m.is_drill=0 GROUP BY c.city_name ORDER BY alert_count DESC LIMIT N",
        "fields": ["alert_details.msg_id", "alert_details.city_id", "cities.city_name", "messages.alert_type"],
    },
    "zones": {
        "title": "Zone Breakdown",
        "dimensions": ["zone_name"],
        "measures": ["alert_count (city-level per zone)", "rockets", "aircraft"],
        "time": "Aggregated across filter range",
        "sql": "SELECT z.zone_name, COUNT(*) as alert_count FROM alert_details ad JOIN messages m ON ad.msg_id=m.msg_id JOIN zones z ON ad.zone_id=z.zone_id WHERE m.message_type='alert' AND m.is_drill=0 GROUP BY z.zone_name",
        "fields": ["alert_details.zone_id", "zones.zone_name", "messages.alert_type"],
    },
    "monthly": {
        "title": "Monthly Trend",
        "dimensions": ["month (YYYY-MM)"],
        "measures": ["total alerts", "rockets", "aircraft"],
        "time": "Each point = one calendar month",
        "sql": "SELECT strftime('%Y-%m', m.datetime_israel) as month, COUNT(*) FROM messages m WHERE m.message_type='alert' AND m.is_drill=0 GROUP BY month",
        "fields": ["messages.datetime_israel", "messages.alert_type"],
    },
    "shelter_times": {
        "title": "Shelter Time Distribution",
        "dimensions": ["shelter_time category"],
        "measures": ["count of alert_details"],
        "time": "All data in filter",
        "sql": "SELECT ad.shelter_time, COUNT(*) as count FROM alert_details ad JOIN messages m ON ad.msg_id=m.msg_id WHERE m.message_type='alert' AND ad.shelter_time IS NOT NULL GROUP BY ad.shelter_time",
        "fields": ["alert_details.shelter_time"],
    },
    "dow": {
        "title": "Day of Week Risk",
        "dimensions": ["day of week (0=Sun..6=Sat)"],
        "measures": ["total alert messages"],
        "time": "Aggregated across all dates",
        "sql": "SELECT CAST(strftime('%w', m.datetime_israel) AS INTEGER) as dow, COUNT(*) as total FROM messages m WHERE m.message_type='alert' AND m.is_drill=0 GROUP BY dow",
        "fields": ["messages.datetime_israel"],
    },
    "escalation": {
        "title": "Escalation Heatmap",
        "dimensions": ["date (top 10 busiest days)", "hour (0-23)"],
        "measures": ["alert count per hour-slot"],
        "time": "Top 10 days by alert volume",
        "sql": "SELECT date(m.datetime_israel) as date, CAST(strftime('%H', m.datetime_israel) AS INTEGER) as hour, COUNT(*) FROM messages m WHERE ... AND date IN (top 10 busiest days) GROUP BY date, hour",
        "fields": ["messages.datetime_israel"],
    },
    "city_timeline": {
        "title": "Top Cities Monthly Timeline",
        "dimensions": ["city_name (top 8)", "month"],
        "measures": ["alert count per city per month"],
        "time": "Monthly breakdown for top 8 cities",
        "sql": "SELECT c.city_name, strftime('%Y-%m', m.datetime_israel) as month, COUNT(*) FROM alert_details ad JOIN messages m ... JOIN cities c ... WHERE city IN (top 8) GROUP BY city, month",
        "fields": ["cities.city_name", "messages.datetime_israel", "alert_details.city_id"],
    },
    "response_time": {
        "title": "Alert Duration (Alert → All-Clear)",
        "dimensions": ["alert_type"],
        "measures": ["total", "with_clearance", "avg_minutes", "min_minutes", "max_minutes"],
        "time": "All data (not filtered — requires event_ended pairing)",
        "sql": "WITH paired AS (SELECT a.alert_type, (SELECT MIN(e.datetime_utc) FROM messages e WHERE e.message_type='event_ended' AND e.datetime_utc > a.datetime_utc AND e.datetime_utc <= datetime(a.datetime_utc, '+3 hours')) as ended_time ... GROUP BY alert_type",
        "fields": ["messages.datetime_utc", "messages.message_type", "messages.alert_type"],
    },
    "multi_zone": {
        "title": "Multi-Zone Attacks",
        "dimensions": ["msg_id (individual messages)"],
        "measures": ["zone_count", "city_count"],
        "time": "Filter range, only messages hitting ≥3 zones",
        "sql": "SELECT m.msg_id, COUNT(DISTINCT ad.zone_id) as zone_count, COUNT(DISTINCT ad.city_id) as city_count FROM ... GROUP BY m.msg_id HAVING zone_count >= 3",
        "fields": ["alert_details.zone_id", "alert_details.city_id", "zones.zone_name"],
    },
    "streaks": {
        "title": "Longest Alert Streaks",
        "dimensions": ["city_name"],
        "measures": ["streak_days", "streak_start", "streak_end"],
        "time": "All data (gap-and-island analysis)",
        "sql": "WITH city_days AS (SELECT DISTINCT city_name, date(datetime_israel) ...), numbered AS (... ROW_NUMBER() OVER ...), streaks AS (... GROUP BY city_name, grp) SELECT ... WHERE streak_days >= 5",
        "fields": ["cities.city_name", "messages.datetime_israel"],
    },
    "safest_10min": {
        "title": "Safest 10-Min Windows",
        "dimensions": ["10-minute time slot (144 per day)"],
        "measures": ["alert count per slot"],
        "time": "Aggregated across all dates",
        "sql": "SELECT hour, minute_bucket/10, COUNT(*) FROM messages WHERE message_type='alert' GROUP BY hour, minute_bucket",
        "fields": ["messages.datetime_israel"],
    },
    "drone_cities": {
        "title": "Top Drone/Aircraft Cities",
        "dimensions": ["city_name"],
        "measures": ["drone_alerts count"],
        "time": "Filter range, only aircraft alerts",
        "sql": "SELECT c.city_name, COUNT(*) as drone_alerts FROM ... WHERE m.alert_type='aircraft' GROUP BY city_name ORDER BY drone_alerts DESC LIMIT 25",
        "fields": ["cities.city_name", "messages.alert_type", "zones.zone_name"],
    },
    "city_zone_anomaly": {
        "title": "City vs Zone Anomaly",
        "dimensions": ["city_name", "zone_name"],
        "measures": ["city_alerts", "zone_avg", "ratio (city/zone_avg)"],
        "time": "Filter range",
        "sql": "WITH city_counts AS (...), zone_avg AS (SELECT zone_name, AVG(city_alerts) ...) SELECT city_name, city_alerts, zone_avg, ratio FROM ... WHERE num_cities >= 3",
        "fields": ["cities.city_name", "zones.zone_name", "alert_details.city_id", "alert_details.zone_id"],
    },
}

TRANSFORMATIONS = [
    {
        "field": "datetime_israel",
        "table": "messages",
        "source": "datetime_utc (from Telegram API)",
        "formula": "datetime_utc + timedelta(hours=3) if Apr-Oct, else + timedelta(hours=2)",
        "note": "Israel timezone: IDT (UTC+3) summer Apr-Oct, IST (UTC+2) winter Nov-Mar. Approximated by month.",
        "purpose": "All dashboard visualizations use Israel local time for X-axis and filtering.",
    },
    {
        "field": "message_type",
        "table": "messages",
        "source": "raw_text (parsed via classify_message())",
        "formula": "Pattern matching: 'alert' if contains siren emoji + threat keywords, 'event_ended' if contains 'האירוע הסתיים', 'heads_up' if contains 'שימו לב', etc. 9 categories total.",
        "note": "Categories: alert, update, event_ended, heads_up, can_leave_shelter, flash, instructions, intercept_report, other",
        "purpose": "Core filter — dashboard only visualizes message_type='alert' rows.",
    },
    {
        "field": "alert_type",
        "table": "messages",
        "source": "raw_text (parsed via classify_message())",
        "formula": "'rockets' if 'רקטות' or 'טילים', 'aircraft' if 'כלי טיס' or 'חדירת', 'infiltration' if 'חדירה', 'earthquake' if 'רעידת', 'hazmat' if 'חומרים'",
        "note": "5 threat types: rockets (63%), aircraft (22%), infiltration, earthquake, hazmat",
        "purpose": "Stacked bar charts, type filters, color coding throughout dashboard.",
    },
    {
        "field": "is_drill",
        "table": "messages",
        "source": "raw_text",
        "formula": "1 if text contains 'תרגיל' (drill), else 0",
        "note": "44 drill messages flagged. Excluded from all alert visualizations.",
        "purpose": "Filter out test/drill messages from real alert statistics.",
    },
    {
        "field": "alert_date / alert_time_local",
        "table": "messages",
        "source": "raw_text (parsed via extract_date_time())",
        "formula": "Regex extraction from 3 format eras: Old (2019-2021) 'בשעה HH:MM' format, Bold (2022-2025) '**date time**' format, New (2025+) '(DD.MM.YYYY) HH:MM:SS'",
        "note": "The date/time embedded in the message text itself (as written by Pikud HaOref), distinct from Telegram's datetime_utc.",
        "purpose": "Cross-validation with datetime_utc; used for messages where Telegram timestamp may differ.",
    },
    {
        "field": "zone_id / zone_name",
        "table": "alert_details / zones",
        "source": "raw_text (parsed via extract_zones_and_cities())",
        "formula": "Extract bold markers **אזור XXX** from text. Key rule: 'אזור' prefix alone is NOT a zone — only text wrapped in bold markdown (**...**) counts. Example: 'אזור תעשייה הדרומי אשקלון' is a city name, not a zone.",
        "note": "36 distinct zones (defense regions). Normalized to zones dimension table.",
        "purpose": "Zone-level aggregation, geographic grouping, treemap, anomaly detection.",
    },
    {
        "field": "city_id / city_name",
        "table": "alert_details / cities",
        "source": "raw_text (parsed via extract_zones_and_cities())",
        "formula": "Comma-split OR space-split (aircraft alerts) with multi-word prefix dictionary (קריית, כפר, בית, נאות, גשר...). ~1,998 cities after space-split fix.",
        "note": "Normalized to cities dimension table. Both dash and space variants kept in raw data.",
        "purpose": "City-level fact table (alert_details), top cities charts, streaks, safety rankings.",
    },
    {
        "field": "canonical_name",
        "table": "cities",
        "source": "city_name (post-build canonicalization)",
        "formula": "If both 'X-Y' and 'X Y' exist, map both to the spelling with more alert_details rows. 9 pairs: אבו-גוש→אבו גוש, בת-ים→בת ים, etc.",
        "note": "Raw city_name preserved. Visualizations use COALESCE(canonical_name, city_name) to display unified name.",
        "purpose": "Prevent double-counting in charts. Same city shouldn't appear twice with partial counts.",
    },
    {
        "field": "shelter_time",
        "table": "alert_details",
        "source": "raw_text (extracted per city block if present)",
        "formula": "Regex for time patterns like 'מיידי', '15 שניות', '30 שניות', 'דקה', 'דקה וחצי' following city names in newer message formats.",
        "note": "Not available in all messages. Represents how long residents have to reach shelter.",
        "purpose": "Shelter time distribution chart, risk assessment per city.",
    },
]


@app.route("/api/data_journey/examples")
def api_data_journey_examples():
    """Return diverse real examples showing the full data transformation journey."""
    import csv as csvmod

    db = get_shared_db()
    if not db:
        return jsonify({"error": "No DB"}), 404

    # Pick diverse examples
    example_queries = [
        ("single_city_rocket", "Single City — Rocket Alert",
         "A simple alert targeting one city in one zone. The most common message type.",
         """SELECT msg_id FROM messages WHERE message_type='alert' AND alert_type='rockets'
            AND msg_id IN (SELECT msg_id FROM alert_details GROUP BY msg_id HAVING COUNT(*)=1)
            ORDER BY RANDOM() LIMIT 1"""),
        ("multi_city_single_zone", "Multi-City, Single Zone — Rocket Barrage",
         "One alert covering multiple cities within the same defense zone.",
         """SELECT msg_id FROM messages WHERE message_type='alert' AND alert_type='rockets'
            AND msg_id IN (SELECT msg_id FROM alert_details GROUP BY msg_id
            HAVING COUNT(*) BETWEEN 3 AND 8 AND COUNT(DISTINCT zone_id)=1)
            ORDER BY RANDOM() LIMIT 1"""),
        ("multi_zone_attack", "Multi-Zone Attack — Mass Barrage",
         "A major attack spanning multiple defense zones and many cities simultaneously.",
         """SELECT msg_id FROM messages WHERE message_type='alert' AND alert_type='rockets'
            AND msg_id IN (SELECT msg_id FROM alert_details GROUP BY msg_id
            HAVING COUNT(DISTINCT zone_id)>=3 AND COUNT(*)>=8)
            ORDER BY RANDOM() LIMIT 1"""),
        ("aircraft_alert", "Aircraft / UAV Alert",
         "Hostile drone or aircraft intrusion alert — different threat type than rockets.",
         """SELECT msg_id FROM messages WHERE message_type='alert' AND alert_type='aircraft'
            AND msg_id IN (SELECT msg_id FROM alert_details GROUP BY msg_id
            HAVING COUNT(*) BETWEEN 2 AND 8)
            ORDER BY RANDOM() LIMIT 1"""),
        ("event_ended", "Event Ended — All Clear",
         "Not an alert — a system message announcing the threat has passed. Parsed as message_type='event_ended', no alert_details rows.",
         """SELECT msg_id FROM messages WHERE message_type='event_ended'
            ORDER BY RANDOM() LIMIT 1"""),
        ("drill_message", "Drill — Test Alert",
         "A real siren test. Detected by 'תרגיל' keyword. Flagged is_drill=1 and excluded from all statistics.",
         """SELECT msg_id FROM messages WHERE is_drill=1 AND message_type='alert'
            ORDER BY RANDOM() LIMIT 1"""),
        ("heads_up_message", "Heads-Up — Early Warning (התרעה מקדימה)",
         "An advance warning: 'alerts expected in your area in the coming minutes'. Classified as message_type='heads_up'. 99.2% are followed by a real alert within 15 min (avg 4.1 min). No alert_details rows — just a warning.",
         """SELECT msg_id FROM messages WHERE message_type='heads_up'
            ORDER BY RANDOM() LIMIT 1"""),
        ("can_leave_shelter", "Can Leave Shelter (ניתן לצאת מהמרחב המוגן)",
         "Notification that residents can leave shelter. Classified as message_type='can_leave_shelter'. Different from event_ended — this is specifically about shelter status.",
         """SELECT msg_id FROM messages WHERE message_type='can_leave_shelter'
            ORDER BY RANDOM() LIMIT 1"""),
    ]

    ver = get_current_version()
    ver_dir = os.path.join(DATA_DIR, f"v{ver}") if ver else None
    metadata = {}
    if ver_dir and os.path.exists(os.path.join(ver_dir, "metadata.json")):
        with open(os.path.join(ver_dir, "metadata.json")) as f:
            metadata = json.load(f)

    examples = []
    for ex_type, title, description, sql in example_queries:
        row = db.execute(sql).fetchone()
        if not row:
            continue
        msg_id = row[0]

        # Get full message
        msg = dict(db.execute("""
            SELECT msg_id, datetime_utc, datetime_israel, alert_date, alert_time_local,
                   message_type, alert_type, is_drill, raw_text, views
            FROM messages WHERE msg_id=?
        """, (msg_id,)).fetchone())

        # Get alert_details
        details = [dict(r) for r in db.execute("""
            SELECT z.zone_name, c.city_name, ad.shelter_time
            FROM alert_details ad
            LEFT JOIN zones z ON ad.zone_id=z.zone_id
            LEFT JOIN cities c ON ad.city_id=c.city_id
            WHERE ad.msg_id=?
        """, (msg_id,)).fetchall()]

        # Find CSV source
        csv_source = None
        for run in metadata.get("runs", []):
            if run["start_msg_id"] <= msg_id <= run["end_msg_id"]:
                csv_source = run["filename"]
                break

        # CSV row data
        csv_row = None
        if csv_source and ver_dir:
            csv_path = os.path.join(ver_dir, csv_source)
            if os.path.exists(csv_path):
                with open(csv_path, "r", encoding="utf-8-sig") as cf:
                    for r in csvmod.DictReader(cf):
                        if int(r["msg_id"]) == msg_id:
                            csv_row = dict(r)
                            break

        # Count contributions
        zone_names = list(set(d["zone_name"] for d in details if d["zone_name"]))
        city_names = [d["city_name"] for d in details if d["city_name"]]

        # Which views include this message
        views_included = []
        if msg["message_type"] == "alert" and not msg["is_drill"]:
            views_included = ["v_alerts_full", "v_city_alert_counts", "v_hourly_distribution", "v_daily_counts"]
        elif msg["message_type"] == "alert":
            views_included = ["v_alerts_full"]

        examples.append({
            "type": ex_type,
            "title": title,
            "description": description,
            "msg_id": msg_id,
            "telegram": {
                "raw_text": msg["raw_text"],
                "datetime_utc": msg["datetime_utc"],
                "views": msg["views"],
            },
            "csv": {
                "source_file": csv_source,
                "row": csv_row,
            },
            "db": {
                "messages_row": {
                    "msg_id": msg["msg_id"],
                    "datetime_utc": msg["datetime_utc"],
                    "datetime_israel": msg["datetime_israel"],
                    "message_type": msg["message_type"],
                    "alert_type": msg["alert_type"],
                    "is_drill": msg["is_drill"],
                    "alert_date": msg["alert_date"],
                    "alert_time_local": msg["alert_time_local"],
                },
                "alert_details": details,
                "detail_count": len(details),
                "zones": zone_names,
                "cities": city_names,
                "zone_count": len(zone_names),
                "city_count": len(city_names),
            },
            "counting": {
                "alert_messages": 1 if msg["message_type"] == "alert" and not msg["is_drill"] else 0,
                "city_alerts": len(city_names) if msg["message_type"] == "alert" and not msg["is_drill"] else 0,
                "zone_alerts": len(zone_names) if msg["message_type"] == "alert" and not msg["is_drill"] else 0,
            },
            "calculated_fields": [
                {"field": "datetime_israel", "value": msg["datetime_israel"],
                 "formula": f"datetime_utc ({msg['datetime_utc']}) + UTC offset"},
                {"field": "message_type", "value": msg["message_type"],
                 "formula": "classify_message(raw_text)"},
                {"field": "alert_type", "value": msg["alert_type"],
                 "formula": "classify_message(raw_text) → threat type"},
                {"field": "is_drill", "value": msg["is_drill"],
                 "formula": "'תרגיל' in raw_text → 1, else 0"},
            ],
            "views_included": views_included,
        })

    return jsonify(examples)


@app.route("/api/stat_sql")
def api_stat_sql():
    """Return SQL metadata for each stat card."""
    return jsonify(STAT_SQL)


@app.route("/api/viz/meta")
def api_viz_meta():
    """Return metadata for all visualizations."""
    return jsonify(VIZ_META)


@app.route("/api/transformations")
def api_transformations():
    """Return calculated field documentation."""
    return jsonify(TRANSFORMATIONS)


@app.route("/api/pipeline/sample_check", methods=["POST"])
def api_pipeline_sample_check():
    """Pick a random message from the DB, show its raw data, CSV source, and DB representation."""
    import csv as csvmod
    import random

    db = get_shared_db()
    if not db:
        return jsonify({"error": "No DB"}), 404

    # Pick random alert message from DB
    total = db.execute("SELECT COUNT(*) c FROM messages WHERE message_type='alert'").fetchone()["c"]
    offset = random.randint(0, max(0, total - 1))
    msg = db.execute("""
        SELECT m.msg_id, m.datetime_utc, m.datetime_israel, m.message_type, m.alert_type,
               m.is_drill, m.raw_text, m.alert_date, m.alert_time_local
        FROM messages m WHERE m.message_type='alert'
        LIMIT 1 OFFSET ?
    """, (offset,)).fetchone()
    if not msg:
        return jsonify({"error": "No messages found"})

    msg_id = msg["msg_id"]

    # Get alert_details for this message
    details = [dict(r) for r in db.execute("""
        SELECT ad.id, z.zone_name, c.city_name, ad.shelter_time
        FROM alert_details ad
        LEFT JOIN zones z ON ad.zone_id=z.zone_id
        LEFT JOIN cities c ON ad.city_id=c.city_id
        WHERE ad.msg_id=?
    """, (msg_id,)).fetchall()]

    # Find which CSV file contains this msg_id
    ver = get_current_version()
    csv_source = None
    csv_row = None
    if ver:
        ver_dir = os.path.join(DATA_DIR, f"v{ver}")
        meta_path = os.path.join(ver_dir, "metadata.json")
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                metadata = json.load(f)
            for run in metadata.get("runs", []):
                if run["start_msg_id"] <= msg_id <= run["end_msg_id"]:
                    csv_path = os.path.join(ver_dir, run["filename"])
                    if os.path.exists(csv_path):
                        with open(csv_path, "r", encoding="utf-8-sig") as cf:
                            for row in csvmod.DictReader(cf):
                                if int(row["msg_id"]) == msg_id:
                                    csv_source = run["filename"]
                                    csv_row = dict(row)
                                    break
                    break

    return jsonify({
        "msg_id": msg_id,
        "db_record": {
            "msg_id": msg["msg_id"],
            "datetime_utc": msg["datetime_utc"],
            "datetime_israel": msg["datetime_israel"],
            "message_type": msg["message_type"],
            "alert_type": msg["alert_type"],
            "is_drill": msg["is_drill"],
            "alert_date": msg["alert_date"],
            "alert_time_local": msg["alert_time_local"],
            "raw_text": msg["raw_text"],
        },
        "alert_details": details,
        "csv_source": csv_source,
        "csv_row": csv_row,
        "detail_count": len(details),
    })


# ============================================================
# API: PIPELINE DATA ENGINEER
# ============================================================

@app.route("/api/pipeline/versions")
def api_pipeline_versions():
    """Full version/file/delta inventory for the data engineer page."""
    import csv as csvmod

    versions = []
    if not os.path.exists(DATA_DIR):
        return jsonify({"versions": [], "current": None})

    current_ver = get_current_version()
    ver_dirs = sorted(d for d in os.listdir(DATA_DIR)
                      if d.startswith("v") and os.path.isdir(os.path.join(DATA_DIR, d)))

    for vdir in ver_dirs:
        ver_num = vdir.replace("v", "")
        ver_path = os.path.join(DATA_DIR, vdir)
        meta_path = os.path.join(ver_path, "metadata.json")
        metadata = {}
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                metadata = json.load(f)

        # List all files with sizes
        files = []
        for fname in sorted(os.listdir(ver_path)):
            fpath = os.path.join(ver_path, fname)
            if os.path.isfile(fpath):
                st = os.stat(fpath)
                files.append({
                    "name": fname,
                    "size_bytes": st.st_size,
                    "size_human": _human_size(st.st_size),
                    "modified": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                })

        # Build run details with DB verification
        db_path = os.path.join(DB_DIR, f"pikud_v{ver_num}.db")
        db_exists = os.path.exists(db_path)
        db_msg_ids = set()
        db_msg_count = 0
        if db_exists:
            try:
                conn = sqlite3.connect(db_path)
                db_msg_count = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
                # Get all msg_ids in DB for delta verification
                db_msg_ids = set(r[0] for r in conn.execute("SELECT msg_id FROM messages").fetchall())
                conn.close()
            except Exception:
                pass

        runs = []
        for i, run in enumerate(metadata.get("runs", [])):
            csv_path = os.path.join(ver_path, run["filename"])
            csv_exists = os.path.exists(csv_path)
            csv_row_count = 0
            csv_msg_ids_in_db = 0
            csv_msg_ids_missing = 0

            if csv_exists and db_exists:
                try:
                    with open(csv_path, "r", encoding="utf-8-sig") as cf:
                        for row in csvmod.DictReader(cf):
                            csv_row_count += 1
                            mid = int(row["msg_id"])
                            if mid in db_msg_ids:
                                csv_msg_ids_in_db += 1
                            else:
                                csv_msg_ids_missing += 1
                except Exception:
                    pass

            file_info = next((f for f in files if f["name"] == run["filename"]), None)
            runs.append({
                "index": i,
                "filename": run["filename"],
                "fetched_at": run.get("fetched_at"),
                "message_count": run.get("message_count", 0),
                "start_msg_id": run.get("start_msg_id"),
                "end_msg_id": run.get("end_msg_id"),
                "start_date": run.get("start_date"),
                "end_date": run.get("end_date"),
                "file_size": file_info["size_human"] if file_info else "—",
                "file_exists": csv_exists,
                "is_initial": i == 0,
                "in_db": csv_msg_ids_in_db,
                "missing_from_db": csv_msg_ids_missing,
                "db_verified": csv_exists and db_exists and csv_msg_ids_missing == 0 and csv_msg_ids_in_db > 0,
            })

        versions.append({
            "version": ver_num,
            "is_current": ver_num == current_ver,
            "path": ver_path,
            "files": files,
            "runs": runs,
            "total_messages": metadata.get("total_messages", 0),
            "last_msg_id": metadata.get("last_msg_id", 0),
            "db_exists": db_exists,
            "db_msg_count": db_msg_count,
            "db_size": _human_size(os.path.getsize(db_path)) if db_exists else "—",
        })

    return jsonify({"versions": versions, "current": current_ver})


@app.route("/api/pipeline/validate/<check>", methods=["POST"])
def api_pipeline_validate_check(check):
    """Run individual validation checks."""
    import csv as csvmod

    ver = get_current_version()
    if not ver:
        return jsonify({"ok": False, "error": "No data version found"})

    ver_dir = os.path.join(DATA_DIR, f"v{ver}")
    db_path = os.path.join(DB_DIR, f"pikud_v{ver}.db")
    meta_path = os.path.join(ver_dir, "metadata.json")

    if not os.path.exists(meta_path):
        return jsonify({"ok": False, "error": "No metadata.json"})

    with open(meta_path) as f:
        metadata = json.load(f)

    result = {"check": check, "ok": True, "details": [], "issues": []}

    if check == "csv_files_exist":
        for run in metadata.get("runs", []):
            path = os.path.join(ver_dir, run["filename"])
            exists = os.path.exists(path)
            result["details"].append(f"{run['filename']}: {'✓ exists' if exists else '✗ MISSING'}")
            if not exists:
                result["ok"] = False
                result["issues"].append(f"Missing: {run['filename']}")

    elif check == "no_duplicate_ids":
        all_ids = set()
        dupes = []
        csv_files = sorted(f for f in os.listdir(ver_dir) if f.startswith("alerts_") and f.endswith(".csv"))
        for csv_file in csv_files:
            with open(os.path.join(ver_dir, csv_file), "r", encoding="utf-8-sig") as cf:
                for row in csvmod.DictReader(cf):
                    mid = int(row["msg_id"])
                    if mid in all_ids:
                        dupes.append(mid)
                    all_ids.add(mid)
        result["details"].append(f"Total unique IDs: {len(all_ids)}")
        if dupes:
            result["ok"] = False
            result["issues"].append(f"{len(dupes)} duplicate msg_ids found: {dupes[:10]}")
        else:
            result["details"].append("✓ No duplicates")

    elif check == "row_count_match":
        csv_files = sorted(f for f in os.listdir(ver_dir) if f.startswith("alerts_") and f.endswith(".csv"))
        total_rows = 0
        for csv_file in csv_files:
            count = 0
            with open(os.path.join(ver_dir, csv_file), "r", encoding="utf-8-sig") as cf:
                for _ in csvmod.DictReader(cf):
                    count += 1
            total_rows += count
            result["details"].append(f"{csv_file}: {count:,} rows")
        expected = metadata.get("total_messages", 0)
        result["details"].append(f"CSV total: {total_rows:,} | Metadata expects: {expected:,}")
        if total_rows != expected:
            result["ok"] = False
            result["issues"].append(f"Mismatch: CSVs={total_rows}, metadata={expected}")

    elif check == "id_ranges_no_overlap":
        ranges = [(r["start_msg_id"], r["end_msg_id"], r["filename"]) for r in metadata.get("runs", [])]
        ranges.sort()
        for i in range(len(ranges)):
            s, e, fn = ranges[i]
            result["details"].append(f"{fn}: {s:,} → {e:,}")
        for i in range(1, len(ranges)):
            if ranges[i][0] <= ranges[i-1][1]:
                result["ok"] = False
                result["issues"].append(f"Overlap: {ranges[i-1][2]} ends at {ranges[i-1][1]}, {ranges[i][2]} starts at {ranges[i][0]}")
        if result["ok"]:
            result["details"].append("✓ No overlaps")

    elif check == "db_msg_count":
        if not os.path.exists(db_path):
            result["ok"] = False
            result["issues"].append("DB file not found")
        else:
            conn = sqlite3.connect(db_path)
            db_count = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            conn.close()
            csv_files = sorted(f for f in os.listdir(ver_dir) if f.startswith("alerts_") and f.endswith(".csv"))
            csv_total = 0
            for csv_file in csv_files:
                with open(os.path.join(ver_dir, csv_file), "r", encoding="utf-8-sig") as cf:
                    csv_total += sum(1 for _ in csvmod.DictReader(cf))
            result["details"].append(f"DB messages: {db_count:,}")
            result["details"].append(f"CSV messages: {csv_total:,}")
            if db_count != csv_total:
                result["ok"] = False
                result["issues"].append(f"DB has {db_count}, CSVs have {csv_total}")

    elif check == "no_orphan_details":
        if not os.path.exists(db_path):
            result["ok"] = False
            result["issues"].append("DB file not found")
        else:
            conn = sqlite3.connect(db_path)
            orphans = conn.execute(
                "SELECT COUNT(*) FROM alert_details WHERE msg_id NOT IN (SELECT msg_id FROM messages)"
            ).fetchone()[0]
            total_details = conn.execute("SELECT COUNT(*) FROM alert_details").fetchone()[0]
            conn.close()
            result["details"].append(f"Total alert_details: {total_details:,}")
            result["details"].append(f"Orphaned rows: {orphans:,}")
            if orphans > 0:
                result["ok"] = False
                result["issues"].append(f"{orphans} orphaned alert_details rows")

    elif check == "deltas_in_db":
        if not os.path.exists(db_path):
            result["ok"] = False
            result["issues"].append("DB file not found")
        else:
            conn = sqlite3.connect(db_path)
            db_ids = set(r[0] for r in conn.execute("SELECT msg_id FROM messages").fetchall())
            conn.close()
            for run in metadata.get("runs", []):
                csv_path = os.path.join(ver_dir, run["filename"])
                if not os.path.exists(csv_path):
                    result["issues"].append(f"{run['filename']}: file missing")
                    result["ok"] = False
                    continue
                found = 0
                missing = 0
                with open(csv_path, "r", encoding="utf-8-sig") as cf:
                    for row in csvmod.DictReader(cf):
                        if int(row["msg_id"]) in db_ids:
                            found += 1
                        else:
                            missing += 1
                status = "✓" if missing == 0 else "✗"
                result["details"].append(f"{status} {run['filename']}: {found:,} in DB, {missing:,} missing")
                if missing > 0:
                    result["ok"] = False
                    result["issues"].append(f"{run['filename']}: {missing} messages not in DB")

    elif check == "db_version_match":
        if not os.path.exists(db_path):
            result["ok"] = False
            result["issues"].append("DB file not found")
        else:
            conn = sqlite3.connect(db_path)
            row = conn.execute("SELECT value FROM db_info WHERE key='data_version'").fetchone()
            conn.close()
            db_ver = row[0] if row else None
            result["details"].append(f"Current data version: v{ver}")
            result["details"].append(f"DB built from version: v{db_ver}" if db_ver else "DB has no version info")
            if db_ver != ver:
                result["ok"] = False
                result["issues"].append(f"Version mismatch: data=v{ver}, DB=v{db_ver}")
    else:
        return jsonify({"ok": False, "error": f"Unknown check: {check}"}), 400

    return jsonify(result)


def _human_size(nbytes):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}" if unit != 'B' else f"{nbytes} B"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


# ============================================================
# API: EXPORT
# ============================================================

@app.route("/api/export/<mode>")
def api_export(mode):
    """Export database as CSV ZIP. mode: 'full' (with calculated) or 'raw' (without)."""
    from dashboard_app.export import export_to_zip
    if mode not in ("full", "raw"):
        return jsonify({"error": "Mode must be 'full' or 'raw'"}), 400
    db = get_shared_db()
    if not db:
        return jsonify({"error": "No DB"}), 404
    include_calc = mode == "full"
    zip_bytes = export_to_zip(db, include_calculated=include_calc)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"pikud_export_{mode}_{ts}.zip"
    return Response(zip_bytes, mimetype="application/zip",
                    headers={"Content-Disposition": f"attachment; filename={filename}"})


# ============================================================
# API: LOGS
# ============================================================

@app.route("/api/logs")
def api_logs():
    logs = []
    for f in sorted(os.listdir(LOGS_DIR), reverse=True):
        if f.endswith(".log"):
            st = os.stat(os.path.join(LOGS_DIR, f))
            logs.append({"name": f, "size": st.st_size,
                         "modified": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")})
    return jsonify(logs)


@app.route("/api/logs/<name>")
def api_log_content(name):
    if ".." in name or "/" in name:
        return jsonify({"error": "Invalid"}), 400
    p = os.path.join(LOGS_DIR, name)
    if not os.path.exists(p):
        return jsonify({"error": "Not found"}), 404
    with open(p) as f:
        return jsonify({"name": name, "content": f.read()})


if __name__ == "__main__":
    print("Pikud HaOref Dashboard → http://localhost:5000")
    host = os.environ.get("HOST", "0.0.0.0")  # 0.0.0.0 = accessible from WiFi network
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV") != "production"
    import subprocess
    try:
        ip = subprocess.check_output(["ipconfig", "getifaddr", "en0"], text=True).strip()
    except Exception:
        ip = "localhost"
    print(f"  → WiFi access: http://{ip}:{port}/summary")
    app.run(host=host, port=port, debug=debug, threaded=True)
