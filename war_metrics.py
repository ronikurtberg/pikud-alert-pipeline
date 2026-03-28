#!/usr/bin/env python3
"""
War Metrics Report — Israel Under Fire
========================================

Generates precise, numbered metrics about the war for US management briefing.
All calculations follow the methods defined in TABLEAU_GUIDE.md and STORY.md:

- **Three counting levels** (STORY.md §"One Message ≠ One City"):
    Attack Events  = messages grouped by 2-min gap (most conservative)
    Zone Alerts    = COUNTD(msg_id + zone_id) pairs (middle ground)
    City Alerts    = COUNT(alert_details.id) (most granular)

- **Measures** from TABLEAU_GUIDE.md Step 5:
    Alert_Count, Alert_Events, Cities_Affected, Zones_Affected,
    Rocket_Alerts, Aircraft_Alerts, Civilian_Danger_Score,
    Avg_Danger_Per_Alert, Immediate_Danger_Rate, Night_Attack_Rate,
    Days_With_Sirens, Zone_Alert_Count

- **Semantic Metrics** from TABLEAU_GUIDE.md Step 5:
    Days_Under_Fire, Multi_Front_Pressure, Immediate_Danger_Pct,
    Night_Terror_Index, Cities_Under_Fire, Avg_Danger_Level

- **Base filter** (TABLEAU_GUIDE.md Step 6):
    Alert_Date_Parsed >= 2026-02-28 AND Is_Real_Alert = TRUE
    → SQL: message_type='alert' AND is_drill=0 AND datetime_israel >= '2026-02-28'

- **Shelter_Danger_Weight** scale (TABLEAU_GUIDE.md):
    מיידי=10, 15s=8, 30s=6, 45s=4, 60s=2, 90s=1, 3m=0.5, else=3

Usage:
    python3 war_metrics.py                # full report
    python3 war_metrics.py --week 09      # ISO week 09 only
    python3 war_metrics.py --json         # machine-readable output
    python3 war_metrics.py --since 2026-03-01  # custom start date
"""

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "db")
DATA_DIR = os.path.join(BASE_DIR, "data")

# War period start (per TABLEAU_GUIDE.md Step 6 base filter)
WAR_START = "2026-02-28"

# Shelter Danger Weight (TABLEAU_GUIDE.md Step 5 — Shelter_Danger_Weight)
SHELTER_DANGER_WEIGHT_SQL = """
CASE ad.shelter_time
    WHEN 'מיידי' THEN 10
    WHEN '15 שניות' THEN 8
    WHEN '30 שניות' THEN 6
    WHEN '45 שניות' THEN 4
    WHEN 'דקה' THEN 2
    WHEN 'דקה וחצי' THEN 1
    WHEN '3 דקות' THEN 0.5
    ELSE 3
END
"""


def get_db():
    """Connect to the current database."""
    link = os.path.join(DB_DIR, "current")
    if os.path.islink(link):
        db_path = os.path.join(DB_DIR, os.readlink(link))
    else:
        db_path = os.path.join(DB_DIR, "pikud_v1.db")
    if not os.path.exists(db_path):
        print(f"ERROR: Database not found at {db_path}", file=sys.stderr)
        sys.exit(1)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def scalar(conn, sql, params=()):
    """Execute SQL and return single scalar value."""
    return conn.execute(sql, params).fetchone()[0]


def compute_metrics(conn, date_from=None, iso_week=None):
    """Compute all war metrics. Returns dict of {metric_name: value}.

    Filtering:
      - Always: message_type='alert' AND is_drill=0  (Is_Real_Alert per TABLEAU_GUIDE)
      - date_from: datetime_israel >= date_from
      - iso_week: strftime('%W', datetime_israel) = week AND year = 2026
    """
    date_from = date_from or WAR_START

    # Build date filter
    if iso_week:
        df = f"AND strftime('%W', m.datetime_israel) = '{iso_week:02d}' AND strftime('%Y', m.datetime_israel) = '2026'"
        df_nomsg = df.replace("m.datetime_israel", "datetime_israel")
        period_label = f"ISO Week {iso_week}, 2026"
    else:
        df = f"AND m.datetime_israel >= '{date_from}'"
        df_nomsg = f"AND datetime_israel >= '{date_from}'"
        period_label = f"{date_from} → now"

    m = {}  # metrics dict
    m["_period"] = period_label

    # ------------------------------------------------------------------
    # DATE RANGE
    # ------------------------------------------------------------------
    row = conn.execute(f"""
        SELECT MIN(datetime_israel) as first_alert,
               MAX(datetime_israel) as last_alert
        FROM messages WHERE message_type='alert' AND is_drill=0 {df_nomsg}
    """).fetchone()
    m["period_start"] = row["first_alert"]
    m["period_end"] = row["last_alert"]

    # ------------------------------------------------------------------
    # CORE COUNTS (TABLEAU_GUIDE Step 5 measures)
    # ------------------------------------------------------------------

    # Alert_Events — COUNTD(msg_id) per TABLEAU_GUIDE
    m["alert_messages"] = scalar(conn, f"""
        SELECT COUNT(*) FROM messages
        WHERE message_type='alert' AND is_drill=0 {df_nomsg}
    """)

    # Attack Events — 2-min gap grouping per STORY.md
    m["attack_events"] = scalar(conn, f"""
        SELECT SUM(is_new) FROM (
            SELECT CASE WHEN LAG(datetime_utc) OVER (ORDER BY datetime_utc) IS NULL
                        OR (julianday(datetime_utc) - julianday(LAG(datetime_utc) OVER (ORDER BY datetime_utc))) * 24 * 60 > 2
                   THEN 1 ELSE 0 END as is_new
            FROM messages WHERE message_type='alert' AND is_drill=0 {df_nomsg}
        )
    """) or 0

    # Alert_Count — COUNT(alert_details.id) per TABLEAU_GUIDE
    m["city_level_alerts"] = scalar(conn, f"""
        SELECT COUNT(*) FROM alert_details ad
        JOIN messages m ON ad.msg_id = m.msg_id
        WHERE m.message_type='alert' AND m.is_drill=0 {df}
    """)

    # Zone_Alert_Count — COUNTD(msg_id || '-' || zone_id) per TABLEAU_GUIDE
    m["zone_level_alerts"] = scalar(conn, f"""
        SELECT COUNT(DISTINCT ad.msg_id || '-' || ad.zone_id)
        FROM alert_details ad JOIN messages m ON ad.msg_id = m.msg_id
        WHERE m.message_type='alert' AND m.is_drill=0 {df}
    """)

    # Cities_Affected — COUNTD(city_id) per TABLEAU_GUIDE
    m["cities_affected"] = scalar(conn, f"""
        SELECT COUNT(DISTINCT ad.city_id)
        FROM alert_details ad JOIN messages m ON ad.msg_id = m.msg_id
        WHERE m.message_type='alert' AND m.is_drill=0 {df}
    """)

    # Zones_Affected — COUNTD(zone_id) per TABLEAU_GUIDE
    m["zones_affected"] = scalar(conn, f"""
        SELECT COUNT(DISTINCT ad.zone_id)
        FROM alert_details ad JOIN messages m ON ad.msg_id = m.msg_id
        JOIN zones z ON ad.zone_id = z.zone_id
        WHERE m.message_type='alert' AND m.is_drill=0 {df}
    """)

    # ------------------------------------------------------------------
    # SEMANTIC METRICS (TABLEAU_GUIDE Step 5 — Semantic Metrics)
    # ------------------------------------------------------------------

    # Days_Under_Fire (Days_With_Sirens) — COUNTD(DATE(Israel_DateTime))
    m["days_with_sirens"] = scalar(conn, f"""
        SELECT COUNT(DISTINCT date(datetime_israel))
        FROM messages WHERE message_type='alert' AND is_drill=0 {df_nomsg}
    """)

    # Total calendar days in period
    if m["period_start"] and m["period_end"]:
        start_dt = m["period_start"][:10]
        end_dt = m["period_end"][:10]
        m["calendar_days"] = scalar(conn,
            "SELECT CAST(julianday(?) - julianday(?) + 1 AS INTEGER)", (end_dt, start_dt))
        if m["calendar_days"] and m["calendar_days"] > 0:
            m["pct_days_with_sirens"] = round(100.0 * m["days_with_sirens"] / m["calendar_days"], 1)
        else:
            m["pct_days_with_sirens"] = None
    else:
        m["calendar_days"] = 0
        m["pct_days_with_sirens"] = None

    # ------------------------------------------------------------------
    # THREAT BREAKDOWN (TABLEAU_GUIDE — Rocket_Alerts, Aircraft_Alerts)
    # ------------------------------------------------------------------

    # Rocket_Alerts — COUNTD(IF Extract_Threat_Type = "rockets" THEN id END)
    m["rocket_alerts_msgs"] = scalar(conn, f"""
        SELECT COUNT(*) FROM messages
        WHERE message_type='alert' AND is_drill=0 AND alert_type='rockets' {df_nomsg}
    """)
    m["aircraft_alerts_msgs"] = scalar(conn, f"""
        SELECT COUNT(*) FROM messages
        WHERE message_type='alert' AND is_drill=0 AND alert_type='aircraft' {df_nomsg}
    """)

    # City-level by threat type
    m["rocket_city_alerts"] = scalar(conn, f"""
        SELECT COUNT(*) FROM alert_details ad
        JOIN messages m ON ad.msg_id = m.msg_id
        WHERE m.message_type='alert' AND m.is_drill=0 AND m.alert_type='rockets' {df}
    """)
    m["aircraft_city_alerts"] = scalar(conn, f"""
        SELECT COUNT(*) FROM alert_details ad
        JOIN messages m ON ad.msg_id = m.msg_id
        WHERE m.message_type='alert' AND m.is_drill=0 AND m.alert_type='aircraft' {df}
    """)

    # ------------------------------------------------------------------
    # DANGER METRICS (TABLEAU_GUIDE — Shelter_Danger_Weight, Civilian_Danger_Score)
    # ------------------------------------------------------------------

    # Civilian_Danger_Score — SUM(Shelter_Danger_Weight)
    m["civilian_danger_score"] = scalar(conn, f"""
        SELECT COALESCE(SUM({SHELTER_DANGER_WEIGHT_SQL}), 0)
        FROM alert_details ad JOIN messages m ON ad.msg_id = m.msg_id
        WHERE m.message_type='alert' AND m.is_drill=0 {df}
    """)

    # Avg_Danger_Per_Alert — SUM(weight) / COUNT(id)
    m["avg_danger_per_alert"] = scalar(conn, f"""
        SELECT ROUND(SUM({SHELTER_DANGER_WEIGHT_SQL}) * 1.0 / COUNT(*), 2)
        FROM alert_details ad JOIN messages m ON ad.msg_id = m.msg_id
        WHERE m.message_type='alert' AND m.is_drill=0 {df}
    """)

    # Immediate_Danger_Rate — % of alerts with shelter_time = 'מיידי'
    m["immediate_danger_pct"] = scalar(conn, f"""
        SELECT ROUND(100.0 * COUNT(CASE WHEN ad.shelter_time = 'מיידי' THEN 1 END)
                     / NULLIF(COUNT(*), 0), 1)
        FROM alert_details ad JOIN messages m ON ad.msg_id = m.msg_id
        WHERE m.message_type='alert' AND m.is_drill=0 {df}
    """)

    # ------------------------------------------------------------------
    # TIME PATTERN METRICS (TABLEAU_GUIDE — Night_Attack_Rate, Hour_Label)
    # ------------------------------------------------------------------

    # Night_Terror_Index — % of alerts between midnight–6 AM
    m["night_attack_pct"] = scalar(conn, f"""
        SELECT ROUND(100.0 * COUNT(CASE
            WHEN CAST(strftime('%H', datetime_israel) AS INTEGER) >= 0
             AND CAST(strftime('%H', datetime_israel) AS INTEGER) < 6 THEN 1 END)
            / NULLIF(COUNT(*), 0), 1)
        FROM messages WHERE message_type='alert' AND is_drill=0 {df_nomsg}
    """)

    # Peak attack hour
    peak_hour = conn.execute(f"""
        SELECT CAST(strftime('%H', datetime_israel) AS INTEGER) as h, COUNT(*) as c
        FROM messages WHERE message_type='alert' AND is_drill=0 {df_nomsg}
        GROUP BY h ORDER BY c DESC LIMIT 1
    """).fetchone()
    if peak_hour:
        h = peak_hour["h"]
        m["peak_hour"] = f"{h:02d}:00–{h:02d}:59"
        m["peak_hour_alerts"] = peak_hour["c"]

    # ------------------------------------------------------------------
    # INTENSITY METRICS (derived)
    # ------------------------------------------------------------------

    days = m["days_with_sirens"] or 1
    m["avg_attack_events_per_day"] = round(m["attack_events"] / days, 1)
    m["avg_city_alerts_per_day"] = round(m["city_level_alerts"] / days, 1)
    m["avg_zones_hit_per_day"] = scalar(conn, f"""
        SELECT ROUND(AVG(z_cnt), 1) FROM (
            SELECT date(m.datetime_israel) as dt, COUNT(DISTINCT ad.zone_id) as z_cnt
            FROM alert_details ad JOIN messages m ON ad.msg_id = m.msg_id
            WHERE m.message_type='alert' AND m.is_drill=0 {df}
            GROUP BY dt
        )
    """)
    m["avg_cities_per_day"] = scalar(conn, f"""
        SELECT ROUND(AVG(c_cnt), 1) FROM (
            SELECT date(m.datetime_israel) as dt, COUNT(DISTINCT c.city_id) as c_cnt
            FROM alert_details ad JOIN messages m ON ad.msg_id = m.msg_id
            JOIN cities c ON ad.city_id = c.city_id
            WHERE m.message_type='alert' AND m.is_drill=0 {df}
            GROUP BY dt
        )
    """)

    # Busiest single day
    busiest = conn.execute(f"""
        SELECT date(datetime_israel) as dt, COUNT(*) as c
        FROM messages WHERE message_type='alert' AND is_drill=0 {df_nomsg}
        GROUP BY dt ORDER BY c DESC LIMIT 1
    """).fetchone()
    if busiest:
        m["busiest_day"] = busiest["dt"]
        m["busiest_day_alert_msgs"] = busiest["c"]
        m["busiest_day_city_alerts"] = scalar(conn, f"""
            SELECT COUNT(*) FROM alert_details ad
            JOIN messages m ON ad.msg_id = m.msg_id
            WHERE m.message_type='alert' AND m.is_drill=0
            AND date(m.datetime_israel) = '{busiest["dt"]}'
        """)

    # ------------------------------------------------------------------
    # TOP TARGETS (English names for US audience)
    # ------------------------------------------------------------------
    m["top_10_cities"] = [
        {"city": r["city"], "alerts": r["alerts"]}
        for r in conn.execute(f"""
            SELECT COALESCE(c.city_name_en, c.canonical_name, c.city_name) as city,
                   COUNT(*) as alerts
            FROM alert_details ad JOIN messages m ON ad.msg_id = m.msg_id
            JOIN cities c ON ad.city_id = c.city_id
            WHERE m.message_type='alert' AND m.is_drill=0 {df}
            GROUP BY city ORDER BY alerts DESC LIMIT 10
        """).fetchall()
    ]

    m["top_zones"] = [
        {"zone": r["zone"], "alert_events": r["evts"], "city_alerts": r["ca"]}
        for r in conn.execute(f"""
            SELECT COALESCE(z.zone_name_en, z.zone_name) as zone,
                   COUNT(DISTINCT ad.msg_id) as evts,
                   COUNT(*) as ca
            FROM alert_details ad JOIN messages m ON ad.msg_id = m.msg_id
            JOIN zones z ON ad.zone_id = z.zone_id
            WHERE m.message_type='alert' AND m.is_drill=0 {df}
            GROUP BY zone ORDER BY ca DESC LIMIT 10
        """).fetchall()
    ]

    # ------------------------------------------------------------------
    # SHELTER TIME DISTRIBUTION (Shelter_Danger_Level per TABLEAU_GUIDE)
    # ------------------------------------------------------------------
    total_with_shelter = scalar(conn, f"""
        SELECT COUNT(*) FROM alert_details ad
        JOIN messages m ON ad.msg_id = m.msg_id
        WHERE m.message_type='alert' AND m.is_drill=0 AND ad.shelter_time IS NOT NULL {df}
    """) or 1

    m["shelter_time_breakdown"] = [
        {"shelter_time": r["st"], "label": r["lbl"], "count": r["cnt"],
         "pct": round(100.0 * r["cnt"] / total_with_shelter, 1)}
        for r in conn.execute(f"""
            SELECT ad.shelter_time as st,
                   CASE ad.shelter_time
                       WHEN 'מיידי' THEN 'Immediate (0s)'
                       WHEN '15 שניות' THEN 'Critical (15s)'
                       WHEN '30 שניות' THEN 'Severe (30s)'
                       WHEN '45 שניות' THEN 'High (45s)'
                       WHEN 'דקה' THEN 'Moderate (60s)'
                       WHEN 'דקה וחצי' THEN 'Standard (90s)'
                       WHEN '3 דקות' THEN 'Extended (3min)'
                       ELSE 'Aircraft (10min)'
                   END as lbl,
                   COUNT(*) as cnt
            FROM alert_details ad JOIN messages m ON ad.msg_id = m.msg_id
            WHERE m.message_type='alert' AND m.is_drill=0 AND ad.shelter_time IS NOT NULL {df}
            GROUP BY ad.shelter_time ORDER BY cnt DESC
        """).fetchall()
    ]

    # ------------------------------------------------------------------
    # WEEKLY BREAKDOWN
    # ------------------------------------------------------------------
    m["weekly"] = [
        {"week": r["wk"], "start": r["ws"], "end": r["we"],
         "alert_msgs": r["am"], "rockets": r["rk"], "aircraft": r["ac"]}
        for r in conn.execute(f"""
            SELECT strftime('%Y-W%W', datetime_israel) as wk,
                   MIN(date(datetime_israel)) as ws,
                   MAX(date(datetime_israel)) as we,
                   COUNT(*) as am,
                   SUM(CASE WHEN alert_type='rockets' THEN 1 ELSE 0 END) as rk,
                   SUM(CASE WHEN alert_type='aircraft' THEN 1 ELSE 0 END) as ac
            FROM messages WHERE message_type='alert' AND is_drill=0 {df_nomsg}
            GROUP BY wk ORDER BY wk
        """).fetchall()
    ]

    return m


def format_report(m):
    """Format metrics as a readable text report for US management."""
    lines = []
    ln = lines.append

    ln("=" * 72)
    ln("  ISRAEL UNDER FIRE — WAR STATUS BRIEFING")
    ln(f"  Period: {m['_period']}")
    ln(f"  Data range: {m.get('period_start', 'N/A')} → {m.get('period_end', 'N/A')}")
    ln(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} (Israel Time)")
    ln("=" * 72)

    ln("")
    ln("─" * 72)
    ln("  SECTION 1: SCALE OF ATTACK")
    ln("─" * 72)
    ln("  Three ways to count the same data (per STORY.md methodology):")
    ln("")
    ln(f"  {'Attack Events':<35} {m['attack_events']:>8,}")
    ln(f"    (alert messages grouped by 2-min gaps — most conservative count)")
    ln(f"  {'Alert Messages':<35} {m['alert_messages']:>8,}")
    ln(f"    (individual siren activation messages from Pikud HaOref)")
    ln(f"  {'Zone-Level Alerts':<35} {m['zone_level_alerts']:>8,}")
    ln(f"    (distinct message+zone pairs — one zone per message)")
    ln(f"  {'City-Level Alerts':<35} {m['city_level_alerts']:>8,}")
    ln(f"    (every city mentioned in every siren — most granular count)")
    ln("")
    ln(f"  {'Cities Affected':<35} {m['cities_affected']:>8,}")
    ln(f"  {'Defense Zones Affected':<35} {m['zones_affected']:>8} out of 36")
    ln(f"  {'Days With Sirens':<35} {m['days_with_sirens']:>8} of {m.get('calendar_days', '?')} calendar days"
       + (f" ({m['pct_days_with_sirens']}%)" if m.get('pct_days_with_sirens') else ""))

    ln("")
    ln("─" * 72)
    ln("  SECTION 2: THREAT BREAKDOWN")
    ln("─" * 72)
    total_msgs = m["alert_messages"] or 1
    ln(f"  {'Rocket/Missile Alerts':<35} {m['rocket_alerts_msgs']:>8,} messages  ({round(100*m['rocket_alerts_msgs']/total_msgs,1)}%)")
    ln(f"  {'Hostile Aircraft/Drone Alerts':<35} {m['aircraft_alerts_msgs']:>8,} messages  ({round(100*m['aircraft_alerts_msgs']/total_msgs,1)}%)")
    ln("")
    total_city = m["city_level_alerts"] or 1
    ln(f"  {'Rocket City-Level Alerts':<35} {m['rocket_city_alerts']:>8,}  ({round(100*m['rocket_city_alerts']/total_city,1)}%)")
    ln(f"  {'Drone City-Level Alerts':<35} {m['aircraft_city_alerts']:>8,}  ({round(100*m['aircraft_city_alerts']/total_city,1)}%)")

    ln("")
    ln("─" * 72)
    ln("  SECTION 3: DANGER SEVERITY")
    ln("─" * 72)
    ln(f"  {'Civilian Danger Score':<35} {m['civilian_danger_score']:>8,.0f}")
    ln(f"    (sum of shelter-time weights: Immediate=10 … 90s=1 — higher = worse)")
    ln(f"  {'Avg Danger Per Alert':<35} {m['avg_danger_per_alert']:>8} / 10")
    ln(f"  {'Immediate Danger Rate':<35} {m['immediate_danger_pct']:>7}%")
    ln(f"    (alerts where civilians have ZERO seconds to reach shelter)")
    ln("")
    ln("  Shelter Time Distribution:")
    for s in m.get("shelter_time_breakdown", []):
        bar = "█" * max(1, int(s["pct"] / 2))
        ln(f"    {s['label']:<22} {s['count']:>7,}  ({s['pct']:>5.1f}%)  {bar}")

    ln("")
    ln("─" * 72)
    ln("  SECTION 4: TIMING PATTERNS")
    ln("─" * 72)
    ln(f"  {'Night Terror Index (12AM–6AM)':<35} {m['night_attack_pct']:>7}%")
    ln(f"    (% of alerts during sleeping hours — psychological warfare)")
    ln(f"  {'Peak Attack Hour':<35} {m.get('peak_hour', 'N/A'):>8}  ({m.get('peak_hour_alerts', 0):,} alerts)")

    ln("")
    ln("─" * 72)
    ln("  SECTION 5: DAILY INTENSITY")
    ln("─" * 72)
    ln(f"  {'Avg Attack Events / Day':<35} {m['avg_attack_events_per_day']:>8}")
    ln(f"  {'Avg City-Level Alerts / Day':<35} {m['avg_city_alerts_per_day']:>8,.1f}")
    ln(f"  {'Avg Zones Hit / Day':<35} {m['avg_zones_hit_per_day']:>8}")
    ln(f"  {'Avg Distinct Cities / Day':<35} {m['avg_cities_per_day']:>8}")
    if m.get("busiest_day"):
        ln(f"  {'Busiest Day':<35} {m['busiest_day']}")
        ln(f"    {m.get('busiest_day_alert_msgs', 0):,} alert messages → {m.get('busiest_day_city_alerts', 0):,} city-level alerts")

    ln("")
    ln("─" * 72)
    ln("  SECTION 6: TOP TARGETS (English names)")
    ln("─" * 72)
    ln("")
    ln("  Top 10 Cities by City-Level Alerts:")
    for i, c in enumerate(m.get("top_10_cities", []), 1):
        ln(f"    {i:>2}. {c['city']:<40} {c['alerts']:>6,}")

    ln("")
    ln("  Top 10 Defense Zones:")
    ln(f"    {'Zone':<30} {'Events':>8} {'City Alerts':>12}")
    for z in m.get("top_zones", []):
        ln(f"    {z['zone']:<30} {z['alert_events']:>8,} {z['city_alerts']:>12,}")

    ln("")
    ln("─" * 72)
    ln("  SECTION 7: WEEKLY BREAKDOWN")
    ln("─" * 72)
    ln(f"    {'Week':<12} {'Dates':<25} {'Alerts':>8} {'Rockets':>9} {'Drones':>8}")
    for w in m.get("weekly", []):
        ln(f"    {w['week']:<12} {w['start']} → {w['end']:<10} {w['alert_msgs']:>8,} {w['rockets']:>9,} {w['aircraft']:>8,}")

    ln("")
    ln("=" * 72)
    ln("  METHODOLOGY NOTES (for data-literate audience)")
    ln("=" * 72)
    ln("  • 'Attack Event' = siren messages within 2 min grouped as one attack")
    ln("  • 'Alert Message' = one Telegram message from Pikud HaOref (IDF Home")
    ln("     Front Command). One message can cover 1–100+ cities simultaneously.")
    ln("  • 'City-Level Alert' = each city mentioned in a siren = 1 count.")
    ln("     This is the most granular metric: 1 message × 34 cities = 34 alerts.")
    ln("  • 'Zone-Level Alert' = distinct (message, zone) pairs.")
    ln("  • Danger Scale: 10=Immediate (no shelter time), 1=90 seconds to shelter.")
    ln("  • All times are Israel local time (IST/IDT), extracted from Pikud's")
    ln("     own published text — not derived from UTC conversion.")
    ln("  • Drills excluded. Only real alerts counted.")
    ln("  • Source: Pikud HaOref official Telegram channel @PikudHaOref_all")
    ln("=" * 72)

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="War metrics report for US management")
    parser.add_argument("--week", type=int, help="ISO week number (e.g. 09)")
    parser.add_argument("--since", type=str, help="Start date (YYYY-MM-DD), default: 2026-02-28")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    conn = get_db()
    metrics = compute_metrics(conn, date_from=args.since, iso_week=args.week)
    conn.close()

    if args.json:
        # Convert non-serializable types
        print(json.dumps(metrics, indent=2, ensure_ascii=False, default=str))
    else:
        print(format_report(metrics))


if __name__ == "__main__":
    main()
