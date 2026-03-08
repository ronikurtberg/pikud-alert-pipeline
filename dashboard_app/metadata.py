"""Static metadata: SQL documentation for stats, viz metadata, and calculated field transformations."""

STAT_SQL: dict = {
    "attack_events": {
        "label": "Attack Events",
        "sql": "WITH msg_times AS (...) SELECT SUM(is_new_event) — groups messages within 2-min gaps",
        "note": "Attack events: consecutive alert messages within 2 minutes of each other are grouped as one event.",
    },
    "msg_alerts": {
        "label": "Alert Messages",
        "sql": "SELECT COUNT(*) FROM messages m WHERE m.message_type='alert' AND m.is_drill=0",
        "note": "Each Telegram message with a siren activation = 1 count. ~3.4 messages per attack event on average.",
    },
    "city_alerts": {
        "label": "City-Level Alerts",
        "sql": "SELECT COUNT(*) FROM alert_details ad JOIN messages m ON ad.msg_id=m.msg_id WHERE m.message_type='alert' AND m.is_drill=0",
        "note": "Each city mentioned in an alert message = 1 count.",
    },
    "zone_alerts": {
        "label": "Zone-Level Alerts",
        "sql": "SELECT COUNT(*) FROM (SELECT DISTINCT ad.msg_id, ad.zone_id FROM alert_details ad JOIN messages m ...)",
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
        "note": "Total distinct defense zones.",
    },
    "busiest_day": {
        "label": "Peak Day",
        "sql": "SELECT date(datetime_israel), COUNT(*) FROM messages WHERE message_type='alert' ... ORDER BY count DESC LIMIT 1",
        "note": "Day with highest number of alert messages.",
    },
}

VIZ_META: dict = {
    "hourly": {"title": "Hourly Risk Distribution", "dimensions": ["hour_israel (0-23)"], "measures": ["total alerts", "rockets", "aircraft"], "time": "Aggregated across all dates in filter", "sql": "SELECT CAST(strftime('%H', m.datetime_israel) AS INTEGER) as hour_israel, COUNT(*) ...", "fields": ["messages.datetime_israel", "messages.alert_type"]},
    "daily": {"title": "Daily Intensity", "dimensions": ["date"], "measures": ["total alerts", "rockets", "aircraft"], "time": "Each bar = one calendar day", "sql": "SELECT date(m.datetime_israel), COUNT(*) ...", "fields": ["messages.datetime_israel", "messages.alert_type"]},
    "top_cities": {"title": "Top Cities by Alert Count", "dimensions": ["city_name"], "measures": ["alert_count", "rocket_count", "aircraft_count"], "time": "Aggregated across filter range", "sql": "SELECT c.city_name, COUNT(*) ... GROUP BY c.city_name ORDER BY alert_count DESC", "fields": ["alert_details.city_id", "cities.city_name", "messages.alert_type"]},
    "zones": {"title": "Zone Breakdown", "dimensions": ["zone_name"], "measures": ["alert_count", "rockets", "aircraft"], "time": "Aggregated", "sql": "SELECT z.zone_name, COUNT(*) ... GROUP BY z.zone_name", "fields": ["alert_details.zone_id", "zones.zone_name"]},
    "monthly": {"title": "Monthly Trend", "dimensions": ["month (YYYY-MM)"], "measures": ["total", "rockets", "aircraft"], "time": "Per calendar month", "sql": "SELECT strftime('%Y-%m', datetime_israel), COUNT(*) ...", "fields": ["messages.datetime_israel", "messages.alert_type"]},
    "shelter_times": {"title": "Shelter Time Distribution", "dimensions": ["shelter_time"], "measures": ["count"], "time": "All data", "sql": "SELECT ad.shelter_time, COUNT(*) ...", "fields": ["alert_details.shelter_time"]},
    "dow": {"title": "Day of Week Risk", "dimensions": ["day of week"], "measures": ["total"], "time": "Aggregated", "sql": "SELECT strftime('%w', datetime_israel), COUNT(*) ...", "fields": ["messages.datetime_israel"]},
    "escalation": {"title": "Escalation Heatmap", "dimensions": ["date (top 10)", "hour"], "measures": ["count"], "time": "Top 10 busiest days", "sql": "SELECT date, hour, COUNT(*) ... WHERE date IN (top 10)", "fields": ["messages.datetime_israel"]},
    "city_timeline": {"title": "Top Cities Monthly Timeline", "dimensions": ["city_name (top 8)", "month"], "measures": ["count"], "time": "Monthly for top 8", "sql": "SELECT city_name, month, COUNT(*) ...", "fields": ["cities.city_name", "messages.datetime_israel"]},
    "response_time": {"title": "Alert Duration", "dimensions": ["alert_type"], "measures": ["total", "with_clearance", "avg_minutes"], "time": "All data", "sql": "WITH paired AS (correlated subquery for event_ended within 3hrs) ...", "fields": ["messages.datetime_utc", "messages.message_type", "messages.alert_type"]},
    "multi_zone": {"title": "Multi-Zone Attacks", "dimensions": ["msg_id"], "measures": ["zone_count", "city_count"], "time": "Filter range, ≥3 zones", "sql": "SELECT COUNT(DISTINCT zone_id) ... HAVING zone_count >= 3", "fields": ["alert_details.zone_id", "alert_details.city_id"]},
    "streaks": {"title": "Longest Alert Streaks", "dimensions": ["city_name"], "measures": ["streak_days"], "time": "All data (gap-and-island)", "sql": "WITH city_days, numbered, streaks ... WHERE streak_days >= 5", "fields": ["cities.city_name", "messages.datetime_israel"]},
    "safest_10min": {"title": "Safest 10-Min Windows", "dimensions": ["10-min slot"], "measures": ["alert count"], "time": "Aggregated", "sql": "SELECT hour, minute/10, COUNT(*) ...", "fields": ["messages.datetime_israel"]},
    "drone_cities": {"title": "Top Drone/Aircraft Cities", "dimensions": ["city_name"], "measures": ["drone_alerts"], "time": "Filter range", "sql": "SELECT city_name, COUNT(*) WHERE alert_type='aircraft' ...", "fields": ["cities.city_name", "messages.alert_type"]},
    "city_zone_anomaly": {"title": "City vs Zone Anomaly", "dimensions": ["city_name", "zone_name"], "measures": ["city_alerts", "zone_avg", "ratio"], "time": "Filter range", "sql": "WITH city_counts, zone_avg ... WHERE num_cities >= 3", "fields": ["cities.city_name", "zones.zone_name"]},
}

TRANSFORMATIONS: list[dict] = [
    {"field": "datetime_israel", "table": "messages", "source": "datetime_utc", "formula": "datetime_utc + timedelta(hours=3 if Apr-Oct else 2)", "note": "Israel timezone approximated by month", "purpose": "All visualizations use Israel local time"},
    {"field": "message_type", "table": "messages", "source": "raw_text", "formula": "Pattern matching: 9 categories (alert, event_ended, heads_up, ...)", "note": "Core filter — dashboard counts only message_type='alert'", "purpose": "Message classification"},
    {"field": "alert_type", "table": "messages", "source": "raw_text", "formula": "'rockets' if 'רקטות', 'aircraft' if 'כלי טיס', etc.", "note": "5 threat types", "purpose": "Type filtering and stacked charts"},
    {"field": "is_drill", "table": "messages", "source": "raw_text", "formula": "1 if 'תרגיל' in text else 0", "note": "44 drills flagged", "purpose": "Exclude test alerts from stats"},
    {"field": "alert_date / alert_time_local", "table": "messages", "source": "raw_text", "formula": "Regex extraction from 3 format eras", "note": "Date/time as written by Pikud HaOref", "purpose": "Cross-validation with datetime_utc"},
    {"field": "zone_id / zone_name", "table": "alert_details / zones", "source": "raw_text", "formula": "Extract **אזור XXX** bold markers — bold required, not just prefix", "note": "36 zones, normalized to dimension table", "purpose": "Zone-level aggregation"},
    {"field": "city_id / city_name", "table": "alert_details / cities", "source": "raw_text", "formula": "Comma-split OR space-split (aircraft alerts) with multi-word prefix dictionary (קריית, כפר, בית, נאות, גשר...)", "note": "~1,998 cities after space-split fix", "purpose": "City-level fact table"},
    {"field": "canonical_name", "table": "cities", "source": "city_name (post-build analysis)", "formula": "REPLACE(city_name, '-', ' ') — if both dash and space variants exist, map both to the more common spelling", "note": "9 pairs unified: אבו-גוש→אבו גוש, בת-ים→בת ים, etc. Raw city_name preserved, canonical used for display via COALESCE(canonical_name, city_name)", "purpose": "Unified city display in visualizations — prevents double-counting"},
    {"field": "shelter_time", "table": "alert_details", "source": "raw_text", "formula": "Regex for time patterns: מיידי, 15 שניות, דקה, etc.", "note": "Not in all messages", "purpose": "Shelter time distribution chart"},
]
