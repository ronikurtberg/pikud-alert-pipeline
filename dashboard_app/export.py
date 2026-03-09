"""CSV export with manifest for Tableau/Salesforce data streams.

Two export modes:
1. WITH calculated fields — all columns as-is from DB, ready to use
2. WITHOUT calculated fields — raw columns only + Tableau formulas manifest

Each table = one CSV file = one data stream.
"""

import csv
import io
import json
import zipfile
from datetime import datetime

# Define which fields are calculated (not from raw Telegram data)
CALCULATED_FIELDS = {
    "messages": {
        "datetime_israel": {
            "source": "datetime_utc",
            "description": "Israel local time (UTC+2/3 by season) — DEPRECATED: use Israel_DateTime from text instead",
            "tableau_formula": "IF DATEPART('month', [Pikud_Message].[datetime_utc]) >= 4 AND DATEPART('month', [Pikud_Message].[datetime_utc]) <= 10 THEN DATEADD('hour', 3, [Pikud_Message].[datetime_utc]) ELSE DATEADD('hour', 2, [Pikud_Message].[datetime_utc]) END",
            "deprecated": True,
        },
        "message_type": {
            "source": "raw_text",
            "description": "Message classification (alert, event_ended, heads_up, update, etc.)",
            "tableau_formula": 'IF CONTAINS([Pikud_Message].[raw_text], "האירוע הסתיים") THEN "event_ended" ELSEIF CONTAINS([Pikud_Message].[raw_text], "בדקות הקרובות") THEN "heads_up" ELSEIF CONTAINS([Pikud_Message].[raw_text], "ירי רקטות") OR CONTAINS([Pikud_Message].[raw_text], "חדירת כלי טיס") THEN "alert" ELSEIF CONTAINS([Pikud_Message].[raw_text], "עדכון") THEN "update" ELSE "other" END',
            "tableau_next_name": "Classify_Raw_Text_Event_Status",
        },
        "alert_type": {
            "source": "raw_text",
            "description": "Threat type (rockets, aircraft, infiltration, earthquake, hazmat)",
            "tableau_formula": 'IF CONTAINS([Pikud_Message].[raw_text], "ירי רקטות") THEN "rockets" ELSEIF CONTAINS([Pikud_Message].[raw_text], "כלי טיס עוין") THEN "aircraft" ELSEIF CONTAINS([Pikud_Message].[raw_text], "חדירת מחבלים") THEN "infiltration" ELSEIF CONTAINS([Pikud_Message].[raw_text], "רעידת אדמה") THEN "earthquake" ELSEIF CONTAINS([Pikud_Message].[raw_text], "חומרים מסוכנים") THEN "hazmat" ELSE NULL END',
            "tableau_next_name": "Extract_Threat_Type",
        },
        "is_drill": {
            "source": "raw_text",
            "description": "1 if message is a drill/test, 0 otherwise",
            "tableau_formula": 'IIF(CONTAINS([Pikud_Message].[raw_text], "תרגיל"), 1, 0)',
            "tableau_next_name": "Is_Drill_Flag",
        },
        "alert_date": {
            "source": "raw_text",
            "description": "Date as written in the alert text (ground truth Israel date)",
            "tableau_formula": 'REGEXP_EXTRACT([Pikud_Message].[raw_text], "[\\[\\(](\\d{1,2}/\\d{1,2}/\\d{4})[\\]\\)]")',
            "tableau_next_name": "Alert_Date",
        },
        "alert_time_local": {
            "source": "raw_text",
            "description": "Time as written in the alert text",
            "tableau_formula": 'REGEXP_EXTRACT([Pikud_Message].[raw_text], "(\\d{1,2}:\\d{2})")',
            "tableau_next_name": "Alert_Time",
        },
    },
    "cities": {
        "canonical_name": {
            "source": "city_name",
            "description": "Normalized display name (dash variants → space). NULL means city_name is canonical.",
            "tableau_formula": 'REPLACE([Pikud_City].[city_name], "-", " ")',
            "tableau_next_name": "Canonical_Name",
        },
    },
}

TABLE_DESCRIPTIONS = {
    "messages": "One row per Telegram message. Contains both alert and non-alert messages (event_ended, heads_up, etc.).",
    "alert_details": "One row per city per alert message. Fact table linking messages to cities and zones.",
    "cities": "Dimension table: unique city/settlement names extracted from alerts.",
    "zones": "Dimension table: 36 defense zones defined by Home Front Command.",
}

FIELD_DESCRIPTIONS = {
    "messages": {
        "msg_id": "Telegram message ID (primary key, auto-incrementing)",
        "datetime_utc": "UTC timestamp from Telegram API",
        "datetime_israel": "[CALCULATED] Israel local time",
        "alert_date": "[CALCULATED] Date extracted from message text",
        "alert_time_local": "[CALCULATED] Time extracted from message text",
        "message_type": "[CALCULATED] Classification: alert, event_ended, heads_up, update, etc.",
        "alert_type": "[CALCULATED] Threat: rockets, aircraft, infiltration, earthquake, hazmat",
        "is_drill": "[CALCULATED] 1=drill/test, 0=real",
        "raw_text": "Original Hebrew text from Telegram",
        "views": "Telegram view count",
    },
    "alert_details": {
        "id": "Auto-increment primary key",
        "msg_id": "Foreign key → messages.msg_id",
        "zone_id": "Foreign key → zones.zone_id",
        "city_id": "Foreign key → cities.city_id",
        "shelter_time": "Time to reach shelter (מיידי, 15 שניות, דקה, etc.)",
    },
    "cities": {
        "city_id": "Auto-increment primary key",
        "city_name": "City name as it appears in Telegram text",
        "canonical_name": "[CALCULATED] Normalized name for display (dash→space unification)",
    },
    "zones": {
        "zone_id": "Auto-increment primary key",
        "zone_name": "Defense zone name (e.g., אזור קו העימות)",
    },
}

RELATIONSHIPS = [
    {
        "from": "alert_details.msg_id",
        "to": "messages.msg_id",
        "type": "N:1",
        "description": "Each alert detail belongs to one message",
    },
    {
        "from": "alert_details.city_id",
        "to": "cities.city_id",
        "type": "N:1",
        "description": "Each alert detail references one city",
    },
    {
        "from": "alert_details.zone_id",
        "to": "zones.zone_id",
        "type": "N:1",
        "description": "Each alert detail references one zone",
    },
]

CROSS_TABLE_FIELDS = {
    "calculated_dimensions": [
        {
            "name": "City_Display_Name",
            "context": "Pikud_Alert_Detail (via Pikud_City)",
            "tableau_formula": "IF NOT ISNULL([Canonical_Name]) THEN [Canonical_Name] ELSE [Pikud_City].[city_name] END",
            "type": "Text",
            "purpose": "Unified city name for grouping (dash/space variants merged)",
        },
        {
            "name": "Is_Real_Alert",
            "context": "Pikud_Alert_Detail (via Pikud_Message)",
            "tableau_formula": '[Classify_Raw_Text_Event_Status] = "alert" AND [Is_Drill_Flag] = 0',
            "type": "Boolean",
            "purpose": "Master filter for real siren activations only (excludes drills, updates, event_ended)",
        },
        {
            "name": "Alert_Date_Parsed",
            "context": "Pikud_Message",
            "tableau_formula": 'DATE(DATEPARSE("d/M/yyyy", [Alert_Date]))',
            "type": "Date",
            "depends_on": "Alert_Date",
            "purpose": "Proper date type from alert text — use for daily charts and date filters",
        },
        {
            "name": "Israel_DateTime",
            "context": "Pikud_Message",
            "tableau_formula": 'DATEPARSE("d/M/yyyy H:mm", [Alert_Date] + " " + [Alert_Time])',
            "type": "DateTime",
            "depends_on": "Alert_Date, Alert_Time",
            "purpose": "Full Israel datetime from Pikud's published text — single source of truth for all time analysis",
        },
        {
            "name": "Hour_Label",
            "context": "Pikud_Message",
            "tableau_formula": "IF DATEPART('hour', [Israel_DateTime]) = 0 THEN \"12 AM\" ELSEIF DATEPART('hour', [Israel_DateTime]) < 12 THEN STR(DATEPART('hour', [Israel_DateTime])) + \" AM\" ELSEIF DATEPART('hour', [Israel_DateTime]) = 12 THEN \"12 PM\" ELSE STR(DATEPART('hour', [Israel_DateTime]) - 12) + \" PM\" END",
            "type": "Text",
            "depends_on": "Israel_DateTime",
            "purpose": "AM/PM formatted hour for hourly distribution charts",
        },
        {
            "name": "Shelter_Danger_Level",
            "context": "Pikud_Alert_Detail",
            "tableau_formula": 'CASE [Pikud_Alert_Detail].[shelter_time] WHEN "מיידי" THEN "1-Immediate" WHEN "15 שניות" THEN "2-Critical (15s)" WHEN "30 שניות" THEN "3-Severe (30s)" WHEN "45 שניות" THEN "4-High (45s)" WHEN "דקה" THEN "5-Moderate (60s)" WHEN "דקה וחצי" THEN "6-Standard (90s)" WHEN "3 דקות" THEN "7-Extended (3m)" ELSE "8-Aircraft (10m)" END',
            "type": "Text",
            "purpose": "Sortable danger level label derived from shelter_time — use for color encoding and filtering",
        },
    ],
    "calculated_measures": [
        {
            "name": "Alert_Count",
            "tableau_formula": "COUNT([Pikud_Alert_Detail].[id])",
            "purpose": "City-level alert count (each city mentioned = 1 count)",
        },
        {
            "name": "Alert_Events",
            "tableau_formula": "COUNTD([Pikud_Alert_Detail].[msg_id])",
            "purpose": "Distinct message count (most conservative measure)",
        },
        {
            "name": "Cities_Affected",
            "tableau_formula": "COUNTD([Pikud_Alert_Detail].[city_id])",
            "purpose": "Distinct cities alerted",
        },
        {
            "name": "Zones_Affected",
            "tableau_formula": "COUNTD([Pikud_Alert_Detail].[zone_id])",
            "purpose": "Distinct defense zones alerted",
        },
        {
            "name": "Rocket_Alerts",
            "tableau_formula": 'COUNTD(IF [Extract_Threat_Type] = "rockets" THEN [Pikud_Alert_Detail].[id] END)',
            "purpose": "City-level rocket alert count",
        },
        {
            "name": "Aircraft_Alerts",
            "tableau_formula": 'COUNTD(IF [Extract_Threat_Type] = "aircraft" THEN [Pikud_Alert_Detail].[id] END)',
            "purpose": "City-level aircraft/drone alert count",
        },
        {
            "name": "Zone_Alert_Count",
            "tableau_formula": 'COUNTD(STR([Pikud_Alert_Detail].[msg_id]) + "-" + STR([Pikud_Alert_Detail].[zone_id]))',
            "purpose": "Distinct message+zone pairs — middle ground between city-level and event-level counting",
        },
        {
            "name": "Shelter_Danger_Weight",
            "tableau_formula": 'CASE [Pikud_Alert_Detail].[shelter_time] WHEN "מיידי" THEN 10 WHEN "15 שניות" THEN 8 WHEN "30 שניות" THEN 6 WHEN "45 שניות" THEN 4 WHEN "דקה" THEN 2 WHEN "דקה וחצי" THEN 1 WHEN "3 דקות" THEN 0.5 ELSE 3 END',
            "purpose": "Row-level danger weight by shelter time (10=immediate, 0.5=3min, 3=aircraft default). Auto-SUMs when aggregated.",
            "level": "Row",
        },
        {
            "name": "Civilian_Danger_Score",
            "tableau_formula": "SUM([Shelter_Danger_Weight])",
            "purpose": "Total danger score — higher means more alerts with less shelter time",
            "depends_on": "Shelter_Danger_Weight",
        },
        {
            "name": "Avg_Danger_Per_Alert",
            "tableau_formula": 'SUM(CASE [Pikud_Alert_Detail].[shelter_time] WHEN "מיידי" THEN 10 WHEN "15 שניות" THEN 8 WHEN "30 שניות" THEN 6 WHEN "45 שניות" THEN 4 WHEN "דקה" THEN 2 WHEN "דקה וחצי" THEN 1 WHEN "3 דקות" THEN 0.5 ELSE 3 END) / COUNT([Pikud_Alert_Detail].[id])',
            "purpose": "Average severity per alert (1-10 scale). Higher = less shelter time on average.",
        },
        {
            "name": "Immediate_Danger_Rate",
            "tableau_formula": 'COUNTD(IF [Pikud_Alert_Detail].[shelter_time] = "מיידי" THEN [Pikud_Alert_Detail].[id] END) / COUNT([Pikud_Alert_Detail].[id]) * 100',
            "purpose": "Percentage of alerts with zero shelter time (מיידי)",
        },
        {
            "name": "Night_Attack_Rate",
            "tableau_formula": "COUNTD(IF DATEPART('hour', [Israel_DateTime]) >= 0 AND DATEPART('hour', [Israel_DateTime]) < 6 THEN [Pikud_Alert_Detail].[id] END) / COUNT([Pikud_Alert_Detail].[id])",
            "purpose": "Fraction of alerts between midnight and 6 AM Israel time",
            "depends_on": "Israel_DateTime",
        },
        {
            "name": "Days_With_Sirens",
            "tableau_formula": "COUNTD(DATE([Israel_DateTime]))",
            "purpose": "Distinct calendar days with at least one alert",
            "depends_on": "Israel_DateTime",
        },
    ],
    "semantic_metrics": [
        {
            "name": "Days_Under_Fire",
            "label": "Days Under Fire",
            "measure": "Days_With_Sirens",
            "time_dimension": "Israel_DateTime",
            "time_grains": ["Day", "Week", "Month", "Quarter", "Year"],
            "description": "Total siren activation events. Each distinct message = one event.",
            "sentiment": "up_is_bad",
        },
        {
            "name": "Multi_Front_Pressure",
            "label": "Multi-Front Pressure",
            "measure": "Zones_Affected",
            "time_dimension": "Israel_DateTime",
            "additional_dimensions": ["Pikud_Zone.zone_name"],
            "time_grains": ["Day", "Week", "Month", "Quarter", "Year"],
            "description": "Defense zones under fire. Israel has 36 total — above 25 = nationwide.",
            "sentiment": "up_is_bad",
        },
        {
            "name": "Immediate_Danger_Pct",
            "label": "Immediate Danger %",
            "measure": "Immediate_Danger_Rate",
            "time_dimension": "Israel_DateTime",
            "additional_dimensions": ["City_Display_Name", "Pikud_Zone.zone_name"],
            "time_grains": ["Day", "Week", "Month", "Quarter", "Year"],
            "description": "Alerts with zero shelter time (מיידי) — civilians in the blast zone with no time to reach shelter.",
            "sentiment": "up_is_bad",
        },
        {
            "name": "Night_Terror_Index",
            "label": "Night Terror Index",
            "measure": "Night_Attack_Rate",
            "time_dimension": "Israel_DateTime",
            "additional_dimensions": ["Extract_Threat_Type", "Pikud_Zone.zone_name"],
            "time_grains": ["Day", "Week", "Month", "Quarter", "Year"],
            "description": "Alerts between midnight and 6 AM Israel time — when civilians are sleeping. Measures psychological warfare.",
            "sentiment": "up_is_bad",
        },
        {
            "name": "Cities_Under_Fire",
            "label": "Cities Under Fire",
            "measure": "Cities_Affected",
            "time_dimension": "Israel_DateTime",
            "additional_dimensions": ["City_Display_Name"],
            "time_grains": ["Day", "Week", "Month", "Quarter", "Year"],
            "description": "Distinct cities hearing sirens in period.",
            "sentiment": "up_is_bad",
        },
        {
            "name": "Avg_Danger_Level",
            "label": "Avg Danger Level",
            "measure": "Avg_Danger_Per_Alert",
            "time_dimension": "Israel_DateTime",
            "additional_dimensions": ["Pikud_Zone.zone_name", "Pikud_City.city_name"],
            "time_grains": ["Day", "Week", "Month", "Quarter", "Year"],
            "description": "Severity scale 1-10. Higher = less shelter time. 10 = immediate, 1 = 90 seconds.",
            "sentiment": "up_is_bad",
        },
    ],
}


def export_to_zip(db, include_calculated: bool = True) -> bytes:
    """Export all tables as CSV files in a ZIP archive with manifest."""
    buf = io.BytesIO()
    tables = ["messages", "zones", "cities", "alert_details"]
    mode = "with_calculated" if include_calculated else "raw_only"

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for table in tables:
            cols_info = [dict(r) for r in db.execute(f"PRAGMA table_info({table})").fetchall()]
            all_cols = [c["name"] for c in cols_info]

            if not include_calculated and table in CALCULATED_FIELDS:
                skip = set(CALCULATED_FIELDS[table].keys())
                export_cols = [c for c in all_cols if c not in skip]
            else:
                export_cols = all_cols

            rows = db.execute(f"SELECT {','.join(export_cols)} FROM {table}").fetchall()

            csv_buf = io.StringIO()
            writer = csv.writer(csv_buf)
            writer.writerow(export_cols)
            for row in rows:
                writer.writerow(list(row))

            zf.writestr(f"{mode}/{table}.csv", csv_buf.getvalue())

        # Manifest
        manifest = {
            "export_mode": mode,
            "exported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "include_calculated_fields": include_calculated,
            "tables": {},
            "relationships": RELATIONSHIPS,
            "semantic_model_fields": {k: v for k, v in CROSS_TABLE_FIELDS.items() if k != "semantic_metrics"},
            "semantic_metrics": CROSS_TABLE_FIELDS.get("semantic_metrics", []),
            "tableau_guide": "See TABLEAU_GUIDE.md for step-by-step loading instructions",
        }

        for table in tables:
            cols_info = [dict(r) for r in db.execute(f"PRAGMA table_info({table})").fetchall()]
            row_count = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            all_cols = [c["name"] for c in cols_info]

            if not include_calculated and table in CALCULATED_FIELDS:
                skip = set(CALCULATED_FIELDS[table].keys())
                exported_cols = [c for c in all_cols if c not in skip]
            else:
                exported_cols = all_cols

            field_defs = {}
            for col in exported_cols:
                fd = {"name": col, "type": next((c["type"] for c in cols_info if c["name"] == col), "TEXT")}
                if table in FIELD_DESCRIPTIONS and col in FIELD_DESCRIPTIONS[table]:
                    fd["description"] = FIELD_DESCRIPTIONS[table][col]
                pk = next((c for c in cols_info if c["name"] == col and c["pk"]), None)
                if pk:
                    fd["primary_key"] = True
                field_defs[col] = fd

            tbl_info = {
                "description": TABLE_DESCRIPTIONS.get(table, ""),
                "row_count": row_count,
                "fields": field_defs,
                "csv_file": f"{table}.csv",
                "stream_name": f"pikud_{table}",
            }

            if not include_calculated and table in CALCULATED_FIELDS:
                tbl_info["calculated_fields_to_add"] = {
                    name: {
                        "description": info["description"],
                        "source_field": info["source"],
                        "tableau_formula": info["tableau_formula"],
                    }
                    for name, info in CALCULATED_FIELDS[table].items()
                }
            elif include_calculated and table in CALCULATED_FIELDS:
                for name in CALCULATED_FIELDS.get(table, {}):
                    if name in field_defs:
                        field_defs[name]["is_calculated"] = True
                        field_defs[name]["source"] = CALCULATED_FIELDS[table][name]["source"]
                        field_defs[name]["formula"] = CALCULATED_FIELDS[table][name]["tableau_formula"]

            manifest["tables"][table] = tbl_info

        zf.writestr(f"{mode}/manifest.json", json.dumps(manifest, indent=2, ensure_ascii=False))

    buf.seek(0)
    return buf.getvalue()
