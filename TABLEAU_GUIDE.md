# Loading Pikud HaOref Data into Tableau Next / Salesforce Data Cloud

Step-by-step guide based on our actual deployment experience. Covers Data Streams, DMOs, Semantic Model, calculated fields (with gotchas we hit and fixed), sanity checks, and ready-to-build visualizations.

---

## Step 1: Export Data

From the dashboard Pipeline page, click **"Download Full Export"** or **"Download Raw + Formulas"**.

| Mode | When to use |
|---|---|
| **Full Export** | One-time analysis. All calculated fields pre-computed in CSV. Load and visualize immediately. |
| **Raw + Formulas** | Recurring data pipeline. Load raw columns only, create calculated fields in Tableau Next so they auto-update on data refresh. |

The ZIP contains 4 CSV files + `manifest.json`.

---

## Step 2: Create Data Streams in Data Cloud

Each CSV = one Data Stream. Use **Full Refresh** for all 4 tables (simplest — the export always dumps complete tables).

| CSV File | Data Stream Name | Primary Key | Category |
|---|---|---|---|
| `messages.csv` | `pikud_messages` | `msg_id` | Engagement (has datetime_utc) |
| `alert_details.csv` | `pikud_alert_details` | `id` | Other (no timestamp field) |
| `cities.csv` | `pikud_cities` | `city_id` | Profile (dimension) |
| `zones.csv` | `pikud_zones` | `zone_id` | Profile (dimension) |

### In Data Cloud:
1. **Setup > Data Cloud > Data Streams > New**
2. Upload each CSV, name it per the table above
3. Set the primary key column
4. Set all 4 to **Full Refresh** ingestion mode

---

## Step 3: Create Data Model Objects (DMOs)

Each Data Stream becomes a DMO:

| DMO Name | Source Stream | Key Field |
|---|---|---|
| `Pikud_Message` | `pikud_messages` | `msg_id` |
| `Pikud_Alert_Detail` | `pikud_alert_details` | `id` |
| `Pikud_City` | `pikud_cities` | `city_id` |
| `Pikud_Zone` | `pikud_zones` | `zone_id` |

### In Data Cloud:
1. **Setup > Data Model > New Data Model Object**
2. Map each field from the Data Stream
3. Set data types: `msg_id` > Number, `datetime_utc` > DateTime, `raw_text` > Text(Long), etc.

---

## Step 4: Create Semantic Model + Relationships

Create a semantic model called `Pikud_Alerts`. Add all 4 DMOs. Star schema with `Pikud_Alert_Detail` as the central fact table.

### Relationships:

| From | To | Join Key | Cardinality |
|---|---|---|---|
| `Pikud_Message` | `Pikud_Alert_Detail` | msg_id | One-to-Many |
| `Pikud_City` | `Pikud_Alert_Detail` | city_id | One-to-Many |
| `Pikud_Zone` | `Pikud_Alert_Detail` | zone_id | One-to-Many |

---

## Step 5: Calculated Fields (Tableau Next Semantic Model)

These are the final, tested formulas deployed in production. Created as calculated fields in the semantic model.

### Important Lesson: Tableau Next AI Gotchas

We used Tableau Next's AI to auto-generate calculated fields from a natural language spec. The AI got several wrong:
- **message_type**: AI only checked for one keyword, returning "event_ended" or "other" — missing alert/heads_up/update entirely
- **alert_type**: AI used exact-match `CASE WHEN` instead of `CONTAINS()` — returned NULL for every row since raw_text is a full paragraph, not a keyword
- **Is Real Alert**: AI hallucinated a field name (`Data_Source_Object87`) and confused `views` with `is_drill`
- **City Display Name**: AI ignored canonical_name entirely, just checked if city_name was not null
- **Is Drill Flag**: AI created it as a Measure (SUM) instead of a row-level Dimension

All were manually corrected to the formulas below.

### Calculated Dimensions (10 fields):

**1. Classify_Raw_Text_Event_Status** (Text) — message classification
```
IF CONTAINS([Pikud_Message].[raw_text], "האירוע הסתיים") THEN "event_ended"
ELSEIF CONTAINS([Pikud_Message].[raw_text], "בדקות הקרובות") THEN "heads_up"
ELSEIF CONTAINS([Pikud_Message].[raw_text], "ירי רקטות") OR CONTAINS([Pikud_Message].[raw_text], "חדירת כלי טיס") THEN "alert"
ELSEIF CONTAINS([Pikud_Message].[raw_text], "עדכון") THEN "update"
ELSE "other" END
```

**2. Extract_Threat_Type** (Text) — threat classification
```
IF CONTAINS([Pikud_Message].[raw_text], "ירי רקטות") THEN "rockets"
ELSEIF CONTAINS([Pikud_Message].[raw_text], "כלי טיס עוין") THEN "aircraft"
ELSEIF CONTAINS([Pikud_Message].[raw_text], "חדירת מחבלים") THEN "infiltration"
ELSEIF CONTAINS([Pikud_Message].[raw_text], "רעידת אדמה") THEN "earthquake"
ELSEIF CONTAINS([Pikud_Message].[raw_text], "חומרים מסוכנים") THEN "hazmat"
ELSE NULL END
```

**3. Is_Real_Alert** (Boolean) — master filter for real alerts
```
[Classify_Raw_Text_Event_Status] = "alert" AND [Is_Drill_Flag] = 0
```

**4. Canonical_Name** (Text) — city name normalization
```
REPLACE([Pikud_City].[city_name], "-", " ")
```

**5. City_Display_Name** (Text) — unified city name for display
```
IF NOT ISNULL([Canonical_Name]) THEN [Canonical_Name] ELSE [Pikud_City].[city_name] END
```

**6. Alert_Date** (Text) — date extracted from raw Hebrew text (dependency for other fields)
```
REGEXP_EXTRACT([Pikud_Message].[raw_text], "[\[\(](\d{1,2}/\d{1,2}/\d{4})[\]\)]")
```

**7. Alert_Time** (Text) — time extracted from raw Hebrew text
```
REGEXP_EXTRACT([Pikud_Message].[raw_text], "(\d{1,2}:\d{2})")
```

**8. Alert_Date_Parsed** (Date) — proper date for date filters and daily chart columns
```
DATE(DATEPARSE("d/M/yyyy", [Alert_Date]))
```

**9. Israel_DateTime** (DateTime) — full Israel datetime from Pikud's own text (single source of truth)
```
DATEPARSE("d/M/yyyy H:mm", [Alert_Date] + " " + [Alert_Time])
```

**10. Hour_Label** (Text) — AM/PM formatted hour based on Israel_DateTime
```
IIF(DATEPART('hour', [Israel_DateTime]) = 0, "12 AM",
IIF(DATEPART('hour', [Israel_DateTime]) < 12, STR(DATEPART('hour', [Israel_DateTime])) + " AM",
IIF(DATEPART('hour', [Israel_DateTime]) = 12, "12 PM",
STR(DATEPART('hour', [Israel_DateTime]) - 12) + " PM")))
```

> **Removed fields:** Adjusted_Datetime_Based_On_Month (UTC+2/3 was unreliable — up to 10 hour gaps from actual Israel time) and Israel_Time_Display (was based on Adjusted). All time fields now derive from Pikud's own published text.

### Calculated Measures (8 fields):

| Measure | Formula |
|---|---|
| **Alert_Count** | `COUNT([Pikud_Alert_Detail].[id])` |
| **Alert_Events** | `COUNTD([Pikud_Alert_Detail].[msg_id])` |
| **Cities_Affected** | `COUNTD([Pikud_Alert_Detail].[city_id])` |
| **Zones_Affected** | `COUNTD([Pikud_Alert_Detail].[zone_id])` |
| **Rocket_Alerts** | `COUNTD(IF [Extract_Threat_Type] = "rockets" THEN [Pikud_Alert_Detail].[id] END)` |
| **Aircraft_Alerts** | `COUNTD(IF [Extract_Threat_Type] = "aircraft" THEN [Pikud_Alert_Detail].[id] END)` |
| **Zone_Alert_Count** | `COUNTD(STR([Pikud_Alert_Detail].[msg_id]) + "-" + STR([Pikud_Alert_Detail].[zone_id]))` |
| **Is_Drill_Flag** | `IIF(CONTAINS([Pikud_Message].[raw_text], "תרגיל"), 1, 0)` |

> **Note on Is_Drill_Flag:** Tableau Next AI created this as a Measure. It works for Is_Real_Alert because the comparison `[Is_Drill_Flag] = 0` evaluates row-level. Ideally it should be a Dimension, but it functions correctly as-is.

### Important: Date/Time Fields — Which to Use Where

| Field | Use for | Why |
|---|---|---|
| **Alert_Date_Parsed** | Date filters (>= 2026-02-28), daily chart columns | Proper Date type from Pikud's text |
| **Israel_DateTime** | Hourly charts, monthly/weekly aggregation, "last updated" | Full DateTime from Pikud's text — single source of truth |
| **Hour_Label** | Hourly chart columns | AM/PM formatted string from Israel_DateTime |
| **Alert_Date** | (hidden) dependency for Alert_Date_Parsed and Israel_DateTime | Raw text string like "8/3/2026" |
| **Alert_Time** | (hidden) dependency for Israel_DateTime | Raw text string like "7:47" |

**Why not UTC + offset?** We initially used Adjusted_Datetime_Based_On_Month (UTC + 2/3 hours). We discovered Telegram's UTC timestamp can differ from Pikud's published time by up to 10 hours — a message at UTC 21:47 March 7 had alert text showing "8/3/2026 7:47". The Pikud text is the official record. All time fields now derive from the raw Hebrew text, not from UTC conversion.

---

## Step 6: Sanity Checks

After creating all calculated fields, run these queries in the Tableau Next semantic query UI to verify.

**Base filter for all tests:** Alert_Date_Parsed >= 2026-02-28 AND Is_Real_Alert = TRUE

### Test 1: Total Alert Count
- Dimension: (none)
- Measure: Alert_Count
- Validates: basic measure works

### Test 2: Threat Type Breakdown
- Dimension: Extract_Threat_Type
- Measure: Alert_Count
- Validates: CONTAINS-based classification works. Rockets should be the majority.

### Test 3: Zone Breakdown
- Dimension: Pikud_Zone.zone_name
- Measure: Alert_Count
- Validates: zone relationship and join work correctly.

### Test 4: City Deduplication
- Dimension: City_Display_Name
- Measure: Alert_Count
- Validates: canonical_name works. Should NOT see both dash and space variants (e.g., only "אבו גוש", not also "אבו-גוש").

### Test 5: Date Sanity
- Dimension: Alert_Date_Parsed
- Measure: Alert_Count
- Validates: dates parse correctly. No rows before 2026-02-28. Each day should have a reasonable count.

### Test 6: Drill Exclusion
- Dimension: Is_Drill_Flag
- Measure: Alert_Count
- Filter: Alert_Date_Parsed >= 2026-02-28 (remove Is_Real_Alert for this test)
- Validates: is_drill works. Should see 0 and 1 values. The 1s should be a very small number.

### Test 7: Cross-check Alert_Date_Parsed vs Raw Text
- Dimension: Alert_Date_Parsed, Pikud_Message.raw_text
- Measure: (none)
- Sort: descending by Alert_Date_Parsed
- Validates: the most recent dates in Alert_Date_Parsed match the dates visible in raw_text.

---

## Step 7: Visualizations in Tableau Next

Dashboard name: **"Under Fire: Israel Alert Data Since February 2026"**

All visualizations use these base filters:
- **Alert_Date_Parsed >= 2026-02-28** (war period)
- **Is_Real_Alert = TRUE** (real alerts only)

### Viz 1: Daily Alert Intensity (Stacked Bar)
- **Columns:** Alert_Date_Parsed (exact date, continuous)
- **Rows:** Alert_Count
- **Color:** Extract_Threat_Type
- **Mark type:** Bar
- **Title:** "Daily City-Level Alerts (each city mentioned in a siren activation = 1 count)"
- **Purpose:** Daily volume over time. Identify escalation days and quiet periods.

### Viz 2: Top 15 Cities (Horizontal Bar)
- **Columns:** Alert_Count
- **Rows:** City_Display_Name (sort descending)
- **Color:** shelter_time (shows warning time per city — red for "immediate", green for "1 minute")
- **Filter:** Top 15 by Alert_Count (drag City_Display_Name to Filters > Top > 15 > By field > Alert_Count > Sum)
- **Labels:** Alert_Count on bars
- **Title:** "Most Alerted Cities — Colored by Shelter Warning Time"

### Viz 3: Hourly Attack Pattern (Bar)
- **Columns:** Hour_Label
- **Rows:** Alert_Count
- **Color:** Extract_Threat_Type
- **Mark type:** Bar
- **Title:** "When Sirens Sound: Hourly Distribution of Rocket and Drone Alerts Across Israel (Since Feb 28, 2026)"
- **Purpose:** Which hours are most dangerous. Night attacks (2-3 AM) are a real pattern.

### Viz 4: Zone Breakdown (Horizontal Bar)
- **Columns:** Zone_Alert_Count
- **Rows:** Pikud_Zone.zone_name (sort descending)
- **Mark type:** Bar
- **Title:** "Defense Zones by Alert Volume"
- **Purpose:** Which zones bear the heaviest load.

### Viz 5: Monthly Trend (Area Chart)
- **Columns:** DATEPART('month', Israel_DateTime) (add DATEPART('year') if data spans multiple years)
- **Rows:** Alert_Count
- **Color:** Extract_Threat_Type
- **Mark type:** Area, stacked
- **Title:** "Monthly Escalation Trend"
- **Purpose:** Is the situation getting worse or better?

### Viz 6: Rockets vs Aircraft by City (Side-by-Side Bar)
- **Columns:** Rocket_Alerts, Aircraft_Alerts
- **Rows:** City_Display_Name (top 15)
- **Mark type:** Bar, side-by-side
- **Title:** "Rockets vs Drones: Threat Mix by City"
- **Purpose:** Some cities get mostly rockets, others mostly drones. Direct comparison.

### Dashboard Layout:
```
+-------------------------------------------+
| "Under Fire" Title + Date Range Filter    |
+---------------------+---------------------+
| Viz 1: Daily        | Viz 2: Top Cities   |
| Intensity           | (horizontal bars)   |
+---------------------+---------------------+
| Viz 3: Hourly       | Viz 4: Zones        |
| Pattern             | (horizontal bars)   |
+---------------------+---------------------+
| Viz 5: Monthly      | Viz 6: Rockets vs   |
| Trend               | Aircraft            |
+---------------------+---------------------+
```

---

## Step 8: Updating Data

### Refresh Flow:
1. Run `python3 pikud.py delta` to fetch new alerts
2. Export from the dashboard Pipeline page (either mode)
3. Upload new CSVs to each Data Stream (Full Refresh mode replaces all data)
4. Wait for DMOs to sync (check: Setup > Data Cloud > Data Model > click DMO > sync status)
5. Semantic model auto-refreshes from DMOs
6. Vizzes update on next query

### Important: DMO Sync Delay
After uploading new Data Stream files, the DMOs need time to process. Check each DMO's sync status before expecting updated numbers in your vizzes.

### Automation:
```bash
# Cron job: fetch + export every hour
0 * * * * cd /path/to/scrape_pikud && python3 pikud.py delta && python3 -c "
from dashboard_app.export import export_to_zip
from dashboard_app.db import get_shared_db
import zipfile, io
db = get_shared_db()
z = export_to_zip(db, include_calculated=False)
with open('/path/to/upload/pikud_raw.zip', 'wb') as f:
    f.write(z)
"
```

---

## Manifest Reference

The `manifest.json` in each export describes:

```json
{
  "export_mode": "raw_only",
  "tables": {
    "messages": {
      "description": "One row per Telegram message",
      "row_count": 24794,
      "stream_name": "pikud_messages",
      "fields": { ... },
      "calculated_fields_to_add": {
        "message_type": {
          "description": "Message classification",
          "source_field": "raw_text",
          "tableau_formula": "IF CONTAINS(...) ..."
        }
      }
    }
  },
  "relationships": [
    {"from": "alert_details.msg_id", "to": "messages.msg_id", "type": "N:1"}
  ],
  "cross_table_fields": [
    {"name": "City Display Name", "formula": "...", "purpose": "Unified city name"}
  ]
}
```
