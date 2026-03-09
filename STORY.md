# Why "Just Build an App" Is Not Enough

## The Naive Approach

It started simple: "scrape Pikud HaOref alerts from Telegram and show them on a dashboard."

A naive approach would be:
1. Fetch messages from the Telegram channel
2. Dump them into a database
3. Build some charts
4. Done

That's what most tutorials show. That's what a weekend project looks like. And it works — until it doesn't.

## What Actually Happened

### The Data Is Not What You Think

The first surprise: there isn't one message format. There are **three**, evolved over 6 years (2019-2025):

```
Old (2019-2021):  🚨 **עדכון - התרעות** 🚨 [date] בשעה time
Bold (2022-2025): 🚨 **ירי רקטות וטילים [date] time** **אזור X** cities
New (2025+):      🚨 ירי רקטות וטילים (date) time **אזור X** cities
```

A parser built for one format silently produces wrong data for the others. No errors. Just wrong numbers that look plausible.

### A "Message" Is Not an "Alert"

The channel has **24,298 messages**. A naive count would say "24K alerts." But:
- Only **19,223** are actual alert messages (`message_type='alert'`)
- **1,183** are "event ended" messages
- **627** are early warnings (התרעה מקדימה)
- **2,724** are updates
- **44** are drills that should be excluded from statistics
- The rest are instructions, intercept reports, shelter status updates

If you count messages, you're 26% wrong before you even start.

### An "Alert" Is Not an "Attack"

Even among the 19,223 real alerts, many are part of the same barrage. Five messages arriving within 30 seconds, all about the same rocket attack from Lebanon — that's 1 attack, not 5.

We computed **Attack Events** by grouping messages within 2-minute gaps: **5,614 actual attack events** from 19,223 messages. The naive count is 3.4x inflated.

### One Message ≠ One City

A single Telegram message might say:

> 🚨 **ירי רקטות וטילים** **אזור דרום הנגב** אבו תלול, ואדי אל נעם, **אזור מערב הנגב** אופקים, תאשור, **אזור מרכז הנגב** באר שבע - דרום, באר שבע - מזרח, לקיה, רהט...

That's **1 message**, **3 zones**, and **34 cities**. Which number do you report?

- "1 alert" — true but hides the scale
- "34 city-level alerts" — true but inflated
- "3 zone-level alerts" — a useful middle ground

All three are correct. All three tell a different story. A serious system needs all three, clearly labeled, with the SQL query behind each number visible on hover.

### "אזור" Doesn't Always Mean Zone

The word "אזור" (zone/area) appears in city names too:

> **אזור תעשייה הדרומי אשקלון** — this is a city name, not a zone

The parser must distinguish: zones are wrapped in `**bold markers**` (`**אזור XXX**`), while city names containing "אזור" are just plain text. Get this wrong and your zone dimension table is polluted with city names.

### Time Zones Are Not Simple

Telegram stores UTC. Israel uses IST (UTC+2) in winter and IDT (UTC+3) in summer. The transition dates change every year. A "simple" conversion is wrong for ~60 days per year near the transitions.

We approximate by month (Apr-Oct = +3, else +2). It's wrong for edge cases, but documented — and the alternative (pulling tz transition tables) adds complexity for marginal accuracy on a dashboard.

### 99.2% of Early Warnings Are Real

The channel sends "התרעה מקדימה" (heads-up) messages before actual alerts. Are they useful? We analyzed all 627:
- **622** (99.2%) were followed by a real alert within 15 minutes
- Average lead time: **4.1 minutes**
- Only **5** false alarms ever

This is a genuine early-warning signal. But you'd never know without the correlation analysis.

### Clearance Rates Vary Wildly by Threat

After an alert, does Pikud HaOref send an "all clear"?
- **Aircraft/UAV alerts**: 97.8% get cleared (avg 20 min)
- **Rocket alerts**: only 29.1% get cleared (avg 60 min)

Why? Probably because rocket barrages are shorter (minutes), so the "event ended" arrives before you'd check. Drone intrusions last longer and have a clear end state. But without measuring, you'd never know the clearance rate is this asymmetric.

## What "Production-Ready" Actually Means

### The Same City, Two Spellings

We found that "אבו גוש" and "אבו-גוש" both exist in the database — they're the same city, but the Telegram operators changed their formatting over time. Same for "בת ים" / "בת-ים", "יהוד מונוסון" / "יהוד-מונוסון", and 6 more pairs.

This is invisible to a naive system. A chart showing "top cities by alert count" would list both variants separately, each with partial counts. Neither would rank correctly.

The fix: keep BOTH spellings in the raw data (data integrity — the CSV says what it says), but add a `canonical_name` column that maps variants to the most common spelling. Visualizations use `COALESCE(canonical_name, city_name)` so they see one unified city. The raw data is untouched.

This is a pattern that repeats in every real-world data system: **the source data is inconsistent, and your system needs a normalization layer that doesn't destroy the original.**

### Cities Without Commas

Aircraft alerts like `**אזור קו העימות** נהריה סער עברון` list cities separated by spaces, not commas. The parser initially stored "נהריה סער עברון" as ONE city name — 742 of 2,156 "cities" were actually multiple cities concatenated.

The fix: when no commas or shelter-time parentheses are found, split on spaces using a dictionary of known Hebrew city-name prefixes (קריית, כפר, בית, נאות, גשר, etc.). This correctly handles "קריית שמונה" (2 words, 1 city) vs "נהריה סער" (2 words, 2 cities). Cities after fix: 2,156 → 1,998.

An AI parser wouldn't catch this. It would produce plausible-looking output with wrong city counts, and you'd never know unless you searched for a specific city and got confused by the results.

### The Prefix+1 Assumption

The space-splitting fix used a "prefix + 1 word" rule: after matching a known prefix like "קריית", always take the next word to form the city name (e.g., "קריית שמונה"). This worked for single-word prefixes but silently broke for compound prefixes.

"אזור תעשייה" (industrial zone) is a 2-word prefix. The rule took prefix + 1 = 3 words: "אזור תעשייה מישור". But the full city name is "אזור תעשייה מישור אדומים" — 4 words. The orphaned "אדומים" became its own phantom city in the database.

This wasn't one city. Investigation revealed **18 truncated industrial zone entries** and their orphaned fragments, affecting "אזור תעשייה", "מרכז אזורי", and "פארק תעשיות" prefixes — all compound prefixes where the city name after the prefix is 2+ words. Cities like "אזור תעשייה רמת דלתון", "מרכז אזורי מבואות חרמון", and "אזור תעשייה קריית ביאליק" were all split.

The fix: a known compound city names lookup with longest-match, tried before the prefix+1 fallback. The lookup is explicit (33 entries), testable, and backward-compatible — unknown compound names still get prefix+1. After rebuild: 18 truncated entries eliminated, orphan fragments gone. The lesson: every heuristic has edge cases, and "works for most data" is not "works for all data."

### Data Integrity Is Non-Negotiable

Every pipeline run validates:
1. All CSVs referenced in metadata exist on disk
2. No duplicate message IDs across delta files
3. Row counts match metadata totals
4. ID ranges don't overlap between deltas
5. DB message count matches CSV totals
6. No orphaned alert_details records
7. Every delta CSV is verified in the database
8. DB version matches current data version

Pre-build validation runs before any DB rebuild. Post-build validation runs after. Both must pass.

### Observability Is Not Optional

Every stat card shows its SQL query on hover. Every visualization has metadata: dimensions, measures, source fields, time range. Every calculated field documents its formula, source, and purpose.

When a number looks wrong (like the חרב לאת bug — 514 city alerts instead of 8), you need to see the SQL immediately to diagnose it. Without observability, you're debugging blind.

### The Data Journey Is the Documentation

A static architecture diagram tells you what the system looks like. The animated Data Journey shows you what the system **does** — with real data, at every stage:

```
📱 Telegram → 📄 CSV → ⚙️ Parser → 🗄️ messages → 📊 alert_details → 👁️ Views
```

8 example types, each showing actual Hebrew text being transformed into structured data. This is both documentation and validation — if the animation shows the wrong output, the parser is broken.

## Taking It to Tableau Next — And What Broke

The Flask dashboard proved the data was solid. The next step: export it to Salesforce Data Cloud and Tableau Next for enterprise-grade analytics. A simple CSV export, right?

### AI-Generated Calculated Fields Were Wrong

Tableau Next's AI can auto-generate calculated fields from a natural language description. We gave it a precise spec: 4
DMOs, exact relationships, 7 calculated fields with Hebrew keyword patterns, 14 measures, and 6 semantic metrics.

The AI got 5 out of 7 fields wrong:
- **message_type**: only checked for ONE keyword ("האירוע הסתיים" → "event_ended", else "other"). Missed alert, heads_up, update entirely. The most critical field — everything filters on it.
- **alert_type**: used exact-match `CASE WHEN [raw_text] = "ירי רקטות"` instead of `CONTAINS()`. Raw text is a full paragraph, not a keyword. Returned NULL for every row.
- **Is Real Alert**: referenced a hallucinated field `[Data_Source_Object87]` and confused `views` (Telegram view count) with `is_drill`.
- **City Display Name**: ignored canonical_name, just checked if city_name was not null.
- **Is Drill Flag**: created as a summed Measure instead of a row-level Dimension.

The lesson: AI can scaffold the structure, but you must verify every formula against actual data. We caught all 5 bugs through sanity check queries comparing Tableau Next results against our Flask dashboard numbers.

### Telegram UTC vs. Pikud's Published Time

A subtle data integrity issue: we computed Israel time as UTC + 2 (or +3 in summer). For daily aggregation, this seemed fine. Then we noticed March 8 was missing from the daily chart.

Investigation revealed: a message at UTC 21:47 March 7 had alert text clearly stating "8/3/2026 7:47". Our computed Israel time was 23:47 March 7 — technically correct per the Telegram timestamp, but the official Pikud date is March 8.

The fix: extract both date AND time directly from the Hebrew alert text using `REGEXP_EXTRACT` + `DATEPARSE("d/M/yyyy H:mm", ...)` to build `Israel_DateTime` — the single source of truth. We abandoned the UTC+offset approach entirely. All time fields (daily charts, hourly distribution, monthly trends, "last updated") now derive from Pikud's own published text, not from Telegram's UTC timestamp.

### Three Levels of Alert Counting — Again

The same counting problem from the Flask dashboard resurfaced in Tableau. `Alert_Count` (COUNT of alert_details rows) gives city-level counts — 17,000 for a single day. Sounds insane to outsiders but is technically correct: one message mentioning 100 cities = 100 counts.

We added `Zone_Alert_Count` — COUNTD of msg_id + zone_id pairs — as a middle ground. Same zone in the same message counts once, but the same zone in different messages counts separately. And `Alert_Events` (COUNTD of msg_id) gives the most conservative count.

Each measure tells a different story. All three are needed. All three must be clearly labeled.

## The Real Lesson

The gap between "I can build a chart" and "I trust this data" is enormous. It's:

- 25 calculated fields + 6 semantic metrics in Tableau Next with documented, tested formulas
- 8 validation checks running before and after every pipeline execution
- 19 visualizations in the Flask dashboard + 6 in Tableau Next, with full metadata and SQL traceability
- 3 different counting methodologies (events, cities, zones) clearly labeled
- Hebrew i18n because the data is Hebrew and the users are Israeli
- Performance optimization (from 4.6s to 49ms on a single query)
- A random message audit that spot-checks parser accuracy
- AI-generated formulas that were wrong 5 out of 7 times and had to be manually corrected

None of this is in the tutorial. All of it is necessary.

The alerts are about people's lives. The data better be right.
