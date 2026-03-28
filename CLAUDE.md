# Pikud Alert Pipeline — Claude Code Instructions

This file tells Claude Code how to work with this project. Users can just say `/setup` or ask questions and Claude will know the full context.

## Project Overview

This is an end-to-end pipeline for Israel Home Front Command (Pikud HaOref) Telegram alert data:
- **Pipeline** (`pikud.py`): Fetches from Telegram API, parses Hebrew text, builds SQLite star schema
- **Dashboard** (`dashboard.py`): Flask app with 7 tabs, 19 visualizations, filters, export
- **Export** (`dashboard_app/export.py`): CSV + manifest for Tableau / any BI tool

## First-Time Setup

If the user is setting up for the first time, guide them through:

1. `pip3 install -r requirements.txt` (or `make install`)
2. They need Telegram API credentials from https://my.telegram.org
3. `cp .env.example .env` then edit with their api_id and api_hash
4. `python3 pikud.py full_refresh` (or `make fetch`) — fetches all messages + builds DB
5. `python3 dashboard.py` (or `make run`) — opens at http://localhost:5000

## Key Commands

| Command | What it does |
|---|---|
| `make fetch` | Smart: full_refresh if no data, delta if data exists |
| `make run` | Start dashboard at localhost:5000 |
| `make test` | Run all tests (DB tests auto-skip if no data) |
| `python3 pikud.py delta` | Fetch only new messages since last run |
| `python3 pikud.py rebuild_db` | Rebuild DB from CSVs (useful after parser changes) |
| `python3 pikud.py validate` | Run 8 data validation checks |

## Architecture

```
Telegram API → pikud.py (parse Hebrew) → SQLite (star schema) → dashboard.py (Flask)
                                                                → export.py (Tableau CSV)
```

**Star schema:**
- `messages` (1 row per Telegram message) — PK: msg_id
- `alert_details` (1 row per city per alert) — PK: id, FKs: msg_id, zone_id, city_id
- `zones` (36 defense zones) — PK: zone_id
- `cities` (~2,000 cities) — PK: city_id

## Domain Knowledge (Critical for Correct Parsing)

These rules are essential — getting them wrong produces plausible but incorrect data:

- **Zone detection**: Only `**bold-marked**` text with "אזור" is a zone. Plain "אזור" can be a city name (e.g., "אזור תעשייה הדרומי אשקלון").
- **City splitting**: Rocket alerts use commas. Aircraft alerts use spaces. Space-split requires a prefix dictionary: קריית, כפר, בית, נאות, גשר, נווה, כוכב, שדה, תל, בני, אבו, גבעת, מעלה, ראש, באר, עין, מצפה, רמת. Compound prefixes ("אזור תעשייה", "מרכז אזורי", "פארק תעשיות") need longest-match against `_KNOWN_COMPOUND_CITIES` — the prefix+1 rule truncates multi-word names like "אזור תעשייה מישור אדומים".
- **Message types**: 9 categories — only `message_type='alert'` are real siren activations. Filter with `message_type='alert' AND is_drill=0`.
- **Canonicalization**: 9 city pairs have dash/space variants (אבו-גוש / אבו גוש). Use `COALESCE(canonical_name, city_name)` for display.
- **Timestamps**: Telegram UTC can be hours off from reality. The date/time in the Hebrew alert text is the ground truth.
- **Counting**: "attack events" groups messages within 2-min gaps (~3.4 messages per event). Always label which count methodology you're using.

## Testing

- `tests/test_parsers.py` — always runs, no DB needed (33 tests)
- `tests/test_api.py` — needs DB, auto-skips without it (77 tests)
- `tests/test_database.py` — needs DB (28 tests)
- `tests/test_data_integrity.py` — needs DB (11 tests)
- `tests/test_performance.py` — needs DB (30 tests)

## When Making Changes

- After modifying parsers in `pikud.py`: run `python3 pikud.py rebuild_db` then `make test`
- After modifying dashboard routes: restart the Flask app
- After modifying export.py: run `python3 -m pytest tests/test_api.py -k export -v`
- The HTML is a single-file SPA at `templates/dashboard.html` — no build step needed
- All viz queries use `COALESCE(c.canonical_name, c.city_name)` for city display — maintain this pattern
