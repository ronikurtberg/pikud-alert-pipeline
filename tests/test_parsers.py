"""Tests for message classification, date extraction, and zone/city parsing."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pikud import classify_message, extract_date_time, extract_zones_and_cities, parse_cities

# ============================================================
# classify_message
# ============================================================


class TestClassifyMessage:
    """Test message type and alert type classification."""

    def test_rocket_alert(self):
        text = "🚨 **ירי רקטות וטילים [7/8/2022] 8:13** **אזור שפלת יהודה** נווה אילן"
        msg_type, alert_type, is_drill = classify_message(text)
        assert msg_type == "alert"
        assert alert_type == "rockets"
        assert is_drill is False

    def test_aircraft_alert(self):
        text = "✈ **חדירת כלי טיס עוין (6/3/2026) 22:07** **אזור קו העימות** קריית שמונה"
        msg_type, alert_type, is_drill = classify_message(text)
        assert msg_type == "alert"
        assert alert_type == "aircraft"

    def test_earthquake_alert(self):
        text = "🚨 **רעידת אדמה [5/2/2024] 10:00** **אזור ים המלח** ערד"
        msg_type, alert_type, is_drill = classify_message(text)
        assert msg_type == "alert"
        assert alert_type == "earthquake"

    def test_hazmat_alert(self):
        text = "🚨 **חומרים מסוכנים [1/1/2024] 12:00** **אזור חיפה** חיפה"
        msg_type, alert_type, is_drill = classify_message(text)
        assert msg_type == "alert"
        assert alert_type == "hazmat"

    def test_infiltration_alert(self):
        text = "🚨 **חדירת מחבלים [7/10/2023] 06:29** **אזור עוטף עזה** שדרות"
        msg_type, alert_type, is_drill = classify_message(text)
        assert msg_type == "alert"
        assert alert_type == "infiltration"

    def test_event_ended(self):
        text = "🚨 **עדכון (6/3/2026) 22:18** **האירוע הסתיים** השוהים במרחב המוגן יכולים לצאת."
        msg_type, alert_type, is_drill = classify_message(text)
        assert msg_type == "event_ended"

    def test_heads_up(self):
        text = "🚨 **מבזק (6/3/2026) 21:54** **בדקות הקרובות צפויות להתקבל התרעות באזורך**"
        msg_type, alert_type, is_drill = classify_message(text)
        assert msg_type == "heads_up"

    def test_can_leave_shelter(self):
        text = "ניתן לצאת מהמרחב המוגן. אין הגבלות."
        msg_type, alert_type, is_drill = classify_message(text)
        assert msg_type == "can_leave_shelter"

    def test_drill(self):
        text = "**תרגיל! תרגיל! תרגיל!** 🚨 **ירי רקטות וטילים [16/5/2022] 10:35** **אזור דן** תל אביב"
        msg_type, alert_type, is_drill = classify_message(text)
        assert msg_type == "alert"
        assert alert_type == "rockets"
        assert is_drill is True

    def test_update(self):
        text = "🚨 **עדכון פיקוד העורף** - מצב ההתרעות באזור הדרום"
        msg_type, alert_type, is_drill = classify_message(text)
        assert msg_type == "update"

    def test_intercept_report(self):
        text = "דיווח על יירוט מוצלח מעל שמי הצפון"
        msg_type, alert_type, is_drill = classify_message(text)
        assert msg_type == "intercept_report"

    def test_instructions(self):
        text = "הנחיות פיקוד העורף למרחב הצפוני"
        msg_type, alert_type, is_drill = classify_message(text)
        assert msg_type == "instructions"

    def test_other(self):
        text = "הודעה כללית שלא מתאימה לשום קטגוריה"
        msg_type, alert_type, is_drill = classify_message(text)
        assert msg_type == "other"
        assert alert_type is None

    def test_alert_type_none_for_non_alert(self):
        text = "🚨 **עדכון (6/3/2026)** **האירוע הסתיים**"
        _, alert_type, _ = classify_message(text)
        # event_ended doesn't have a threat type
        assert alert_type is None

    def test_drill_flag_independent_of_type(self):
        """Drill flag should be set even for non-alert messages."""
        text = "תרגיל - הנחיות פיקוד העורף"
        _, _, is_drill = classify_message(text)
        assert is_drill is True


# ============================================================
# extract_date_time
# ============================================================


class TestExtractDateTime:
    """Test date/time extraction from different message formats."""

    def test_bracket_format(self):
        text = "🚨 **ירי רקטות וטילים [7/8/2022] 8:13** **אזור X**"
        date, time = extract_date_time(text)
        assert date == "7/8/2022"
        assert time == "8:13"

    def test_paren_format(self):
        text = "🚨 ירי רקטות וטילים (28/2/2026) 11:29 **אזור ירושלים**"
        date, time = extract_date_time(text)
        assert date == "28/2/2026"
        assert time == "11:29"

    def test_no_date(self):
        text = "האירוע הסתיים. ניתן לצאת מהמרחב."
        date, time = extract_date_time(text)
        assert date is None
        assert time is None

    def test_with_beshaa(self):
        """Old format with 'בשעה' prefix."""
        text = "🚨 **עדכון - התרעות** 🚨 [15/11/2019] בשעה 14:52"
        date, time = extract_date_time(text)
        assert date == "15/11/2019"
        assert time == "14:52"


# ============================================================
# extract_zones_and_cities
# ============================================================


class TestExtractZonesAndCities:
    """Test zone and city extraction from alert messages."""

    def test_single_zone_single_city(self):
        text = (
            "🚨 **ירי רקטות וטילים [23/10/2024] 14:52** **אזור קו העימות** קריית שמונה (מיידי) **היכנסו למרחב המוגן**"
        )
        results = extract_zones_and_cities(text)
        assert len(results) == 1
        zone, cities = results[0]
        assert zone == "אזור קו העימות"
        assert any("קריית שמונה" in c[0] for c in cities)

    def test_single_zone_multiple_cities(self):
        text = "🚨 **ירי רקטות וטילים [13/5/2023] 12:56** **אזור לכיש** בית עזרא (45 שניות) אשדוד - ג,ו,ז (45 שניות) אמונים (45 שניות) **היכנסו למרחב המוגן**"
        results = extract_zones_and_cities(text)
        assert len(results) == 1
        zone, cities = results[0]
        assert zone == "אזור לכיש"
        assert len(cities) >= 3

    def test_multiple_zones(self):
        text = "🚨 **ירי רקטות וטילים (28/2/2026) 11:29** **אזור ירושלים** אבן ספיר, אורה (**דקה וחצי**) **אזור שומרון** בית חורון (**דקה וחצי**) **אזור שפלת יהודה** עין נקובא (**דקה וחצי**) **היכנסו למרחב המוגן.**"
        results = extract_zones_and_cities(text)
        assert len(results) == 3
        zone_names = [r[0] for r in results]
        assert "אזור ירושלים" in zone_names
        assert "אזור שומרון" in zone_names
        assert "אזור שפלת יהודה" in zone_names

    def test_no_zones_in_non_alert(self):
        text = "האירוע הסתיים. השוהים במרחב המוגן יכולים לצאת."
        results = extract_zones_and_cities(text)
        assert len(results) == 0

    def test_city_name_with_azon_prefix(self):
        """City names starting with 'אזור' (like 'אזור תעשייה') should not be confused with zones."""
        text = "🚨 **ירי רקטות וטילים** **אזור לכיש** אזור תעשייה הדרומי אשקלון (45 שניות) **היכנסו**"
        results = extract_zones_and_cities(text)
        assert len(results) == 1
        zone, cities = results[0]
        assert zone == "אזור לכיש"
        # The city "אזור תעשייה הדרומי אשקלון" should be in cities, not treated as a zone
        city_names = [c[0] for c in cities]
        assert any("אזור תעשייה" in cn for cn in city_names)

    def test_shelter_time_extraction(self):
        text = "**אזור קו העימות** קריית שמונה (מיידי) נאות מרדכי (15 שניות) **היכנסו**"
        results = extract_zones_and_cities(text)
        assert len(results) == 1
        _, cities = results[0]
        shelter_times = {c[0]: c[1] for c in cities}
        assert shelter_times.get("קריית שמונה") == "מיידי"
        assert shelter_times.get("נאות מרדכי") == "15 שניות"


# ============================================================
# parse_cities
# ============================================================


class TestParseCities:
    """Test city name parsing from text blocks."""

    def test_comma_separated(self):
        text = "חולתה, שדה אליעזר, איילת השחר"
        cities = parse_cities(text)
        assert len(cities) == 3
        names = [c[0] for c in cities]
        assert "חולתה" in names
        assert "שדה אליעזר" in names

    def test_with_shelter_times(self):
        text = "קריית שמונה (מיידי) נאות מרדכי (15 שניות)"
        cities = parse_cities(text)
        assert len(cities) >= 2
        for name, shelter in cities:
            if name == "קריית שמונה":
                assert shelter == "מיידי"

    def test_stop_words_filtered(self):
        text = "היכנסו למרחב המוגן ושהו בו למשך 10 דקות"
        cities = parse_cities(text)
        assert len(cities) == 0

    def test_strips_bold_markers(self):
        text = "**תל אביב**, **חיפה**"
        cities = parse_cities(text)
        names = [c[0] for c in cities]
        assert "תל אביב" in names
        assert "חיפה" in names

    def test_single_char_filtered(self):
        """Single character strings should be filtered out."""
        text = "א, תל אביב, ב"
        cities = parse_cities(text)
        names = [c[0] for c in cities]
        assert "תל אביב" in names
        assert "א" not in names

    def test_space_separated_cities(self):
        """Aircraft alerts use spaces instead of commas between city names."""
        cities = parse_cities("נהריה סער עברון")
        names = [c[0] for c in cities]
        assert "נהריה" in names
        assert "סער" in names
        assert "עברון" in names
        assert len(names) == 3

    def test_space_separated_multi_word_preserved(self):
        """Multi-word city names should stay together in space-split mode."""
        cities = parse_cities("קריית שמונה נאות מרדכי יפתח")
        names = [c[0] for c in cities]
        assert "קריית שמונה" in names
        assert "נאות מרדכי" in names
        assert "יפתח" in names

    def test_space_separated_with_known_prefix(self):
        cities = parse_cities("גשר הזיו כפר בלום בן עמי")
        names = [c[0] for c in cities]
        assert "גשר הזיו" in names
        assert "כפר בלום" in names
        assert "בן עמי" in names

    def test_space_separated_compound_city_not_truncated(self):
        """Compound city names like 'אזור תעשייה מישור אדומים' must not be split."""
        cities = parse_cities("אזור תעשייה מישור אדומים")
        names = [c[0] for c in cities]
        assert "אזור תעשייה מישור אדומים" in names
        assert "אדומים" not in names

    def test_space_separated_compound_city_with_neighbors(self):
        """Compound city in a list of space-separated cities."""
        cities = parse_cities("כפר אדומים אזור תעשייה מישור אדומים מעלה אדומים")
        names = [c[0] for c in cities]
        assert "כפר אדומים" in names
        assert "אזור תעשייה מישור אדומים" in names
        assert "מעלה אדומים" in names
        assert len(names) == 3

    def test_space_separated_compound_industrial_zones(self):
        """Multiple compound industrial zone names parsed correctly."""
        cities = parse_cities("אזור תעשייה רמת דלתון אזור תעשייה אכזיב מילואות נהריה")
        names = [c[0] for c in cities]
        assert "אזור תעשייה רמת דלתון" in names
        assert "אזור תעשייה אכזיב מילואות" in names
        assert "נהריה" in names
        assert "דלתון" not in names
        assert "מילואות" not in names

    def test_space_separated_merkaz_azori(self):
        """מרכז אזורי compound names not truncated."""
        cities = parse_cities("מרכז אזורי מבואות חרמון דלתון")
        names = [c[0] for c in cities]
        assert "מרכז אזורי מבואות חרמון" in names
        assert "דלתון" in names
        assert "חרמון" not in names

    def test_space_separated_unknown_compound_falls_back(self):
        """Unknown compound after prefix falls back to prefix + 1 word."""
        cities = parse_cities("אזור תעשייה חדש נהריה")
        names = [c[0] for c in cities]
        assert "אזור תעשייה חדש" in names
        assert "נהריה" in names

    def test_space_separated_compound_with_dash_suffix(self):
        """Compound city names with dash suffix parsed correctly."""
        cities = parse_cities("אזור תעשייה נשר - רמלה")
        names = [c[0] for c in cities]
        assert "אזור תעשייה נשר - רמלה" in names
        assert len(names) == 1

    def test_space_separated_compound_known_with_dash_suffix(self):
        """Known compound city name with dash suffix parsed correctly."""
        cities = parse_cities("אזור תעשייה הר טוב - צרעה נהריה")
        names = [c[0] for c in cities]
        assert "אזור תעשייה הר טוב - צרעה" in names
        assert "נהריה" in names

    def test_space_separated_single_word_dash_suffix(self):
        """Single-word city with dash suffix (e.g. 'חיפה - מערב')."""
        cities = parse_cities("נהריה חיפה - מערב עכו")
        names = [c[0] for c in cities]
        assert "נהריה" in names
        assert "חיפה - מערב" in names
        assert "עכו" in names

    def test_space_separated_prefix_at_end_of_text(self):
        """Multi-word prefix at end of text with no following word."""
        cities = parse_cities("נהריה אזור תעשייה")
        names = [c[0] for c in cities]
        assert "נהריה" in names
        assert "אזור תעשייה" in names

    def test_fallback_zone_parser_no_bold(self):
        """Fallback zone parsing when no bold markers present."""
        from pikud import extract_zones_and_cities

        text = "אזור קו העימות נהריה סער עברון"
        results = extract_zones_and_cities(text)
        assert len(results) == 1
        zone, cities = results[0]
        assert zone == "אזור קו העימות"
        names = [c[0] for c in cities]
        assert "נהריה" in names

    def test_ashdod_neighborhood_comma_groups(self):
        """Ashdod neighborhoods in comma groups must keep the city prefix."""
        # Bug: "אשדוד - א,ב,ד,ה" was splitting to "אשדוד - א", "ב", "ד", "ה"
        text = "🚨 **עדכון - התרעות** 🚨 [12/11/2019] בשעה 5:50 הופעלה התרעה ב: **אזור לכיש** אשדוד - יא,יב,טו,יז,מרינה (45 שניות) אשדוד - א,ב,ד,ה (45 שניות)"
        results = extract_zones_and_cities(text)
        assert len(results) == 1
        _, cities = results[0]
        names = [c[0] for c in cities]
        assert "אשדוד - יא" in names
        assert "אשדוד - יב" in names
        assert "אשדוד - טו" in names
        assert "אשדוד - יז" in names
        assert "אשדוד - מרינה" in names
        assert "אשדוד - א" in names
        assert "אשדוד - ב" in names
        assert "אשדוד - ד" in names
        assert "אשדוד - ה" in names
        assert "יב" not in names
        assert "טו" not in names
        assert "מרינה" not in names
        assert "ב" not in names

    def test_ashdod_no_space_before_dash(self):
        """'אשדוד -יא,יב' (no space before dash) must also be prefixed correctly."""
        text = "🚨 **ירי רקטות וטילים [6/8/2022] 12:01** **אזור לכיש** אשדוד - א,ב,ד,ה (45 שניות) אשדוד -יא,יב,טו,יז,מרינה,סיטי (45 שניות)"
        results = extract_zones_and_cities(text)
        assert len(results) == 1
        _, cities = results[0]
        names = [c[0] for c in cities]
        assert "אשדוד - א" in names
        assert "אשדוד - ב" in names
        # No-space dash variant
        assert "אשדוד -יא" in names or "אשדוד - יא" in names
        assert "יב" not in names
        assert "מרינה" not in names
        assert "סיטי" not in names

    def test_ashdod_multiple_groups_no_double_prefix(self):
        """When multiple 'City - ...' groups are comma-separated, no double-prefix."""
        # Pattern: "אשדוד - א,ב,ד,ה, אשדוד - ג,ו,ז"
        text = "**אזור לכיש** אשדוד - א,ב,ד,ה, אשדוד - ג,ו,ז (**45 שניות**)"
        results = extract_zones_and_cities(text)
        assert len(results) == 1
        _, cities = results[0]
        names = [c[0] for c in cities]
        assert "אשדוד - א" in names
        assert "אשדוד - ב" in names
        assert "אשדוד - ג" in names
        assert "אשדוד - ו" in names
        # Must NOT produce double prefix
        assert not any("אשדוד - אשדוד" in n for n in names)

    def test_hashas_not_parsed_as_city(self):
        """'חשש לחדירת מחבלים' suffix must not produce city names."""
        text = "🔓 **חדירת מחבלים [29/2/2024] 17:28** **אזור שומרון** עלי **חשש לחדירת מחבלים** היכנסו מיד למרחב מוגן"
        results = extract_zones_and_cities(text)
        assert len(results) == 1
        _, cities = results[0]
        names = [c[0] for c in cities]
        assert "עלי" in names
        assert "חשש" not in names
        assert "מחבלים" not in names
        assert "לחדירת" not in names


class TestParseCitiesSpaceSplitMissingPrefixes:
    """Regression tests for space-split compound cities whose prefixes were missing."""

    def test_vered_hagalil(self):
        cities = parse_cities("ורד הגליל כורזים")
        names = [c[0] for c in cities]
        assert "ורד הגליל" in names
        assert "ורד" not in names

    def test_mevo_hama(self):
        cities = parse_cities("מבוא חמה כפר חרוב")
        names = [c[0] for c in cities]
        assert "מבוא חמה" in names
        assert "חמה" not in names

    def test_majdal_shams(self):
        cities = parse_cities("מג'דל שמס מסעדה")
        names = [c[0] for c in cities]
        assert "מג'דל שמס" in names
        assert "שמס" not in names

    def test_kadmat_tzvi(self):
        cities = parse_cities("קדמת צבי קצרין")
        names = [c[0] for c in cities]
        assert "קדמת צבי" in names
        assert "צבי" not in names

    def test_kerem_ben_zimra(self):
        cities = parse_cities("כרם בן זמרה יראון")
        names = [c[0] for c in cities]
        assert "כרם בן זמרה" in names
        assert "כרם" not in names

    def test_ali_zahav(self):
        cities = parse_cities("עלי זהב")
        names = [c[0] for c in cities]
        assert "עלי זהב" in names
        assert "זהב" not in names

    def test_karmi_tzur(self):
        cities = parse_cities("כרמי צור")
        names = [c[0] for c in cities]
        assert "כרמי צור" in names
        assert "צור" not in names

    def test_neve_itan(self):
        """'נוה' (alternate spelling of נווה) should work as prefix."""
        cities = parse_cities("נוה איתן כפר רופין")
        names = [c[0] for c in cities]
        assert "נוה איתן" in names
        assert "נוה" not in names

    def test_sdei_trumot(self):
        cities = parse_cities("שדי תרומות בית שאן")
        names = [c[0] for c in cities]
        assert "שדי תרומות" in names
        assert "שדי" not in names

    def test_gani_hoga(self):
        cities = parse_cities("גני חוגה בית שאן")
        names = [c[0] for c in cities]
        assert "גני חוגה" in names
        assert "גני" not in names

    def test_rafting_nahar_hayarden(self):
        cities = parse_cities("רפטינג נהר הירדן")
        names = [c[0] for c in cities]
        assert "רפטינג נהר הירדן" in names
        assert "נהר" not in names

    def test_batei_malon_yam_hamelach(self):
        cities = parse_cities("בתי מלון ים המלח")
        names = [c[0] for c in cities]
        assert "בתי מלון ים המלח" in names
        assert "בתי" not in names

    def test_pardes_hana_karkur(self):
        cities = parse_cities("פרדס חנה כרכור")
        names = [c[0] for c in cities]
        assert "פרדס חנה כרכור" in names
        assert "חנה" not in names

    def test_um_al_fahm(self):
        cities = parse_cities("אום אל פחם")
        names = [c[0] for c in cities]
        assert "אום אל פחם" in names
        assert "פחם" not in names

    def test_hod_hasharon(self):
        cities = parse_cities("הוד השרון")
        names = [c[0] for c in cities]
        assert "הוד השרון" in names
        assert "הוד" not in names
