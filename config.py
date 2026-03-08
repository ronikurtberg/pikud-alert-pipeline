"""
Configuration: Pre-defined date filters for significant events.
Add new events here — they appear automatically in the dashboard filter bar.
"""

PREFILTERS = [
    {
        "id": "all",
        "icon": "🌍",
        "name_en": "All Time",
        "name_he": "כל התקופה",
        "date_from": None,
        "date_to": None,
    },
    {
        "id": "iran_war_2026",
        "icon": "🔥",
        "name_en": "Israel-Iran-USA War",
        "name_he": "מלחמת ישראל-איראן-ארה״ב",
        "date_from": "2026-02-28",
        "date_to": None,  # ongoing
    },
    {
        "id": "oct7_war",
        "icon": "⚔️",
        "name_en": "Iron Swords War",
        "name_he": "מלחמת חרבות ברזל",
        "date_from": "2023-10-07",
        "date_to": "2024-12-01",
    },
    {
        "id": "oct7_day",
        "icon": "🖤",
        "name_en": "October 7th",
        "name_he": "7 באוקטובר",
        "date_from": "2023-10-07",
        "date_to": "2023-10-07",
    },
    {
        "id": "guardian_walls",
        "icon": "🛡️",
        "name_en": "Guardian of the Walls",
        "name_he": "שומר החומות",
        "date_from": "2021-05-10",
        "date_to": "2021-05-21",
    },
    {
        "id": "iran_attack_apr24",
        "icon": "🚀",
        "name_en": "Iran Direct Attack Apr 2024",
        "name_he": "מתקפת איראן אפריל 2024",
        "date_from": "2024-04-13",
        "date_to": "2024-04-14",
    },
    {
        "id": "north_escalation",
        "icon": "🏔️",
        "name_en": "Northern Escalation",
        "name_he": "הסלמה בצפון",
        "date_from": "2024-09-01",
        "date_to": "2024-11-30",
    },
    {
        "id": "last_7d",
        "icon": "📅",
        "name_en": "Last 7 Days",
        "name_he": "7 ימים אחרונים",
        "date_from": "__LAST_7D__",  # dynamic, resolved at runtime
        "date_to": None,
    },
    {
        "id": "last_30d",
        "icon": "📆",
        "name_en": "Last 30 Days",
        "name_he": "30 יום אחרונים",
        "date_from": "__LAST_30D__",
        "date_to": None,
    },
]
