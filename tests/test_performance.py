"""Performance tests -- ensure no endpoint regresses beyond acceptable thresholds.

Requires a populated database. Run 'make fetch' first.
"""
import pytest
import json
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from conftest import requires_db

pytestmark = requires_db

MAX_MS = {
    "fast": 100,     # simple queries
    "medium": 300,   # joins with filters
    "slow": 600,     # complex analytics
}


class TestEndpointLatency:
    """Every API endpoint must respond within its latency budget."""

    FAST_ENDPOINTS = [
        "/api/stats", "/api/stat_sql", "/api/viz/meta", "/api/transformations",
        "/api/prefilters", "/api/viz/hourly", "/api/viz/daily", "/api/viz/dow",
        "/api/viz/monthly", "/api/viz/calendar", "/api/viz/erd",
        "/api/viz/drone_cities", "/api/viz/heads_up_correlation",
    ]
    MEDIUM_ENDPOINTS = [
        "/api/filtered_counts", "/api/filter_options",
        "/api/viz/top_cities", "/api/viz/zones", "/api/viz/shelter_times",
        "/api/viz/response_time", "/api/viz/multi_zone",
        "/api/viz/city_zone_anomaly", "/api/viz/event_ended_analysis",
        "/api/data_profile", "/api/pipeline/versions",
    ]
    SLOW_ENDPOINTS = [
        "/api/viz/streaks", "/api/viz/city_safety_rank",
        "/api/viz/escalation", "/api/viz/city_timeline",
        "/api/data_journey/examples",
    ]

    @pytest.mark.parametrize("endpoint", FAST_ENDPOINTS)
    def test_fast_endpoint(self, client, endpoint):
        t0 = time.time()
        r = client.get(endpoint)
        ms = (time.time() - t0) * 1000
        assert r.status_code == 200
        assert ms < MAX_MS["fast"], f"{endpoint} took {ms:.0f}ms (max {MAX_MS['fast']}ms)"

    @pytest.mark.parametrize("endpoint", MEDIUM_ENDPOINTS)
    def test_medium_endpoint(self, client, endpoint):
        t0 = time.time()
        r = client.get(endpoint)
        ms = (time.time() - t0) * 1000
        assert r.status_code == 200
        assert ms < MAX_MS["medium"], f"{endpoint} took {ms:.0f}ms (max {MAX_MS['medium']}ms)"

    @pytest.mark.parametrize("endpoint", SLOW_ENDPOINTS)
    def test_slow_endpoint(self, client, endpoint):
        t0 = time.time()
        r = client.get(endpoint)
        ms = (time.time() - t0) * 1000
        assert r.status_code == 200
        assert ms < MAX_MS["slow"], f"{endpoint} took {ms:.0f}ms (max {MAX_MS['slow']}ms)"


class TestTotalLatency:
    """All endpoints combined should complete within budget."""

    def test_total_under_3_seconds(self, client):
        all_eps = (TestEndpointLatency.FAST_ENDPOINTS +
                   TestEndpointLatency.MEDIUM_ENDPOINTS +
                   TestEndpointLatency.SLOW_ENDPOINTS)
        t0 = time.time()
        for ep in all_eps:
            r = client.get(ep)
            assert r.status_code == 200
        total_ms = (time.time() - t0) * 1000
        assert total_ms < 3000, f"Total {len(all_eps)} endpoints took {total_ms:.0f}ms (max 3000ms)"
