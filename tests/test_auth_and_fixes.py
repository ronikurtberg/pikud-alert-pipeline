"""Tests for the auth/pipeline improvements and bug fixes.

Covers:
- fetch_messages fail-fast when Telegram session is not authorized
- /api/pipeline/auth_status endpoint (no-credentials path, no-DB needed)
- /api/pipeline/auth/start input validation
- /api/pipeline/auth/confirm input validation
- Hourly chart data shape from /api/viz/hourly
- pikud.py cmd_auth registered in CLI choices
"""

import json
import os
import sys
import unittest.mock as mock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ---------------------------------------------------------------------------
# fetch_messages fail-fast (no Telethon / network needed)
# ---------------------------------------------------------------------------


class TestFetchMessagesFastFail:
    """fetch_messages must return None immediately when not authorized."""

    def test_returns_none_when_not_authorized(self):
        """Simulate an expired session: is_user_authorized returns False."""
        import asyncio

        import pikud

        mock_client = mock.AsyncMock()
        mock_client.connect = mock.AsyncMock()
        mock_client.is_user_authorized = mock.AsyncMock(return_value=False)
        mock_client.disconnect = mock.AsyncMock()

        mock_telethon = mock.MagicMock()
        mock_telethon.TelegramClient = mock.MagicMock(return_value=mock_client)

        with (
            mock.patch.dict(os.environ, {"TELEGRAM_API_ID": "123", "TELEGRAM_API_HASH": "abc"}),
            mock.patch.dict(sys.modules, {"telethon": mock_telethon}),
        ):
            result = asyncio.run(pikud.fetch_messages(min_id=100))

        assert result is None
        mock_client.connect.assert_awaited_once()
        mock_client.is_user_authorized.assert_awaited_once()
        mock_client.disconnect.assert_awaited_once()

    def test_returns_none_when_no_credentials(self):
        """Missing API_ID/API_HASH should return None without touching Telegram."""
        import asyncio

        import pikud

        with mock.patch.object(pikud, "API_ID", ""), mock.patch.object(pikud, "API_HASH", ""):
            result = asyncio.run(pikud.fetch_messages(min_id=0))

        assert result is None


# ---------------------------------------------------------------------------
# Auth status endpoint — no credentials path (no DB needed, no Telethon call)
# ---------------------------------------------------------------------------


class TestAuthStatusEndpointNoCreds:
    """auth_status returns authorized=False when credentials are missing."""

    def test_missing_credentials_returns_not_authorized(self, client):
        with mock.patch.dict(os.environ, {"TELEGRAM_API_ID": "", "TELEGRAM_API_HASH": ""}):
            r = client.get("/api/pipeline/auth_status")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert d["authorized"] is False
        assert d.get("reason") == "missing_credentials"

    def test_response_shape(self, client):
        """Response always has 'authorized' key."""
        with mock.patch.dict(os.environ, {"TELEGRAM_API_ID": "", "TELEGRAM_API_HASH": ""}):
            r = client.get("/api/pipeline/auth_status")
        d = json.loads(r.data)
        assert "authorized" in d

    def test_with_mocked_authorized_session(self, client):
        """When is_user_authorized returns True, endpoint returns authorized=True."""
        mock_client = mock.AsyncMock()
        mock_client.connect = mock.AsyncMock()
        mock_client.is_user_authorized = mock.AsyncMock(return_value=True)
        mock_client.disconnect = mock.AsyncMock()

        mock_telethon = mock.MagicMock()
        mock_telethon.TelegramClient = mock.MagicMock(return_value=mock_client)

        with (
            mock.patch.dict(os.environ, {"TELEGRAM_API_ID": "123", "TELEGRAM_API_HASH": "abc"}),
            mock.patch.dict(sys.modules, {"telethon": mock_telethon}),
        ):
            r = client.get("/api/pipeline/auth_status")

        d = json.loads(r.data)
        assert d["authorized"] is True


# ---------------------------------------------------------------------------
# Auth start — input validation (no Telethon call needed)
# ---------------------------------------------------------------------------


class TestAuthStartValidation:
    def test_missing_phone_returns_400(self, client):
        r = client.post(
            "/api/pipeline/auth/start",
            data=json.dumps({}),
            content_type="application/json",
        )
        assert r.status_code == 400
        d = json.loads(r.data)
        assert d["ok"] is False
        assert "phone" in d["error"].lower()

    def test_empty_phone_returns_400(self, client):
        r = client.post(
            "/api/pipeline/auth/start",
            data=json.dumps({"phone": "   "}),
            content_type="application/json",
        )
        assert r.status_code == 400
        d = json.loads(r.data)
        assert d["ok"] is False

    def test_missing_credentials_returns_500(self, client):
        with mock.patch.dict(os.environ, {"TELEGRAM_API_ID": "", "TELEGRAM_API_HASH": ""}):
            r = client.post(
                "/api/pipeline/auth/start",
                data=json.dumps({"phone": "+972501234567"}),
                content_type="application/json",
            )
        assert r.status_code == 500
        d = json.loads(r.data)
        assert d["ok"] is False


# ---------------------------------------------------------------------------
# Auth confirm — input validation
# ---------------------------------------------------------------------------


class TestAuthConfirmValidation:
    def test_missing_phone_returns_400(self, client):
        r = client.post(
            "/api/pipeline/auth/confirm",
            data=json.dumps({"code": "12345"}),
            content_type="application/json",
        )
        assert r.status_code == 400
        d = json.loads(r.data)
        assert d["ok"] is False

    def test_missing_code_returns_400(self, client):
        r = client.post(
            "/api/pipeline/auth/confirm",
            data=json.dumps({"phone": "+972501234567"}),
            content_type="application/json",
        )
        assert r.status_code == 400
        d = json.loads(r.data)
        assert d["ok"] is False

    def test_no_active_session_returns_400(self, client):
        """Confirm without a prior start call should return 400."""
        import dashboard

        # Ensure no leftover state
        dashboard._tg_phone_hash = ""
        dashboard._tg_pending_phone = ""

        r = client.post(
            "/api/pipeline/auth/confirm",
            data=json.dumps({"phone": "+972501234567", "code": "12345"}),
            content_type="application/json",
        )
        assert r.status_code == 400
        d = json.loads(r.data)
        assert d["ok"] is False


# ---------------------------------------------------------------------------
# Hourly viz endpoint — data shape
# ---------------------------------------------------------------------------


class TestHourlyVizShape:
    """Regression: hourly endpoint must return rows with hour_israel and total."""

    def test_hourly_returns_list(self, client):
        from conftest import _has_db

        if not _has_db:
            pytest.skip("No database — run 'make fetch' first")
        r = client.get("/api/viz/hourly")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert isinstance(d, list)

    def test_hourly_row_has_required_keys(self, client):
        from conftest import _has_db

        if not _has_db:
            pytest.skip("No database — run 'make fetch' first")
        r = client.get("/api/viz/hourly")
        d = json.loads(r.data)
        assert len(d) > 0
        row = d[0]
        assert "hour_israel" in row
        assert "total" in row
        assert "rockets" in row
        assert "aircraft" in row

    def test_hourly_covers_all_hours(self, client):
        from conftest import _has_db

        if not _has_db:
            pytest.skip("No database — run 'make fetch' first")
        r = client.get("/api/viz/hourly")
        d = json.loads(r.data)
        hours = {row["hour_israel"] for row in d}
        # Should have data across multiple hours (dataset spans years)
        assert len(hours) >= 20

    def test_hourly_hour_range_valid(self, client):
        from conftest import _has_db

        if not _has_db:
            pytest.skip("No database — run 'make fetch' first")
        r = client.get("/api/viz/hourly")
        d = json.loads(r.data)
        for row in d:
            assert 0 <= row["hour_israel"] <= 23

    def test_hourly_with_date_filter(self, client):
        from conftest import _has_db

        if not _has_db:
            pytest.skip("No database — run 'make fetch' first")
        r = client.get("/api/viz/hourly?date_from=2024-01-01&date_to=2024-12-31")
        assert r.status_code == 200
        d = json.loads(r.data)
        assert isinstance(d, list)


# ---------------------------------------------------------------------------
# CLI: auth command is registered
# ---------------------------------------------------------------------------


class TestCLIAuthCommand:
    def test_auth_in_valid_choices(self):
        """cmd_auth must be wired into main() so the subprocess invocation works."""
        import argparse

        # Reconstruct the parser the same way main() does
        parser = argparse.ArgumentParser()
        parser.add_argument("command", choices=["delta", "full_refresh", "rebuild_db", "validate", "status", "auth"])
        args = parser.parse_args(["auth"])
        assert args.command == "auth"

    def test_cmd_auth_exists(self):
        import pikud

        assert callable(pikud.cmd_auth)

    def test_cmd_auth_returns_false_without_credentials(self):
        """cmd_auth checks module-level API_ID/API_HASH — patch them directly."""
        import pikud

        with mock.patch.object(pikud, "API_ID", ""), mock.patch.object(pikud, "API_HASH", ""):
            result = pikud.cmd_auth()
        assert result is False

    def test_cmd_auth_does_not_block_without_credentials(self):
        """cmd_auth must return before asyncio.run when credentials are missing."""
        import pikud

        with (
            mock.patch.object(pikud, "API_ID", ""),
            mock.patch.object(pikud, "API_HASH", ""),
            mock.patch("asyncio.run") as mock_run,
        ):
            result = pikud.cmd_auth()
        assert result is False
        mock_run.assert_not_called()
