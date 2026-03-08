"""Shared fixtures for all tests."""
import os
import sys
import sqlite3
import tempfile
import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Marker: skip tests that require a populated database
_db_dir = os.path.join(os.path.dirname(__file__), "db")
_has_db = os.path.exists(_db_dir) and any(f.endswith(".db") for f in os.listdir(_db_dir)) if os.path.exists(_db_dir) else False

requires_db = pytest.mark.skipif(not _has_db, reason="No database found — run 'make fetch' first")


@pytest.fixture
def real_db_path():
    """Path to the real production database (read-only tests only)."""
    path = os.path.join(os.path.dirname(__file__), "db", "pikud_v1.db")
    if not os.path.exists(path):
        pytest.skip("Production DB not found")
    return path


@pytest.fixture
def real_db(real_db_path):
    """Read-only connection to real production DB."""
    conn = sqlite3.connect(real_db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    yield conn
    conn.close()


@pytest.fixture
def temp_db():
    """Temporary SQLite database for write tests."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    conn = sqlite3.connect(path)
    yield conn, path
    conn.close()
    os.unlink(path)


@pytest.fixture
def app():
    """Flask test app."""
    import dashboard
    dashboard.reset_shared_db()
    dashboard.app.config["TESTING"] = True
    return dashboard.app


@pytest.fixture
def client(app):
    """Flask test client."""
    with app.test_client() as c:
        yield c
