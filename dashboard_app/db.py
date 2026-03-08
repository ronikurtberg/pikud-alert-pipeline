"""Database connection management, query helpers, and SQL latency tracking."""
import json
import os
import sqlite3
import threading
import time
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_DIR = os.path.join(BASE_DIR, "db")
DATA_DIR = os.path.join(BASE_DIR, "data")
LOGS_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

SQL_LOG_PATH = os.path.join(LOGS_DIR, "sql_latency.jsonl")
_sql_stats: list[dict] = []
_sql_stats_lock = threading.Lock()
SQL_STATS_MAX = 5000

_shared_conn = None
_shared_conn_lock = threading.Lock()


def _log_sql(sql: str, ms: float, rows: int | None = None, result_bytes: int = 0, endpoint: str = "") -> None:
    entry = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "sql": sql.strip()[:500],
        "ms": round(ms, 1),
        "rows": rows,
        "bytes": result_bytes,
        "endpoint": endpoint,
    }
    with _sql_stats_lock:
        _sql_stats.append(entry)
        if len(_sql_stats) > SQL_STATS_MAX:
            _sql_stats.pop(0)
    try:
        with open(SQL_LOG_PATH, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass


def get_sql_stats() -> list[dict]:
    with _sql_stats_lock:
        return list(_sql_stats)


def get_sql_summary() -> dict:
    """Compute summary stats from the in-memory ring buffer."""
    stats = get_sql_stats()
    if not stats:
        return {"total": 0, "percentiles": {}, "avg_ms": 0, "top_slow": [], "by_endpoint": [], "log_path": SQL_LOG_PATH}

    ms_vals = sorted(e["ms"] for e in stats)
    n = len(ms_vals)
    def pct(p):
        return round(ms_vals[min(int(n * p / 100), n - 1)], 1)

    # Top slow by unique SQL
    by_sql = {}
    for e in stats:
        key = e["sql"][:200]
        if key not in by_sql:
            by_sql[key] = {"sql": e["sql"], "calls": 0, "total_ms": 0, "max_ms": 0,
                           "min_ms": 999999, "total_rows": 0, "total_bytes": 0}
        s = by_sql[key]
        s["calls"] += 1
        s["total_ms"] += e["ms"]
        s["max_ms"] = max(s["max_ms"], e["ms"])
        s["min_ms"] = min(s["min_ms"], e["ms"])
        s["total_rows"] += e.get("rows") or 0
        s["total_bytes"] += e.get("bytes") or 0
    top_slow = sorted(by_sql.values(), key=lambda x: -x["max_ms"])[:15]
    for t in top_slow:
        t["avg_ms"] = round(t["total_ms"] / t["calls"], 1)
        t["avg_rows"] = round(t["total_rows"] / t["calls"])

    # By endpoint
    by_ep = {}
    for e in stats:
        ep = e.get("endpoint") or "unknown"
        if ep not in by_ep:
            by_ep[ep] = {"endpoint": ep, "calls": 0, "total_ms": 0, "max_ms": 0}
        by_ep[ep]["calls"] += 1
        by_ep[ep]["total_ms"] += e["ms"]
        by_ep[ep]["max_ms"] = max(by_ep[ep]["max_ms"], e["ms"])
    ep_list = sorted(by_ep.values(), key=lambda x: -x["total_ms"])[:20]
    for ep in ep_list:
        ep["avg_ms"] = round(ep["total_ms"] / ep["calls"], 1)

    return {
        "total": n,
        "percentiles": {"min": pct(0), "p50": pct(50), "p90": pct(90), "p95": pct(95), "p99": pct(99), "max": pct(100)},
        "avg_ms": round(sum(ms_vals) / n, 1),
        "total_rows": sum(e.get("rows") or 0 for e in stats),
        "total_bytes": sum(e.get("bytes") or 0 for e in stats),
        "top_slow": top_slow,
        "by_endpoint": ep_list,
        "log_path": SQL_LOG_PATH,
    }


def get_db_path() -> str | None:
    link = os.path.join(DB_DIR, "current")
    if os.path.islink(link):
        return os.path.join(DB_DIR, os.readlink(link))
    dbs = sorted(f for f in os.listdir(DB_DIR) if f.endswith(".db")) if os.path.exists(DB_DIR) else []
    return os.path.join(DB_DIR, dbs[-1]) if dbs else None


def get_db() -> sqlite3.Connection | None:
    path = get_db_path()
    if not path or not os.path.exists(path):
        return None
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-20000")
    conn.execute("PRAGMA mmap_size=268435456")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


def get_shared_db() -> sqlite3.Connection | None:
    global _shared_conn
    with _shared_conn_lock:
        if _shared_conn is None:
            path = get_db_path()
            if not path or not os.path.exists(path):
                return None
            _shared_conn = sqlite3.connect(path, check_same_thread=False)
            _shared_conn.row_factory = sqlite3.Row
            _shared_conn.execute("PRAGMA journal_mode=WAL")
            _shared_conn.execute("PRAGMA synchronous=NORMAL")
            _shared_conn.execute("PRAGMA cache_size=-20000")
            _shared_conn.execute("PRAGMA mmap_size=268435456")
            _shared_conn.execute("PRAGMA temp_store=MEMORY")
            _shared_conn.execute("PRAGMA query_only=ON")
        return _shared_conn


def reset_shared_db() -> None:
    global _shared_conn
    with _shared_conn_lock:
        if _shared_conn:
            try:
                _shared_conn.close()
            except Exception:
                pass
            _shared_conn = None


def query_db(sql: str, params: tuple = (), endpoint: str = "") -> list[dict]:
    conn = get_shared_db()
    if not conn:
        return []
    t0 = time.time()
    rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    ms = (time.time() - t0) * 1000
    result_bytes = sum(len(str(v)) for r in rows for v in r.values()) if rows else 0
    _log_sql(sql, ms, len(rows), result_bytes, endpoint)
    return rows
