"""DuckDB-backed persistent cache.

Provides a key-value store that survives app restarts. All values are
stored as JSON text so any JSON-serialisable Python object can be cached.
Thread-safe via a single lock around all DuckDB operations.

Falls back to in-memory-only mode if DuckDB cannot open the database.
"""

import json
import os
import threading
import time

# Bump this version whenever the app's data schema changes.
# On startup, if the stored version differs, all cached data is cleared
# so users get a clean re-fetch instead of serving stale/incompatible data.
CACHE_SCHEMA_VERSION = "5"

DB_PATH = os.path.join(os.path.dirname(__file__), ".cache", "health_check_cache.duckdb")
_lock = threading.Lock()
_conn = None
_fallback = False
_mem_cache = {}


def _get_conn():
    global _conn, _fallback
    if _fallback:
        return None
    if _conn is None:
        try:
            import duckdb
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            _conn = duckdb.connect(DB_PATH)
            _conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kv_cache (
                    key   VARCHAR PRIMARY KEY,
                    data  VARCHAR NOT NULL,
                    ts    DOUBLE  NOT NULL
                )
                """
            )
            _check_schema_version()
        except Exception as e:
            print(f"Warning: DuckDB unavailable ({e}), using in-memory cache only")
            _fallback = True
            return None
    return _conn


def _check_schema_version():
    """Clear all cached data if the schema version has changed."""
    try:
        row = _conn.execute(
            "SELECT data FROM kv_cache WHERE key = '_schema_version'"
        ).fetchone()
        stored = row[0] if row else None
        if stored == CACHE_SCHEMA_VERSION:
            return
        if stored is not None:
            print(f"  Cache schema changed (v{stored} -> v{CACHE_SCHEMA_VERSION}), clearing stale data...")
        else:
            print(f"  Initializing cache (schema v{CACHE_SCHEMA_VERSION})...")
        _conn.execute("DELETE FROM kv_cache")
        _conn.execute(
            "INSERT OR REPLACE INTO kv_cache (key, data, ts) VALUES (?, ?, ?)",
            ["_schema_version", CACHE_SCHEMA_VERSION, time.time()],
        )
    except Exception:
        pass


def cache_get(key, ttl=None):
    with _lock:
        conn = _get_conn()
        if conn is None:
            entry = _mem_cache.get(key)
            if entry is None:
                return None
            data, ts = entry
            if ttl is not None and (time.time() - ts) > ttl:
                return None
            return data
        try:
            row = conn.execute(
                "SELECT data, ts FROM kv_cache WHERE key = ?", [key]
            ).fetchone()
        except Exception:
            return None
    if row is None:
        return None
    data_str, ts = row
    if ttl is not None and (time.time() - ts) > ttl:
        return None
    return json.loads(data_str)


def cache_set(key, data):
    with _lock:
        conn = _get_conn()
        if conn is None:
            _mem_cache[key] = (data, time.time())
            return
        data_str = json.dumps(data, default=_json_default)
        try:
            conn.execute(
                "INSERT OR REPLACE INTO kv_cache (key, data, ts) VALUES (?, ?, ?)",
                [key, data_str, time.time()],
            )
        except Exception:
            _mem_cache[key] = (data, time.time())


def cache_exists(key, ttl=None):
    with _lock:
        conn = _get_conn()
        if conn is None:
            entry = _mem_cache.get(key)
            if entry is None:
                return False
            if ttl is not None and (time.time() - entry[1]) > ttl:
                return False
            return True
        try:
            row = conn.execute(
                "SELECT ts FROM kv_cache WHERE key = ?", [key]
            ).fetchone()
        except Exception:
            return False
    if row is None:
        return False
    if ttl is not None and (time.time() - row[0]) > ttl:
        return False
    return True


def cache_delete(key):
    with _lock:
        _mem_cache.pop(key, None)
        conn = _get_conn()
        if conn is None:
            return
        try:
            conn.execute("DELETE FROM kv_cache WHERE key = ?", [key])
        except Exception:
            pass


def cache_clear():
    with _lock:
        _mem_cache.clear()
        conn = _get_conn()
        if conn is None:
            return
        try:
            conn.execute("DELETE FROM kv_cache")
        except Exception:
            pass


def cache_get_timestamp(key):
    """Return epoch timestamp when key was last set, or None."""
    with _lock:
        conn = _get_conn()
        if conn is None:
            entry = _mem_cache.get(key)
            return entry[1] if entry else None
        try:
            row = conn.execute(
                "SELECT ts FROM kv_cache WHERE key = ?", [key]
            ).fetchone()
            return row[0] if row else None
        except Exception:
            return None


def cache_clear_for_update():
    """Clear all caches, prune entries older than 30 days."""
    with _lock:
        _mem_cache.clear()
        conn = _get_conn()
        if conn is None:
            return
        try:
            conn.execute("DELETE FROM kv_cache")
        except Exception:
            pass


def close():
    global _conn, _fallback
    with _lock:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:
                pass
            _conn = None
        _fallback = False
        _mem_cache.clear()


def _json_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
