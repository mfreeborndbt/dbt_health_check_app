"""Incremental Candidates — identify table models that may benefit from incremental materialization."""

import re
import hashlib
import time
import threading
from collections import defaultdict
from discovery_client import DbtClient, load_credentials
from cache_db import cache_get as db_get, cache_set as db_set, cache_delete as db_delete

_API_TTL = 6 * 3600
_CACHE_LOCK = threading.Lock()
_DETAIL_CACHE = {}

_DBT_BUILTINS = frozenset({
    "ref", "source", "config", "this", "var", "env_var", "return", "log",
    "adapter", "exceptions", "set", "is_incremental", "target", "schema",
    "database", "project_name", "run_started_at", "invocation_id",
    "graph", "model", "modules", "flags", "execute", "print",
    "fromjson", "tojson", "fromyaml", "toyaml", "zip", "set_strict",
    "builtins", "dbt_version", "selected_resources",
})


def _cache_key(prefix, *args):
    raw = f"{prefix}:{'|'.join(str(a) for a in args)}"
    return hashlib.md5(raw.encode()).hexdigest()


def is_incremental_candidates_cached(client):
    key = _cache_key("ic_summary_v1", client.account_id, client.environment_id)
    return db_get(f"api:{key}", ttl=_API_TTL) is not None


def invalidate_incremental_candidates_cache(client):
    key = _cache_key("ic_summary_v1", client.account_id, client.environment_id)
    db_delete(f"api:{key}")


# ---------------------------------------------------------------------------
# Code complexity analysis (from SAO model_details.py)
# ---------------------------------------------------------------------------

def _compute_complexity(raw_code):
    if not raw_code:
        return {}
    lower = raw_code.lower()
    lines = raw_code.strip().split("\n")
    non_blank = [l for l in lines if l.strip()]

    all_calls = re.findall(r'\{[{%][^}%]*?(\w+)\s*\(', raw_code)
    custom_macros = sorted(set(
        name for name in all_calls if name.lower() not in _DBT_BUILTINS
    ))

    return {
        "lines": len(lines),
        "lines_non_blank": len(non_blank),
        "joins": len(re.findall(r"\bjoin\b", lower)) - lower.count("cross join"),
        "ctes": lower.count(" as (") + lower.count(" as("),
        "subqueries": max(0, lower.count("select") - 1),
        "case_stmts": len(re.findall(r"\bcase\b", lower)),
        "window_fns": len(re.findall(r"\bover\s*\(", lower)),
        "group_bys": len(re.findall(r"\bgroup by\b", lower)),
        "unions": len(re.findall(r"\bunion\b", lower)),
        "custom_macros": custom_macros,
        "has_custom_macros": len(custom_macros) > 0,
    }


# ---------------------------------------------------------------------------
# Fetch detailed model metadata (code, catalog, config) for table models
# ---------------------------------------------------------------------------

def _fetch_model_details(client: DbtClient):
    """Fetch full model metadata including rawCode, catalog columns, and config."""
    cache_key = str(client.environment_id)
    with _CACHE_LOCK:
        entry = _DETAIL_CACHE.get(cache_key)
        if entry and (time.time() - entry[0]) < _API_TTL:
            return entry[1]
    db_key = f"ic_details:{cache_key}"
    db_data = db_get(db_key, ttl=_API_TTL)
    if db_data is not None:
        with _CACHE_LOCK:
            _DETAIL_CACHE[cache_key] = (time.time(), db_data)
        return db_data

    print(f"[{client.name}] Fetching model details for incremental analysis...")
    query = """
    query ($environmentId: BigInt!, $first: Int!, $after: String) {
      environment(id: $environmentId) {
        applied {
          models(first: $first, after: $after) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                uniqueId
                name
                materializedType
                rawCode
                config
                filePath
                tags
                access
                contractEnforced
                catalog {
                  columns { name type }
                }
                children { uniqueId resourceType }
                tests { uniqueId name columnName }
              }
            }
          }
        }
      }
    }
    """
    all_models = []
    cursor = None
    while True:
        variables = {"environmentId": client.environment_id, "first": 500}
        if cursor:
            variables["after"] = cursor
        data = client.query_discovery(query, variables=variables)
        models_data = data["environment"]["applied"]["models"]
        for edge in models_data["edges"]:
            all_models.append(edge["node"])
        if not models_data["pageInfo"]["hasNextPage"]:
            break
        cursor = models_data["pageInfo"]["endCursor"]

    # Process only table models (incremental candidates)
    details = {}
    for node in all_models:
        mat = node.get("materializedType") or "unknown"
        if mat != "table":
            continue

        uid = node["uniqueId"]
        config = node.get("config") or {}
        raw_code = node.get("rawCode") or ""
        complexity = _compute_complexity(raw_code)

        # Date/timeseries column detection
        catalog = node.get("catalog") or {}
        col_data = catalog.get("columns") or []
        column_names = [c.get("name", "").lower() for c in col_data]
        date_indicators = ("date", "timestamp", "_at", "created", "updated", "modified")
        has_date_column = any(
            any(ind in cn for ind in date_indicators) for cn in column_names
        )
        timeseries_type_patterns = ("timestamp", "datetime")
        timeseries_name_patterns = ("timestamp", "_at", "created", "updated", "modified")
        date_type = None
        if has_date_column:
            is_timeseries = False
            for c in col_data:
                cn = (c.get("name") or "").lower()
                ct = (c.get("type") or "").lower()
                if not any(ind in cn for ind in date_indicators):
                    continue
                if any(tp in ct for tp in timeseries_type_patterns):
                    is_timeseries = True
                    break
                if any(tp in cn for tp in timeseries_name_patterns):
                    is_timeseries = True
                    break
            date_type = "Timeseries" if is_timeseries else "Date"

        # Primary key detection from unique_key config
        unique_key = config.get("unique_key")

        # PK detection from unique + not_null tests on same column
        children = node.get("children") or []
        test_uids = [c["uniqueId"] for c in children if c.get("resourceType") == "test"]
        model_name = node["name"]
        unique_test_cols = {}
        not_null_test_cols = {}
        for t_uid in test_uids:
            parts = t_uid.split(".")
            if len(parts) >= 3:
                test_part = parts[2]
                if test_part.startswith(f"unique_{model_name}_"):
                    col = test_part[len(f"unique_{model_name}_"):]
                    unique_test_cols[col] = t_uid
                elif test_part.startswith(f"not_null_{model_name}_"):
                    col = test_part[len(f"not_null_{model_name}_"):]
                    not_null_test_cols[col] = t_uid
        pk_columns_from_tests = sorted(set(unique_test_cols.keys()) & set(not_null_test_cols.keys()))

        # PK value columns: names containing id/key/sk/pk
        pk_value_cols = [cn for cn in column_names if re.search(r'(^|_)(id|key|sk|pk)(_|$)', cn)]

        has_potential_pk = bool(unique_key) or bool(pk_columns_from_tests) or bool(pk_value_cols)

        details[uid] = {
            "unique_id": uid,
            "name": model_name,
            "file_path": node.get("filePath") or "",
            "raw_code": raw_code,
            "has_date_column": has_date_column,
            "date_type": date_type,
            "has_window_function": bool(re.search(r'\bover\s*\(', raw_code.lower())),
            "has_potential_pk": has_potential_pk,
            "unique_key": unique_key,
            "pk_columns_from_tests": pk_columns_from_tests,
            "pk_value_cols": pk_value_cols,
            "column_count": len(column_names),
            **complexity,
        }

    with _CACHE_LOCK:
        _DETAIL_CACHE[cache_key] = (time.time(), details)
    db_set(db_key, details)
    print(f"[{client.name}] Fetched details for {len(details)} table models")
    return details


# ---------------------------------------------------------------------------
# Main: build incremental candidates summary
# ---------------------------------------------------------------------------

def fetch_incremental_candidates(client: DbtClient):
    """Identify table models that are good candidates for incremental materialization.

    A model is a stronger candidate when it has:
    - A date/timestamp column (for incremental strategy)
    - A primary key (unique_key for merge)
    - High execution time
    - High downstream impact
    - High-impact classification

    Returns summary dict with candidates list and metadata.
    """
    summary_key = _cache_key("ic_summary_v1", client.account_id, client.environment_id)
    cached = db_get(f"api:{summary_key}", ttl=_API_TTL)
    if cached is not None:
        print(f"[{client.name}] Serving incremental candidates from cache")
        return cached

    t0 = time.time()

    # Reuse existing fetchers
    from project_health import (
        _fetch_all_models, _fetch_model_run_times,
        _infer_layer, _parse_path, _percentile,
    )
    from data_quality import (
        _fetch_high_impact_signals, _fetch_dependency_counts,
        _fetch_model_usage_query_counts,
    )

    models = _fetch_all_models(client)
    model_map = {m["uniqueId"]: m for m in models}

    # Get detailed analysis for table models
    details = _fetch_model_details(client)

    # Run times
    table_uids = list(details.keys())
    run_times = _fetch_model_run_times(client, table_uids)

    # Dependencies
    downstream_counts, upstream_counts, _ = _fetch_dependency_counts(client)

    # Query counts
    query_counts = _fetch_model_usage_query_counts(client)

    # High-impact signals
    creds = load_credentials() or {}
    hi_data = _fetch_high_impact_signals(client)
    raw_signals = hi_data.get("signals", {}) if isinstance(hi_data, dict) and "signals" in hi_data else {}
    hi_signals = {uid: set(reasons) for uid, reasons in raw_signals.items()}

    # Apply user config for high-impact filtering
    selected_tags = creds.get("high_impact_tags", [])
    model_tags_map = hi_data.get("model_tags", {}) if isinstance(hi_data, dict) else {}
    public_uids = set(hi_data.get("public_model_uids", []) if isinstance(hi_data, dict) else [])
    contract_uids = set(hi_data.get("contract_model_uids", []) if isinstance(hi_data, dict) else [])

    # Add tag-based signals
    if selected_tags:
        for uid, tags in model_tags_map.items():
            if any(t in selected_tags for t in tags):
                hi_signals.setdefault(uid, set()).add("Tagged")

    # Add public model signals based on user config
    public_mode = creds.get("public_model_mode", "public_with_contract")
    if public_mode in ("public_only", "public_with_contract"):
        for uid in public_uids:
            hi_signals.setdefault(uid, set()).add("Public Model")
    if public_mode == "public_with_contract":
        for uid in contract_uids:
            hi_signals.setdefault(uid, set()).add("Contract Enforced")

    # Strip disabled signals
    if not creds.get("high_impact_include_semantic_models", True):
        for uid in list(hi_signals.keys()):
            hi_signals[uid].discard("Semantic Model")
            if not hi_signals[uid]:
                del hi_signals[uid]
    if not creds.get("high_impact_include_exposure_dependents", True):
        for uid in list(hi_signals.keys()):
            hi_signals[uid].discard("Dependent Exposure")
            if not hi_signals[uid]:
                del hi_signals[uid]

    # Usage-based high impact (top N% by query count)
    heavy_pct = creds.get("heavy_usage_pct", 20)
    all_qc = sorted(query_counts.values(), reverse=True)
    if all_qc and heavy_pct > 0:
        threshold_idx = max(0, int(len(all_qc) * heavy_pct / 100) - 1)
        threshold = all_qc[threshold_idx] if threshold_idx < len(all_qc) else 0
        if threshold > 0:
            for uid, qc in query_counts.items():
                if qc >= threshold:
                    hi_signals.setdefault(uid, set()).add("Heavy Usage")

    print(f"[{client.name}] Building incremental candidates...")
    candidates = []
    for uid, detail in details.items():
        m = model_map.get(uid)
        if not m:
            continue

        folder, subfolder, subsubfolder = _parse_path(m.get("filePath"))
        layer = _infer_layer(m)

        times = sorted(run_times.get(uid, []))
        perf = {}
        if times:
            perf = {
                "min": round(min(times), 1),
                "p50": round(_percentile(times, 50), 1),
                "p95": round(_percentile(times, 95), 1),
                "max": round(max(times), 1),
                "avg": round(sum(times) / len(times), 1),
            }

        # Readiness score (0-5): how ready is this model for incremental?
        readiness = 0
        if detail["has_date_column"]:
            readiness += 1
            if detail["date_type"] == "Timeseries":
                readiness += 1  # timeseries is better than plain date
        if detail["pk_columns_from_tests"]:
            readiness += 2  # tested PK is best
        elif detail.get("unique_key"):
            readiness += 2  # config unique_key
        elif detail["pk_value_cols"]:
            readiness += 1  # potential PK column names

        # Complexity warnings
        warnings = []
        if detail.get("window_fns", 0) > 0:
            warnings.append("Window functions")
        if detail.get("group_bys", 0) > 0:
            warnings.append("GROUP BY")
        if detail.get("unions", 0) > 0:
            warnings.append("UNIONs")
        if detail.get("has_custom_macros"):
            warnings.append("Custom macros")

        is_high_impact = uid in hi_signals
        hi_reasons = sorted(hi_signals.get(uid, set()))

        candidates.append({
            "unique_id": uid,
            "name": detail["name"],
            "folder": folder,
            "subfolder": subfolder,
            "layer": layer,
            "file_path": detail["file_path"],
            "run_count": len(times),
            "performance": perf,
            "downstream_count": downstream_counts.get(uid, 0),
            "upstream_count": upstream_counts.get(uid, 0),
            "query_count": query_counts.get(uid, 0),
            "has_date_column": detail["has_date_column"],
            "date_type": detail["date_type"],
            "has_potential_pk": detail["has_potential_pk"],
            "unique_key": detail.get("unique_key"),
            "pk_columns_from_tests": detail["pk_columns_from_tests"],
            "pk_value_cols": detail["pk_value_cols"],
            "has_window_function": detail["has_window_function"],
            "window_fns": detail.get("window_fns", 0),
            "group_bys": detail.get("group_bys", 0),
            "unions": detail.get("unions", 0),
            "has_custom_macros": detail.get("has_custom_macros", False),
            "custom_macros": detail.get("custom_macros", []),
            "lines": detail.get("lines", 0),
            "column_count": detail.get("column_count", 0),
            "readiness": readiness,
            "warnings": warnings,
            "is_high_impact": is_high_impact,
            "hi_reasons": hi_reasons,
        })

    # Sort by: high-impact first, then by avg execution time descending
    candidates.sort(key=lambda c: (
        0 if c["is_high_impact"] else 1,
        -(c["performance"].get("avg", 0)),
    ))

    hi_count = sum(1 for c in candidates if c["is_high_impact"])
    ready_count = sum(1 for c in candidates if c["readiness"] >= 3)
    elapsed = time.time() - t0
    print(f"[{client.name}] Incremental candidates built in {elapsed:.1f}s — "
          f"{len(candidates)} table models, {hi_count} high-impact, {ready_count} ready")

    result = {
        "candidates": candidates,
        "total_models": len(models),
        "table_count": len(candidates),
        "high_impact_count": hi_count,
        "ready_count": ready_count,
    }
    db_set(f"api:{summary_key}", result)
    return result
