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
    key = _cache_key("ic_summary_v2", client.account_id, client.environment_id)
    return db_get(f"api:{key}") is not None


def invalidate_incremental_candidates_cache(client):
    key = _cache_key("ic_summary_v2", client.account_id, client.environment_id)
    db_delete(f"api:{key}")


# ---------------------------------------------------------------------------
# Code complexity analysis
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
# Fetch detailed model metadata for table models
# ---------------------------------------------------------------------------

def _fetch_model_details(client: DbtClient):
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

        unique_key = config.get("unique_key")

        # PK from unique + not_null tests on same column
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
    summary_key = _cache_key("ic_summary_v2", client.account_id, client.environment_id)
    cached = db_get(f"api:{summary_key}")
    if cached is not None:
        print(f"[{client.name}] Serving incremental candidates from cache")
        return cached

    t0 = time.time()

    from project_health import (
        _fetch_all_models, _fetch_model_run_stats,
        _infer_layer, _parse_path, _percentile,
    )
    from data_quality import (
        _fetch_high_impact_signals, _fetch_dependency_counts,
        _fetch_model_usage_query_counts, apply_hi_signals_from_config,
    )

    models = _fetch_all_models(client)
    model_map = {m["uniqueId"]: m for m in models}

    details = _fetch_model_details(client)
    table_uids = list(details.keys())
    run_stats = _fetch_model_run_stats(client, table_uids)
    downstream_counts, upstream_counts, children_of = _fetch_dependency_counts(client)
    query_counts = _fetch_model_usage_query_counts(client)

    # High-impact signals (shared logic)
    creds = load_credentials() or {}
    hi_data = _fetch_high_impact_signals(client)
    raw_signals = hi_data.get("signals", {}) if isinstance(hi_data, dict) and "signals" in hi_data else {}
    hi_signals = {uid: set(reasons) for uid, reasons in raw_signals.items()}
    apply_hi_signals_from_config(hi_signals, hi_data, creds)

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

    # Compute downstream high-impact dependency counts per model
    def _count_downstream_hi(uid):
        """Count how many downstream models are high-impact."""
        visited = set()
        queue = list(children_of.get(uid, []))
        count = 0
        while queue:
            child = queue.pop(0)
            if child in visited or child == uid:
                continue
            visited.add(child)
            if child in hi_signals:
                count += 1
            queue.extend(children_of.get(child, []))
        return count

    # First pass: compute medians to determine top 20% threshold
    print(f"[{client.name}] Building incremental candidates...")
    medians = {}
    for uid in table_uids:
        stats = run_stats.get(uid, {})
        times = sorted(stats.get("times", []))
        if times:
            medians[uid] = _percentile(times, 50)

    sorted_medians = sorted(medians.values(), reverse=True)
    if sorted_medians:
        top20_idx = max(0, int(len(sorted_medians) * 0.2) - 1)
        median_threshold = sorted_medians[top20_idx]
    else:
        median_threshold = float('inf')

    candidates = []
    for uid, detail in details.items():
        m = model_map.get(uid)
        if not m:
            continue

        folder, subfolder, subsubfolder = _parse_path(m.get("filePath"))
        layer = _infer_layer(m)

        stats = run_stats.get(uid, {})
        times = sorted(stats.get("times", []))
        perf = {}
        if times:
            perf = {
                "p10": round(_percentile(times, 10), 1),
                "median": round(_percentile(times, 50), 1),
                "p90": round(_percentile(times, 90), 1),
                "variation": round(_percentile(times, 90) - _percentile(times, 10), 1),
            }

        is_high_impact = uid in hi_signals
        hi_reasons = sorted(hi_signals.get(uid, set()))
        downstream_hi_count = _count_downstream_hi(uid)

        # Readiness: 5 binary checks
        check_no_groupby_window = (detail.get("group_bys", 0) == 0 and detail.get("window_fns", 0) == 0)
        check_top20_median = (perf.get("median", 0) >= median_threshold) if median_threshold < float('inf') else False
        check_pk_or_date = (detail["has_potential_pk"] or detail["has_date_column"])
        check_hi_or_downstream = (is_high_impact or downstream_hi_count >= 5)
        check_no_unions_macros = (detail.get("unions", 0) == 0 and not detail.get("has_custom_macros", False))

        readiness = sum([
            check_no_groupby_window,
            check_top20_median,
            check_pk_or_date,
            check_hi_or_downstream,
            check_no_unions_macros,
        ])

        readiness_details = {
            "no_groupby_window": check_no_groupby_window,
            "top20_median": check_top20_median,
            "pk_or_date": check_pk_or_date,
            "hi_or_downstream": check_hi_or_downstream,
            "no_unions_macros": check_no_unions_macros,
        }

        candidates.append({
            "unique_id": uid,
            "name": detail["name"],
            "folder": folder,
            "subfolder": subfolder,
            "subsubfolder": subsubfolder,
            "layer": layer,
            "file_path": detail["file_path"],
            "run_count": len(times),
            "performance": perf,
            "downstream_count": downstream_counts.get(uid, 0),
            "upstream_count": upstream_counts.get(uid, 0),
            "downstream_hi_count": downstream_hi_count,
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
            "earliest_rows": stats.get("earliest_rows"),
            "latest_rows": stats.get("latest_rows"),
            "row_delta": stats.get("row_delta"),
            "avg_new_rows": stats.get("avg_new_rows"),
            "readiness": readiness,
            "readiness_details": readiness_details,
            "is_high_impact": is_high_impact,
            "hi_reasons": hi_reasons,
        })

    candidates.sort(key=lambda c: (
        -c["readiness"],
        -(c["performance"].get("median", 0)),
    ))

    hi_count = sum(1 for c in candidates if c["is_high_impact"])
    ready_count = sum(1 for c in candidates if c["readiness"] >= 4)
    elapsed = time.time() - t0
    print(f"[{client.name}] Incremental candidates built in {elapsed:.1f}s — "
          f"{len(candidates)} table models, {hi_count} high-impact, {ready_count} ready (4+/5)")

    result = {
        "candidates": candidates,
        "total_models": len(models),
        "table_count": len(candidates),
        "high_impact_count": hi_count,
        "ready_count": ready_count,
        "median_threshold": round(median_threshold, 1) if median_threshold < float('inf') else 0,
    }
    db_set(f"api:{summary_key}", result)
    return result
