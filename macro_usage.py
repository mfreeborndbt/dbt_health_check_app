"""Macro Usage — extract macros from models, show usage counts and high-impact dependencies."""

import re
import hashlib
import time
from collections import defaultdict
from discovery_client import DbtClient, load_credentials
from cache_db import cache_get as db_get, cache_set as db_set, cache_delete as db_delete

_API_TTL = 6 * 3600

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


def is_macro_usage_cached(client):
    key = _cache_key("macro_usage_v1", client.account_id, client.environment_id)
    return db_get(f"api:{key}", ttl=_API_TTL) is not None


def _extract_macros_from_code(raw_code):
    """Extract custom macro names from model raw code."""
    if not raw_code:
        return []
    all_calls = re.findall(r'\{[{%][^}%]*?(\w+)\s*\(', raw_code)
    return sorted(set(
        name for name in all_calls if name.lower() not in _DBT_BUILTINS
    ))


def _fetch_macro_definitions(client: DbtClient, macro_names):
    """Fetch macro metadata from the Discovery API."""
    key = _cache_key("macro_defs_v1", client.account_id, client.environment_id)
    cached = db_get(f"api:{key}", ttl=_API_TTL)
    if cached is not None:
        return cached

    print(f"[{client.name}] Fetching macro definitions...")
    query = """
    query ($environmentId: BigInt!, $first: Int!, $after: String) {
      environment(id: $environmentId) {
        applied {
          macros(first: $first, after: $after) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                uniqueId
                name
                description
                macroSql
                dependsOn
                packageName
              }
            }
          }
        }
      }
    }
    """
    all_macros = []
    cursor = None
    while True:
        variables = {"environmentId": client.environment_id, "first": 500}
        if cursor:
            variables["after"] = cursor
        try:
            data = client.query_discovery(query, variables=variables)
        except Exception as e:
            print(f"[{client.name}] Could not fetch macros: {e}")
            break
        macros_data = data["environment"]["applied"]["macros"]
        for edge in macros_data["edges"]:
            all_macros.append(edge["node"])
        if not macros_data["pageInfo"]["hasNextPage"]:
            break
        cursor = macros_data["pageInfo"]["endCursor"]

    print(f"[{client.name}] Fetched {len(all_macros)} macro definitions")
    db_set(f"api:{key}", all_macros)
    return all_macros


def fetch_macro_usage(client: DbtClient):
    """Build macro usage summary from model code + macro definitions.

    Returns summary dict with macros list sorted by model usage count.
    """
    summary_key = _cache_key("macro_usage_v1", client.account_id, client.environment_id)
    cached = db_get(f"api:{summary_key}", ttl=_API_TTL)
    if cached is not None:
        print(f"[{client.name}] Serving macro usage from cache")
        return cached

    t0 = time.time()

    from project_health import _fetch_all_models, _infer_layer, _parse_path
    from data_quality import (
        _fetch_high_impact_signals, _fetch_dependency_counts,
        apply_hi_signals_from_config,
    )

    models = _fetch_all_models(client)
    model_map = {m["uniqueId"]: m for m in models}

    # High-impact signals
    creds = load_credentials() or {}
    hi_data = _fetch_high_impact_signals(client)
    raw_signals = hi_data.get("signals", {}) if isinstance(hi_data, dict) and "signals" in hi_data else {}
    hi_signals = {uid: set(reasons) for uid, reasons in raw_signals.items()}
    apply_hi_signals_from_config(hi_signals, hi_data, creds)

    # Downstream counts for high-impact downstream check
    downstream_counts, _, children_of = _fetch_dependency_counts(client)

    def _count_downstream_hi(uid):
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

    # Extract macro usage from all model code
    print(f"[{client.name}] Extracting macro usage from {len(models)} models...")
    macro_to_models = defaultdict(list)  # macro_name -> [model info]
    all_macro_names = set()

    for m in models:
        raw_code = m.get("rawCode") or ""
        macros = _extract_macros_from_code(raw_code)
        if not macros:
            continue

        uid = m["uniqueId"]
        is_hi = uid in hi_signals
        hi_reasons = sorted(hi_signals.get(uid, set()))
        ds_hi = _count_downstream_hi(uid)
        folder, subfolder, _ = _parse_path(m.get("filePath"))
        layer = _infer_layer(m)

        model_info = {
            "unique_id": uid,
            "name": m["name"],
            "layer": layer,
            "folder": folder,
            "materialization": m.get("materializedType") or "",
            "is_high_impact": is_hi,
            "hi_reasons": hi_reasons,
            "downstream_hi_count": ds_hi,
        }

        for macro_name in macros:
            all_macro_names.add(macro_name)
            macro_to_models[macro_name].append(model_info)

    # Fetch macro definitions from API
    macro_defs = _fetch_macro_definitions(client, all_macro_names)
    defs_by_name = {}
    for md in macro_defs:
        name = md.get("name", "")
        if name in all_macro_names:
            defs_by_name[name] = md

    # Build results
    results = []
    for macro_name in sorted(all_macro_names):
        model_list = macro_to_models[macro_name]
        definition = defs_by_name.get(macro_name, {})

        hi_model_count = sum(1 for m in model_list if m["is_high_impact"])
        hi_downstream_model_count = sum(1 for m in model_list if m["downstream_hi_count"] > 0)

        # Sort models: high-impact first, then by name
        model_list.sort(key=lambda m: (0 if m["is_high_impact"] else 1, m["name"]))

        results.append({
            "name": macro_name,
            "package": definition.get("packageName", ""),
            "unique_id": definition.get("uniqueId", ""),
            "description": definition.get("description", ""),
            "macro_sql": definition.get("macroSql", ""),
            "model_count": len(model_list),
            "hi_model_count": hi_model_count,
            "hi_downstream_model_count": hi_downstream_model_count,
            "models": model_list,
        })

    # Sort by model count descending
    results.sort(key=lambda r: (-r["model_count"], r["name"]))

    elapsed = time.time() - t0
    print(f"[{client.name}] Macro usage built in {elapsed:.1f}s — "
          f"{len(results)} macros across {len(models)} models")

    result = {
        "macros": results,
        "total_macros": len(results),
        "total_models": len(models),
    }
    db_set(f"api:{summary_key}", result)
    return result
