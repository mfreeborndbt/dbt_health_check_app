"""Dead Models — identify potentially unused models via upstream BFS pruning."""

import hashlib
import time
from collections import defaultdict
from discovery_client import DbtClient, load_credentials
from cache_db import cache_get as db_get, cache_set as db_set, cache_delete as db_delete

_API_TTL = 6 * 3600
_QUERY_THRESHOLD = 5  # models with <= this many queries are "low usage"


def _cache_key(prefix, *args):
    raw = f"{prefix}:{'|'.join(str(a) for a in args)}"
    return hashlib.md5(raw.encode()).hexdigest()


def is_dead_models_cached(client):
    key = _cache_key("dm_summary_v1", client.account_id, client.environment_id)
    return db_get(f"api:{key}") is not None


def invalidate_dead_models_cache(client):
    key = _cache_key("dm_summary_v1", client.account_id, client.environment_id)
    db_delete(f"api:{key}")


def fetch_dead_models(client: DbtClient):
    """Identify candidate dead models via upstream BFS pruning.

    A model is a dead-model candidate if ALL of:
      1. No downstream model dependencies (leaf node) — or all downstream are also dead
      2. <= 5 warehouse queries in the past month
      3. No exposure or semantic model references it

    Starting from leaf nodes, we walk upstream: a parent becomes a candidate
    only when ALL of its children are already candidates.

    Returns project-health-style metadata for each candidate (modeling,
    testing, documentation rules) minus high-impact info.
    """
    summary_key = _cache_key("dm_summary_v1", client.account_id, client.environment_id)
    cached = db_get(f"api:{summary_key}")
    if cached is not None:
        print(f"[{client.name}] Serving dead models from cache")
        return cached

    t0 = time.time()

    # --- Reuse project_health fetchers (they cache internally) ---
    from project_health import (
        _fetch_all_models,
        _fetch_all_sources,
        _fetch_all_exposures,
        _fetch_model_run_stats,
        _infer_layer,
        _parse_path,
        _percentile,
    )
    from data_quality import (
        _fetch_high_impact_signals,
        _fetch_model_usage_query_counts,
        _fetch_dependency_counts,
    )

    models = _fetch_all_models(client)
    sources = _fetch_all_sources(client)
    exposures = _fetch_all_exposures(client)

    model_map = {m["uniqueId"]: m for m in models}
    source_map = {s["uniqueId"]: s for s in sources}
    source_children = defaultdict(list)
    for s in sources:
        for c in (s.get("children") or []):
            if c.get("resourceType") == "model":
                source_children[s["uniqueId"]].append(c["uniqueId"])

    # --- Build adjacency ---
    children_of = defaultdict(set)   # uid -> set of child model uids
    parents_of = defaultdict(set)    # uid -> set of parent model uids
    for m in models:
        uid = m["uniqueId"]
        for c in (m.get("children") or []):
            if c.get("resourceType") == "model":
                children_of[uid].add(c["uniqueId"])
        for p in (m.get("parents") or []):
            if p.get("resourceType") == "model":
                parents_of[uid].add(p["uniqueId"])

    # --- Models referenced by exposures (transitive ancestors count) ---
    exposure_ancestors = set()
    for exp in exposures:
        for p in (exp.get("parents") or []):
            if p.get("resourceType") == "model":
                exposure_ancestors.add(p["uniqueId"])

    # --- Models referenced by semantic models ---
    hi_data = _fetch_high_impact_signals(client)
    semantic_parents = set()
    raw_signals = hi_data.get("signals", {}) if isinstance(hi_data, dict) and "signals" in hi_data else {}
    for uid, reasons in raw_signals.items():
        if "Semantic Model Parent" in reasons:
            semantic_parents.add(uid)

    # --- Query counts ---
    query_counts = _fetch_model_usage_query_counts(client)

    # --- BFS pruning from leaves upward ---
    print(f"[{client.name}] Identifying dead model candidates...")
    all_uids = set(model_map.keys())
    protected = exposure_ancestors | semantic_parents  # cannot be dead

    candidates = set()

    # Seed: leaf models (no downstream model children) that meet criteria
    leaves = {uid for uid in all_uids if not children_of.get(uid)}
    queue = list(leaves)
    visited = set()

    while queue:
        uid = queue.pop(0)
        if uid in visited:
            continue
        visited.add(uid)

        if uid in protected:
            continue

        qc = query_counts.get(uid, 0)
        if qc > _QUERY_THRESHOLD:
            continue

        # Check all children are already candidates (or model has no children)
        model_children = children_of.get(uid, set())
        if model_children and not model_children.issubset(candidates):
            continue

        candidates.add(uid)

        # Walk upstream: enqueue parents for evaluation
        for parent_uid in parents_of.get(uid, set()):
            if parent_uid not in visited:
                queue.append(parent_uid)

    # --- Fetch run stats only for candidates (not all 1664 models) ---
    run_stats = _fetch_model_run_stats(client, list(candidates))

    # --- Build results ---
    print(f"[{client.name}] Building metadata for {len(candidates)} candidates...")
    results = []
    downstream_counts, upstream_counts, _ = _fetch_dependency_counts(client)

    for uid in candidates:
        m = model_map[uid]
        folder, subfolder, subsubfolder = _parse_path(m.get("filePath"))
        layer = _infer_layer(m)

        stats = run_stats.get(uid, {})
        times = sorted(stats.get("times", []))
        perf = {}
        if times:
            perf = {
                "median": round(_percentile(times, 50), 1),
            }

        total_children = len(children_of.get(uid, set()))

        results.append({
            "unique_id": uid,
            "name": m["name"],
            "folder": folder,
            "subfolder": subfolder,
            "layer": layer,
            "materialization": m.get("materializedType") or "",
            "query_count": query_counts.get(uid, 0),
            "run_count": len(times),
            "performance": perf,
            "downstream_count": downstream_counts.get(uid, 0),
            "upstream_count": upstream_counts.get(uid, 0),
            "is_leaf": total_children == 0,
            "earliest_rows": stats.get("earliest_rows"),
            "latest_rows": stats.get("latest_rows"),
            "row_delta": stats.get("row_delta"),
            "avg_new_rows": stats.get("avg_new_rows"),
        })

    results.sort(key=lambda r: (0 if r["is_leaf"] else 1, r["query_count"], r["name"]))

    leaf_count = sum(1 for r in results if r["is_leaf"])
    upstream_count = len(results) - leaf_count
    elapsed = time.time() - t0
    print(f"[{client.name}] Dead models built in {elapsed:.1f}s — {leaf_count} leaf, {upstream_count} upstream, {len(results)} total candidates")

    result = {
        "candidates": results,
        "total_models": len(models),
        "candidate_count": len(results),
        "leaf_count": leaf_count,
        "upstream_count": upstream_count,
        "query_threshold": _QUERY_THRESHOLD,
    }
    db_set(f"api:{summary_key}", result)
    return result
