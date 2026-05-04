"""Project Health evaluation — simplified dbt project evaluator via Discovery API."""

import hashlib
import time
import statistics
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from discovery_client import DbtClient, load_credentials
from cache_db import cache_get as db_get, cache_set as db_set

_API_TTL = 6 * 3600


def is_project_health_cached(client):
    """Check if project health data is cached."""
    key = _cache_key("ph_summary_v5", client.account_id, client.environment_id)
    return db_get(f"api:{key}", ttl=_API_TTL) is not None


def _cache_key(prefix, *args):
    raw = f"{prefix}:{'|'.join(str(a) for a in args)}"
    return hashlib.md5(raw.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def _fetch_all_models(client: DbtClient):
    """Fetch all model metadata needed for project health checks."""
    key = _cache_key("ph_models_v2", client.account_id, client.environment_id)
    cached = db_get(f"api:{key}", ttl=_API_TTL)
    if cached is not None:
        return cached

    print(f"[{client.name}] Fetching all models for project health...")
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
                description
                filePath
                materializedType
                access
                contractEnforced
                modelingLayer
                tags
                rawCode
                parents { uniqueId resourceType }
                children { uniqueId resourceType }
                tests { uniqueId name columnName }
              }
            }
          }
        }
      }
    }
    """
    models = []
    cursor = None
    while True:
        variables = {"environmentId": client.environment_id, "first": 500}
        if cursor:
            variables["after"] = cursor
        data = client.query_discovery(query, variables=variables)
        models_data = data["environment"]["applied"]["models"]
        for edge in models_data["edges"]:
            models.append(edge["node"])
        if not models_data["pageInfo"]["hasNextPage"]:
            break
        cursor = models_data["pageInfo"]["endCursor"]

    db_set(f"api:{key}", models)
    print(f"[{client.name}] Fetched {len(models)} models")
    return models


def _fetch_all_sources(client: DbtClient):
    """Fetch all source metadata."""
    key = _cache_key("ph_sources_v2", client.account_id, client.environment_id)
    cached = db_get(f"api:{key}", ttl=_API_TTL)
    if cached is not None:
        return cached

    print(f"[{client.name}] Fetching all sources for project health...")
    query = """
    query ($environmentId: BigInt!, $first: Int!, $after: String) {
      environment(id: $environmentId) {
        applied {
          sources(first: $first, after: $after) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                uniqueId
                name
                sourceName
                description
                sourceDescription
                children { uniqueId resourceType }
                freshness { freshnessStatus }
              }
            }
          }
        }
      }
    }
    """
    sources = []
    cursor = None
    while True:
        variables = {"environmentId": client.environment_id, "first": 500}
        if cursor:
            variables["after"] = cursor
        data = client.query_discovery(query, variables=variables)
        sources_data = data["environment"]["applied"]["sources"]
        for edge in sources_data["edges"]:
            sources.append(edge["node"])
        if not sources_data["pageInfo"]["hasNextPage"]:
            break
        cursor = sources_data["pageInfo"]["endCursor"]

    db_set(f"api:{key}", sources)
    print(f"[{client.name}] Fetched {len(sources)} sources")
    return sources


def _fetch_all_exposures(client: DbtClient):
    """Fetch all exposure metadata."""
    key = _cache_key("ph_exposures_v1", client.account_id, client.environment_id)
    cached = db_get(f"api:{key}", ttl=_API_TTL)
    if cached is not None:
        return cached

    print(f"[{client.name}] Fetching exposures for project health...")
    query = """
    query ($environmentId: BigInt!, $first: Int!, $after: String) {
      environment(id: $environmentId) {
        applied {
          exposures(first: $first, after: $after) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                uniqueId
                name
                parents { uniqueId resourceType }
              }
            }
          }
        }
      }
    }
    """
    exposures = []
    cursor = None
    while True:
        variables = {"environmentId": client.environment_id, "first": 500}
        if cursor:
            variables["after"] = cursor
        data = client.query_discovery(query, variables=variables)
        exp_data = data["environment"]["applied"]["exposures"]
        for edge in exp_data["edges"]:
            exposures.append(edge["node"])
        if not exp_data["pageInfo"]["hasNextPage"]:
            break
        cursor = exp_data["pageInfo"]["endCursor"]

    db_set(f"api:{key}", exposures)
    print(f"[{client.name}] Fetched {len(exposures)} exposures")
    return exposures


def _fetch_model_run_times(client: DbtClient, model_uids):
    """Fetch execution times for models using batched modelHistoricalRuns aliases."""
    key = _cache_key("ph_runtimes_v1", client.account_id, client.environment_id)
    cached = db_get(f"api:{key}", ttl=_API_TTL)
    if cached is not None:
        return cached

    print(f"[{client.name}] Fetching model run times for {len(model_uids)} models...")
    run_times = {}
    batch_size = 25
    uids = list(model_uids)

    for i in range(0, len(uids), batch_size):
        batch = uids[i:i + batch_size]
        aliases = []
        for j, uid in enumerate(batch):
            safe_uid = uid.replace('"', '\\"')
            aliases.append(
                f'm{j}: modelHistoricalRuns(uniqueId: "{safe_uid}", lastRunCount: 30) {{ uniqueId executionTime status }}'
            )
        gql = (
            "query ($environmentId: BigInt!) {\n"
            "  environment(id: $environmentId) {\n"
            "    applied {\n"
            "      " + "\n      ".join(aliases) + "\n"
            "    }\n"
            "  }\n"
            "}"
        )
        try:
            data = client.query_discovery(gql, variables={"environmentId": client.environment_id})
            applied = data["environment"]["applied"]
            for j, uid in enumerate(batch):
                runs = applied.get(f"m{j}") or []
                times = [r["executionTime"] for r in runs
                         if r.get("status") == "success" and r.get("executionTime") and r["executionTime"] > 0]
                run_times[uid] = times
        except Exception as e:
            print(f"  Batch {i // batch_size} error: {e}")

        done = min(i + batch_size, len(uids))
        if done < len(uids):
            print(f"  Fetched run times {done}/{len(uids)} models...")

    db_set(f"api:{key}", run_times)
    print(f"[{client.name}] Fetched run times for {len(run_times)} models")
    return run_times


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _parse_path(file_path):
    """Extract folder, subfolder, sub-subfolder from filePath."""
    if not file_path:
        return "", "", ""
    parts = file_path.replace("\\", "/").split("/")
    # Remove filename
    dirs = [p for p in parts[:-1] if p and p != "models"]
    # Skip dbt_packages prefix
    if len(dirs) >= 1 and dirs[0] == "dbt_packages":
        dirs = dirs[1:]  # remove dbt_packages
        if dirs:
            dirs = dirs[1:]  # remove package name
    folder = dirs[0] if len(dirs) > 0 else ""
    subfolder = dirs[1] if len(dirs) > 1 else ""
    subsubfolder = dirs[2] if len(dirs) > 2 else ""
    return folder, subfolder, subsubfolder


def _infer_layer(model):
    """Infer modeling layer from modelingLayer, name prefix, or file path."""
    layer = model.get("modelingLayer") or ""
    if layer:
        return layer.lower()
    name = model.get("name", "")
    if name.startswith("stg_"):
        return "staging"
    if name.startswith("int_"):
        return "intermediate"
    if name.startswith("dim_") or name.startswith("fct_") or name.startswith("rpt_"):
        return "marts"
    # Try file path
    fp = (model.get("filePath") or "").lower()
    if "/staging/" in fp:
        return "staging"
    if "/intermediate/" in fp or "/int/" in fp:
        return "intermediate"
    if "/marts/" in fp or "/mart/" in fp:
        return "marts"
    return "other"


def _percentile(sorted_vals, pct):
    """Compute percentile from sorted list."""
    if not sorted_vals:
        return 0
    k = (len(sorted_vals) - 1) * pct / 100
    f = int(k)
    c = f + 1 if f < len(sorted_vals) - 1 else f
    d = k - f
    return sorted_vals[f] + d * (sorted_vals[c] - sorted_vals[f])


# ---------------------------------------------------------------------------
# Rule evaluations
# ---------------------------------------------------------------------------

def _evaluate_modeling_rules(model, model_map, source_map, source_children):
    """Evaluate modeling rules for a single model. Returns dict of rule -> pass/fail."""
    uid = model["uniqueId"]
    name = model["name"]
    layer = _infer_layer(model)
    parent_uids = [p["uniqueId"] for p in (model.get("parents") or []) if p.get("resourceType") in ("model", "source")]
    parent_models = [p["uniqueId"] for p in (model.get("parents") or []) if p.get("resourceType") == "model"]
    parent_sources = [p["uniqueId"] for p in (model.get("parents") or []) if p.get("resourceType") == "source"]
    child_models = [c["uniqueId"] for c in (model.get("children") or []) if c.get("resourceType") == "model"]

    results = {}

    # 1. Staging dependent on staging
    if layer == "staging":
        other_stg = [p for p in parent_models if _infer_layer(model_map.get(p, {})) == "staging"]
        results["staging_on_staging"] = len(other_stg) == 0

    # 2. Staging dependent on downstream
    if layer == "staging":
        downstream_deps = [p for p in parent_models
                          if _infer_layer(model_map.get(p, {})) in ("intermediate", "marts")]
        results["staging_on_downstream"] = len(downstream_deps) == 0

    # 3. Marts/intermediate dependent on source
    if layer in ("intermediate", "marts"):
        results["marts_on_source"] = len(parent_sources) == 0

    # 4. Direct join to source (model refs both models and sources)
    if parent_models and parent_sources:
        results["direct_join_to_source"] = False
    elif parent_sources and not parent_models and layer != "staging":
        results["direct_join_to_source"] = False
    else:
        results["direct_join_to_source"] = True

    # 5. Root model (no parents at all)
    results["root_model"] = len(parent_uids) > 0

    # 6. Model fanout (>3 direct child models)
    results["model_fanout"] = len(child_models) <= 3

    # 7. Too many joins (>7 parent models/sources)
    results["too_many_joins"] = len(parent_uids) <= 7

    # 8. Multiple sources joined
    if layer == "staging":
        results["multiple_sources"] = len(parent_sources) <= 1

    return results


def _evaluate_testing_rules(model):
    """Evaluate testing rules for a model."""
    tests = model.get("tests") or []
    test_names = [t.get("name", "").lower() for t in tests]

    results = {}

    # 1. Has any test at all
    results["has_test"] = len(tests) > 0

    # 2. Has primary key test (not_null + unique on same column, or unique_combination_of_columns)
    has_pk = False
    # Check for unique_combination_of_columns
    for tn in test_names:
        if "unique_combination_of_columns" in tn:
            has_pk = True
            break
    if not has_pk:
        # Check for not_null + unique on same column
        columns_with_unique = set()
        columns_with_not_null = set()
        for t in tests:
            tn = (t.get("name") or "").lower()
            col = t.get("columnName") or ""
            if not col:
                continue
            if "unique" in tn and "combination" not in tn:
                columns_with_unique.add(col)
            if "not_null" in tn:
                columns_with_not_null.add(col)
        if columns_with_unique & columns_with_not_null:
            has_pk = True
    results["primary_key_test"] = has_pk

    return results


def _evaluate_documentation_rules(model):
    """Evaluate documentation rules for a model."""
    results = {}
    desc = (model.get("description") or "").strip()
    results["has_description"] = len(desc) > 0
    return results


def _evaluate_governance_rules(model):
    """Evaluate governance rules for a model."""
    results = {}
    access = model.get("access") or ""
    contract = model.get("contractEnforced", False)

    # Public model without contract
    if access == "public":
        results["public_has_contract"] = contract
        results["public_has_description"] = len((model.get("description") or "").strip()) > 0
    return results


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def fetch_project_health(client: DbtClient):
    """Build the project health summary."""
    key = _cache_key("ph_summary_v5", client.account_id, client.environment_id)
    cached = db_get(f"api:{key}", ttl=_API_TTL)
    if cached is not None:
        print(f"[{client.name}] Serving project health from cache")
        return cached

    t0 = time.time()
    models = _fetch_all_models(client)
    sources = _fetch_all_sources(client)
    exposures = _fetch_all_exposures(client)

    # Build lookup maps
    model_map = {m["uniqueId"]: m for m in models}
    source_map = {s["uniqueId"]: s for s in sources}
    source_children = defaultdict(list)
    for s in sources:
        for c in (s.get("children") or []):
            if c.get("resourceType") == "model":
                source_children[s["uniqueId"]].append(c["uniqueId"])

    # High impact signals (reuse from data_quality)
    from data_quality import _fetch_high_impact_signals, _fetch_model_usage_query_counts
    hi_data = _fetch_high_impact_signals(client)
    raw_signals = hi_data.get("signals", {}) if isinstance(hi_data, dict) and "signals" in hi_data else {}
    hi_signals = {uid: set(reasons) for uid, reasons in raw_signals.items()}
    public_uids = set(hi_data.get("public_model_uids", []) if isinstance(hi_data, dict) else [])
    contract_uids = set(hi_data.get("contract_model_uids", []) if isinstance(hi_data, dict) else [])
    model_tags_map = hi_data.get("model_tags", {}) if isinstance(hi_data, dict) else {}

    creds = load_credentials() or {}
    public_mode = creds.get("public_model_mode", "public_with_contract")
    if public_mode == "public_only":
        for uid in public_uids:
            hi_signals.setdefault(uid, set()).add("Public Model")
    else:
        for uid in public_uids & contract_uids:
            hi_signals.setdefault(uid, set()).add("Public Model")

    hi_tag_set = set(creds.get("high_impact_tags") or [])
    if hi_tag_set:
        for uid, tags in model_tags_map.items():
            if hi_tag_set & set(tags):
                hi_signals.setdefault(uid, set()).add("High Impact Tag")

    model_query_stats = _fetch_model_usage_query_counts(client)
    heavy_pct = creds.get("heavy_usage_pct", 20)
    nonzero = sorted([c for c in model_query_stats.values() if c > 0], reverse=True)
    query_threshold = nonzero[max(1, len(nonzero) * heavy_pct // 100) - 1] if nonzero else float('inf')

    model_hi_reasons = defaultdict(set)
    for uid, reasons in hi_signals.items():
        model_hi_reasons[uid] |= reasons
    for uid, qc in model_query_stats.items():
        if qc > 0 and qc >= query_threshold:
            model_hi_reasons[uid].add("Heavy Usage")

    # Exposure parent lookup
    exposure_parents = set()
    for exp in exposures:
        for p in (exp.get("parents") or []):
            if p.get("resourceType") == "model":
                exposure_parents.add(p["uniqueId"])

    # Fetch run times (batched)
    print(f"[{client.name}] Fetching model run times...")
    run_times = _fetch_model_run_times(client, [m["uniqueId"] for m in models])

    # Build per-model results
    print(f"[{client.name}] Evaluating {len(models)} models...")
    model_results = []
    for m in models:
        uid = m["uniqueId"]
        name = m["name"]
        folder, subfolder, subsubfolder = _parse_path(m.get("filePath"))
        layer = _infer_layer(m)

        # Performance metrics
        times = sorted(run_times.get(uid, []))
        perf = {}
        if times:
            perf = {
                "min": round(min(times), 1),
                "p20": round(_percentile(times, 20), 1),
                "median": round(_percentile(times, 50), 1),
                "p80": round(_percentile(times, 80), 1),
                "max": round(max(times), 1),
            }

        # High impact
        reasons = sorted(model_hi_reasons.get(uid, set()))
        is_hi = len(reasons) > 0

        # Rule evaluations
        modeling = _evaluate_modeling_rules(m, model_map, source_map, source_children)
        testing = _evaluate_testing_rules(m)
        documentation = _evaluate_documentation_rules(m)
        governance = _evaluate_governance_rules(m)

        model_results.append({
            "unique_id": uid,
            "name": name,
            "folder": folder,
            "subfolder": subfolder,
            "subsubfolder": subsubfolder,
            "layer": layer,
            "materialization": m.get("materializedType") or "",
            "performance": perf,
            "is_high_impact": is_hi,
            "high_impact_reasons": reasons,
            "modeling": modeling,
            "testing": testing,
            "documentation": documentation,
            "governance": governance,
            "modeling_pass_count": sum(1 for v in modeling.values() if v),
            "modeling_total_count": len(modeling),
            "testing_pass_count": sum(1 for v in testing.values() if v),
            "testing_total_count": len(testing),
            "documentation_pass_count": sum(1 for v in documentation.values() if v),
            "documentation_total_count": len(documentation),
            "governance_pass_count": sum(1 for v in governance.values() if v),
            "governance_total_count": len(governance),
        })

    # Source health
    source_results = []
    for s in sources:
        uid = s["uniqueId"]
        desc = (s.get("description") or "").strip()
        src_desc = (s.get("sourceDescription") or "").strip()
        children = [c["uniqueId"] for c in (s.get("children") or []) if c.get("resourceType") == "model"]
        freshness_status = (s.get("freshness") or {}).get("freshnessStatus") or "Unconfigured"

        source_results.append({
            "unique_id": uid,
            "name": s["name"],
            "source_name": s.get("sourceName", ""),
            "has_description": len(desc) > 0,
            "source_has_description": len(src_desc) > 0,
            "child_count": len(children),
            "is_unused": len(children) == 0,
            "has_freshness": freshness_status not in ("Unconfigured", None, ""),
            "freshness_status": freshness_status,
        })

    # Aggregate stats
    total = len(model_results)
    stats = {
        "total_models": total,
        "total_sources": len(source_results),
        "models_with_description": sum(1 for m in model_results if m["documentation"].get("has_description")),
        "models_with_tests": sum(1 for m in model_results if m["testing"].get("has_test")),
        "models_with_pk_test": sum(1 for m in model_results if m["testing"].get("primary_key_test")),
        "high_impact_count": sum(1 for m in model_results if m["is_high_impact"]),
    }

    elapsed = time.time() - t0
    print(f"[{client.name}] Project health built in {elapsed:.1f}s")

    result = {
        "models": model_results,
        "sources": source_results,
        "stats": stats,
    }
    db_set(f"api:{key}", result)
    return result
