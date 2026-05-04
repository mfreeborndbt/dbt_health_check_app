import hashlib
import time
import threading
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from discovery_client import DbtClient
from cache_db import cache_get as db_get, cache_set as db_set, cache_exists as db_exists

_AGG_TTL = 6 * 3600
_API_TTL = 6 * 3600

# ---------------------------------------------------------------------------
# In-memory aggregate cache backed by DuckDB
# ---------------------------------------------------------------------------
_SUMMARY_CACHE = {}
_SUMMARY_CACHE_LOCK = threading.Lock()


def _summary_db_key(client):
    return f"dq_summary_v8:{client.account_id}:{client.project_id}:{client.environment_id}"


def _summary_cache_get(client):
    key = _summary_db_key(client)
    with _SUMMARY_CACHE_LOCK:
        entry = _SUMMARY_CACHE.get(key)
        if entry and (time.time() - entry[0]) < _AGG_TTL:
            return entry[1]
    data = db_get(key, ttl=_AGG_TTL)
    if data is not None:
        with _SUMMARY_CACHE_LOCK:
            _SUMMARY_CACHE[key] = (time.time(), data)
        return data
    return None


def _summary_cache_set(client, data):
    key = _summary_db_key(client)
    with _SUMMARY_CACHE_LOCK:
        _SUMMARY_CACHE[key] = (time.time(), data)
    db_set(key, data)


def is_summary_cached(client):
    key = _summary_db_key(client)
    with _SUMMARY_CACHE_LOCK:
        entry = _SUMMARY_CACHE.get(key)
        if entry and (time.time() - entry[0]) < _AGG_TTL:
            return True
    return db_exists(key, ttl=_AGG_TTL)


# ---------------------------------------------------------------------------
# Per-API-call cache
# ---------------------------------------------------------------------------

def _cache_key(prefix, *args):
    raw = f"{prefix}:{'|'.join(str(a) for a in args)}"
    return hashlib.md5(raw.encode()).hexdigest()


def _cache_get(key):
    return db_get(f"api:{key}", ttl=_API_TTL)


def _cache_set(key, data):
    db_set(f"api:{key}", data)


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def _get_scheduled_jobs(client: DbtClient):
    key = _cache_key("scheduled_jobs", client.account_id, client.project_id)
    cached = _cache_get(key)
    if cached is not None:
        return {int(k): v for k, v in cached.items()}

    scheduled = {}
    offset = 0
    while True:
        data = client.admin_get("jobs/", params={
            "project_id": client.project_id,
            "offset": offset,
            "limit": 100,
        })
        batch = data["data"]
        if not batch:
            break
        for job in batch:
            triggers = job.get("triggers") or {}
            if triggers.get("schedule"):
                scheduled[job["id"]] = {
                    "name": job.get("name", ""),
                    "environment_id": job.get("environment_id"),
                }
        offset += 100

    _cache_set(key, scheduled)
    return scheduled


def _fetch_runs(client: DbtClient, days=30):
    key = _cache_key("runs_v3", client.account_id, client.project_id, client.environment_id, days)
    cached = _cache_get(key)
    if cached is not None:
        print(f"[{client.name}] Serving {len(cached)} runs from cache")
        return cached

    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")
    runs = []
    offset = 0

    while True:
        params = {
            "project_id": client.project_id,
            "order_by": "-created_at",
            "limit": 100,
            "offset": offset,
        }
        data = client.admin_get("runs/", params=params)
        batch = data["data"]
        if not batch:
            break
        for r in batch:
            if r["created_at"][:19] < cutoff:
                _cache_set(key, runs)
                return runs
            runs.append(r)
        offset += 100

    _cache_set(key, runs)
    return runs


def _fetch_single_run_details(client, job_id, run_id):
    """Fetch model/test details for a single run."""
    key = _cache_key("run_details_v3", client.environment_id, job_id, run_id)
    cached = _cache_get(key)
    if cached is not None:
        return run_id, cached

    query = """
    query ($jobId: BigInt!, $runId: BigInt) {
      job(id: $jobId, runId: $runId) {
        models {
          uniqueId
          name
          status
          executionTime
        }
        tests {
          uniqueId
          name
          status
          columnName
          dependsOn
        }
      }
    }
    """
    try:
        data = client.query_discovery(query, variables={"jobId": job_id, "runId": run_id})
        job_data = data.get("job")
        if not job_data:
            return run_id, None
        models = job_data.get("models") or []
        tests = job_data.get("tests") or []
        result = {
            "models": models,
            "tests": tests,
            "total_model_count": len(models),
            "skipped_model_count": sum(1 for m in models if m.get("status") == "skipped"),
            "error_model_count": sum(1 for m in models if m.get("status") in ("error", "fail")),
        }
        _cache_set(key, result)
        return run_id, result
    except Exception as e:
        print(f"  Skipping run {run_id}: {e}")
        return run_id, None


def _fetch_all_run_details_parallel(client, runs, max_workers=8, label=""):
    """Fetch model/test details for runs in parallel."""
    results = {}
    uncached_runs = []

    for run in runs:
        run_id = run["id"]
        job_id = run["job_definition_id"]
        key = _cache_key("run_details_v3", client.environment_id, job_id, run_id)
        cached = _cache_get(key)
        if cached is not None:
            results[run_id] = cached
        else:
            uncached_runs.append(run)

    if uncached_runs:
        cached_count = len(results)
        total = len(runs)
        print(f"[{client.name}] {label}: {cached_count}/{total} runs cached, fetching {len(uncached_runs)} from API ({max_workers} parallel)...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _fetch_single_run_details, client, run["job_definition_id"], run["id"]
                ): run["id"]
                for run in uncached_runs
            }
            done_count = 0
            for future in as_completed(futures):
                run_id, data = future.result()
                done_count += 1
                if data is not None:
                    results[run_id] = data
                if done_count % 10 == 0 or done_count == len(uncached_runs):
                    print(f"  Fetched {done_count}/{len(uncached_runs)} {label} runs...")
    else:
        print(f"[{client.name}] All {len(runs)} {label} run details served from cache")

    return results


# ---------------------------------------------------------------------------
# Run results artifact (per-model error messages)
# ---------------------------------------------------------------------------

def _fetch_single_run_results(client, run_id):
    """Fetch run_results.json artifact for a run to get per-model error messages."""
    key = _cache_key("run_results", client.account_id, run_id)
    cached = _cache_get(key)
    if cached is not None:
        return run_id, cached

    try:
        data = client.admin_get(f"runs/{run_id}/artifacts/run_results.json")
        results = data.get("results") or []
        # Extract only failed nodes with their messages
        error_map = {}
        for r in results:
            uid = r.get("unique_id", "")
            status = r.get("status", "")
            if status in ("error", "fail"):
                message = r.get("message") or ""
                # Clean up message — take first meaningful lines
                lines = message.strip().split("\n")
                cleaned = []
                for line in lines[:10]:
                    stripped = line.strip()
                    if stripped:
                        cleaned.append(stripped)
                error_msg = "\n".join(cleaned) if cleaned else "Unknown error"
                failures = r.get("failures")
                compiled_code = r.get("compiled_code") or r.get("compiled_sql") or ""
                error_map[uid] = {
                    "status": status,
                    "message": error_msg,
                    "failures": failures,
                    "compiled_code": compiled_code,
                }
        _cache_set(key, error_map)
        return run_id, error_map
    except Exception as e:
        # Artifact might not be available for all runs
        _cache_set(key, {})
        return run_id, {}


def _fetch_all_run_results_parallel(client, failed_runs, max_workers=8):
    """Fetch run_results.json for all failed runs in parallel."""
    results = {}
    uncached_runs = []

    for run in failed_runs:
        run_id = run["id"]
        key = _cache_key("run_results", client.account_id, run_id)
        cached = _cache_get(key)
        if cached is not None:
            results[run_id] = cached
        else:
            uncached_runs.append(run)

    if uncached_runs:
        cached_count = len(results)
        total = len(failed_runs)
        print(f"[{client.name}] Fetching error details: {cached_count}/{total} cached, fetching {len(uncached_runs)} run_results.json ({max_workers} parallel)...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_fetch_single_run_results, client, run["id"]): run["id"]
                for run in uncached_runs
            }
            done_count = 0
            for future in as_completed(futures):
                run_id, data = future.result()
                done_count += 1
                results[run_id] = data
                if done_count % 10 == 0 or done_count == len(uncached_runs):
                    print(f"  Fetched {done_count}/{len(uncached_runs)} run results...")
    else:
        print(f"[{client.name}] All {len(failed_runs)} run results served from cache")

    return results


# ---------------------------------------------------------------------------
# Downstream dependency counts (total transitive via BFS)
# ---------------------------------------------------------------------------

def _fetch_dependency_counts(client: DbtClient):
    """Get TOTAL transitive downstream AND upstream model counts via BFS, plus adjacency."""
    key = _cache_key("dep_counts_v5", client.account_id, client.environment_id)
    cached = _cache_get(key)
    if cached is not None:
        return cached["downstream"], cached["upstream"], cached["children_of"]

    print(f"[{client.name}] Fetching model dependency graph...")
    query = """
    query ($environmentId: BigInt!, $first: Int!, $after: String) {
      environment(id: $environmentId) {
        applied {
          models(first: $first, after: $after) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                uniqueId
                parents { uniqueId resourceType }
                children { uniqueId resourceType }
              }
            }
          }
        }
      }
    }
    """
    # Build adjacency graphs
    children_of = {}  # uid -> [child model uids]
    parents_of = {}   # uid -> [parent model uids]
    cursor = None
    while True:
        variables = {"environmentId": client.environment_id, "first": 500}
        if cursor:
            variables["after"] = cursor
        data = client.query_discovery(query, variables=variables)
        models_data = data["environment"]["applied"]["models"]
        for edge in models_data["edges"]:
            node = edge["node"]
            uid = node["uniqueId"]
            children = node.get("children") or []
            parents = node.get("parents") or []
            children_of[uid] = [c["uniqueId"] for c in children if c.get("resourceType") == "model"]
            parents_of[uid] = [p["uniqueId"] for p in parents if p.get("resourceType") == "model"]
        if not models_data["pageInfo"]["hasNextPage"]:
            break
        cursor = models_data["pageInfo"]["endCursor"]

    print(f"[{client.name}] Built dependency graph for {len(children_of)} models, computing BFS...")

    def bfs_count(start_uid, adjacency):
        visited = set()
        queue = list(adjacency.get(start_uid, []))
        while queue:
            uid = queue.pop(0)
            if uid in visited or uid == start_uid:
                continue
            visited.add(uid)
            queue.extend(adjacency.get(uid, []))
        return len(visited)

    downstream = {}
    upstream = {}
    for uid in children_of:
        downstream[uid] = bfs_count(uid, children_of)
        upstream[uid] = bfs_count(uid, parents_of)

    _cache_set(key, {"downstream": downstream, "upstream": upstream, "children_of": children_of})
    print(f"[{client.name}] Computed dependency counts for {len(downstream)} models")
    return downstream, upstream, children_of




# ---------------------------------------------------------------------------
# Model query history (targeted batch via modelHistoricalRuns aliases)
# ---------------------------------------------------------------------------

def _fetch_model_query_history_batch(client: DbtClient, model_uids):
    """Fetch execution counts for a targeted set of models via batched aliases."""
    if not model_uids:
        return {}

    key = _cache_key("model_qhist_v3", client.account_id, client.environment_id,
                     hashlib.md5(",".join(sorted(model_uids)).encode()).hexdigest())
    cached = _cache_get(key)
    if cached is not None:
        return cached

    print(f"[{client.name}] Fetching query history for {len(model_uids)} models...")
    query_counts = {}
    batch_size = 25
    uids = list(model_uids)

    for i in range(0, len(uids), batch_size):
        batch = uids[i:i + batch_size]
        aliases = []
        for j, uid in enumerate(batch):
            safe_uid = uid.replace('"', '\\"')
            aliases.append(
                f'm{j}: modelHistoricalRuns(uniqueId: "{safe_uid}", lastRunCount: 100) {{ uniqueId }}'
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
                query_counts[uid] = len(runs)
        except Exception as e:
            print(f"  Batch {i // batch_size} error: {e}")
            for uid in batch:
                query_counts[uid] = 0

        done = min(i + batch_size, len(uids))
        if done < len(uids):
            print(f"  Fetched {done}/{len(uids)} models...")

    _cache_set(key, query_counts)
    print(f"[{client.name}] Fetched query history for {len(query_counts)} models")
    return query_counts


# ---------------------------------------------------------------------------
# Test compiled SQL (from Discovery API)
# ---------------------------------------------------------------------------

def _fetch_test_compiled_code(client: DbtClient):
    """Fetch compiledCode for all tests from Discovery API."""
    key = _cache_key("test_compiled_v1", client.account_id, client.environment_id)
    cached = _cache_get(key)
    if cached is not None:
        return cached

    print(f"[{client.name}] Fetching test compiled SQL...")
    query = """
    query ($environmentId: BigInt!, $first: Int!, $after: String) {
      environment(id: $environmentId) {
        applied {
          tests(first: $first, after: $after) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                uniqueId
                compiledCode
              }
            }
          }
        }
      }
    }
    """
    test_sql = {}
    cursor = None
    while True:
        variables = {"environmentId": client.environment_id, "first": 500}
        if cursor:
            variables["after"] = cursor
        try:
            data = client.query_discovery(query, variables=variables)
        except Exception as e:
            print(f"[{client.name}] Could not fetch test compiled SQL: {e}")
            _cache_set(key, {})
            return {}
        tests_data = data["environment"]["applied"]["tests"]
        for edge in tests_data["edges"]:
            node = edge["node"]
            code = (node.get("compiledCode") or "").strip()
            if code:
                test_sql[node["uniqueId"]] = code
        if not tests_data["pageInfo"]["hasNextPage"]:
            break
        cursor = tests_data["pageInfo"]["endCursor"]

    _cache_set(key, test_sql)
    print(f"[{client.name}] Fetched compiled SQL for {len(test_sql)} tests")
    return test_sql


# ---------------------------------------------------------------------------
# Semantic model references
# ---------------------------------------------------------------------------

def _fetch_semantic_model_refs(client: DbtClient):
    """Return set of model unique_ids connected to semantic models."""
    key = _cache_key("semantic_refs_v1", client.account_id, client.environment_id)
    cached = _cache_get(key)
    if cached is not None:
        return set(cached)

    print(f"[{client.name}] Fetching semantic models...")
    query = """
    query ($environmentId: BigInt!, $first: Int!, $after: String) {
      environment(id: $environmentId) {
        applied {
          semanticModels(first: $first, after: $after) {
            pageInfo { hasNextPage endCursor }
            edges {
              node {
                uniqueId
                dependsOn
              }
            }
          }
        }
      }
    }
    """
    refs = set()
    cursor = None
    while True:
        variables = {"environmentId": client.environment_id, "first": 500}
        if cursor:
            variables["after"] = cursor
        try:
            data = client.query_discovery(query, variables=variables)
        except Exception as e:
            print(f"[{client.name}] Could not fetch semantic models: {e}")
            _cache_set(key, [])
            return set()
        sm_data = data["environment"]["applied"]["semanticModels"]
        for edge in sm_data["edges"]:
            node = edge["node"]
            for dep in (node.get("dependsOn") or []):
                if dep.startswith("model."):
                    refs.add(dep)
        if not sm_data["pageInfo"]["hasNextPage"]:
            break
        cursor = sm_data["pageInfo"]["endCursor"]

    _cache_set(key, list(refs))
    print(f"[{client.name}] Found {len(refs)} models connected to semantic models")
    return refs


# ---------------------------------------------------------------------------
# Main summary builder
# ---------------------------------------------------------------------------

def fetch_data_quality_summary(client: DbtClient, days=30):
    """Build the data quality summary for the past N days."""
    cached = _summary_cache_get(client)
    if cached is not None:
        print(f"[{client.name}] Serving data quality summary from cache")
        return cached

    t0 = time.time()

    scheduled_jobs = _get_scheduled_jobs(client)
    print(f"[{client.name}] Found {len(scheduled_jobs)} scheduled jobs")

    all_runs = _fetch_runs(client, days=days)
    print(f"[{client.name}] Found {len(all_runs)} total runs in the past {days} days")

    prod_runs = [
        r for r in all_runs
        if r["job_definition_id"] in scheduled_jobs
        and str(r.get("environment_id")) == str(client.environment_id)
    ]
    print(f"[{client.name}] {len(prod_runs)} runs from scheduled production jobs")

    failed_runs = [r for r in prod_runs if r["status"] == 20]
    successful_runs = [r for r in prod_runs if r["status"] == 10]

    # Fetch downstream + upstream counts (BFS transitive) and adjacency
    downstream_counts, upstream_counts, children_of = _fetch_dependency_counts(client)

    # Fetch semantic model refs and test compiled SQL from Discovery API
    semantic_model_refs = _fetch_semantic_model_refs(client)
    test_compiled_sql = _fetch_test_compiled_code(client)

    # Fetch details for failed runs (parallel)
    failed_run_details = {}
    if failed_runs:
        print(f"[{client.name}] Fetching details from {len(failed_runs)} failed runs...")
        failed_run_details = _fetch_all_run_details_parallel(client, failed_runs, max_workers=8, label="failed")

    # Fetch run_results.json for per-model error messages (parallel)
    run_error_maps = {}
    if failed_runs:
        run_error_maps = _fetch_all_run_results_parallel(client, failed_runs, max_workers=8)

    # Sample successful runs per job for avg model counts
    successful_by_job = defaultdict(list)
    for r in successful_runs:
        successful_by_job[r["job_definition_id"]].append(r)

    sampled_success_runs = []
    for jid, runs_list in successful_by_job.items():
        sampled_success_runs.extend(runs_list[:5])

    success_run_details = {}
    if sampled_success_runs:
        print(f"[{client.name}] Sampling {len(sampled_success_runs)} successful runs for model counts...")
        success_run_details = _fetch_all_run_details_parallel(client, sampled_success_runs, max_workers=8, label="success-sample")


    # -----------------------------------------------------------------------
    # Job-level stats
    # -----------------------------------------------------------------------
    job_run_counts = defaultdict(lambda: {"total": 0, "success": 0, "failed": 0, "cancelled": 0})
    for r in prod_runs:
        jid = r["job_definition_id"]
        job_run_counts[jid]["total"] += 1
        if r["status"] == 10:
            job_run_counts[jid]["success"] += 1
        elif r["status"] == 20:
            job_run_counts[jid]["failed"] += 1
        elif r["status"] == 30:
            job_run_counts[jid]["cancelled"] += 1

    job_avg_models_success = {}
    for jid, runs_list in successful_by_job.items():
        model_counts = []
        for r in runs_list[:5]:
            details = success_run_details.get(r["id"])
            if details:
                model_counts.append(details["total_model_count"])
        if model_counts:
            job_avg_models_success[jid] = round(sum(model_counts) / len(model_counts))

    failed_by_job = defaultdict(list)
    for r in failed_runs:
        failed_by_job[r["job_definition_id"]].append(r)

    job_avg_skipped_failed = {}
    for jid, runs_list in failed_by_job.items():
        skipped_counts = []
        for r in runs_list:
            details = failed_run_details.get(r["id"])
            if details:
                skipped_counts.append(details["skipped_model_count"])
        if skipped_counts:
            job_avg_skipped_failed[jid] = round(sum(skipped_counts) / len(skipped_counts))

    job_success_rates = []
    for jid, info in scheduled_jobs.items():
        counts = job_run_counts.get(jid, {"total": 0, "success": 0, "failed": 0, "cancelled": 0})
        total = counts["total"]
        success_pct = round(counts["success"] / total * 100, 1) if total > 0 else 100.0
        job_success_rates.append({
            "job_id": jid,
            "name": info["name"],
            "total_runs": total,
            "successful_runs": counts["success"],
            "failed_runs": counts["failed"],
            "cancelled_runs": counts["cancelled"],
            "success_pct": success_pct,
            "avg_models_success": job_avg_models_success.get(jid),
            "avg_skipped_failed": job_avg_skipped_failed.get(jid),
        })
    job_success_rates.sort(key=lambda j: j["success_pct"])

    # -----------------------------------------------------------------------
    # Summary stats
    # -----------------------------------------------------------------------
    jobs_with_failures = set()
    for r in failed_runs:
        jobs_with_failures.add(r["job_definition_id"])

    total_scheduled = len(scheduled_jobs)
    failure_pct = round(len(jobs_with_failures) / total_scheduled * 100, 1) if total_scheduled > 0 else 0

    # Daily failure time series + per-job breakdown
    daily_failures = defaultdict(int)
    job_day_failures = defaultdict(lambda: defaultdict(int))
    for r in failed_runs:
        day = r["created_at"][:10]
        jid = r["job_definition_id"]
        daily_failures[day] += 1
        job_day_failures[jid][day] += 1

    # Per-job per-date run counts for date filtering
    job_date_runs = defaultdict(lambda: defaultdict(lambda: {"total": 0, "success": 0, "failed": 0, "cancelled": 0}))
    for r in prod_runs:
        day = r["created_at"][:10]
        jid = r["job_definition_id"]
        job_date_runs[jid][day]["total"] += 1
        if r["status"] == 10:
            job_date_runs[jid][day]["success"] += 1
        elif r["status"] == 20:
            job_date_runs[jid][day]["failed"] += 1
        elif r["status"] == 30:
            job_date_runs[jid][day]["cancelled"] += 1

    now = datetime.now(timezone.utc)
    date_range = []
    for i in range(days):
        d = (now - timedelta(days=days - 1 - i)).strftime("%Y-%m-%d")
        date_range.append(d)

    # Serialize for JS
    job_date_runs_serialized = {}
    for jid in scheduled_jobs:
        job_date_runs_serialized[str(jid)] = {d: dict(job_date_runs[jid][d]) for d in date_range if job_date_runs[jid][d]["total"] > 0}

    daily_failure_series = [{"date": d, "count": daily_failures.get(d, 0)} for d in date_range]
    job_daily_map = {}
    for jid in scheduled_jobs:
        job_daily_map[str(jid)] = {d: job_day_failures[jid].get(d, 0) for d in date_range}

    day_job_breakdown = {}
    for d in date_range:
        jobs_on_day = []
        for jid, info in scheduled_jobs.items():
            count = job_day_failures[jid].get(d, 0)
            if count > 0:
                jobs_on_day.append({"job_id": jid, "name": info["name"], "count": count})
        if jobs_on_day:
            jobs_on_day.sort(key=lambda x: x["count"], reverse=True)
            day_job_breakdown[d] = jobs_on_day

    # -----------------------------------------------------------------------
    # Model & test failures with detailed error messages
    # -----------------------------------------------------------------------
    # Track both execution errors and test failures as per-occurrence rows
    model_entries = defaultdict(lambda: {"name": "", "unique_id": "", "errors": [], "test_errors": []})

    if failed_runs:
        for run in failed_runs:
            run_id = run["id"]
            details = failed_run_details.get(run_id)
            if details is None:
                continue

            run_datetime = run["created_at"]
            run_date = run_datetime[:10]
            run_time = run_datetime[11:16] if len(run_datetime) > 16 else ""
            job_id = run["job_definition_id"]
            job_name = scheduled_jobs.get(job_id, {}).get("name", f"Job {job_id}")

            # Get per-node error messages from run_results.json
            error_map = run_error_maps.get(run_id, {})

            # Model execution errors
            for m in details["models"]:
                if m["status"] in ("error", "fail"):
                    uid = m["uniqueId"]
                    model_entries[uid]["name"] = m["name"]
                    model_entries[uid]["unique_id"] = uid

                    run_result = error_map.get(uid, {})
                    error_msg = run_result.get("message", "")
                    error_type = _classify_error(error_msg, m["status"])

                    model_entries[uid]["errors"].append({
                        "date": run_date,
                        "time": run_time,
                        "job_name": job_name,
                        "error_type": error_type,
                        "error_details": error_msg or "No details available",
                        "compiled_code": "",
                        "run_id": run_id,
                    })

            # Test failures — per-occurrence with date/time/job
            for t in details["tests"]:
                if t["status"] in ("error", "fail"):
                    depends_on = t.get("dependsOn") or []
                    test_uid = t["uniqueId"]
                    test_name = t.get("name") or test_uid
                    column_name = t.get("columnName") or ""

                    # Get test error details from run_results.json
                    test_result = error_map.get(test_uid, {})
                    test_error_msg = test_result.get("message", "")
                    test_failures_count = test_result.get("failures")
                    test_error_type = _classify_test_error(test_name, test_error_msg)
                    # Get compiled SQL from Discovery API (not run_results.json)
                    test_compiled_code = test_compiled_sql.get(test_uid, "")

                    # Build rich error detail — prefer compiled SQL for tests
                    detail_parts = []
                    if test_failures_count is not None:
                        detail_parts.append(f"{test_failures_count} failing record{'s' if test_failures_count != 1 else ''}")
                    if test_error_msg:
                        detail_parts.append(test_error_msg)
                    error_detail = "\n".join(detail_parts) if detail_parts else "Test failed"

                    for dep in depends_on:
                        if dep.startswith("model."):
                            model_name = dep.split(".")[-1]
                            model_entries[dep]["name"] = model_name
                            model_entries[dep]["unique_id"] = dep
                            model_entries[dep]["test_errors"].append({
                                "date": run_date,
                                "time": run_time,
                                "job_name": job_name,
                                "test_name": test_name,
                                "column_name": column_name,
                                "error_type": test_error_type,
                                "error_details": error_detail,
                                "compiled_code": test_compiled_code,
                                "failures_count": test_failures_count,
                                "run_id": run_id,
                            })

    # -----------------------------------------------------------------------
    # Query history + high impact (targeted to failed models + downstream)
    # -----------------------------------------------------------------------
    failed_uids = set(model_entries.keys())
    # Collect transitive downstream UIDs for all failed models
    relevant_uids = set(failed_uids)
    for uid in failed_uids:
        visited = set()
        queue = list(children_of.get(uid, []))
        while queue:
            node = queue.pop(0)
            if node in visited or node == uid:
                continue
            visited.add(node)
            queue.extend(children_of.get(node, []))
        relevant_uids |= visited

    model_query_stats = _fetch_model_query_history_batch(client, relevant_uids)

    # Compute high-impact set: top 10% by query count OR connected to semantic model
    nonzero_counts = sorted([c for c in model_query_stats.values() if c > 0], reverse=True)
    if nonzero_counts:
        threshold_idx = max(1, len(nonzero_counts) // 10)
        query_threshold = nonzero_counts[threshold_idx - 1]
    else:
        query_threshold = float('inf')

    high_impact_set = set()
    for uid, qc in model_query_stats.items():
        if (qc > 0 and qc >= query_threshold) or uid in semantic_model_refs:
            high_impact_set.add(uid)
    high_impact_set |= semantic_model_refs

    # Compute downstream query counts and high-impact downstream for failed models
    downstream_query_counts = {}
    high_impact_downstream = {}
    for uid in failed_uids:
        visited = set()
        queue = list(children_of.get(uid, []))
        dq_total = 0
        has_hi = False
        while queue:
            node = queue.pop(0)
            if node in visited or node == uid:
                continue
            visited.add(node)
            dq_total += model_query_stats.get(node, 0)
            if node in high_impact_set:
                has_hi = True
            queue.extend(children_of.get(node, []))
        downstream_query_counts[uid] = dq_total
        high_impact_downstream[uid] = has_hi

    # Build final list
    failed_models = []
    for uid, info in model_entries.items():
        name = info["name"] or uid.split(".")[-1]
        errors = info["errors"]
        test_errors = info["test_errors"]

        # Count unique tests (for summary column)
        unique_tests = set()
        for te in test_errors:
            unique_tests.add(te["test_name"])

        # Collect all dates this model had failures
        failure_dates = set()
        for e in errors:
            failure_dates.add(e["date"])
        for te in test_errors:
            failure_dates.add(te["date"])

        # Merge into single combined list with source tag
        all_errors = []
        for e in errors:
            all_errors.append({**e, "source": "execution"})
        for te in test_errors:
            all_errors.append({**te, "source": "test"})
        all_errors.sort(key=lambda e: (e["date"], e["time"]), reverse=True)

        failed_models.append({
            "unique_id": uid,
            "name": name,
            "error_count": len(errors),
            "test_failure_count": len(unique_tests),
            "test_occurrence_count": len(test_errors),
            "has_model_errors": len(errors) > 0,
            "has_test_failures": len(test_errors) > 0,
            "all_errors": all_errors,
            "downstream_count": downstream_counts.get(uid, 0),
            "upstream_count": upstream_counts.get(uid, 0),
            "query_count": model_query_stats.get(uid, 0),
            "downstream_query_count": downstream_query_counts.get(uid, 0),
            "is_high_impact": uid in high_impact_set,
            "is_high_impact_downstream": high_impact_downstream.get(uid, False),
            "failure_dates": sorted(failure_dates),
        })

    failed_models.sort(key=lambda m: (
        -(1 if m["has_model_errors"] and m["has_test_failures"] else 0),
        -m["error_count"],
        -m["test_failure_count"],
    ))

    elapsed = time.time() - t0
    print(f"[{client.name}] Data quality summary built in {elapsed:.1f}s")

    # Build date -> job_ids mapping for bar-click filtering
    date_job_ids = {}
    for d in date_range:
        jids = []
        for jid in scheduled_jobs:
            if job_day_failures[jid].get(d, 0) > 0:
                jids.append(str(jid))
        if jids:
            date_job_ids[d] = jids

    result = {
        "failure_pct": failure_pct,
        "daily_failures": daily_failure_series,
        "failed_models": failed_models,
        "total_scheduled_jobs": total_scheduled,
        "jobs_with_failures": len(jobs_with_failures),
        "total_runs": len(prod_runs),
        "failed_runs": len(failed_runs),
        "job_success_rates": job_success_rates,
        "job_daily_map": job_daily_map,
        "day_job_breakdown": day_job_breakdown,
        "date_job_ids": date_job_ids,
        "job_date_runs": job_date_runs_serialized,
    }

    _summary_cache_set(client, result)
    return result


def _classify_error(message, status):
    """Classify an error message into a human-readable type."""
    if not message:
        return "Execution Error" if status == "error" else "Test Failure"
    msg_lower = message.lower()
    if "compilation error" in msg_lower:
        return "Compilation Error"
    if "database error" in msg_lower:
        return "Database Error"
    if "runtime error" in msg_lower:
        return "Runtime Error"
    if "relation" in msg_lower and ("does not exist" in msg_lower or "not found" in msg_lower):
        return "Missing Relation"
    if "permission denied" in msg_lower or "access denied" in msg_lower:
        return "Permission Error"
    if "timeout" in msg_lower:
        return "Timeout"
    if "syntax error" in msg_lower:
        return "Syntax Error"
    if "dependency error" in msg_lower:
        return "Dependency Error"
    return "Execution Error"


def _classify_test_error(test_name, message):
    """Classify a test failure into a human-readable type."""
    name_lower = test_name.lower()
    if "not_null" in name_lower:
        return "Not Null"
    if "unique" in name_lower:
        return "Uniqueness"
    if "accepted_values" in name_lower:
        return "Accepted Values"
    if "relationships" in name_lower:
        return "Referential Integrity"
    if "freshness" in name_lower:
        return "Freshness"
    if message:
        msg_lower = message.lower()
        if "got" in msg_lower and "result" in msg_lower:
            return "Row Count"
        if "compilation error" in msg_lower:
            return "Compilation Error"
    return "Test Failure"
