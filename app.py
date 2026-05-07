import json
import os
import queue
import shutil
import sys
import threading
from flask import Flask, render_template, request, redirect, url_for, flash, Response
from discovery_client import load_credentials, save_credentials, DbtClient, get_client_from_config, CREDENTIALS_PATH
from data_quality import fetch_data_quality_summary, is_summary_cached
import cache_db

app = Flask(__name__)
app.secret_key = "dbt-health-check-key"


def _fmt_time(seconds):
    """Format seconds into human-readable time."""
    if seconds is None or seconds < 0:
        return "\u2014"
    s = float(seconds)
    if s < 60:
        return f"{s:.1f}s"
    if s < 3600:
        return f"{int(s // 60)}m {int(s % 60)}s"
    h = int(s // 3600)
    m = int((s % 3600) // 60)
    return f"{h}h {m}m"

app.jinja_env.globals['fmtTime'] = _fmt_time


# ---------------------------------------------------------------------------
# Log capture for streaming loading progress to the browser
# ---------------------------------------------------------------------------

class LogTee:
    """Tee stdout to both the original stream and a queue for SSE streaming."""
    def __init__(self, original, q):
        self.original = original
        self.queue = q

    def write(self, text):
        self.original.write(text)
        stripped = text.rstrip()
        if stripped:
            self.queue.put(stripped)
        return len(text)

    def flush(self):
        self.original.flush()

    def __getattr__(self, name):
        return getattr(self.original, name)


def _preload_observability():
    creds = load_credentials()
    if creds is None:
        raise Exception("Not configured")
    client = get_client_from_config()
    fetch_data_quality_summary(client, days=30)


def _preload_best_practices():
    creds = load_credentials()
    if creds is None:
        raise Exception("Not configured")
    client = get_client_from_config()
    from project_health import fetch_project_health
    fetch_project_health(client)


def _preload_dead_models():
    creds = load_credentials()
    if creds is None:
        raise Exception("Not configured")
    client = get_client_from_config()
    from dead_models import fetch_dead_models
    fetch_dead_models(client)


def _preload_incremental_candidates():
    creds = load_credentials()
    if creds is None:
        raise Exception("Not configured")
    client = get_client_from_config()
    from incremental_candidates import fetch_incremental_candidates
    fetch_incremental_candidates(client)


def _invalidate_summary():
    """Clear the summary cache so config changes take effect."""
    from data_quality import _summary_db_key, _SUMMARY_CACHE, _SUMMARY_CACHE_LOCK
    from cache_db import cache_delete
    from project_health import invalidate_project_health_summary
    client = get_client_from_config()
    if client:
        key = _summary_db_key(client)
        cache_delete(key)
        with _SUMMARY_CACHE_LOCK:
            _SUMMARY_CACHE.pop(key, None)
        invalidate_project_health_summary(client)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.errorhandler(500)
def handle_500(e):
    flash(f"Something went wrong: {e}. Try again or check your credentials.", "error")
    return redirect(url_for("setup"))


@app.route("/")
def index():
    creds = load_credentials()
    if creds is None:
        return redirect(url_for("setup"))
    return redirect(url_for("observability"))


@app.route("/setup")
def setup():
    creds = load_credentials()
    return render_template("setup.html", creds=creds)


@app.route("/setup/save", methods=["POST"])
def setup_save():
    prefix = request.form.get("account_prefix", "").strip().lower()
    prefix = prefix.replace("https://", "").replace("http://", "").split(".")[0]
    region = request.form.get("region", "us1").strip().lower()

    data = {
        "account_prefix": prefix,
        "region": region,
        "host_url": f"{prefix}.{region}.dbt.com" if prefix else "",
        "discovery_url": f"https://{prefix}.metadata.{region}.dbt.com/graphql" if prefix else "",
        "account_id": request.form.get("account_id", "").strip(),
        "project_id": request.form.get("project_id", "").strip(),
        "environment_id": request.form.get("environment_id", "").strip(),
        "token": request.form.get("token", "").strip(),
    }

    existing = load_credentials()
    if not data["token"] and existing:
        data["token"] = existing["token"]
    if existing:
        for k in (
            "high_impact_tags",
            "heavy_usage_pct",
            "public_model_mode",
            "high_impact_include_semantic_models",
            "high_impact_include_exposure_dependents",
        ):
            if k in existing:
                data[k] = existing[k]

    if prefix:
        data["name"] = prefix

    required = ["account_prefix", "account_id", "project_id", "environment_id", "token"]
    missing = [k for k in required if not data.get(k)]
    if missing:
        flash(f"Missing required fields: {', '.join(missing)}", "error")
        return redirect(url_for("setup"))

    try:
        client = DbtClient(data)
        client.test_connection()
        save_credentials(data)
        flash("Connection successful! Credentials saved.", "success")
        return redirect(url_for("observability"))
    except Exception as e:
        flash(f"Connection failed: {e}", "error")
        save_credentials(data)
        return redirect(url_for("setup"))


@app.route("/setup/clear")
def setup_clear():
    if os.path.exists(CREDENTIALS_PATH):
        os.remove(CREDENTIALS_PATH)
    cache_db.close()
    cache_dir = os.path.join(os.path.dirname(__file__), ".cache")
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir, ignore_errors=True)
    flash("Credentials cleared.", "success")
    return redirect(url_for("setup"))


# ---------------------------------------------------------------------------
# High Impact Config
# ---------------------------------------------------------------------------

@app.route("/config")
def config_page():
    creds = load_credentials()
    if creds is None:
        return redirect(url_for("setup"))
    all_tags = []
    client = get_client_from_config()
    if client:
        from data_quality import _fetch_high_impact_signals
        try:
            hi_data = _fetch_high_impact_signals(client)
            all_tags = hi_data.get("all_tags", [])
        except Exception:
            pass
    selected_tags = creds.get("high_impact_tags", [])
    heavy_pct = creds.get("heavy_usage_pct", 20)
    public_mode = creds.get("public_model_mode", "public_with_contract")
    include_semantic = creds.get("high_impact_include_semantic_models", True)
    include_exposure = creds.get("high_impact_include_exposure_dependents", True)
    return render_template(
        "config.html",
        creds=creds,
        all_tags=all_tags,
        selected_tags=selected_tags,
        heavy_pct=heavy_pct,
        public_mode=public_mode,
        include_semantic=include_semantic,
        include_exposure=include_exposure,
    )


@app.route("/config/save", methods=["POST"])
def config_save():
    creds = load_credentials()
    if creds is None:
        flash("Configure your connection first.", "error")
        return redirect(url_for("setup"))
    creds["high_impact_tags"] = request.form.getlist("high_impact_tags")
    creds["heavy_usage_pct"] = int(request.form.get("heavy_usage_pct", 20))
    creds["public_model_mode"] = request.form.get("public_model_mode", "public_with_contract")
    creds["high_impact_include_semantic_models"] = "1" in request.form.getlist(
        "high_impact_include_semantic_models"
    )
    creds["high_impact_include_exposure_dependents"] = "1" in request.form.getlist(
        "high_impact_include_exposure_dependents"
    )
    save_credentials(creds)
    _invalidate_summary()
    flash("High-impact configuration saved. Data will recalculate on next load.", "success")
    return redirect(url_for("config_page"))


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def _needs_loading(page="/observability"):
    creds = load_credentials()
    if creds is None:
        return False
    client = get_client_from_config()
    if client is None:
        return False
    if page == "/best-practices":
        from project_health import is_project_health_cached
        return not is_project_health_cached(client)
    if page == "/dead-models":
        from dead_models import is_dead_models_cached
        return not is_dead_models_cached(client)
    if page == "/incremental-candidates":
        from incremental_candidates import is_incremental_candidates_cached
        return not is_incremental_candidates_cached(client)
    return not is_summary_cached(client)


@app.route("/loading")
def loading():
    next_page = request.args.get("next", "/observability")
    creds = load_credentials()
    project_name = creds.get("name", "Project") if creds else "Project"
    page_names = {
        "/observability": "Observability",
        "/best-practices": "Best Practice Checks",
        "/dead-models": "Dead Models",
        "/incremental-candidates": "Incremental Candidates",
    }
    page_name = page_names.get(next_page, next_page)
    return render_template("loading.html", next_page=next_page, page_name=page_name, project_name=project_name)


@app.route("/api/load")
def api_load():
    page = request.args.get("page", "/observability")

    def generate():
        q = queue.Queue()
        result = {"status": "done", "error": None}

        def do_load():
            old_stdout = sys.stdout
            sys.stdout = LogTee(old_stdout, q)
            try:
                if page == "/best-practices":
                    _preload_best_practices()
                elif page == "/dead-models":
                    _preload_dead_models()
                elif page == "/incremental-candidates":
                    _preload_incremental_candidates()
                else:
                    _preload_observability()
            except Exception as e:
                result["status"] = "error"
                result["error"] = str(e)
            finally:
                sys.stdout = old_stdout
                q.put(None)

        thread = threading.Thread(target=do_load, daemon=True)
        thread.start()

        while True:
            try:
                msg = q.get(timeout=0.5)
                if msg is None:
                    if result["status"] == "done":
                        yield f"data: {json.dumps({'type': 'done', 'redirect': page})}\n\n"
                    else:
                        yield f"data: {json.dumps({'type': 'error', 'message': result['error']})}\n\n"
                    break
                yield f"data: {json.dumps({'type': 'log', 'message': msg})}\n\n"
            except queue.Empty:
                yield ": heartbeat\n\n"

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ---------------------------------------------------------------------------
# Tab routes
# ---------------------------------------------------------------------------

@app.route("/observability")
def observability():
    creds = load_credentials()
    if creds is None:
        return redirect(url_for("setup"))

    if _needs_loading():
        return redirect(url_for("loading", next="/observability"))

    client = get_client_from_config()
    try:
        summary = fetch_data_quality_summary(client, days=30)
    except Exception as e:
        err = str(e)
        if any(s in err for s in ("401", "403", "Unauthorized", "Forbidden")):
            flash("API authentication failed. Please update your service token.", "error")
            return redirect(url_for("setup"))
        if "429" in err or "Too Many" in err:
            flash("Rate limited by dbt Cloud API. Please wait a moment and try again.", "error")
            return redirect(url_for("setup"))
        flash(f"Error fetching data: {e}", "error")
        return redirect(url_for("setup"))

    return render_template(
        "observability.html",
        creds=creds,
        summary=summary,
    )


@app.route("/best-practices")
def best_practices():
    creds = load_credentials()
    if creds is None:
        return redirect(url_for("setup"))

    if _needs_loading("/best-practices"):
        return redirect(url_for("loading", next="/best-practices"))

    client = get_client_from_config()
    try:
        from project_health import fetch_project_health
        summary = fetch_project_health(client)
    except Exception as e:
        err = str(e)
        if any(s in err for s in ("401", "403", "Unauthorized", "Forbidden")):
            flash("API authentication failed.", "error")
            return redirect(url_for("setup"))
        flash(f"Error fetching project health: {e}", "error")
        return redirect(url_for("observability"))
    return render_template("best_practices.html", creds=creds, summary=summary)


@app.route("/dead-models")
def dead_models():
    creds = load_credentials()
    if creds is None:
        return redirect(url_for("setup"))

    if _needs_loading("/dead-models"):
        return redirect(url_for("loading", next="/dead-models"))

    client = get_client_from_config()
    try:
        from dead_models import fetch_dead_models
        summary = fetch_dead_models(client)
    except Exception as e:
        err = str(e)
        if any(s in err for s in ("401", "403", "Unauthorized", "Forbidden")):
            flash("API authentication failed.", "error")
            return redirect(url_for("setup"))
        flash(f"Error fetching dead models: {e}", "error")
        return redirect(url_for("observability"))
    return render_template("dead_models.html", creds=creds, summary=summary)


@app.route("/incremental-candidates")
def incremental_candidates():
    creds = load_credentials()
    if creds is None:
        return redirect(url_for("setup"))

    if _needs_loading("/incremental-candidates"):
        return redirect(url_for("loading", next="/incremental-candidates"))

    client = get_client_from_config()
    try:
        from incremental_candidates import fetch_incremental_candidates
        summary = fetch_incremental_candidates(client)
    except Exception as e:
        err = str(e)
        if any(s in err for s in ("401", "403", "Unauthorized", "Forbidden")):
            flash("API authentication failed.", "error")
            return redirect(url_for("setup"))
        flash(f"Error fetching incremental candidates: {e}", "error")
        return redirect(url_for("observability"))
    return render_template("incremental_candidates.html", creds=creds, summary=summary)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        default=os.environ.get("FLASK_HOST", "127.0.0.1"),
        help="Bind address (default: 127.0.0.1; use 0.0.0.0 for Docker/WSL port forwarding)",
    )
    parser.add_argument("--port", type=int, default=5556)
    args = parser.parse_args()
    app.run(host=args.host, port=args.port, use_reloader=False, threaded=True)
