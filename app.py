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


def _preload_data_quality():
    """Run data loading, warming caches."""
    creds = load_credentials()
    if creds is None:
        raise Exception("Not configured")
    client = get_client_from_config()
    fetch_data_quality_summary(client, days=30)


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
    return redirect(url_for("data_quality"))


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
        return redirect(url_for("data_quality"))
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


def _needs_loading():
    """Check if we should redirect to the loading page (cache is cold)."""
    creds = load_credentials()
    if creds is None:
        return False
    client = get_client_from_config()
    if client is None:
        return False
    return not is_summary_cached(client)


@app.route("/loading")
def loading():
    next_page = request.args.get("next", "/data-quality")
    creds = load_credentials()
    project_name = creds.get("name", "Project") if creds else "Project"
    page_names = {
        "/data-quality": "Data Quality",
    }
    page_name = page_names.get(next_page, next_page)
    return render_template("loading.html", next_page=next_page, page_name=page_name, project_name=project_name)


@app.route("/api/load")
def api_load():
    page = request.args.get("page", "/data-quality")

    def generate():
        q = queue.Queue()
        result = {"status": "done", "error": None}

        def do_load():
            old_stdout = sys.stdout
            sys.stdout = LogTee(old_stdout, q)
            try:
                _preload_data_quality()
            except Exception as e:
                result["status"] = "error"
                result["error"] = str(e)
            finally:
                sys.stdout = old_stdout
                q.put(None)  # sentinel

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


@app.route("/data-quality")
def data_quality():
    creds = load_credentials()
    if creds is None:
        return redirect(url_for("setup"))

    if _needs_loading():
        return redirect(url_for("loading", next="/data-quality"))

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
        "data_quality.html",
        creds=creds,
        summary=summary,
    )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5556)
    args = parser.parse_args()
    app.run(port=args.port, use_reloader=False, threaded=True)
