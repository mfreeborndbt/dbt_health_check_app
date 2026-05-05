#!/usr/bin/env python3
"""
dbt Health Check — cross-platform launcher.

Creates a local virtual environment, installs dependencies from requirements.txt,
and starts the Flask app. Works on macOS, Linux, and Windows (Python 3.8+).

Usage:
    python run.py                  # default http://127.0.0.1:5556 (or next free port)
    python run.py --port 8080
    python run.py --host 0.0.0.0   # listen on all interfaces (e.g. Docker)
    python run.py --no-update      # skip optional git fetch / pull
"""

import argparse
import os
import platform
import socket
import subprocess
import sys
import threading
import time
import webbrowser

MIN_PYTHON = (3, 8)
DEFAULT_PORT = 5556
DEFAULT_HOST = "127.0.0.1"
VENV_DIR = ".venv"
REQUIREMENTS = "requirements.txt"
REPO_REMOTE = "origin"
REPO_BRANCH = "main"

ROOT = os.path.dirname(os.path.abspath(__file__))


def _bind_probe_addr(host):
    """Use loopback to probe port availability when binding to all interfaces."""
    if host in ("0.0.0.0", "", None):
        return "127.0.0.1"
    return host


def port_is_available(host, port):
    """True if nothing appears to be listening on this TCP port for our bind mode."""
    addr = _bind_probe_addr(host)
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((addr, port))
    except OSError:
        return False
    return True


def resolve_listen_port(host, preferred, span=64):
    """Return preferred, or the next free port in [preferred, preferred + span)."""
    for offset in range(span):
        p = preferred + offset
        if p > 65535:
            break
        if port_is_available(host, p):
            return p
    sys.exit(
        f"Error: No free TCP port found starting at {preferred}. "
        "Stop other programs using those ports, or run with a high port, e.g. "
        "`python run.py --port 18080`."
    )


def check_python_version():
    if sys.version_info < MIN_PYTHON:
        sys.exit(
            f"Error: Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ is required. "
            f"You have {platform.python_version()}.\n"
            "Download from https://python.org"
        )


def venv_python():
    """Return the path to the Python binary inside the venv."""
    if platform.system() == "Windows":
        return os.path.join(ROOT, VENV_DIR, "Scripts", "python.exe")
    return os.path.join(ROOT, VENV_DIR, "bin", "python")


def ensure_venv():
    """Create a virtual environment if one doesn't exist."""
    py = venv_python()
    if os.path.isfile(py):
        return py

    print("Creating virtual environment...")
    subprocess.check_call([sys.executable, "-m", "venv", os.path.join(ROOT, VENV_DIR)])
    if not os.path.isfile(py):
        sys.exit(f"Error: Failed to create virtual environment at {VENV_DIR}/")
    return py


def install_deps(py):
    """Install/upgrade dependencies from requirements.txt."""
    req_path = os.path.join(ROOT, REQUIREMENTS)
    if not os.path.isfile(req_path):
        sys.exit(f"Error: {REQUIREMENTS} not found in {ROOT}")

    print("Installing dependencies...")
    subprocess.check_call(
        [py, "-m", "pip", "install", "-q", "--upgrade", "pip"],
        stdout=subprocess.DEVNULL,
    )
    subprocess.check_call([py, "-m", "pip", "install", "-q", "-r", req_path])


def get_local_commit():
    """Return the short commit hash of the current HEAD, or 'unknown'."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            cwd=ROOT,
        )
        return result.stdout.strip() if result.returncode == 0 else "unknown"
    except FileNotFoundError:
        return "unknown"


def check_for_updates():
    """Fetch from remote and fast-forward if behind. Never fails the app."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--is-inside-work-tree"],
            capture_output=True,
            text=True,
            cwd=ROOT,
        )
        if result.returncode != 0:
            return

        subprocess.run(
            ["git", "fetch", REPO_REMOTE, REPO_BRANCH, "--quiet"],
            capture_output=True,
            text=True,
            cwd=ROOT,
            timeout=10,
        )

        local = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            cwd=ROOT,
        ).stdout.strip()
        remote = subprocess.run(
            ["git", "rev-parse", f"{REPO_REMOTE}/{REPO_BRANCH}"],
            capture_output=True,
            text=True,
            cwd=ROOT,
        ).stdout.strip()

        if local != remote:
            behind = subprocess.run(
                ["git", "rev-list", "--count", f"HEAD..{REPO_REMOTE}/{REPO_BRANCH}"],
                capture_output=True,
                text=True,
                cwd=ROOT,
            ).stdout.strip()
            print(f"\n  *** Update available! You are {behind} commit(s) behind. Auto-updating... ***")
            pull = subprocess.run(
                ["git", "pull", REPO_REMOTE, REPO_BRANCH, "--ff-only"],
                capture_output=True,
                text=True,
                cwd=ROOT,
                timeout=30,
            )
            if pull.returncode == 0:
                new_commit = get_local_commit()
                print(f"  *** Updated successfully! Now at {new_commit}. ***\n")
            else:
                print(
                    f"  *** Auto-update failed (local changes?). Run 'git pull {REPO_REMOTE} {REPO_BRANCH}' manually. ***"
                )
                err = (pull.stderr or "").strip()
                if err:
                    print(f"  *** {err} ***\n")
        else:
            print("  Up to date.")
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
        print(f"  (Could not check for updates: {e})")


def browser_url(host, port):
    """URL to open in the browser (bind 0.0.0.0 still opens via loopback)."""
    if host in ("0.0.0.0", "::", "[::]"):
        return f"http://127.0.0.1:{port}"
    return f"http://{host}:{port}"


def open_browser(host, port):
    """Open the browser after a short delay to let the server start."""
    time.sleep(2)
    webbrowser.open(browser_url(host, port))


def main():
    os.chdir(ROOT)
    check_python_version()

    parser = argparse.ArgumentParser(description="dbt Health Check — launcher")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="HTTP port (default: %(default)s)")
    parser.add_argument(
        "--host",
        default=os.environ.get("FLASK_HOST", DEFAULT_HOST),
        help="Bind address (default: %(default)s; use 0.0.0.0 inside Docker)",
    )
    parser.add_argument("--no-update", action="store_true", help="Skip git fetch / optional auto-pull")
    args = parser.parse_args()

    py = ensure_venv()
    install_deps(py)

    commit = get_local_commit()
    print()
    print("  dbt Health Check")
    print(f"  Version: {commit}")
    print("  ------------------------------------")
    if not args.no_update:
        check_for_updates()

    port = resolve_listen_port(args.host, args.port)
    if port != args.port:
        print(f"  Note: Port {args.port} is already in use; using port {port} instead.")
        print("  (Stop the other app or pass --port to choose a specific free port.)\n")

    url = browser_url(args.host, port)
    print(f"  Running at: {url}")
    print("  Stop with:  Ctrl+C")
    print("  (Use --no-update to skip update check)")
    print()

    threading.Thread(target=open_browser, args=(args.host, port), daemon=True).start()

    app_py = os.path.join(ROOT, "app.py")
    r = None
    try:
        r = subprocess.run(
            [py, app_py, "--host", args.host, "--port", str(port)],
            check=False,
        )
    except KeyboardInterrupt:
        print("\nShutting down.")
        return

    if r is None:
        return
    if r.returncode == 0:
        return
    # Child killed by a signal (e.g. SIGINT) — exit quietly without a scary traceback
    if r.returncode < 0:
        return
    print(
        f"\nError: The app stopped with exit code {r.returncode}. "
        "See the messages above (often a missing dependency or import error).",
        file=sys.stderr,
    )
    sys.exit(r.returncode)


if __name__ == "__main__":
    main()
