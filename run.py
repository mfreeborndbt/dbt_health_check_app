#!/usr/bin/env python3
"""
dbt Health Check — bulletproof launcher.

Creates a venv, installs deps (skips if unchanged), kills any stale instance,
finds a free port, and opens the browser. Works on macOS, Linux, Windows (3.8+).

Usage:
    python3 run.py               # just works
    python3 run.py --port 8080   # prefer a specific port
    python3 run.py --no-update   # skip git auto-pull
"""

import argparse
import hashlib
import os
import platform
import signal
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
PID_FILE = ".pid"
DEPS_HASH_FILE = ".deps_hash"

ROOT = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# PID file — detect & kill stale instances
# ---------------------------------------------------------------------------

def _pid_path():
    return os.path.join(ROOT, PID_FILE)


def _read_pid_file():
    """Return (pid, port) from the PID file, or (None, None)."""
    try:
        with open(_pid_path()) as f:
            parts = f.read().strip().split(":")
            return int(parts[0]), int(parts[1])
    except (FileNotFoundError, ValueError, IndexError):
        return None, None


def _write_pid_file(pid, port):
    with open(_pid_path(), "w") as f:
        f.write(f"{pid}:{port}")


def _remove_pid_file():
    try:
        os.remove(_pid_path())
    except FileNotFoundError:
        pass


def _process_alive(pid):
    """Check if a process is running."""
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def _kill_stale_instance():
    """If a previous instance is still running, kill it so we can take over."""
    pid, port = _read_pid_file()
    if pid is None:
        return
    if not _process_alive(pid):
        _remove_pid_file()
        return
    print(f"  Stopping previous instance (PID {pid}, port {port})...")
    try:
        os.kill(pid, signal.SIGTERM)
        # Give it a moment to shut down
        for _ in range(20):
            if not _process_alive(pid):
                break
            time.sleep(0.25)
        else:
            os.kill(pid, signal.SIGKILL)
    except (OSError, ProcessLookupError):
        pass
    _remove_pid_file()


# ---------------------------------------------------------------------------
# Port helpers
# ---------------------------------------------------------------------------

def _probe_addr(host):
    if host in ("0.0.0.0", "", None):
        return "127.0.0.1"
    return host


def _port_free(host, port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((_probe_addr(host), port))
        return True
    except OSError:
        return False


def _find_port(host, preferred, span=64):
    for offset in range(span):
        p = preferred + offset
        if p > 65535:
            break
        if _port_free(host, p):
            return p
    sys.exit(f"Error: No free port in {preferred}–{preferred + span - 1}. Try --port 18080.")


# ---------------------------------------------------------------------------
# Venv & deps (skip install when requirements.txt hasn't changed)
# ---------------------------------------------------------------------------

def _venv_python():
    if platform.system() == "Windows":
        return os.path.join(ROOT, VENV_DIR, "Scripts", "python.exe")
    return os.path.join(ROOT, VENV_DIR, "bin", "python")


def _ensure_venv():
    py = _venv_python()
    if os.path.isfile(py):
        return py
    if sys.version_info < MIN_PYTHON:
        sys.exit(f"Error: Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ required (you have {platform.python_version()}).")
    print("  Creating virtual environment...")
    subprocess.check_call([sys.executable, "-m", "venv", os.path.join(ROOT, VENV_DIR)])
    if not os.path.isfile(py):
        sys.exit("Error: venv creation failed.")
    return py


def _requirements_hash():
    req = os.path.join(ROOT, REQUIREMENTS)
    if not os.path.isfile(req):
        return None
    return hashlib.md5(open(req, "rb").read()).hexdigest()


def _deps_up_to_date():
    """True if requirements.txt hasn't changed since last install."""
    hash_path = os.path.join(ROOT, DEPS_HASH_FILE)
    current = _requirements_hash()
    if current is None:
        return False
    try:
        with open(hash_path) as f:
            return f.read().strip() == current
    except FileNotFoundError:
        return False


def _install_deps(py):
    if _deps_up_to_date():
        return
    req = os.path.join(ROOT, REQUIREMENTS)
    if not os.path.isfile(req):
        sys.exit(f"Error: {REQUIREMENTS} not found.")
    print("  Installing dependencies...")
    subprocess.check_call([py, "-m", "pip", "install", "-q", "--upgrade", "pip"], stdout=subprocess.DEVNULL)
    subprocess.check_call([py, "-m", "pip", "install", "-q", "-r", req])
    # Record hash so we skip next time
    with open(os.path.join(ROOT, DEPS_HASH_FILE), "w") as f:
        f.write(_requirements_hash())


# ---------------------------------------------------------------------------
# Git auto-update
# ---------------------------------------------------------------------------

def _git(*args, timeout=10):
    return subprocess.run(
        ["git"] + list(args),
        capture_output=True, text=True, cwd=ROOT, timeout=timeout,
    )


def _get_commit():
    r = _git("rev-parse", "--short", "HEAD")
    return r.stdout.strip() if r.returncode == 0 else "unknown"


def _check_for_updates():
    try:
        if _git("rev-parse", "--is-inside-work-tree").returncode != 0:
            return
        _git("fetch", REPO_REMOTE, REPO_BRANCH, "--quiet")
        local = _git("rev-parse", "HEAD").stdout.strip()
        remote = _git("rev-parse", f"{REPO_REMOTE}/{REPO_BRANCH}").stdout.strip()
        if local == remote:
            print("  Up to date.")
            return
        behind = _git("rev-list", "--count", f"HEAD..{REPO_REMOTE}/{REPO_BRANCH}").stdout.strip()
        print(f"\n  *** {behind} commit(s) behind — auto-updating... ***")
        pull = _git("pull", REPO_REMOTE, REPO_BRANCH, "--ff-only", timeout=30)
        if pull.returncode == 0:
            print(f"  *** Updated to {_get_commit()}. ***\n")
        else:
            print(f"  *** Auto-update failed (local changes?). Run 'git pull' manually. ***")
            if pull.stderr and pull.stderr.strip():
                print(f"  *** {pull.stderr.strip()} ***\n")
    except Exception as e:
        print(f"  (Could not check for updates: {e})")


# ---------------------------------------------------------------------------
# Browser
# ---------------------------------------------------------------------------

def _browser_url(host, port):
    if host in ("0.0.0.0", "::", "[::]"):
        return f"http://127.0.0.1:{port}"
    return f"http://{host}:{port}"


def _open_browser(host, port):
    time.sleep(2)
    webbrowser.open(_browser_url(host, port))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.chdir(ROOT)

    parser = argparse.ArgumentParser(description="dbt Health Check — launcher")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--host", default=os.environ.get("FLASK_HOST", DEFAULT_HOST))
    parser.add_argument("--no-update", action="store_true", help="Skip git auto-pull")
    args = parser.parse_args()

    # Kill any stale instance from a previous run
    _kill_stale_instance()

    py = _ensure_venv()
    _install_deps(py)

    commit = _get_commit()
    print()
    print("  dbt Health Check")
    print(f"  Version: {commit}")
    print("  ------------------------------------")
    if not args.no_update:
        _check_for_updates()

    port = _find_port(args.host, args.port)
    if port != args.port:
        print(f"  Note: Port {args.port} busy — using {port} instead.\n")

    url = _browser_url(args.host, port)
    print(f"  Running at: {url}")
    print("  Stop with:  Ctrl+C")
    print()

    threading.Thread(target=_open_browser, args=(args.host, port), daemon=True).start()

    app_py = os.path.join(ROOT, "app.py")
    r = None
    try:
        proc = subprocess.Popen([py, app_py, "--host", args.host, "--port", str(port)])
        _write_pid_file(proc.pid, port)
        r = proc.wait()
    except KeyboardInterrupt:
        print("\nShutting down.")
        _remove_pid_file()
        return
    finally:
        _remove_pid_file()

    if r is None or r == 0 or r < 0:
        return
    print(f"\nError: exit code {r}. See messages above.", file=sys.stderr)
    sys.exit(r)


if __name__ == "__main__":
    main()
