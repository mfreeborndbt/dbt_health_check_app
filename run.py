#!/usr/bin/env python3
"""Launcher for the dbt Health Check app."""
import os
import subprocess
import sys
import webbrowser
import time

PORT = 5556
APP_DIR = os.path.dirname(os.path.abspath(__file__))
VENV_DIR = os.path.join(APP_DIR, ".venv")
REQ_FILE = os.path.join(APP_DIR, "requirements.txt")


def ensure_venv():
    if not os.path.exists(VENV_DIR):
        print("Creating virtual environment...")
        subprocess.check_call([sys.executable, "-m", "venv", VENV_DIR])

    pip = os.path.join(VENV_DIR, "bin", "pip") if os.name != "nt" else os.path.join(VENV_DIR, "Scripts", "pip.exe")
    python = os.path.join(VENV_DIR, "bin", "python") if os.name != "nt" else os.path.join(VENV_DIR, "Scripts", "python.exe")

    print("Installing dependencies...")
    subprocess.check_call([pip, "install", "-q", "-r", REQ_FILE])
    return python


def main():
    python = ensure_venv()
    url = f"http://127.0.0.1:{PORT}"
    print(f"\nStarting dbt Health Check at {url}")
    print("Press Ctrl+C to stop.\n")

    # Open browser after a short delay
    def open_browser():
        time.sleep(1.5)
        webbrowser.open(url)

    import threading
    threading.Thread(target=open_browser, daemon=True).start()

    app_py = os.path.join(APP_DIR, "app.py")
    subprocess.call([python, app_py, "--port", str(PORT)])


if __name__ == "__main__":
    main()
