#!/usr/bin/env bash
# dbt Health Check — launcher (macOS / Linux)
# Delegates to run.py which handles venv, deps, and startup.

set -e
PYTHON=${PYTHON:-python3}
exec "$PYTHON" "$(dirname "$0")/run.py" "$@"
