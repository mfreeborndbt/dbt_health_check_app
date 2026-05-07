#!/usr/bin/env bash
set -e
exec "${PYTHON:-python3}" "$(dirname "$0")/run.py" "$@"
