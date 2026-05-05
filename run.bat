@echo off
REM dbt Health Check — launcher (Windows)
REM Delegates to run.py which handles venv, deps, and startup.

python "%~dp0run.py" %*
if errorlevel 1 (
    echo.
    echo If "python" was not found, install Python 3.8+ from https://python.org
    echo Make sure to check "Add Python to PATH" during installation.
    pause
)
