# dbt Health Check

Local dashboard that connects to **dbt Cloud** (Discovery + Admin APIs) and summarizes **Data Quality** (production job failures, model errors, and tests) and **Project Health** (modeling, testing, and documentation signals). All data stays on your machine except HTTPS calls to your own dbt Cloud account.

## Getting started

**You need:** Python **3.8+** on your PATH and a dbt Cloud **service token** with read + metadata access.

Copy and paste **one** of the blocks below (pick your OS), then open the URL shown in the terminal (your browser may open automatically).

If you **already cloned** the repo, skip `git clone` and run `cd dbt_health_check_app` then `git pull` to update.

### macOS / Linux

```bash
git clone https://github.com/mfreeborndbt/dbt_health_check_app.git
cd dbt_health_check_app
chmod +x run.sh
./run.sh
```

If `python3` is not the name of your interpreter, set `PYTHON` first, for example:

```bash
PYTHON=python3.11 ./run.sh
```

### Windows (Command Prompt or PowerShell)

```powershell
git clone https://github.com/mfreeborndbt/dbt_health_check_app.git
cd dbt_health_check_app
python run.py
```

Or double-click `run.bat` after opening a terminal in this folder.

**What happens:** the first run creates a `.venv` folder here, installs packages from `requirements.txt`, optionally fast-forwards `main` if you are behind `origin`, and starts the app at **http://127.0.0.1:5556** by default. If **5556 is already in use** (for example a previous run still open), the launcher picks the **next free port** (5557, 5558, …) and tells you in the terminal.

| Option | Example |
|--------|---------|
| Different port | `./run.sh --port 8080` or `python run.py --port 8080` |
| Listen on all interfaces (Docker, remote VM) | `./run.sh --host 0.0.0.0` |
| Skip git update check | `./run.sh --no-update` |

You can also set `FLASK_HOST` instead of passing `--host`.

## After it starts

1. Open **Settings** and enter your dbt Cloud connection fields (see table below).
2. Use **Data Quality** and **Project Health** tabs; tune **High Impact Config** as needed.

Credentials are stored only in `config/credentials.json` (the whole `config/` directory is gitignored).

## What you need from dbt Cloud

| Field | Where to find it |
|-------|------------------|
| Account prefix | Subdomain in the URL — `abc123` from `abc123.us1.dbt.com` |
| Region | e.g. `us1`, `eu1` (matches your dbt Cloud region) |
| Account ID | In the URL: `/deploy/{account_id}/...` |
| Project ID | In the URL: `/projects/{project_id}/...` |
| Environment ID | Your **production** (or target) deployment environment ID |
| Service token | Account **Settings → Service tokens** with **read** + **metadata** |

## Stopping the app

Press **Ctrl+C** in the terminal. If you start the app again while an old process is still bound to **5556**, either stop the old process or use the next port the launcher prints (or run `python run.py --port 18080`).

## Troubleshooting

| Issue | What to do |
|-------|------------|
| `fatal: destination path ... already exists` | You already have the repo. Run `cd dbt_health_check_app` and `git pull`, then `python run.py` (no second clone). |
| `Address already in use` / port busy | Use a new pull of `main`: the launcher auto-picks a free port. Or stop the other process, or run `python run.py --port 18080`. |

## Screenshot

![Data Quality dashboard showing failed runs, filters, and model-level issues](docs/dashboard-screenshot.png)

## High impact configuration

Use **High Impact Config** to tune how models are classified as high impact (semantic layer parents, exposure dependents, public/contract access, tags, and heavy usage thresholds).

## Portable layout

Everything is relative to the repo directory: virtualenv (`.venv`), cache (`.cache/`), and `config/credentials.json`. Clone or copy the folder anywhere; no global install required.
