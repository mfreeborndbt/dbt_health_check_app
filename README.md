# dbt Health Check

Flask app that connects to dbt Cloud Discovery API to summarize **Data Quality** (production job failures, model-level errors and tests) and **Project Health** (modeling, testing, and documentation signals).

## Screenshot

![Data Quality dashboard showing failed runs, filters, and model-level issues](docs/dashboard-screenshot.png)

## Setup

Configure your dbt Cloud account prefix, region, account/project/environment IDs, and service token from the in-app **Settings** screen after install.

## High impact configuration

Use **High Impact Config** to tune how models are classified as high impact (semantic layer parents, exposure dependents, public/contract access, tags, and heavy usage thresholds).
