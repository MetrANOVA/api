# api
MetrANOVA API - Admin

## Workspace structure

This repository uses a `uv` workspace with three packages:

- `metranova_core` (workspace root `.`): shared code in `src/metranova`
- `admin_api` (`packages/admin_api`): admin service package
- `pipeline` (`packages/pipeline`): pipeline service package

Both `admin_api` and `pipeline` depend on the workspace `metranova_core` package and call shared logic from `metranova`.

## Running services

- `python bin/start-admin-api.py`
- `python bin/start-pipeline.py`

Each `bin/` script runs `uv run --package <package> <script>` so the correct workspace package and dependencies are used.

## Running tests

Run all package tests:

- `python bin/run-tests.py`

Run tests per package:

- `uv run --group dev --package metranova_core pytest tests/metranova_core -q`
- `uv run --group dev --package admin_api pytest packages/admin_api/tests -q`
- `uv run --group dev --package pipeline pytest packages/pipeline/tests -q`

## Formatting

Format Python code with Black:

- `uv run --group dev black .`

## Docker build

Build one image for the whole workspace (core + admin_api + pipeline):

- `docker build -t metranova-app .`

Run `admin_api` (default container command):

- `docker run --rm metranova-app`

Run `pipeline` from the same image:

- `docker run --rm metranova-app pipeline`
