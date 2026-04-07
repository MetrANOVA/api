# MetrANOVA API

## Workspace structure

This repository uses a `uv` workspace with three packages:

- `metranova_core` (workspace root `.`): shared code in `src/metranova`
- `admin_api` (`src/admin_api`): admin service package
- `pipeline` (`src/pipeline`): pipeline service package

Both `admin_api` and `pipeline` depend on the workspace `metranova_core` package and call shared logic from `metranova`.

## Running services

- `python bin/start-admin-api.py`
- `python bin/start-pipeline.py`

Each `bin/` script runs the package module form (`python -m <package>`) via `uv run --package <package> ...` so the correct workspace package and dependencies are used.

## Running tests

Run all package tests:

- `python bin/run-tests.py`

Run tests per package:

- `uv run --group dev --package metranova_core pytest tests/metranova_core -q`
- `uv run --group dev --package admin_api pytest src/admin_api/tests -q`
- `uv run --group dev --package pipeline pytest src/pipeline/tests -q`

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

## Devcontainer

The devcontainer spins up a full local development environment using Docker Compose. All services are started automatically when you open the project in VS Code with the Dev Containers extension.

### Services

| Service | Description | Ports |
|---|---|---|
| **api** | Python 3.14 dev container — your working environment | — |
| **clickhouse** | ClickHouse OLAP database, database `metranova` | `8123` (HTTP), `9000` (native) |
| **kafka** | Apache Kafka in KRaft mode (no Zookeeper) | `9092` (internal), `9094` (external) |
| **telegraf** | Collects SNMP metrics and publishes them to Kafka topic `snmp.telegraf.metrics` | — |
| **snmp-simulator** | Simulates SNMP devices for local development | `161/udp` |
| **grafana** | Grafana with the ClickHouse datasource pre-configured | `3000` |

### Prerequisites

- Docker Engine + Compose (or Docker Desktop)
- [VS Code](https://code.visualstudio.com/) with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

### Starting the environment

Open the project in VS Code and click **Reopen in Container** when prompted, or run:

```
Dev Containers: Reopen in Container
```

from the command palette (`Ctrl+Shift+P` / `Cmd+Shift+P`).

To start the supporting services manually from a terminal outside the container:

```bash
docker compose -f .devcontainer/docker-compose.yml up -d
```

### Accessing services

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | `admin` / `admin` |
| ClickHouse HTTP | http://localhost:8123 | user: `default`, no password |
| Kafka (external) | `localhost:9094` | — |

### Database schema

SQL scripts in `.devcontainer/clickhouse/init/` are applied automatically in alphabetical order whenever the Compose stack starts. On a brand-new ClickHouse volume they are also picked up by the built-in container init hook.

To reset the database and re-run init scripts:

```bash
docker compose -f .devcontainer/docker-compose.yml down -v
docker compose -f .devcontainer/docker-compose.yml up -d
```
