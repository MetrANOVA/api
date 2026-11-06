# MetrANOVA API

## API-owned Telegraf configuration (development)

Collector create/update requests store desired settings only. GET `/collector/resource_configurations/{id}/render`
renders without publishing. GET `.../{id}/preview` validates and checks for
changes without publication; its response does not include sensitive TOML.
POST `.../{id}/apply` is the explicit activation request. Use the router's
configured prefix/root path when constructing URLs.

For `telegraf_kube`, provision `<release>-telegraf-resource-configurations`
before applying and grant the API ServiceAccount get/patch on that ConfigMap
and patch on `<release>-telegraf`. Configuration files are keyed by collector
ID, not resource type, so multiple collectors cannot overwrite one another.
The deployment base config owns outputs; generated files contain inputs only.
Supply `TELEGRAF_BINARY_PATH`, `TELEGRAF_VERSION` (exact deployed version), and
`TELEGRAF_BASE_CONFIG_PATH` (the deployment base TOML) to the API. Missing
settings/binary, version mismatch, and validation errors block publication.
Validation loads the base plus all candidate ConfigMap fragments in a private
temporary file with a finite timeout and Telegraf test wait of zero. It does
not establish ingestion. Telegraf `--test` still gathers from inputs even
with zero test wait: applying requires authorization to contact selected devices.
An unreachable device can therefore block this test. Outputs are loaded but
not exercised by test mode. A connectivity-independent validation method is
not implemented.

Serialize apply requests with other configuration writers. Changed ConfigMap
data is published only after validation; the API patches a configuration hash
onto the Deployment, then records its acknowledgement in the ConfigMap.
If activation acknowledgement fails, apply returns an error and a rerun
retries activation even when the data already matches. A successful request
means a restart was requested, not that rollout or ingestion was verified.
Invalid output leaves the ConfigMap unchanged. No automatic ConfigMap creation
or destructive schema repair is performed.

Optional `source`/`source_id` fields identify owned node/collector
records. Owned collectors require explicit nonempty node selection, either
through selectors or active memberships, and cannot select unowned or
differently owned nodes. Fresh tables include these fields; existing
installations require an operator-reviewed metadata schema upgrade before
these APIs can use them. No migration is performed by this change. These fields
alone are not authorization. CDS synchronization and deployment/image wiring
remain unfinished.

## Resource-configuration device memberships

The admin API exposes:

- `GET /resource-configs/{resource_id}` -> `{"devices": ["hostname-or-IP", ...]}`
- `POST /resource-configs/{resource_id}/devices/{device_id}` with `{}` to add
- `DELETE /resource-configs/{resource_id}/devices/{device_id}` to remove

Device identities prefer the normalized node name and fall back to canonical
management IP. Adds require exactly one matching node and are idempotent;
removes are idempotent and can clear stale identities. The API stores
append-only active/tombstone records in
`resource_configuration_memberships`. A resource configuration with membership
history is rendered from its active memberships, including the empty set.
Before the first membership write, selector-based legacy devices are copied
into membership state so the first add/remove does not unexpectedly discard
other previously selected devices. Configurations without membership history
retain their existing `node_selectors` behavior.

Membership rows are created on demand by the API and are also included in the
development ClickHouse initialization SQL. No transaction or concurrent-writer
guarantee is provided. These routes have the same trusted-network deployment
boundary as the existing admin API; this code does not add authentication, and
the deployed ingress/network policy has not been verified.

Offline validation: `CLICKHOUSE_DB=metranova uv run --python 3.14 pytest -q`.
Existing storage fixtures assume that database name; all storage and Kubernetes
operations in these tests use doubles, not live infrastructure. Validator tests
stub the executable; a real deployment-matching Telegraf check is still required.

## Cloning

This repo uses submodules. When cloining please remember to use the recurse-submodules flag.

- `git clone --recurse-submodules https://github.com/MetrANOVA/api.git`

When using metranova-core it may be required to periodically update
```
uv lock --upgrade-package metranova-core
```

## Workspace structure

This repository uses a `uv` workspace with three packages:

- `admin_api` (workspace root `.`): admin service package
- `metranova` (`packages/metranova`): shared code for `admin_api` and `pipeline`
- `pipeline` (`packages/pipeline`): pipeline service package. Git submodule [MetrANOVA/pipeline](https://github.com/MetrANOVA/pipeline).

## Running services

- `python bin/start-admin-api.py`
- `python bin/start-pipeline.py`
- `python bin/start-redeploy-watcher.py`

## Bootstrapping definitions

Use the bootstrap script to upload type and transformer definitions from YAML.

- `python bin/bootstrap.py --from bootstrap --api-url http://localhost:8000`

Notes:

- `--from` accepts either a single YAML file or a directory.
- Directories are searched recursively for `.yaml` and `.yml` files.
- YAML files must include top-level `types` and/or `transformers`.

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

| Service            | Description                                                                     | Ports                                |
| ------------------ | ------------------------------------------------------------------------------- | ------------------------------------ |
| **api**            | Python 3.14 dev container — your working environment                            | —                                    |
| **clickhouse**     | ClickHouse OLAP database, database `metranova`                                  | `8123` (HTTP), `9000` (native)       |
| **kafka**          | Apache Kafka in KRaft mode (no Zookeeper)                                       | `9092` (internal), `9094` (external) |
| **telegraf**       | Collects SNMP metrics and publishes them to Kafka topic `snmp.telegraf.metrics` | —                                    |
| **snmp-simulator** | Simulates SNMP devices for local development                                    | `161/udp`                            |
| **grafana**        | Grafana with the ClickHouse datasource pre-configured                           | `3000`                               |

### Prerequisites

- Docker Engine + Compose (or Docker Desktop)
- [VS Code](https://code.visualstudio.com/) with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

### Starting the environment

If you haven't already load git submodules.
```
make setup
```

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

| Service          | URL                   | Credentials                  |
| ---------------- | --------------------- | ---------------------------- |
| Grafana          | http://localhost:3000 | `admin` / `admin`            |
| ClickHouse HTTP  | http://localhost:8123 | user: `default`, no password |
| Kafka (external) | `localhost:9094`      | —                            |

### Database schema

SQL scripts in `.devcontainer/clickhouse/init/` are applied automatically in alphabetical order whenever the Compose stack starts. On a brand-new ClickHouse volume they are also picked up by the built-in container init hook.

To reset the database and re-run init scripts:

```bash
docker compose -f .devcontainer/docker-compose.yml down -v
docker compose -f .devcontainer/docker-compose.yml up -d
```

### Making sure stuff works

```
/opt/kafka/bin/kafka-dump-log.sh \
--files /var/lib/kafka/data/metranova_snmp-0/00000000000000000000.log \
--print-data-log
```

## Notes

### Demo Resource Types

```json
{
    "name": "interface",
    "meta_fields": [
        {
            "field_name": "device",
            "field_type": "String",
            "nullable": false
        },
        {
            "field_name": "intf",
            "field_type": "String",
            "nullable": false
        }
    ],
    "data_fields": [
        {
            "field_name": "output",
            "field_type": "Nullable(Int64)",
            "nullable": true
        },
        {
            "field_name": "input",
            "field_type": "Nullable(Int64)",
            "nullable": true
        },
        {
            "field_name": "device",
            "field_type": "String",
            "nullable": false
        },
        {
            "field_name": "intf",
            "field_type": "String",
            "nullable": false
        }
    ],
    "identifier": [
      "device","intf"
    ],
    "ttl": "365 DAY"
}
```

```json
{
    "name": "cpu",
    "meta_fields": [
        {
            "field_name": "device",
            "field_type": "String",
            "nullable": false
        },
        {
            "field_name": "cpu",
            "field_type": "String",
            "nullable": false
        }
    ],
    "data_fields": [
        {
            "field_name": "cpu_total",
            "field_type": "Nullable(Int32)",
            "nullable": true
        },
        {
            "field_name": "device",
            "field_type": "String",
            "nullable": false
        },
        {
            "field_name": "cpu",
            "field_type": "String",
            "nullable": false
        }
    ],
    "identifier": [
      "device","cpu"
    ],
    "ttl": "365 DAY"
}
```

### Demo SNMP Resource Configs

```json
{
  "name": "interface",
  "resource_type": "interface",
  "collector_plugin": "telegraf_vscode",
  "interval": 60,
  "timeout": 15,
  "field_mappings": {
    "intf": {
      "oid": ".1.3.6.1.2.1.31.1.1.1.1",
      "is_tag": true
    },
    "input": {
      "oid": ".1.3.6.1.2.1.31.1.1.1.6"
    },
    "output": {
      "oid": ".1.3.6.1.2.1.31.1.1.1.10"
    }
  },
  "node_selectors": []
}
```

```json
{
  "name": "cpu",
  "resource_type": "cpu",
  "collector_plugin": "telegraf_vscode",
  "interval": 60,
  "timeout": 15,
  "field_mappings": {
    "cpu": {
      "oid": ".1.3.6.1.4.1.9.9.109.1.1.1.1.2",
      "is_tag": true
    },
    "cpu_total": {
      "oid": ".1.3.6.1.4.1.9.9.109.1.1.1.1.7"
    }
  },
  "node_selectors": []
}
```
