# MetrANOVA API

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

For pipeline-created device/interface metadata, use the flow compatibility
bootstrap below instead of importing the legacy device/interface YAML files.

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
            "field_name": "node",
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
            "field_name": "tx_bytes",
            "field_type": "Nullable(Int64)",
            "nullable": true
        },
        {
            "field_name": "rx_bytes",
            "field_type": "Nullable(Int64)",
            "nullable": true
        },
        {
            "field_name": "node",
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
      "node","intf"
    ],
    "ttl": "365 DAY"
}
```

```json
{
    "name": "cpu",
    "meta_fields": [
        {
            "field_name": "node",
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
            "field_name": "node",
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
      "node","cpu"
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
    "rx_bytes": {
      "oid": ".1.3.6.1.2.1.31.1.1.1.6"
    },
    "tx_bytes": {
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
# Flow metadata compatibility bootstrap

For installations using pipeline-created metadata tables, run this **instead of
loading the legacy `bootstrap/types/device.yaml` and `interface.yaml` definitions**.
Those definitions have a different interface schema. This is a manual demo
compatibility step, not a schema ownership migration.

```bash
# Inside the installed API environment, using its ClickHouse connection variables:
python bin/bootstrap-flow-metadata.py --dry-run
python bin/bootstrap-flow-metadata.py

# In the development container:
docker exec -w /app api_devcontainer-api-1 /app/.venv/bin/python bin/bootstrap-flow-metadata.py
docker compose -p api_devcontainer -f .devcontainer/docker-compose.yml up -d --build pipeline-flow
python3 bin/inject-kafka-message.py debug/sample_flow_message.json --topic metranova_flow
```

**The automated metadata writers are not ready for use.** The current SNMP
worker sends partial PUTs, which the API no longer supports. Both it and the CDS
importer use PUT for ordinary changes; these must create new versions through
POST instead. PUT remains available for deliberate corrections to a specified
historical version, and the API continues to enforce required fields.

The optional SNMP index worker uses the same private meta-manager image as the
Kubernetes deployment. Authenticate to the registry if required. The published
image must include the new `metranova-snmp-index-worker` command. Image
verification is deferred until publication. After the write behavior is fixed,
start the API (`python bin/start-admin-api.py` inside the devcontainer) first,
then enable the worker:

```bash
docker compose -p api_devcontainer -f .devcontainer/docker-compose.yml \
  --profile metadata pull snmp-index-worker
docker compose -p api_devcontainer -f .devcontainer/docker-compose.yml \
  --profile metadata up -d snmp-index-worker
python3 bin/inject-kafka-message.py debug/sample_snmp_interface_message.json
```

The worker uses its own Kafka group and updates `flow_index` through the API,
retrying when imported metadata is not present yet. It does not run CDS import;
run `metranova-meta-manager` separately with your CDS settings. No sibling source
checkout is needed to run the worker.

The bootstrap creates `meta_device` and `meta_interface` when absent, or validates
existing pipeline schemas before adding API timestamps and policy columns.
It registers the actual columns with identifier `id`, preserving existing
records, IDs, references, engines, and additional columns. Conflicting existing
definitions are rejected rather than overwritten. DDL uses the detected
ClickHouse cluster; run one bootstrap process at a time. Reruns are safe.
Neither external metadata populators nor their tables are changed.

The dev flow service consumes `metranova_flow` with its own consumer group,
does not run metadata file import, and refreshes its metadata cache every ten
seconds. Populate metadata through the API/meta-manager before expecting
resolved flow references.

`debug/sample_flow_message.json` is synthetic, not a production capture or proof
of the production message format. Its timestamps are illustrative; old replayed
data may be removed by retention policies. Use a fresh production capture to
verify the actual message format.

`debug/sample_snmp_interface_message.json` originated as a production capture,
but its timestamp was changed for local replay and `tags.index` was added
synthetically. These edits do not establish that production Telegraf emits an
index. Confirm `tags.index` from a fresh production capture and the deployed
Telegraf configuration before relying on SNMP index enrichment.

The meta-manager uses its own read-only ClickHouse connection to discover
distinct node names from recent `data_interface` records and exporter device
IDs from recent `data_flow` records. It tolerates absent data tables, not
incompatible schemas, and also syncs existing devices and enumerates their
CDS interfaces. Metadata writes still go through the API.
SNMP index enrichment is a separate meta-manager worker; see that repository's
README. Edge classification is left unchanged. Existing unresolved flow
interface references cannot be reconstructed because raw indices are not
retained by the current shared flow processor.
