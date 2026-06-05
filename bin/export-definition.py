#!/usr/bin/env python3
"""
Export a definition and its associated data from ClickHouse to a .sql file.

Run this script on the prod API Kubernetes pod. It uses the same connection
environment variables as the API itself:

    CLICKHOUSE_HOST      (default: localhost)
    CLICKHOUSE_PORT      (default: 8123)
    CLICKHOUSE_DB        (default: metranova)
    CLICKHOUSE_USERNAME  (default: default)
    CLICKHOUSE_PASSWORD  (default: "")

Usage:
    python bin/export-definition.py --definition-slug interface-traffic
    python bin/export-definition.py --definition-slug interface-traffic --output /tmp/dump.sql

The resulting .sql file can be imported into a local ClickHouse with:
    docker exec -i clickhouse clickhouse-client --multiquery < dump.sql
"""

import argparse
import os
import re
import sys
from pathlib import Path

import clickhouse_connect

# ---------------------------------------------------------------------------
# DDL adaptation — strip cluster-specific syntax so the DDL runs locally
# ---------------------------------------------------------------------------

_ENGINE_SUBS = [
    (re.compile(r"\bReplicatedCoalescingMergeTree\b"), "CoalescingMergeTree"),
    (re.compile(r"\bReplicatedMergeTree\b"), "MergeTree"),
    (re.compile(r"\bReplicatedReplacingMergeTree\b"), "ReplacingMergeTree"),
    (re.compile(r"\bReplicatedAggregatingMergeTree\b"), "AggregatingMergeTree"),
    (re.compile(r"\bReplicatedSummingMergeTree\b"), "SummingMergeTree"),
    # Strip replica path/name args: ReplicatedMergeTree('/path', '{replica}')
    (re.compile(r"(MergeTree)\s*\('[^']*',\s*'[^']*'\)"), r"\1()"),
    (re.compile(r"(MergeTree)\s*\('[^']*',\s*'[^']*',\s*"), r"\1("),
]
_ON_CLUSTER_RE = re.compile(r"\s+ON CLUSTER\s+\S+", re.IGNORECASE)
_CREATE_RE = re.compile(r"\bCREATE TABLE\b", re.IGNORECASE)


def adapt_ddl(ddl: str) -> str:
    ddl = _CREATE_RE.sub("CREATE TABLE IF NOT EXISTS", ddl, count=1)
    ddl = _ON_CLUSTER_RE.sub("", ddl)
    for pattern, replacement in _ENGINE_SUBS:
        ddl = pattern.sub(replacement, ddl)
    return ddl


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------


def write_insert(
    f, client, table: str, database: str, where: str = "", limit: int = 0
) -> int:
    """
    Write an INSERT block in JSONEachRow format for the given table/filter.
    Returns the number of rows written.
    """
    sql = f"SELECT * FROM `{database}`.`{table}`"
    if where:
        sql += f" WHERE {where}"
    if limit:
        sql += f" LIMIT {limit}"

    data: bytes = client.raw_query(sql, fmt="JSONEachRow")
    if not data.strip():
        return 0

    row_count = data.count(b"\n") + (1 if not data.endswith(b"\n") else 0)
    f.write(f"-- {table} ({row_count} row(s))\n")
    f.write(f"INSERT INTO `{database}`.`{table}` FORMAT JSONEachRow\n")
    f.write(data.decode("utf-8"))
    if not data.endswith(b"\n"):
        f.write("\n")
    f.write(";\n\n")
    return row_count


def write_ddl(f, client, table: str, database: str) -> bool:
    """Write an adapted CREATE TABLE IF NOT EXISTS statement. Returns False if table doesn't exist."""
    try:
        result = client.query(f"SHOW CREATE TABLE `{database}`.`{table}`")
    except Exception:
        return False

    if not result.result_rows:
        return False

    ddl = adapt_ddl(result.result_rows[0][0])
    f.write(f"-- DDL for {table}\n")
    f.write(ddl + ";\n\n")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export a definition + transformers + data/meta table DDL to a .sql dump file"
    )
    parser.add_argument(
        "--definition-slug", required=True, help="Slug of the definition to export"
    )
    parser.add_argument("--output", help="Output file path (default: <slug>_dump.sql)")
    args = parser.parse_args()

    slug = args.definition_slug
    database = os.getenv("CLICKHOUSE_DB", "metranova")

    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        database=database,
        username=os.getenv("CLICKHOUSE_USERNAME", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )

    # Step 1: resolve definition by slug → get id and ref
    def_result = client.query(
        f"SELECT id, ref FROM `{database}`.`definition` WHERE slug = '{slug}' ORDER BY updated_at"
    )
    if not def_result.result_rows:
        print(f"Error: no definition found with slug '{slug}'", file=sys.stderr)
        return 1

    def_ids = [row[0] for row in def_result.result_rows]
    def_ids_sql = ", ".join(f"'{i}'" for i in def_ids)
    print(f"Definition ids: {def_ids}")

    # Step 2: resolve transformers using definition.id → transformer.definition_ref
    t_result = client.query(
        f"SELECT id, ref FROM `{database}`.`transformer` WHERE definition_ref IN ({def_ids_sql})"
    )
    t_ids = [row[0] for row in t_result.result_rows]
    t_refs = [row[1] for row in t_result.result_rows]
    t_ids_sql = ", ".join(f"'{i}'" for i in t_ids) if t_ids else None
    t_refs_sql = ", ".join(f"'{r}'" for r in t_refs) if t_refs else None
    print(f"Transformer ids: {t_ids}")

    # Step 3: resolve transformer_columns using transformer.ref → transformer_column.transformer_ref
    if t_refs_sql:
        tc_result = client.query(
            f"SELECT id FROM `{database}`.`transformer_column` WHERE transformer_ref IN ({t_refs_sql})"
        )
        print(f"Transformer column rows: {len(tc_result.result_rows)}")
    else:
        tc_result = None

    output_path = Path(args.output or f"{slug}_dump.sql")

    with output_path.open("w") as f:
        f.write(f"-- ClickHouse dump: definition slug '{slug}'\n")
        f.write(
            f"-- Import with: docker exec -i clickhouse clickhouse-client --multiquery < {output_path.name}\n\n"
        )

        # definition rows (identified by slug)
        n = write_insert(f, client, "definition", database, where=f"slug = '{slug}'")
        print(f"  definition:         {n} row(s)")

        # transformer rows (linked via definition.id → transformer.definition_ref)
        n = write_insert(
            f,
            client,
            "transformer",
            database,
            where=f"definition_ref IN ({def_ids_sql})",
        )
        print(f"  transformer:        {n} row(s)")

        # transformer_column rows (linked via transformer.ref → transformer_column.transformer_ref)
        if t_refs_sql:
            n = write_insert(
                f,
                client,
                "transformer_column",
                database,
                where=f"transformer_ref IN ({t_refs_sql})",
            )
            print(f"  transformer_column: {n} row(s)")
        else:
            print("  transformer_column: 0 row(s) (no transformers found)")

        # data / meta tables — DDL only
        for prefix in ("data", "meta"):
            table = f"{prefix}_{slug}"

            if not write_ddl(f, client, table, database):
                print(f"  {table}: skipped (table does not exist)")
                continue

            print(f"  {table}: DDL exported")

    print(f"\nWrote {output_path}")
    print(
        f"To import: docker exec -i clickhouse clickhouse-client --multiquery < {output_path.name}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
