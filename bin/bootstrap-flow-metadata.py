#!/usr/bin/env python3
"""Temporary demo bootstrap; run with the API's ClickHouse environment variables."""

import argparse
import asyncio
import sys

from metranova.storage.clickhouse import Clickhouse

RESERVED_COLUMNS = {
    "id", "ref", "hash", "created_at", "insert_time", "updated_at", "tag",
    "policy_level", "policy_scope", "policy_originator", "ext",
}

DEVICE_FIELDS = [
    ("type", "LowCardinality(String)"),
    ("loopback_ip", "Array(IPv6)"),
    ("management_ip", "Array(IPv6)"),
    ("hostname", "LowCardinality(Nullable(String))"),
    ("location_name", "LowCardinality(Nullable(String))"),
    ("location_type", "Array(LowCardinality(Nullable(String)))"),
    ("city_name", "LowCardinality(Nullable(String))"),
    ("continent_name", "LowCardinality(Nullable(String))"),
    ("country_name", "LowCardinality(Nullable(String))"),
    ("country_code", "LowCardinality(Nullable(String))"),
    ("country_sub_name", "LowCardinality(Nullable(String))"),
    ("country_sub_code", "LowCardinality(Nullable(String))"),
    ("latitude", "Nullable(Float64)"),
    ("longitude", "Nullable(Float64)"),
    ("manufacturer", "LowCardinality(Nullable(String))"),
    ("model", "LowCardinality(Nullable(String))"),
    ("network", "LowCardinality(Nullable(String))"),
    ("os", "LowCardinality(Nullable(String))"),
    ("role", "LowCardinality(Nullable(String))"),
    ("state", "LowCardinality(Nullable(String))"),
]
INTERFACE_FIELDS = [
    ("type", "LowCardinality(String)"),
    ("description", "Nullable(String)"),
    ("device_id", "String"),
    ("device_ref", "Nullable(String)"),
    ("edge", "Bool"),
    ("flow_index", "Nullable(UInt32)"),
    ("ipv4", "Nullable(IPv4)"),
    ("ipv6", "Nullable(IPv6)"),
    ("name", "String"),
    ("speed", "Nullable(UInt64)"),
    ("circuit_id", "Array(String)"),
    ("circuit_ref", "Array(Nullable(String))"),
    ("peer_as_id", "Nullable(UInt32)"),
    ("peer_as_ref", "Nullable(String)"),
    ("peer_interface_ipv4", "Nullable(IPv4)"),
    ("peer_interface_ipv6", "Nullable(IPv6)"),
    ("lag_member_interface_id", "Array(LowCardinality(String))"),
    ("lag_member_interface_ref", "Array(Nullable(String))"),
    ("port_interface_id", "LowCardinality(Nullable(String))"),
    ("port_interface_ref", "Nullable(String)"),
    ("remote_interface_id", "LowCardinality(Nullable(String))"),
    ("remote_interface_ref", "Nullable(String)"),
    ("remote_organization_id", "LowCardinality(Nullable(String))"),
    ("remote_organization_ref", "Nullable(String)"),
]
BASE_FIELDS = [
    ("id", "String"), ("ref", "String"), ("hash", "String"),
    ("insert_time", "DateTime DEFAULT now()"), ("ext", "JSON"),
    ("tag", "Array(LowCardinality(String))"),
]
COMPATIBILITY_FIELDS = [
    ("created_at", "DateTime DEFAULT insert_time"),
    ("updated_at", "DateTime DEFAULT insert_time"),
    ("policy_level", "LowCardinality(String) DEFAULT ''"),
    ("policy_scope", "Array(LowCardinality(String))"),
    ("policy_originator", "LowCardinality(String) DEFAULT ''"),
]


def is_nullable(column_type: str) -> bool:
    return column_type.startswith(("Nullable(", "LowCardinality(Nullable("))


async def ensure_definition_table(storage: Clickhouse) -> None:
    table = storage._qualified_table_name("definition")
    if await storage._table_exists("definition"):
        return
    cluster = await storage._get_on_cluster_clause(storage.metadata_engine)
    await storage.client.command(f"""
        CREATE TABLE IF NOT EXISTS {table}{cluster}
        (
            id String,
            ref String,
            name String,
            slug String,
            type Enum8('data' = 1, 'metadata' = 2),
            meta_fields Array(Tuple(
                field_name String, field_type String, nullable Bool, table String
            )),
            data_fields Array(Tuple(
                field_name String, field_type String, nullable Bool
            )),
            identifier Array(String),
            ttl String,
            engine_type String DEFAULT 'CoalescingMergeTree',
            is_replicated Bool DEFAULT true,
            updated_at DateTime DEFAULT now()
        )
        ENGINE = {storage._validated_engine_name(storage.metadata_engine)}()
        ORDER BY ref
    """)


async def bootstrap_flow_metadata(storage, *, dry_run: bool = False) -> list[str]:
    """Preflight both tables, then add columns and register their actual schemas."""
    plans = []
    cluster = await storage._get_on_cluster_clause()
    for slug, fields in (("device", DEVICE_FIELDS), ("interface", INTERFACE_FIELDS)):
        table = f"meta_{slug}"
        result = await storage.client.query(
            "SELECT name, type FROM system.columns "
            "WHERE database = {db:String} AND table = {table:String} ORDER BY position",
            parameters={"db": storage.database, "table": table},
        )
        columns = dict(result.result_rows)
        existing = await storage.find_resource_type_by_slug(slug)
        if existing and existing["identifier"] != ["id"]:
            raise ValueError(
                f"Definition '{slug}' conflicts with adoption: expected metadata "
                "with identifier ['id']. No existing definitions are overwritten."
            )
        if columns:
            for name, expected in BASE_FIELDS + fields:
                actual = columns.get(name)
                expected = expected.split(" DEFAULT ")[0]
                if actual != expected and not (
                    name == "ext" and actual and actual.startswith("JSON")
                ):
                    raise ValueError(
                        f"{table}.{name}: expected {expected}, found {actual!r}; "
                        "no columns have been changed."
                    )
            for name, expected in COMPATIBILITY_FIELDS:
                if name in columns and columns[name] != expected.split(" DEFAULT ")[0]:
                    raise ValueError(f"{table}.{name} has incompatible type {columns[name]}")
        if existing:
            actual_fields = [
                (f["field_name"], f["field_type"], f["nullable"], f["table"])
                for f in existing["meta_fields"]
            ]
            expected_fields = [
                (name, typ, is_nullable(typ), "")
                for name, typ in columns.items()
                if name not in RESERVED_COLUMNS or name == "id"
            ]
            if actual_fields != expected_fields:
                raise ValueError(
                    f"Definition '{slug}' does not match physical columns; "
                    "no tables or definitions have been changed."
                )
        plans.append((slug, fields, columns, existing))

    messages = []
    for slug, fields, columns, existing in plans:
        table = f"meta_{slug}"
        qualified = storage._qualified_table_name(table)
        if not columns:
            messages.append(f"Create {table}")
            if not dry_run:
                declarations = ",\n".join(
                    f"`{name}` {typ}" for name, typ in BASE_FIELDS + fields
                )
                await storage.client.command(
                    f"CREATE TABLE IF NOT EXISTS {qualified}{cluster} "
                    f"({declarations}) ENGINE = {storage.metadata_engine}() "
                    "ORDER BY (ref, id, insert_time)"
                )
        for name, typ in COMPATIBILITY_FIELDS:
            if name not in columns:
                messages.append(f"Add {table}.{name}: {typ}")
                if not dry_run:
                    await storage.client.command(
                        f"ALTER TABLE {qualified}{cluster} ADD COLUMN IF NOT EXISTS `{name}` {typ}"
                    )
        if dry_run:
            messages.append(
                f"Definition {slug} already matches" if existing
                else f"Register {slug} definition from actual columns"
            )
            continue
        result = await storage.client.query(
            "SELECT name, type FROM system.columns "
            "WHERE database = {db:String} AND table = {table:String} ORDER BY position",
            parameters={"db": storage.database, "table": table},
        )
        meta_fields = [
            (name, typ, is_nullable(typ), "")
            for name, typ in result.result_rows
            if name not in RESERVED_COLUMNS or name == "id"
        ]
        if existing:
            actual_fields = [
                (f["field_name"], f["field_type"], f["nullable"], f["table"])
                for f in existing["meta_fields"]
            ]
            if actual_fields != meta_fields:
                raise ValueError(
                    f"Definition '{slug}' does not match physical columns; "
                    "review it manually. Existing definition left unchanged."
                )
            messages.append(f"Definition {slug} already matches")
            continue
        tables = await storage.client.query(
            "SELECT engine FROM system.tables WHERE database = {db:String} "
            "AND name = {table:String}",
            parameters={"db": storage.database, "table": table},
        )
        engine = tables.result_rows[0][0]
        data_columns = await storage.client.query(
            "SELECT name, type FROM system.columns "
            "WHERE database = {db:String} AND table = {table:String} ORDER BY position",
            parameters={"db": storage.database, "table": f"data_{slug}"},
        )
        data_fields = [
            (name, typ, is_nullable(typ))
            for name, typ in data_columns.result_rows
            if name != "insert_time"
        ]
        await storage.client.insert(
            database=storage.database, table="definition",
            column_names=[
                "id", "ref", "name", "slug", "type", "meta_fields",
                "data_fields", "identifier", "ttl", "engine_type", "is_replicated",
            ],
            data=[[
                f"def_{slug}", f"def_{slug}__v1", slug, slug, "metadata",
                meta_fields, data_fields, ["id"], "", engine, engine.startswith("Replicated"),
            ]],
        )
        messages.append(f"Registered {slug}")
    return messages


async def run(dry_run: bool):
    storage = Clickhouse()
    try:
        await storage.connect()
        if not dry_run:
            await ensure_definition_table(storage)
        for message in await bootstrap_flow_metadata(storage, dry_run=dry_run):
            print(message)
    finally:
        await storage.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    try:
        asyncio.run(run(args.dry_run))
    except Exception as exc:
        print(f"Bootstrap failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
