import asyncio
from types import SimpleNamespace

from metranova_core import CollectionField, CollectionType, ConsumerType
from metranova_core import Clickhouse
from metranova_core.storage.base import MetaCollectionField


class FakeAsyncClient:
    def __init__(self):
        self.command_calls = []
        self.definition_rows = []
        self.definition_exists = False
        self.closed = False

    async def ping(self):
        return True

    def close(self):
        self.closed = True

    async def command(self, query: str):
        self.command_calls.append(query)
        if "CREATE TABLE IF NOT EXISTS" in query and ".definition" in query:
            self.definition_exists = True

    async def insert(self, database, table, data, column_names):
        if table == "definition":
            for row in data:
                normalized = list(row)
                normalized.append("2026-04-02 00:00:00")
                self.definition_rows.append(tuple(normalized))

    async def query(self, query: str, parameters=None):
        normalized_query = " ".join(query.strip().split())

        if normalized_query.startswith("EXISTS TABLE") and ".definition" in normalized_query:
            return SimpleNamespace(result_rows=[[1 if self.definition_exists else 0]])

        if "SELECT * FROM" in normalized_query and ".definition WHERE slug = %s" in normalized_query:
            slug = parameters[0]
            candidates = [row for row in self.definition_rows if row[3] == slug]
            row = candidates[-1] if candidates else None
            named_rows = []
            if row:
                keys = [
                    "id",
                    "ref",
                    "name",
                    "slug",
                    "type",
                    "consumer_type",
                    "consumer_config",
                    "fields",
                    "primary_key",
                    "partition_by",
                    "ttl",
                    "engine_type",
                    "is_replicated",
                    "updated_at",
                ]
                named_rows = [dict(zip(keys, row))]
            return SimpleNamespace(
                result_rows=[row] if row else [],
                named_results=lambda: iter(named_rows),
            )

        if "SELECT" in normalized_query and ".definition" in normalized_query:
            return SimpleNamespace(result_rows=self.definition_rows)

        if normalized_query.startswith("DESCRIBE TABLE"):
            return SimpleNamespace(
                result_rows=[
                    (
                        "collector_id",
                        "LowCardinality(String)",
                        None,
                        None,
                        None,
                        None,
                        None,
                    ),
                    ("if_name", "String", None, None, None, None, None),
                    ("timestamp", "DateTime64", None, None, None, None, None),
                ]
            )

        return SimpleNamespace(result_rows=[])


def test_clickhouse_create_and_schema_flow_with_hyphen_slug(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = FakeAsyncClient()
    async def mock_get_ch_types():
        return ["String", "DateTime64", "Float64"]

    storage._get_ch_types = mock_get_ch_types

    success, message = asyncio.run(
        storage.create_resource_type(
            name="Interface Traffic",
            slug="interface-traffic",
            data_fields=[
                CollectionField("if_name", "String", False),
                CollectionField("timestamp", "DateTime64", False),
            ],
            meta_fields=[
                MetaCollectionField("if_name", "String", False),
            ],
            identifier=["if_name", "timestamp"],
            ttl="365 DAY",
            engine_type="MergeTree()",
        )
    )

    assert success is True
    assert "successfully created" in message
    assert any(
        "CREATE TABLE `metranova`.`data_interface-traffic`" in query
        for query in storage.client.command_calls
    )
    assert any(
        "CREATE TABLE `metranova`.`meta_interface-traffic`" in query
        for query in storage.client.command_calls
    )

    by_slug = asyncio.run(storage.find_resource_type_by_slug("interface-traffic"))
    assert by_slug is not None

    schema = asyncio.run(storage.find_resource_type_schema_by_slug("interface-traffic"))
    assert schema is not None
    assert schema["columns"][0]["name"] == "collector_id"


def test_clickhouse_update_resource_type_integration(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = FakeAsyncClient()
    async def mock_get_ch_types():
        return ["String", "DateTime64", "Float64"]

    storage._get_ch_types = mock_get_ch_types

    asyncio.run(
        storage.create_resource_type(
            name="IP Address",
            slug="ip_address",
            data_fields=[CollectionField("ip", "String", False)],
            meta_fields=[MetaCollectionField("ip", "String", False)],
            identifier=["ip"],
            ttl="365 DAY",
            engine_type="MergeTree()",
        )
    )

    success, message = asyncio.run(
        storage.update_resource_type(
            slug="ip_address",
            fields=[CollectionField("hostname", "String", True)],
        )
    )

    assert success is True
    assert "__v2" in message
    assert any(
        "ALTER TABLE" in query and "ip_address" in query
        for query in storage.client.command_calls
    )

    all_rows = asyncio.run(storage.find_all_resource_types())
    assert all_rows is not None
    assert len(all_rows) == 3
    assert all_rows[-1]["slug"] == "ip_address"


def test_clickhouse_create_resource_type_rejects_duplicate_slug(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = FakeAsyncClient()
    async def mock_get_ch_types():
        return ["String", "DateTime64", "Float64"]

    storage._get_ch_types = mock_get_ch_types

    first = asyncio.run(
        storage.create_resource_type(
            name="Interface Traffic",
            slug="interface-traffic",
            data_fields=[CollectionField("if_name", "String", False)],
            meta_fields=[MetaCollectionField("if_name", "String", False)],
            identifier=["if_name"],
            ttl="365 DAY",
            engine_type="MergeTree()",
        )
    )
    second = asyncio.run(
        storage.create_resource_type(
            name="Interface Traffic",
            slug="interface-traffic",
            data_fields=[CollectionField("if_name", "String", False)],
            meta_fields=[MetaCollectionField("if_name", "String", False)],
            identifier=["if_name"],
            ttl="365 DAY",
            engine_type="MergeTree()",
        )
    )

    assert first[0] is True
    assert second[0] is False
    assert "already exists" in second[1]


def test_clickhouse_find_schema_returns_none_for_missing_slug(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = FakeAsyncClient()

    schema = asyncio.run(storage.find_resource_type_schema_by_slug("missing-slug"))
    assert schema is None


def test_clickhouse_update_resource_type_returns_not_found_for_missing_slug(
    monkeypatch,
):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = FakeAsyncClient()

    success, message = asyncio.run(
        storage.update_resource_type(
            slug="missing-slug",
            fields=[CollectionField("hostname", "String", True)],
            consumer_config_updates={"topic": "snmp.metrics.v2"},
            ext_updates={"team": "network"},
        )
    )

    assert success is False
    assert "not found" in message.lower()
