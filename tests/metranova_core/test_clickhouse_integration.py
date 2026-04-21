import asyncio
from types import SimpleNamespace

from metranova import CollectionField, CollectionType, ConsumerType
from metranova import Clickhouse
from metranova.storage.base import MetadataField


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

    _DEFINITION_COLUMNS = [
        "id", "ref", "name", "slug", "meta_fields", "data_fields",
        "identifier", "ttl", "engine_type", "is_replicated", "updated_at",
    ]

    async def query(self, query: str, parameters=None):
        normalized_query = " ".join(query.strip().split())

        if normalized_query.startswith("EXISTS TABLE") and ".definition" in normalized_query:
            return SimpleNamespace(result_rows=[[1 if self.definition_exists else 0]], row_count=1)

        if "SELECT * FROM" in normalized_query and ".definition WHERE slug = %s" in normalized_query:
            slug = parameters[0]
            candidates = [row for row in self.definition_rows if row[3] == slug]
            row = candidates[-1] if candidates else None
            rows = [row] if row else []
            cols = FakeAsyncClient._DEFINITION_COLUMNS

            def named_results():
                for r in rows:
                    yield dict(zip(cols, r))

            return SimpleNamespace(result_rows=rows, row_count=len(rows), named_results=named_results)

        if (
            ".definition" in normalized_query
            and "SELECT" in normalized_query
        ):
            return SimpleNamespace(result_rows=self.definition_rows, row_count=len(self.definition_rows))

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
                ],
                row_count=3,
            )

        return SimpleNamespace(result_rows=[], row_count=0)


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
            data_fields=[
                CollectionField("if_name", "String", False),
                CollectionField("timestamp", "DateTime64", False),
            ],
            meta_fields=[
                MetadataField(name="if_name", type="String", nullable=False),
                MetadataField(name="timestamp", type="DateTime64", nullable=False),
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
    assert by_slug["slug"] == "interface-traffic"

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
            data_fields=[CollectionField("ip", "String", False)],
            meta_fields=[MetadataField(name="ip", type="String", nullable=False)],
            identifier=["ip"],
            ttl="365 DAY",
            engine_type="MergeTree()",
        )
    )

    success, message = asyncio.run(
        storage.update_resource_type(
            slug="ip-address",
            fields=[CollectionField("hostname", "String", True)],
        )
    )

    assert success is True
    assert "__v2" in message
    assert any(
        "ALTER TABLE" in query and "ip-address" in query
        for query in storage.client.command_calls
    )

    all_rows = asyncio.run(storage.find_all_resource_types())
    assert all_rows is not None
    assert len(all_rows) == 2
    assert all_rows[-1]["slug"] == "ip-address"


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
            data_fields=[CollectionField("if_name", "String", False)],
            meta_fields=[MetadataField(name="if_name", type="String", nullable=False)],
            identifier=["if_name"],
            ttl="365 DAY",
            engine_type="MergeTree()",
        )
    )
    second = asyncio.run(
        storage.create_resource_type(
            name="Interface Traffic",
            data_fields=[CollectionField("if_name", "String", False)],
            meta_fields=[MetadataField(name="if_name", type="String", nullable=False)],
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
