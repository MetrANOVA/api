import asyncio
from types import SimpleNamespace

from metranova.storage.base import CollectionField, CollectionType, ConsumerType
from metranova.storage.clickhouse import Clickhouse


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
        if "CREATE TABLE IF NOT EXISTS metranova.definition" in query:
            self.definition_exists = True

    async def insert(self, database, table, data, column_names):
        if database == "metranova" and table == "definition":
            for row in data:
                normalized = list(row)
                normalized.append("2026-04-02 00:00:00")
                self.definition_rows.append(tuple(normalized))

    async def query(self, query: str, parameters=None):
        normalized_query = " ".join(query.strip().split())

        if normalized_query.startswith("EXISTS TABLE metranova.definition"):
            return SimpleNamespace(result_rows=[[1 if self.definition_exists else 0]])

        if normalized_query.startswith(
            "SELECT * FROM metranova.definition WHERE slug = %s"
        ):
            slug = parameters[0]
            candidates = [row for row in self.definition_rows if row[3] == slug]
            row = candidates[-1] if candidates else None
            return SimpleNamespace(result_rows=[row] if row else [])

        if (
            "FROM metranova.definition" in normalized_query
            and "SELECT" in normalized_query
        ):
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

    success, message = asyncio.run(
        storage.create_resource_type(
            name="Interface Traffic",
            slug="interface-traffic",
            collection_type=CollectionType.DATA,
            consumer_type=ConsumerType.KAFKA,
            consumer_config={"topic": "snmp.metrics"},
            fields=[
                CollectionField("if_name", "String", False),
                CollectionField("timestamp", "DateTime64", False),
            ],
            primary_key=["if_name", "timestamp"],
            partition_by="toYYYYMM(timestamp)",
            ttl="365 DAY",
            engine_type="MergeTree()",
            is_replicated=True,
        )
    )

    assert success is True
    assert "successfully created" in message
    assert any(
        "CREATE TABLE `metranova`.`data_interface-traffic`" in query
        for query in storage.client.command_calls
    )

    by_slug = asyncio.run(storage.find_resource_type_by_slug("interface-traffic"))
    assert by_slug is not None
    assert by_slug[3] == "interface-traffic"

    schema = asyncio.run(storage.find_resource_type_schema_by_slug("interface-traffic"))
    assert schema is not None
    assert schema["table"] == "metranova.data_interface-traffic"
    assert schema["columns"][0]["name"] == "collector_id"


def test_clickhouse_update_resource_type_integration(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = FakeAsyncClient()

    asyncio.run(
        storage.create_resource_type(
            name="IP Address",
            slug="ip_address",
            collection_type=CollectionType.DATA,
            consumer_type=ConsumerType.KAFKA,
            consumer_config={"topic": "snmp.metrics", "ext": {"owner": "ops"}},
            fields=[CollectionField("ip", "String", False)],
            primary_key=["ip"],
            partition_by="toYYYYMM(insert_time)",
            ttl="365 DAY",
            engine_type="MergeTree()",
            is_replicated=True,
        )
    )

    success, message = asyncio.run(
        storage.update_resource_type(
            slug="ip_address",
            fields=[CollectionField("hostname", "String", True)],
            consumer_config_updates={"topic": "snmp.metrics.v2"},
            ext_updates={"team": "network"},
        )
    )

    assert success is True
    assert "__v2" in message
    assert any(
        "ALTER TABLE `metranova`.`data_ip_address`" in query
        for query in storage.client.command_calls
    )

    all_rows = asyncio.run(storage.find_all_resource_types())
    assert all_rows is not None
    assert len(all_rows) == 2
    assert all_rows[-1]["slug"] == "ip_address"


def test_clickhouse_create_resource_type_rejects_duplicate_slug(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = FakeAsyncClient()

    first = asyncio.run(
        storage.create_resource_type(
            name="Interface Traffic",
            slug="interface-traffic",
            collection_type=CollectionType.DATA,
            consumer_type=ConsumerType.KAFKA,
            consumer_config={"topic": "snmp.metrics"},
            fields=[CollectionField("if_name", "String", False)],
            primary_key=["if_name"],
            partition_by="toYYYYMM(timestamp)",
            ttl="365 DAY",
            engine_type="MergeTree()",
            is_replicated=True,
        )
    )
    second = asyncio.run(
        storage.create_resource_type(
            name="Interface Traffic",
            slug="interface-traffic",
            collection_type=CollectionType.DATA,
            consumer_type=ConsumerType.KAFKA,
            consumer_config={"topic": "snmp.metrics"},
            fields=[CollectionField("if_name", "String", False)],
            primary_key=["if_name"],
            partition_by="toYYYYMM(timestamp)",
            ttl="365 DAY",
            engine_type="MergeTree()",
            is_replicated=True,
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
