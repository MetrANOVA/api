import asyncio
from types import SimpleNamespace

import pytest

from metranova.storage.base import CollectionField
from metranova.storage.clickhouse import Clickhouse


class DummySyncClient:
    def __init__(self):
        self.commands = []
        self.closed = False

    def command(self, query):
        self.commands.append(query)

    def close(self):
        self.closed = True


class DummyAsyncClient:
    def __init__(self, ping_value=True):
        self.ping_value = ping_value
        self.insert_calls = []
        self.query_result = [[1]]
        self.command_calls = []
        self.closed = False

    async def ping(self):
        return self.ping_value

    async def insert(self, **kwargs):
        self.insert_calls.append(kwargs)

    async def query(self, query):
        return SimpleNamespace(result_rows=self.query_result)

    async def command(self, query):
        self.command_calls.append(query)

    def close(self):
        self.closed = True


def test_init_sets_config_from_env(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    monkeypatch.setenv("CLICKHOUSE_HOST", "ch-host")
    monkeypatch.setenv("CLICKHOUSE_DB", "metranova")
    monkeypatch.setenv("CLICKHOUSE_USERNAME", "user1")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "pass1")
    monkeypatch.setenv("CLICKHOUSE_CLUSTER_NAME", "cluster-a")

    storage = Clickhouse()

    assert storage.host == "ch-host"
    assert storage.database == "metranova"
    assert storage.username == "user1"
    assert storage.password == "pass1"
    assert storage.cluster_name == "cluster-a"
    assert storage.client is None


def test_init_calls_create_database_when_not_skipped(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "false")
    called = {"value": False}

    def fake_create_database(self):
        called["value"] = True

    monkeypatch.setattr(Clickhouse, "create_database", fake_create_database)
    Clickhouse()

    assert called["value"] is True


def test_create_database_executes_create_db_query(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    monkeypatch.setenv("CLICKHOUSE_DB", "metranova")
    monkeypatch.setenv("CLICKHOUSE_CLUSTER_NAME", "cluster-a")
    client = DummySyncClient()

    monkeypatch.setattr(
        "metranova.storage.clickhouse.clickhouse_connect.create_client",
        lambda **kwargs: client,
    )

    storage = Clickhouse()
    storage.create_database()

    assert len(client.commands) == 1
    assert "CREATE DATABASE IF NOT EXISTS metranova" in client.commands[0]
    assert "ON CLUSTER 'cluster-a'" in client.commands[0]
    assert client.closed is True


def test_close_closes_client(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    storage.close()

    assert storage.client.closed is True


def test_create_returns_initialized_instance(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    state = {"connect": False, "ensure": False}

    async def fake_connect(self):
        state["connect"] = True

    async def fake_ensure(self):
        state["ensure"] = True

    monkeypatch.setattr(Clickhouse, "connect", fake_connect)
    monkeypatch.setattr(Clickhouse, "_ensure_definition_table", fake_ensure)

    storage = asyncio.run(Clickhouse.create())

    assert isinstance(storage, Clickhouse)
    assert state["connect"] is True
    assert state["ensure"] is True


def test_connect_creates_async_client_and_pings(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    monkeypatch.setenv("CLICKHOUSE_SECURE", "true")
    created = {}
    async_client = DummyAsyncClient()

    async def fake_create_async_client(**kwargs):
        created.update(kwargs)
        return async_client

    monkeypatch.setattr(
        "metranova.storage.clickhouse.clickhouse_connect.create_async_client",
        fake_create_async_client,
    )

    storage = Clickhouse()
    asyncio.run(storage.connect())

    assert storage.client is async_client
    assert created["host"] == storage.host
    assert created["database"] == storage.database
    assert created["secure"] is True


def test_is_connected_true(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient(ping_value=True)

    assert asyncio.run(storage.is_connected()) is True


def test_is_connected_false_when_client_missing(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()

    assert asyncio.run(storage.is_connected()) is False


def test_create_resource_type_inserts_definition_row(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    fields = [
        CollectionField(field_name="if_name", field_type="String", nullable=True),
        CollectionField(field_name="rx_bps", field_type="Float64", nullable=False),
    ]

    success = asyncio.run(
        storage.create_resource_type(
            name="Interface Traffic",
            slug="interface-traffic",
            type="data",
            consumer_type="kafka",
            consumer_config={"topic": "snmp.metrics"},
            fields=fields,
            primary_key=["host", "timestamp"],
            partition_by="toYYYYMM(timestamp)",
            ttl="365 DAY",
        )
    )

    assert success is True
    assert len(storage.client.insert_calls) == 1
    call = storage.client.insert_calls[0]
    assert call["database"] == "metranova"
    assert call["table"] == "definition"
    assert call["data"][0][3] == "interface-traffic"
    assert call["data"][0][7] == [
        ("if_name", "String", True),
        ("rx_bps", "Float64", False),
    ]


def test_create_resource_type_returns_false_on_insert_error(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    async def failing_insert(**kwargs):
        raise RuntimeError("insert failed")

    storage.client.insert = failing_insert

    success = asyncio.run(
        storage.create_resource_type(
            name="Interface Traffic",
            slug="interface-traffic",
            type="data",
            consumer_type="kafka",
            consumer_config={"topic": "snmp.metrics"},
            fields=[CollectionField("if_name", "String", True)],
            primary_key=["host", "timestamp"],
            partition_by="toYYYYMM(timestamp)",
            ttl="365 DAY",
        )
    )

    assert success is False


def test_ensure_definition_table_skips_when_exists(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()
    storage.client.query_result = [[1]]

    asyncio.run(storage._ensure_definition_table())

    assert storage.client.command_calls == []


def test_ensure_definition_table_creates_when_missing(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()
    storage.client.query_result = [[0]]

    asyncio.run(storage._ensure_definition_table())

    assert len(storage.client.command_calls) == 1
    assert (
        "CREATE TABLE IF NOT EXISTS metranova.definition"
        in storage.client.command_calls[0]
    )


def test_find_methods_currently_return_none(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()

    assert asyncio.run(storage.find_all_resource_types()) is None
    assert asyncio.run(storage.find_resource_type_by_name("foo")) is None
    assert asyncio.run(storage.update_resource_type("foo")) is None
