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

    async def query(self, query, parameters=None):
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


def test_close_closes_client(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    storage.close()

    assert storage.client.closed is True


def test_create_returns_initialized_instance(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    state = {"connect": False}

    async def fake_connect(self):
        state["connect"] = True

    monkeypatch.setattr(Clickhouse, "connect", fake_connect)

    storage = asyncio.run(Clickhouse.create())

    assert isinstance(storage, Clickhouse)
    assert state["connect"] is True


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

    # Mock query to return empty results for slug lookup but [[1]] for EXISTS TABLE
    async def mock_query(query, parameters=None):
        if "WHERE slug" in query:
            return SimpleNamespace(result_rows=[])
        return SimpleNamespace(result_rows=[[1]])

    storage.client.query = mock_query

    fields = [
        CollectionField(field_name="if_name", field_type="String", nullable=True),
        CollectionField(field_name="rx_bps", field_type="Float64", nullable=False),
    ]

    success = asyncio.run(
        storage.create_resource_type(
            name="Interface Traffic",
            slug="interface-traffic",
            collection_type="data",
            consumer_type="kafka",
            consumer_config={"topic": "snmp.metrics"},
            fields=fields,
            primary_key=["if_name"],
            partition_by="toYYYYMM(timestamp)",
            ttl="365 DAY",
        )
    )

    assert success[0] is True
    assert success[1] == "Type Interface Traffic has been successfully created"
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

    # Mock query to return empty results for slug lookup but [[1]] for EXISTS TABLE
    async def mock_query(query, parameters=None):
        if "WHERE slug" in query:
            return SimpleNamespace(result_rows=[])
        return SimpleNamespace(result_rows=[[1]])

    storage.client.query = mock_query

    async def failing_insert(**kwargs):
        raise RuntimeError("insert failed")

    storage.client.insert = failing_insert

    success = asyncio.run(
        storage.create_resource_type(
            name="Interface Traffic",
            slug="interface-traffic",
            collection_type="data",
            consumer_type="kafka",
            consumer_config={"topic": "snmp.metrics"},
            fields=[CollectionField("if_name", "String", True)],
            primary_key=["if_name"],
            partition_by="toYYYYMM(timestamp)",
            ttl="365 DAY",
        )
    )

    assert success[0] is False
    assert success[1] == "Error during type definition insertion"


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


def test_find_resource_type_by_slug_returns_row_when_found(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()
    storage.client.query_result = [
        ("def_interface-traffic", "def_interface-traffic__v1", "Interface Traffic")
    ]

    result = asyncio.run(
        storage.find_resource_type_by_slug_and_type("interface-traffic", "data")
    )

    assert result == (
        "def_interface-traffic",
        "def_interface-traffic__v1",
        "Interface Traffic",
    )


def test_find_resource_type_by_slug_returns_none_when_not_found(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()
    storage.client.query_result = []

    result = asyncio.run(
        storage.find_resource_type_by_slug_and_type("nonexistent", "data")
    )

    assert result is None


def test_find_resource_type_by_slug_returns_none_on_error(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    async def failing_query(query, parameters=None):
        raise RuntimeError("query failed")

    storage.client.query = failing_query

    result = asyncio.run(
        storage.find_resource_type_by_slug_and_type("interface-traffic", "data")
    )

    assert result is None


def test_create_resource_type_returns_false_when_slug_already_exists(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    # Mock the query method to have different results based on the query
    original_query = storage.client.query

    async def mock_query(query, parameters=None):
        # EXISTS query returns [[0]] or [[1]]
        if "EXISTS TABLE" in query:
            return SimpleNamespace(result_rows=[[1]])
        # Query for slug returns a result tuple
        if (
            "WHERE slug" in query
            and parameters
            and parameters[0] == "interface-traffic"
            and parameters[1] == "data"
        ):
            return SimpleNamespace(result_rows=[("def_interface-traffic", "...")])
        return await original_query(query, parameters)

    storage.client.query = mock_query

    success = asyncio.run(
        storage.create_resource_type(
            name="Interface Traffic",
            slug="interface-traffic",
            collection_type="data",
            consumer_type="kafka",
            consumer_config={"topic": "snmp.metrics"},
            fields=[CollectionField("if_name", "String", True)],
            primary_key=["if_name"],
            partition_by="toYYYYMM(timestamp)",
            ttl="365 DAY",
        )
    )

    assert success[0] is False
    assert "already exists" in success[1]
    assert len(storage.client.insert_calls) == 0
