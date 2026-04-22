import asyncio
import json
from types import SimpleNamespace

import pytest

from metranova.storage.base import CollectionField, MetadataField
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
        self.query_calls = []
        self.command_calls = []
        self.closed = False

    async def ping(self):
        return self.ping_value

    async def insert(self, **kwargs):
        self.insert_calls.append(kwargs)

    _DEFINITION_COLUMNS = [
        "id", "ref", "name", "slug", "type", "consumer_type",
        "consumer_config", "fields", "primary_key", "partition_by",
        "ttl", "engine_type", "is_replicated", "updated_at",
    ]

    async def query(self, query, parameters=None):
        self.query_calls.append((query, parameters))
        rows = self.query_result

        def named_results():
            for row in rows:
                yield dict(zip(DummyAsyncClient._DEFINITION_COLUMNS, row))

        return SimpleNamespace(
            result_rows=rows,
            row_count=len(rows),
            named_results=named_results,
        )

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

    asyncio.run(storage.close())

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


def test_create_database_standalone_query(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    async def mock_get_cluster_info():
        return {"mode": "standalone", "clusters": []}

    storage.get_cluster_info = mock_get_cluster_info

    asyncio.run(storage.create_database())

    assert len(storage.client.command_calls) == 1
    assert (
        storage.client.command_calls[0]
        == "CREATE DATABASE IF NOT EXISTS metranova"
    )


def test_create_database_clustered_query(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    async def mock_get_cluster_info():
        return {
            "mode": "clustered",
            "cluster_name": "cluster-a",
            "clusters": [("cluster-a", 1, 1, "host", "127.0.0.1", 9000)],
        }

    storage.get_cluster_info = mock_get_cluster_info

    asyncio.run(storage.create_database())

    assert len(storage.client.command_calls) == 1
    assert (
        storage.client.command_calls[0]
        == "CREATE DATABASE IF NOT EXISTS metranova ON CLUSTER 'cluster-a'"
    )


def test_create_resource_type_inserts_definition_row(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    async def mock_query(query, parameters=None):
        if "WHERE slug" in query:
            return SimpleNamespace(result_rows=[])
        return SimpleNamespace(result_rows=[[1]])

    storage.client.query = mock_query

    async def mock_get_ch_types():
        return ["String", "Float64", "DateTime64"]

    storage._get_ch_types = mock_get_ch_types

    data_fields = [
        CollectionField(field_name="if_name", field_type="String", nullable=True),
        CollectionField(field_name="rx_bps", field_type="Float64", nullable=False),
    ]
    meta_fields = [
        MetadataField(name="if_name", type="String", nullable=True),
    ]

    success = asyncio.run(
        storage.create_resource_type(
            name="Interface Traffic",
            slug="interface-traffic",
            data_fields=data_fields,
            meta_fields=meta_fields,
            identifier=["if_name"],
            ttl="365 DAY",
        )
    )

    assert success[0] is True
    assert success[1] == "Type Interface Traffic has been successfully created"
    assert len(storage.client.insert_calls) == 1
    call = storage.client.insert_calls[0]
    assert call["database"] == "metranova"
    assert call["table"] == "definition"
    assert len(call["data"]) == 1
    definition_row = call["data"][0]
    assert definition_row[3] == "interface-traffic"
    assert definition_row[4] == "data"
    assert definition_row[5] == [
        ("if_name", "String", True, ""),
    ]
    assert definition_row[6] == [
        ("if_name", "String", True),
        ("rx_bps", "Float64", False),
    ]
    assert definition_row[7] == ["if_name"]
    assert definition_row[8] == "365 DAY"


def test_create_resource_type_normalizes_nested_field_types(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    async def mock_query(query, parameters=None):
        if "WHERE slug" in query:
            return SimpleNamespace(result_rows=[])
        return SimpleNamespace(result_rows=[[1]])

    storage.client.query = mock_query

    async def mock_get_ch_types():
        return ["String", "Array", "Nullable", "LowCardinality"]

    storage._get_ch_types = mock_get_ch_types

    success = asyncio.run(
        storage.create_resource_type(
            name="Interface Alias",
            slug="interface-alias",
            data_fields=[
                CollectionField(field_name="if_name", field_type="string", nullable=True),
                CollectionField(field_name="aliases", field_type="array(string)", nullable=True),
            ],
            meta_fields=[
                MetadataField(name="if_name", type="reference", nullable=True, table="interfaces"),
                MetadataField(name="site", type="nullable(string)", nullable=True),
            ],
            identifier=["if_name"],
            ttl="365 DAY",
        )
    )

    assert success[0] is True
    call = storage.client.insert_calls[0]
    definition_row = call["data"][0]
    assert definition_row[6] == [
        ("if_name", "String", True),
        ("aliases", "Array(String)", True),
    ]
    assert definition_row[5] == [
        ("if_name", "String", True, "interfaces"),
        ("site", "Nullable(String)", True, ""),
    ]


def test_create_resource_type_returns_false_on_insert_error(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    async def mock_query(query, parameters=None):
        if "WHERE slug" in query:
            return SimpleNamespace(result_rows=[])
        return SimpleNamespace(result_rows=[[1]])

    storage.client.query = mock_query

    async def mock_get_ch_types():
        return ["String", "Float64", "DateTime64"]

    storage._get_ch_types = mock_get_ch_types

    async def failing_insert(**kwargs):
        raise RuntimeError("insert failed")

    storage.client.insert = failing_insert

    success = asyncio.run(
        storage.create_resource_type(
            name="Interface Traffic",
            slug="interface-traffic",
            data_fields=[CollectionField("if_name", "String", True)],
            meta_fields=[MetadataField(name="if_name", type="String", nullable=True)],
            identifier=["if_name"],
            ttl="365 DAY",
        )
    )

    assert success[0] is False
    assert success[1] == "Error during type definition insertion"


def test_create_resource_type_table_creation_failure_prevents_definition_insert(
    monkeypatch,
):
    """If the data table cannot be created, neither table nor definition row is written."""
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    async def mock_query(query, parameters=None):
        if "WHERE slug" in query:
            return SimpleNamespace(result_rows=[])
        return SimpleNamespace(result_rows=[[1]])

    storage.client.query = mock_query

    async def mock_get_ch_types():
        return ["String", "Float64", "DateTime64"]

    storage._get_ch_types = mock_get_ch_types

    async def failing_command(query):
        raise RuntimeError("DDL failed")

    storage.client.command = failing_command

    success = asyncio.run(
        storage.create_resource_type(
            name="Interface Traffic",
            slug="interface-traffic",
            data_fields=[CollectionField("if_name", "String", True)],
            meta_fields=[MetadataField(name="if_name", type="String", nullable=True)],
            identifier=["if_name"],
            ttl="365 DAY",
        )
    )

    assert success[0] is False
    assert success[1] == "Error during data table creation"
    assert len(storage.client.insert_calls) == 0


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
        "CREATE TABLE IF NOT EXISTS `metranova`.`definition`"
        in storage.client.command_calls[0]
    )


def test_ensure_definition_table_uses_on_cluster_when_clustered(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()
    storage.client.query_result = [[0]]

    async def mock_get_cluster_info():
        return {
            "mode": "clustered",
            "cluster_name": "cluster-a",
            "clusters": [("cluster-a", 1, 1, "host", "127.0.0.1", 9000)],
        }

    storage.get_cluster_info = mock_get_cluster_info

    asyncio.run(storage._ensure_definition_table())

    assert len(storage.client.command_calls) == 1
    assert "ON CLUSTER 'cluster-a'" in storage.client.command_calls[0]


def test_create_data_table_executes_expected_query(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    asyncio.run(
        storage.create_data_table(
            slug="interface_traffic",
            primary_key=["if_name", "timestamp"],
            ttl="365 DAY",
            fields=[
                ("if_name", "String", False),
                ("timestamp", "DateTime64", False),
                ("rx_bps", "Float64", True),
            ],
        )
    )

    assert len(storage.client.command_calls) == 1
    query = storage.client.command_calls[0]
    assert "CREATE TABLE `metranova`.`data_interface_traffic`" in query
    assert "`if_name` String NOT NULL" in query
    assert "`rx_bps` Float64" in query
    assert "PRIMARY KEY (collector_id, `if_name`, `timestamp`)" in query
    assert "PARTITION BY toYYYYMM(insert_time)" in query
    assert "ORDER BY (collector_id, `if_name`, `timestamp`)" in query
    assert "TTL insert_time + INTERVAL 365 DAY" in query
    assert "insert_time DateTime DEFAULT now()," in query


def test_create_data_table_uses_on_cluster_when_clustered(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    async def mock_get_cluster_info():
        return {
            "mode": "clustered",
            "cluster_name": "cluster-a",
            "clusters": [("cluster-a", 1, 1, "host", "127.0.0.1", 9000)],
        }

    storage.get_cluster_info = mock_get_cluster_info

    asyncio.run(
        storage.create_data_table(
            slug="interface_traffic",
            primary_key=["if_name"],
            ttl="365 DAY",
            fields=[("if_name", "String", False)],
        )
    )

    assert len(storage.client.command_calls) == 1
    assert "ON CLUSTER 'cluster-a'" in storage.client.command_calls[0]


def test_create_meta_table_executes_expected_query(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    asyncio.run(
        storage.create_meta_table(
            slug="device_inventory",
            fields=[
                MetadataField(name="hostname", type="String", nullable=False),
                MetadataField(name="site", type="String", nullable=True),
            ],
            primary_key=["hostname"],
        )
    )

    assert len(storage.client.command_calls) == 1
    query = storage.client.command_calls[0]
    assert "CREATE TABLE `metranova`.`meta_device_inventory`" in query
    assert "`hostname` String NOT NULL" in query
    assert "`site` String" in query
    assert "PRIMARY KEY (id, `hostname`)" in query
    assert "PARTITION BY toYYYYMM(created_at)" in query
    assert "ORDER BY (id, `hostname`)" in query


def test_create_meta_table_uses_on_cluster_when_clustered(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    async def mock_get_cluster_info():
        return {
            "mode": "clustered",
            "cluster_name": "cluster-a",
            "clusters": [("cluster-a", 1, 1, "host", "127.0.0.1", 9000)],
        }

    storage.get_cluster_info = mock_get_cluster_info

    asyncio.run(
        storage.create_meta_table(
            slug="device_inventory",
            fields=[MetadataField(name="hostname", type="String", nullable=False)],
            primary_key=["hostname"],
        )
    )

    assert len(storage.client.command_calls) == 1
    assert "ON CLUSTER 'cluster-a'" in storage.client.command_calls[0]


def test_add_columns_to_table_rejects_malicious_field_name(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    with pytest.raises(ValueError):
        asyncio.run(
            storage._add_columns_to_table(
                table_name="data_interface_traffic",
                fields=[("host; DROP TABLE x", "String", True)],
            )
        )

    assert storage.client.command_calls == []


def test_add_columns_to_table_rejects_malicious_field_type(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    with pytest.raises(ValueError):
        asyncio.run(
            storage._add_columns_to_table(
                table_name="data_interface_traffic",
                fields=[("host", "String); DROP TABLE x;--", True)],
            )
        )

    assert storage.client.command_calls == []


def test_create_data_table_rejects_malicious_primary_key_identifier(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    with pytest.raises(ValueError):
        asyncio.run(
            storage.create_data_table(
                slug="interface_traffic",
                primary_key=["if_name`, now()); DROP TABLE metranova.definition;--"],
                ttl="365 DAY",
                fields=[("if_name", "String", False)],
            )
        )

    assert storage.client.command_calls == []


def test_create_meta_table_rejects_malicious_field_type(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    with pytest.raises(ValueError):
        asyncio.run(
            storage.create_meta_table(
                slug="device_inventory",
                fields=[
                    MetadataField(name="hostname", type="String/*bad*/", nullable=False)],
                primary_key=["hostname"],
            )
        )

    assert storage.client.command_calls == []


def test_find_methods_currently_return_none(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()

    assert asyncio.run(storage.find_all_resource_types()) is None
    update_result = asyncio.run(storage.update_resource_type("foo"))
    assert update_result[0] is False


def test_find_all_resource_types_returns_list_of_dicts(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()
    storage.client.query_result = [
        (
            "def_interface-traffic",
            "def_interface-traffic__v1",
            "Interface Traffic",
            "interface-traffic",
            [("if_name", "String", True, "")],
            [("if_name", "String", True)],
            ["if_name"],
            "365 DAY",
            "MergeTree()",
            True,
            "2026-03-31 00:00:00",
        )
    ]

    result = asyncio.run(storage.find_all_resource_types())

    assert isinstance(result, list)
    assert isinstance(result[0], dict)
    assert result[0]["slug"] == "interface-traffic"
    assert result[0]["data_fields"] == [("if_name", "String", True)]
    assert result[0]["identifier"] == ["if_name"]


def test_find_all_resource_types_returns_none_on_query_error(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    async def failing_query(query, parameters=None):
        raise RuntimeError("query failed")

    storage.client.query = failing_query

    result = asyncio.run(storage.find_all_resource_types())

    assert result is None


def test_find_resource_type_by_slug_returns_row_when_found(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()
    storage.client.query_result = [
        ("def_interface-traffic", "def_interface-traffic__v1", "Interface Traffic")
    ]

    result = asyncio.run(storage.find_resource_type_by_slug("interface-traffic"))

    assert result["id"] == "def_interface-traffic"
    assert result["ref"] == "def_interface-traffic__v1"
    assert result["name"] == "Interface Traffic"
    query, parameters = storage.client.query_calls[-1]
    assert "WHERE slug = %s" in query
    assert parameters == ["interface-traffic"]


def test_find_resource_type_by_slug_returns_none_when_not_found(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()
    storage.client.query_result = []

    result = asyncio.run(storage.find_resource_type_by_slug("nonexistent"))

    assert result is None


def test_find_resource_type_by_slug_returns_none_on_error(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    async def failing_query(query, parameters=None):
        raise RuntimeError("query failed")

    storage.client.query = failing_query

    result = asyncio.run(storage.find_resource_type_by_slug("interface-traffic"))

    assert result is None


def test_find_resource_type_schema_by_slug_returns_schema_dict_for_data(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    async def mock_query(query, parameters=None):
        if "WHERE slug" in query:
            _COLS = [
                "id", "ref", "name", "slug", "type", "consumer_type",
                "consumer_config", "fields", "primary_key", "partition_by",
                "ttl", "engine_type", "is_replicated", "updated_at",
            ]
            rows = [
                (
                    "def_interface-traffic",
                    "def_interface-traffic__v1",
                    "Interface Traffic",
                    "interface-traffic",
                    "data",
                )
            ]

            def named_results():
                for r in rows:
                    yield dict(zip(_COLS, r))

            return SimpleNamespace(
                result_rows=rows,
                row_count=len(rows),
                named_results=named_results,
            )
        if "DESCRIBE TABLE" in query:
            return SimpleNamespace(
                result_rows=[
                    ("collector_id", "LowCardinality(String)", "", "", "", "", ""),
                    ("if_name", "String", "", "", "", "", ""),
                ]
            )
        return SimpleNamespace(result_rows=[])

    storage.client.query = mock_query

    result = asyncio.run(storage.find_resource_type_schema_by_slug("interface-traffic"))

    assert result is not None
    assert result["slug"] == "interface-traffic"
    assert result["type"] == "data"
    assert result["table"] == "metranova.data_interface-traffic"
    assert result["columns"][0]["name"] == "collector_id"
    assert result["columns"][1]["type"] == "String"


def test_find_resource_type_schema_by_slug_returns_none_when_definition_missing(
    monkeypatch,
):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()
    storage.client.query_result = []

    result = asyncio.run(storage.find_resource_type_schema_by_slug("missing-slug"))

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
            return SimpleNamespace(result_rows=[[1]], row_count=1)
        # Query for slug returns a result tuple
        if (
            "WHERE slug" in query
            and parameters
            and parameters[0] == "interface-traffic"
        ):
            rows = [("def_interface-traffic", "...")]

            def named_results():
                for r in rows:
                    yield {"id": r[0]}

            return SimpleNamespace(
                result_rows=rows,
                row_count=len(rows),
                named_results=named_results,
            )
        return await original_query(query, parameters)

    storage.client.query = mock_query

    success = asyncio.run(
        storage.create_resource_type(
            name="Interface Traffic",
            slug="interface-traffic",
            data_fields=[CollectionField("if_name", "String", True)],
            meta_fields=[MetadataField(name="if_name", type="String", nullable=True)],
            identifier=["if_name"],
            ttl="365 DAY",
        )
    )

    assert success[0] is False
    assert "already exists" in success[1]
    assert len(storage.client.insert_calls) == 0


def test_update_resource_type_adds_new_fields_and_increments_ref(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    existing = (
        "def_ip_address",
        "def_ip_address__v1",
        "IP Address",
        "ip_address",
        [("ip", "String", False, "")],
        [("ip", "String", False)],
        ["ip"],
        "365 DAY",
        "MergeTree()",
        True,
        "2026-03-31 00:00:00",
    )

    async def mock_find(slug):
        return existing if slug == "ip_address" else None

    storage.find_resource_type_by_slug = mock_find

    async def mock_get_ch_types():
        return ["String", "Float64", "DateTime64"]

    storage._get_ch_types = mock_get_ch_types

    success = asyncio.run(
        storage.update_resource_type(
            slug="ip_address",
            fields=[CollectionField("hostname", "String", True)],
            consumer_config_updates={"topic": "snmp.metrics.v2"},
            ext_updates={"team": "network"},
        )
    )

    assert success[0] is True
    assert "__v2" in success[1]
    assert len(storage.client.command_calls) == 1
    assert (
        "ALTER TABLE `metranova`.`data_ip_address`" in storage.client.command_calls[0]
    )
    assert len(storage.client.insert_calls) == 1
    row = storage.client.insert_calls[0]["data"][0]
    assert row[1] == "def_ip_address__v2"
    assert ("hostname", "String", True) in row[5]
    assert row[6] == ["ip"]


def test_update_resource_type_fails_when_field_already_exists(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    existing = {
        "id": "def_ip_address",
        "ref": "def_ip_address__v1",
        "name": "IP Address",
        "slug": "ip_address",
        "meta_fields": [("ip", "String", False, "")],
        "data_fields": [("ip", "String", False)],
        "identifier": ["ip"],
        "ttl": "365 DAY",
        "engine_type": "MergeTree()",
        "is_replicated": True,
    }

    async def mock_find(slug):
        return existing

    storage.find_resource_type_by_slug = mock_find

    success = asyncio.run(
        storage.update_resource_type(
            slug="ip_address",
            fields=[CollectionField("ip", "String", False)],
        )
    )

    assert success[0] is False
    assert "already exists" in success[1]
    assert len(storage.client.insert_calls) == 0


def test_update_resource_type_fails_when_slug_not_found(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_SKIP_DB_CREATE", "true")
    storage = Clickhouse()
    storage.client = DummyAsyncClient()

    async def mock_find(slug):
        return None

    storage.find_resource_type_by_slug = mock_find

    success = asyncio.run(storage.update_resource_type(slug="does_not_exist"))

    assert success[0] is False
    assert "not found" in success[1].lower()
