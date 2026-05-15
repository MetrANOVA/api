import asyncio
from types import SimpleNamespace

import pytest

from admin_api.metadata.service import MetadataField, MetadataService


class DummyClient:
    def __init__(self):
        self.command_calls = []
        self.insert_calls = []

    async def command(self, query):
        self.command_calls.append(query)

    async def insert(self, **kwargs):
        self.insert_calls.append(kwargs)


class DummyStorage:
    def __init__(self, table_exists: bool):
        self.database = "metranova"
        self.metadata_engine = "MergeTree"
        self.client = DummyClient()
        self._table_exists_value = table_exists

    async def find_resource_type_by_slug(self, slug: str):
        return None

    def _quoted_identifier(self, name: str) -> str:
        return f"`{name}`"

    def _validated_column_type(self, field_type: str) -> str:
        return field_type

    def _validated_engine_name(self, engine: str) -> str:
        return engine

    def _qualified_table_name(self, table_name: str) -> str:
        return f"`{self.database}`.`{table_name}`"

    async def _table_exists(self, table_name: str) -> bool:
        return self._table_exists_value

    async def _get_on_cluster_clause(self, engine_name: str | None = None) -> str:
        return ""


class ClusteredDummyStorage(DummyStorage):
    async def _get_on_cluster_clause(self, engine_name: str | None = None) -> str:
        return " ON CLUSTER 'cluster-a'"


class QueryingDummyClient(DummyClient):
    def __init__(self, query_result=None):
        super().__init__()
        self.query_calls = []
        self.query_result = query_result or []

    async def query(self, query, parameters=None):
        self.query_calls.append((query, parameters))
        rows = self.query_result
        return SimpleNamespace(
            row_count=len(rows),
            named_results=lambda: iter(rows),
        )


def test_create_metadata_type_fails_when_table_already_exists():
    storage = DummyStorage(table_exists=True)
    service = MetadataService(storage)

    with pytest.raises(ValueError) as exc_info:
        asyncio.run(
            service.create_metadata_type(
                name="POP",
                identifier=["pop_id"],
                fields=[
                    MetadataField(name="pop_id", type="String", nullable=False),
                ],
            )
        )

    assert storage.client.command_calls == []
    assert len(storage.client.insert_calls) == 0
    assert "already exists" in str(exc_info.value)


def test_create_metadata_type_keeps_identifier_empty_when_omitted():
    storage = DummyStorage(table_exists=False)
    service = MetadataService(storage)

    asyncio.run(
        service.create_metadata_type(
            name="POP",
            identifier=[],
            fields=[
                MetadataField(name="pop_id", type="String", nullable=False),
                MetadataField(name="name", type="String", nullable=False),
            ],
        )
    )

    insert_call = storage.client.insert_calls[0]
    assert insert_call["data"][0][7] == []


def test_create_metadata_type_uses_id_only_keys_when_identifier_omitted():
    storage = DummyStorage(table_exists=False)
    service = MetadataService(storage)

    asyncio.run(
        service.create_metadata_type(
            name="POP",
            identifier=[],
            fields=[
                MetadataField(name="pop_id", type="String", nullable=False),
            ],
        )
    )

    assert len(storage.client.command_calls) == 1
    query = storage.client.command_calls[0]
    assert "ORDER BY (id)" in query
    assert "PRIMARY KEY (id)" in query


def test_create_metadata_type_uses_on_cluster_clause_when_available():
    storage = ClusteredDummyStorage(table_exists=False)
    service = MetadataService(storage)

    asyncio.run(
        service.create_metadata_type(
            name="POP",
            identifier=["pop_id"],
            fields=[
                MetadataField(name="pop_id", type="String", nullable=False),
            ],
        )
    )

    query = storage.client.command_calls[0]
    assert "ON CLUSTER 'cluster-a'" in query
    assert "CREATE TABLE `metranova`.`meta_pop` ON CLUSTER 'cluster-a'" in query


def test_create_metadata_type_rejects_empty_fields():
    storage = DummyStorage(table_exists=False)
    service = MetadataService(storage)

    with pytest.raises(ValueError) as exc_info:
        asyncio.run(
            service.create_metadata_type(
                name="POP",
                identifier=[],
                fields=[],
            )
        )

    assert "at least one field" in str(exc_info.value).lower()


# def test_get_metadata_records_uses_created_at_for_latest_version_lookup():
#     storage = DummyStorage(table_exists=False)
#     storage.client = QueryingDummyClient()
#     service = MetadataService(storage)

#     asyncio.run(service.get_metadata_records("interface-traffic"))

#     query, parameters = storage.client.query_calls[0]
#     assert "max(created_at) AS max_created_at" in query
#     assert "t.created_at = latest.max_created_at" in query
#     assert parameters == {"db": "metranova", "table": "meta_interface-traffic"}


def test_create_metadata_record_does_not_explicitly_insert_insert_time():
    storage = DummyStorage(table_exists=False)
    storage.client = QueryingDummyClient()
    service = MetadataService(storage)

    result = asyncio.run(
        service.create_metadata_record(
            {"slug": "interface-traffic", "identifier": ["node", "intf"]},
            {
                "node": "router-1",
                "intf": "xe-0/0/0",
                "insert_time": "should-be-ignored",
            },
        )
    )

    insert_call = storage.client.insert_calls[0]
    assert result["unchanged"] is False
    assert "insert_time" not in insert_call["column_names"]
    assert "created_at" in insert_call["column_names"]
    assert "updated_at" in insert_call["column_names"]


def test_create_metadata_record_uses_scalar_identifier_field_for_id():
    storage = DummyStorage(table_exists=False)
    storage.client = QueryingDummyClient()
    service = MetadataService(storage)

    result = asyncio.run(
        service.create_metadata_record(
            {"slug": "pop", "identifier": ["pop_id"]},
            {
                "id": "341",
                "pop_id": 341,
                "name": "BB: Indianapolis - IU (roadm)",
                "locality": "ICTC",
                "type": "BB",
            },
        )
    )

    assert result["id"] == "341"

    insert_call = storage.client.insert_calls[0]
    id_index = insert_call["column_names"].index("id")
    assert insert_call["data"][0][id_index] == "341"
