import asyncio

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
