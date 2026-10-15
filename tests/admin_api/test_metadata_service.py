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


def test_id_identifier_does_not_allow_missing_required_fields():
    service = MetadataService(DummyStorage(table_exists=True))
    definition = {
        "identifier": ["id"],
        "meta_fields": [
            {"field_name": "hostname", "field_type": "String", "nullable": False},
        ],
    }
    with pytest.raises(ValueError, match="Field 'hostname' is required"):
        asyncio.run(service.validate_metadata_record(definition, {"id": "router"}))
    asyncio.run(service.validate_metadata_record(
        definition, {"id": "router", "hostname": "router.example.net"},
    ))


def test_metadata_correction_keeps_version_without_merging_existing_fields():
    from datetime import datetime

    storage = DummyStorage(table_exists=True)
    storage.client = QueryingDummyClient([{
        "id": "router::port", "ref": "router::port__v1",
        "created_at": datetime(2026, 1, 1),
        "insert_time": datetime(2026, 1, 1),
        "updated_at": datetime(2026, 1, 1),
        "description": "preserve", "edge": True,
        "ext": {"custom": "preserve"}, "flow_index": None,
    }])
    service = MetadataService(storage)
    payload = {"id": "router::port", "flow_index": 42, "ext": {"custom": "incoming"}}
    asyncio.run(service.update_metadata_record(
        {"slug": "interface", "identifier": ["id"]},
        payload, "1",
    ))
    insert = storage.client.insert_calls[0]
    record = dict(zip(insert["column_names"], insert["data"][0]))
    assert "description" not in record
    assert "edge" not in record
    assert record["ext"] == {}
    assert record["flow_index"] == 42
    assert record["ref"] == "router::port__v1"
    assert record["created_at"] == datetime(2026, 1, 1)
    assert "insert_time" not in record
    assert "ref" not in payload


def test_metadata_change_creates_new_version_without_mutating_history():
    from datetime import datetime

    old = {
        "id": "router", "ref": "router__v1", "hash": "old-hash",
        "hostname": "old-name", "created_at": datetime(2026, 1, 1),
    }
    storage = DummyStorage(table_exists=True)
    storage.client = QueryingDummyClient([old])
    service = MetadataService(storage)
    result = asyncio.run(service.create_metadata_record(
        {"slug": "device", "identifier": ["id"]},
        {"id": "router", "hostname": "new-name", "ext": {"custom": "incoming"}},
    ))
    assert result["ref"] == "router__v2"
    assert old["hostname"] == "old-name"
    assert old["ref"] == "router__v1"
    assert len(storage.client.insert_calls) == 1
    insert = storage.client.insert_calls[0]
    record = dict(zip(insert["column_names"], insert["data"][0]))
    assert record["hostname"] == "new-name"
    assert record["ext"] == {}


def test_put_returns_corrected_ref_when_service_copies_request_body(monkeypatch):
    from datetime import datetime

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from admin_api.metadata.router import router

    async def get_type(self, slug):
        return {
            "slug": slug, "identifier": ["id"],
            "meta_fields": [
                {"field_name": "hostname", "field_type": "String", "nullable": False},
            ],
        }

    monkeypatch.setattr(MetadataService, "get_metadata_type", get_type)
    storage = DummyStorage(table_exists=True)
    storage.client = QueryingDummyClient([{
        "id": "router", "ref": "router__v3", "hostname": "typo",
        "created_at": datetime(2026, 1, 1),
    }])
    app = FastAPI()
    app.state.se = storage
    app.include_router(router, prefix="/metadata")
    client = TestClient(app)

    response = client.put(
        "/metadata/device/router/3", json={"id": "router", "hostname": "corrected"},
    )
    assert response.status_code == 200
    assert response.json() == {"id": "router", "ref": "router__v3"}
    insert = storage.client.insert_calls[0]
    record = dict(zip(insert["column_names"], insert["data"][0]))
    assert record["ref"] == "router__v3"
    assert record["hostname"] == "corrected"

    response = client.put("/metadata/device/router/3", json={"id": "router"})
    assert response.status_code == 400
    assert len(storage.client.insert_calls) == 1


def test_metadata_list_uses_configured_database_and_hides_internal_rank():
    source = {"id": "router", "ref": "router__v1", "_rn": 1}
    storage = DummyStorage(table_exists=True)
    storage.database = "custom_database"
    storage.client = QueryingDummyClient([source])
    records = asyncio.run(MetadataService(storage).get_metadata_records("device"))
    assert storage.client.query_calls[0][1] == {
        "db": "custom_database", "table": "meta_device",
    }
    assert records == [{"id": "router", "ref": "router__v1"}]
    assert source["_rn"] == 1
