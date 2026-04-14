import pytest
from fastapi.testclient import TestClient

from admin_api.app import app


class FakeStorage:
    def __init__(self):
        self.closed = False
        self.created_payload = None
        self.resource = {
            "id": "def_interface-traffic",
            "ref": "def_interface-traffic__v1",
            "name": "Interface Traffic",
            "slug": "interface-traffic",
            "type": "data",
            "consumer_type": "kafka",
            "consumer_config": '{"topic":"snmp.metrics"}',
            "fields": [("if_name", "String", True), ("timestamp", "DateTime64", False)],
            "primary_key": ["if_name", "timestamp"],
            "partition_by": "toYYYYMM(timestamp)",
            "ttl": "365 DAY",
            "engine_type": "MergeTree()",
            "is_replicated": True,
            "updated_at": "2026-04-01 00:00:00",
        }

    def close(self):
        self.closed = True

    async def create_resource_type(
        self,
        name,
        slug,
        data_fields,
        meta_fields,
        identifier,
        ttl,
        engine_type="CoalescingMergeTree",
    ):
        self.created_payload = {
            "name": name,
            "slug": slug,
            "data_fields": data_fields,
            "meta_fields": meta_fields,
            "identifier": identifier,
            "ttl": ttl,
            "engine_type": engine_type,
        }
        return True, f"Type {name} has been successfully created"

    async def find_all_resource_types(self):
        return [self.resource]

    async def find_resource_type_by_slug(self, slug: str):
        if slug == self.resource["slug"]:
            return self.resource
        return None

    async def find_resource_type_schema_by_slug(self, slug: str):
        if slug != self.resource["slug"]:
            return None
        return {
            "slug": slug,
            "type": "data",
            "table": f"metranova.data_{slug}",
            "columns": [
                {"name": "collector_id", "type": "LowCardinality(String)"},
                {"name": "if_name", "type": "String"},
            ],
        }

    async def update_resource_type(
        self,
        slug: str,
        fields,
        consumer_config_updates,
        ext_updates,
    ):
        if slug != self.resource["slug"]:
            return False, f"Resource type with slug '{slug}' not found"
        return True, f"Resource type '{slug}' updated to def_interface-traffic__v2"


@pytest.fixture
def api_client(monkeypatch):
    fake_storage = FakeStorage()

    async def fake_create(cls):
        return fake_storage

    monkeypatch.setattr("admin_api.context.Clickhouse.create", classmethod(fake_create))

    with TestClient(app) as client:
        yield client, fake_storage


def test_type_api_crud_flow(api_client):
    client, fake_storage = api_client

    create_payload = {
        "name": "Interface Traffic",
        "data": {
            "fields": [
                {"field_name": "if_name", "field_type": "string", "nullable": True},
                {"field_name": "timestamp", "field_type": "datetime64", "nullable": False},
            ]
        },
        "neta": {
            "fields": [
                {"field_name": "if_name", "field_type": "string", "nullable": True},
            ]
        },
        "identifier": ["if_name", "timestamp"],
        "ttl": "365 DAY",
    }

    create_response = client.post("/type/", json=create_payload)
    assert create_response.status_code == 200
    assert "successfully created" in create_response.json()["message"]
    assert fake_storage.created_payload is not None
    assert fake_storage.created_payload["slug"] == "interface-traffic"
    assert fake_storage.created_payload["data_fields"][0].field_type == "string"
    assert fake_storage.created_payload["data_fields"][1].field_type == "datetime64"

    list_response = client.get("/type/")
    assert list_response.status_code == 200
    assert list_response.json()[0]["slug"] == "interface-traffic"

    get_response = client.get("/type/interface-traffic")
    assert get_response.status_code == 200
    assert get_response.json()["id"] == "def_interface-traffic"

    schema_response = client.get("/type/interface-traffic/schema")
    assert schema_response.status_code == 200
    assert schema_response.json()["table"] == "metranova.data_interface-traffic"

    update_response = client.put(
        "/type/interface-traffic",
        json={
            "fields": [
                {
                    "field_name": "rx_bps",
                    "field_type": "float64",
                    "nullable": True,
                }
            ],
            "consumer_config": {"topic": "snmp.metrics.v2"},
            "ext": {"team": "network"},
        },
    )
    assert update_response.status_code == 200
    assert "updated to" in update_response.json()["message"]


def test_lifespan_closes_storage_client(api_client):
    _, fake_storage = api_client
    assert fake_storage.closed is False


def test_lifespan_shutdown_closes_storage(monkeypatch):
    fake_storage = FakeStorage()

    async def fake_create(cls):
        return fake_storage

    monkeypatch.setattr("admin_api.context.Clickhouse.create", classmethod(fake_create))

    with TestClient(app) as client:
        response = client.get("/")
        assert response.status_code == 200
        assert fake_storage.closed is False

    assert fake_storage.closed is True


def test_type_api_create_returns_500_on_duplicate_slug(api_client, monkeypatch):
    client, _ = api_client

    async def fake_create_resource_type(*args, **kwargs):
        return False, "Resource type with slug 'interface-traffic' already exists"

    monkeypatch.setattr(
        "admin_api.context.get_clickhouse",
        lambda request: request.app.state.se,
    )
    monkeypatch.setattr(
        type(client.app.state.se),
        "create_resource_type",
        fake_create_resource_type,
        raising=True,
    )

    response = client.post(
        "/type/",
        json={
            "name": "Interface Traffic",
            "data": {"fields": [{"field_name": "if_name", "field_type": "string", "nullable": True}]},
            "neta": {"fields": [{"field_name": "if_name", "field_type": "string", "nullable": True}]},
            "identifier": ["if_name"],
            "ttl": "365 DAY",
        },
    )

    assert response.status_code == 500
    assert "already exists" in response.json()["detail"]


def test_type_api_returns_404_for_missing_slug_paths(api_client):
    client, _ = api_client

    get_response = client.get("/type/missing-slug")
    assert get_response.status_code == 404
    assert "not found" in get_response.json()["detail"]

    schema_response = client.get("/type/missing-slug/schema")
    assert schema_response.status_code == 404
    assert "not found" in schema_response.json()["detail"]

    update_response = client.put(
        "/type/missing-slug",
        json={"fields": [], "consumer_config": {}, "ext": {}},
    )
    assert update_response.status_code == 404
    assert "not found" in update_response.json()["detail"]


def test_type_api_create_returns_422_for_invalid_ttl(api_client):
    client, _ = api_client

    response = client.post(
        "/type/",
        json={
            "name": "Interface Traffic",
            "data": {"fields": [{"field_name": "if_name", "field_type": "string", "nullable": True}]},
            "neta": {"fields": [{"field_name": "if_name", "field_type": "string", "nullable": True}]},
            "identifier": ["if_name"],
            "ttl": "invalid ttl",
        },
    )

    assert response.status_code == 422
    assert "ttl" in str(response.json()).lower()


def test_type_api_create_ignores_slug_in_request_body(api_client):
    client, _ = api_client

    response = client.post(
        "/type/",
        json={
            "name": "Interface Traffic",
            "slug": "Interface Traffic",
            "data": {"fields": [{"field_name": "if_name", "field_type": "string", "nullable": True}]},
            "neta": {"fields": [{"field_name": "if_name", "field_type": "string", "nullable": True}]},
            "identifier": ["if_name"],
            "ttl": "365 DAY",
        },
    )

    assert response.status_code == 200


def test_type_api_create_ignores_partition_by_in_request_body(api_client):
    client, _ = api_client

    response = client.post(
        "/type/",
        json={
            "name": "Interface Traffic",
            "data": {"fields": [{"field_name": "if_name", "field_type": "string", "nullable": True}]},
            "neta": {"fields": [{"field_name": "if_name", "field_type": "string", "nullable": True}]},
            "identifier": ["if_name"],
            "partition_by": "toYYYYMM(timestamp; DROP TABLE metranova.definition)",
            "ttl": "365 DAY",
        },
    )

    assert response.status_code == 200
