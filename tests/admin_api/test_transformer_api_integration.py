import pytest
from fastapi.testclient import TestClient

from admin_api.app import app


class QueryResult:
    def __init__(self, row_count: int, result_rows=None, named_rows=None):
        self.row_count = row_count
        self.result_rows = result_rows or []
        self._named_rows = named_rows or []

    def named_results(self):
        return iter(self._named_rows)


SNMP_TRANSFORMER = {
    "id": "snmp_normalizer",
    "ref": "snmp_normalizer__v1",
    "definition_ref": "def_snmp__v1",
    "name": "SNMP Normalizer",
    "slug": "snmp_normalizer",
    "description": "Normalize SNMP metrics",
    "match_field": "device_type",
    "updated_at": "2026-04-23 00:00:00",
}

FLOW_TRANSFORMER = {
    "id": "flow_normalizer",
    "ref": "flow_normalizer__v1",
    "definition_ref": "def_flow__v1",
    "name": "Flow Normalizer",
    "slug": "flow_normalizer",
    "description": "Normalize flow metrics",
    "match_field": "device_type",
    "updated_at": "2026-04-23 00:00:00",
}


class FakeClickhouseClient:
    def __init__(self, storage):
        self.storage = storage

    async def query(self, query: str, parameters: dict):
        query_lc = query.lower()

        if (
            "select id, ref, definition_ref, name, slug, description, match_field, updated_at"
            in query_lc
        ):
            if "where id =" in query_lc:
                # get by id
                if self.storage.transformer_by_id is None:
                    return QueryResult(0)
                return QueryResult(1, named_rows=[self.storage.transformer_by_id])
            else:
                # list all / filtered
                rows = list(self.storage.transformer_list)
                if "where definition_ref like" in query_lc and parameters.get(
                    "definition_ref"
                ):
                    prefix = parameters["definition_ref"].removesuffix("__%")
                    rows = [
                        r for r in rows if r["definition_ref"].startswith(prefix + "__")
                    ]
                return QueryResult(len(rows), named_rows=rows)

        if "select id, name, slug, description, match_field" in query_lc:
            # existence check inside update_transformer
            if self.storage.transformer_by_id is None:
                return QueryResult(0)
            return QueryResult(1, named_rows=[self.storage.transformer_by_id])

        if "from metranova.transformer" in query_lc:
            if self.storage.duplicate_transformer:
                return QueryResult(1)
            return QueryResult(0)

        if "from metranova.definition" in query_lc:
            if self.storage.missing_definition:
                return QueryResult(0)
            return QueryResult(1)

        raise AssertionError(f"Unexpected query: {query}")

    async def insert(self, table: str, database: str, column_names, data):
        self.storage.inserted = {
            "table": table,
            "database": database,
            "column_names": list(column_names),
            "data": data,
        }

    async def command(self, query: str, parameters: dict = None):
        self.storage.last_command = {"query": query, "parameters": parameters or {}}


class FakeStorage:
    def __init__(self):
        self.closed = False
        self.database = "metranova"
        self.duplicate_transformer = False
        self.missing_definition = False
        self.ensure_called = False
        self.inserted = None
        self.last_command = None
        self.transformer_list = [SNMP_TRANSFORMER, FLOW_TRANSFORMER]
        self.transformer_by_id = dict(SNMP_TRANSFORMER)
        self.client = FakeClickhouseClient(self)

    def close(self):
        self.closed = True

    async def ensure_transformer_table(self):
        self.ensure_called = True

    def _qualified_table_name(self, table: str) -> str:
        return f"{self.database}.{table}"


@pytest.fixture
def transformer_api_client(monkeypatch):
    fake_storage = FakeStorage()

    async def fake_create(cls):
        return fake_storage

    monkeypatch.setattr("admin_api.context.Clickhouse.create", classmethod(fake_create))

    with TestClient(app) as client:
        yield client, fake_storage


# --- POST ---


def test_transformer_api_create_success(transformer_api_client):
    client, fake_storage = transformer_api_client

    response = client.post(
        "/transformer/",
        json={
            "name": "SNMP Normalizer",
            "definition_ref": "def_snmp__v1",
            "description": "Normalize SNMP metrics",
            "match_field": "device_type",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "snmp_normalizer"
    assert body["ref"] == "snmp_normalizer__v1"
    assert body["definition_ref"] == "def_snmp__v1"
    assert fake_storage.ensure_called is True
    assert fake_storage.inserted is not None
    assert fake_storage.inserted["table"] == "transformer"
    assert "id" in fake_storage.inserted["column_names"]


def test_transformer_api_create_duplicate_returns_500(transformer_api_client):
    client, fake_storage = transformer_api_client
    fake_storage.duplicate_transformer = True

    response = client.post(
        "/transformer/",
        json={
            "name": "SNMP Normalizer",
            "definition_ref": "def_snmp__v1",
            "description": "Normalize SNMP metrics",
            "match_field": "device_type",
        },
    )

    assert response.status_code == 500
    assert response.json()["detail"] == "Error creating transformer"


def test_transformer_api_create_returns_422_for_invalid_payload(transformer_api_client):
    client, _ = transformer_api_client

    response = client.post(
        "/transformer/",
        json={
            "name": "SNMP Normalizer",
            "definition_ref": "def_snmp__v1",
            "description": "Normalize SNMP metrics",
        },
    )

    assert response.status_code == 422


# --- GET / ---


def test_transformer_api_get_all_returns_list(transformer_api_client):
    client, _ = transformer_api_client

    response = client.get("/transformer/")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    ids = [t["id"] for t in body]
    assert "snmp_normalizer" in ids
    assert "flow_normalizer" in ids


def test_transformer_api_get_all_filters_by_definition_ref(transformer_api_client):
    client, _ = transformer_api_client

    # Pass the base ref without version — should match def_snmp__v1, __v2, etc.
    response = client.get("/transformer/", params={"definition_ref": "def_snmp"})

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["id"] == "snmp_normalizer"


def test_transformer_api_get_all_returns_empty_list_when_none(transformer_api_client):
    client, fake_storage = transformer_api_client
    fake_storage.transformer_list = []

    response = client.get("/transformer/")

    assert response.status_code == 200
    assert response.json() == []


# --- GET /{id} ---


def test_transformer_api_get_by_id_success(transformer_api_client):
    client, _ = transformer_api_client

    response = client.get("/transformer/snmp_normalizer")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "snmp_normalizer"
    assert body["ref"] == "snmp_normalizer__v1"


def test_transformer_api_get_by_id_returns_404_when_missing(transformer_api_client):
    client, fake_storage = transformer_api_client
    fake_storage.transformer_by_id = None

    response = client.get("/transformer/missing_transformer")

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


# --- PUT /{id} ---


def test_transformer_api_update_name_and_slug(transformer_api_client):
    client, fake_storage = transformer_api_client

    response = client.put(
        "/transformer/snmp_normalizer",
        json={"name": "SNMP Normalizer V2"},
    )

    assert response.status_code == 200
    cmd = fake_storage.last_command
    assert "ALTER TABLE" in cmd["query"]
    assert "name = {name:String}" in cmd["query"]
    assert "slug = {slug:String}" in cmd["query"]
    assert cmd["parameters"]["name"] == "SNMP Normalizer V2"
    assert cmd["parameters"]["slug"] == "snmp_normalizer_v2"


def test_transformer_api_update_description_only(transformer_api_client):
    client, fake_storage = transformer_api_client

    response = client.put(
        "/transformer/snmp_normalizer",
        json={"description": "Updated description"},
    )

    assert response.status_code == 200
    cmd = fake_storage.last_command
    assert "description = {description:String}" in cmd["query"]
    assert "name" not in cmd["parameters"]
    assert cmd["parameters"]["description"] == "Updated description"


def test_transformer_api_update_match_field(transformer_api_client):
    client, fake_storage = transformer_api_client

    response = client.put(
        "/transformer/snmp_normalizer",
        json={"match_field": "vendor"},
    )

    assert response.status_code == 200
    cmd = fake_storage.last_command
    assert "match_field = {match_field:String}" in cmd["query"]
    assert cmd["parameters"]["match_field"] == "vendor"


def test_transformer_api_update_returns_404_for_missing_id(transformer_api_client):
    client, fake_storage = transformer_api_client
    fake_storage.transformer_by_id = None

    response = client.put(
        "/transformer/missing_transformer",
        json={"description": "Updated"},
    )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_transformer_api_update_returns_400_for_empty_body(transformer_api_client):
    client, _ = transformer_api_client

    response = client.put("/transformer/snmp_normalizer", json={})

    assert response.status_code == 400
    assert "no fields" in response.json()["detail"].lower()
