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
            "select id, transformer_ref, target_column, match_value, vendor_match_field, vendor_match_value, operation, config, default_value, `order`"
            in query_lc
        ):
            rows = list(self.storage.transformer_columns)
            ref = parameters.get("transformer_ref")
            if ref is not None:
                rows = [r for r in rows if r["transformer_ref"] == ref]
            col_id = parameters.get("id")
            if col_id is not None:
                rows = [r for r in rows if r["id"] == col_id]
            return QueryResult(len(rows), named_rows=rows)

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
        self.transformer_columns = [
            {
                "id": "map_utilization",
                "transformer_ref": "snmp_normalizer__v1",
                "target_column": "utilization",
                "match_value": None,
                "vendor_match_field": None,
                "vendor_match_value": None,
                "operation": "field",
                "config": {"source": "if_util"},
                "default_value": None,
                "order": 1,
            },
            {
                "id": "map_vendor",
                "transformer_ref": "snmp_normalizer__v1",
                "target_column": "vendor",
                "match_value": None,
                "vendor_match_field": None,
                "vendor_match_value": None,
                "operation": "field",
                "config": {"source": "vendor_name"},
                "default_value": None,
                "order": 2,
            },
            {
                "id": "map_flow",
                "transformer_ref": "flow_normalizer__v1",
                "target_column": "bytes",
                "match_value": None,
                "vendor_match_field": None,
                "vendor_match_value": None,
                "operation": "field",
                "config": {"source": "byte_count"},
                "default_value": None,
                "order": 1,
            },
        ]
        self.client = FakeClickhouseClient(self)

    def close(self):
        self.closed = True

    async def ensure_transformer_table(self):
        self.ensure_called = True

    async def ensure_transformer_column_table(self):
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
        "/transformers/",
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
        "/transformers/",
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
        "/transformers/",
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

    response = client.get("/transformers/")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    ids = [t["id"] for t in body]
    assert "snmp_normalizer" in ids
    assert "flow_normalizer" in ids


def test_transformer_api_get_all_filters_by_definition_ref(transformer_api_client):
    client, _ = transformer_api_client

    # Pass the base ref without version — should match def_snmp__v1, __v2, etc.
    response = client.get("/transformers/", params={"definition_ref": "def_snmp"})

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["id"] == "snmp_normalizer"


def test_transformer_api_get_all_returns_empty_list_when_none(transformer_api_client):
    client, fake_storage = transformer_api_client
    fake_storage.transformer_list = []

    response = client.get("/transformers/")

    assert response.status_code == 200
    assert response.json() == []


# --- GET /{id} ---


def test_transformer_api_get_by_id_success(transformer_api_client):
    client, _ = transformer_api_client

    response = client.get("/transformers/snmp_normalizer")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "snmp_normalizer"
    assert body["ref"] == "snmp_normalizer__v1"


def test_transformer_api_get_by_id_returns_404_when_missing(transformer_api_client):
    client, fake_storage = transformer_api_client
    fake_storage.transformer_by_id = None

    response = client.get("/transformers/missing_transformer")

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


# --- PUT /{id} ---


def test_transformer_api_update_name_and_slug(transformer_api_client):
    client, fake_storage = transformer_api_client

    response = client.put(
        "/transformers/snmp_normalizer",
        json={"name": "SNMP Normalizer V2"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "SNMP Normalizer V2"
    assert body["slug"] == "snmp_normalizer_v2"
    assert body["ref"] == "snmp_normalizer__v2"
    assert fake_storage.inserted is not None
    assert fake_storage.inserted["table"] == "transformer"
    inserted = dict(
        zip(fake_storage.inserted["column_names"], fake_storage.inserted["data"][0])
    )
    assert inserted["name"] == "SNMP Normalizer V2"
    assert inserted["slug"] == "snmp_normalizer_v2"
    assert inserted["ref"] == "snmp_normalizer__v2"


def test_transformer_api_update_description_only(transformer_api_client):
    client, fake_storage = transformer_api_client

    response = client.put(
        "/transformers/snmp_normalizer",
        json={"description": "Updated description"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["description"] == "Updated description"
    assert body["name"] == "SNMP Normalizer"
    assert body["ref"] == "snmp_normalizer__v2"
    inserted = dict(
        zip(fake_storage.inserted["column_names"], fake_storage.inserted["data"][0])
    )
    assert inserted["description"] == "Updated description"


def test_transformer_api_update_match_field(transformer_api_client):
    client, fake_storage = transformer_api_client

    response = client.put(
        "/transformers/snmp_normalizer",
        json={"match_field": "vendor"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["match_field"] == "vendor"
    assert body["ref"] == "snmp_normalizer__v2"
    inserted = dict(
        zip(fake_storage.inserted["column_names"], fake_storage.inserted["data"][0])
    )
    assert inserted["match_field"] == "vendor"


def test_transformer_api_update_returns_404_for_missing_id(transformer_api_client):
    client, fake_storage = transformer_api_client
    fake_storage.transformer_by_id = None

    response = client.put(
        "/transformers/missing_transformer",
        json={"description": "Updated"},
    )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_transformer_api_update_returns_400_for_empty_body(transformer_api_client):
    client, _ = transformer_api_client

    response = client.put("/transformers/snmp_normalizer", json={})

    assert response.status_code == 400
    assert "no fields" in response.json()["detail"].lower()


# --- POST /transformers/{id}/columns ---


def test_transformer_columns_create_uses_transformer_ref(transformer_api_client):
    client, fake_storage = transformer_api_client

    response = client.post(
        "/transformers/snmp_normalizer/columns",
        json={
            "id": "map_utilization",
            "target_column": "utilization",
            "match_value": None,
            "vendor_match_field": None,
            "vendor_match_value": None,
            "operation": "field",
            "config": {"source": "if_util"},
            "default_value": None,
            "order": 1,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "map_utilization"
    assert body["transformer_ref"] == "snmp_normalizer__v1"
    assert fake_storage.inserted is not None
    assert fake_storage.inserted["table"] == "transformer_column"


def test_transformer_columns_create_returns_404_when_transformer_missing(
    transformer_api_client,
):
    client, fake_storage = transformer_api_client
    fake_storage.transformer_by_id = None

    response = client.post(
        "/transformers/missing/columns",
        json={
            "id": "map_utilization",
            "target_column": "utilization",
            "operation": "field",
            "config": {"source": "if_util"},
        },
    )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_transformer_columns_create_returns_400_for_unknown_operation(
    transformer_api_client,
):
    client, _ = transformer_api_client

    response = client.post(
        "/transformers/snmp_normalizer/columns",
        json={
            "id": "map_utilization",
            "target_column": "utilization",
            "operation": "does_not_exist",
            "config": {},
        },
    )

    assert response.status_code == 400
    assert "unknown operation" in response.json()["detail"].lower()


# --- GET /transformers/{id}/columns/ ---


def test_transformer_columns_get_all_for_transformer(transformer_api_client):
    client, _ = transformer_api_client

    response = client.get("/transformers/snmp_normalizer/columns/")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    assert all(c["transformer_ref"] == "snmp_normalizer__v1" for c in body)


def test_transformer_columns_get_returns_404_when_transformer_missing(
    transformer_api_client,
):
    client, fake_storage = transformer_api_client
    fake_storage.transformer_by_id = None

    response = client.get("/transformers/missing/columns/")

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


# --- GET /transformers/{id}/columns/{column_id} ---


def test_transformer_columns_get_by_id_success(transformer_api_client):
    client, _ = transformer_api_client

    response = client.get("/transformers/snmp_normalizer/columns/map_utilization")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "map_utilization"
    assert body["transformer_ref"] == "snmp_normalizer__v1"


def test_transformer_columns_get_by_id_returns_404_when_missing_column(
    transformer_api_client,
):
    client, _ = transformer_api_client

    response = client.get("/transformers/snmp_normalizer/columns/does_not_exist")

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


# --- PUT /transformers/{id}/columns/{column_id} ---


def test_transformer_columns_update_success(transformer_api_client):
    client, fake_storage = transformer_api_client

    response = client.put(
        "/transformers/snmp_normalizer/columns/map_utilization",
        json={
            "target_column": "utilization_pct",
            "operation": "field",
            "config": {"source": "if_util_pct"},
            "order": 3,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["target_column"] == "utilization_pct"
    assert body["operation"] == "field"
    assert body["config"] == '{"source": "if_util_pct"}'
    assert body["order"] == 3
    cmd = fake_storage.last_command
    assert "ALTER TABLE" in cmd["query"]
    assert "target_column = {target_column:String}" in cmd["query"]
    assert "operation = {operation:String}" in cmd["query"]
    assert "config = {config:String}" in cmd["query"]
    assert "`order` = {order:UInt16}" in cmd["query"]
    assert cmd["parameters"]["id"] == "map_utilization"


def test_transformer_columns_update_returns_404_when_transformer_missing(
    transformer_api_client,
):
    client, fake_storage = transformer_api_client
    fake_storage.transformer_by_id = None

    response = client.put(
        "/transformers/missing/columns/map_utilization",
        json={"target_column": "utilization_pct"},
    )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_transformer_columns_update_returns_404_when_column_missing(
    transformer_api_client,
):
    client, _ = transformer_api_client

    response = client.put(
        "/transformers/snmp_normalizer/columns/missing_col",
        json={"target_column": "utilization_pct"},
    )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_transformer_columns_update_returns_400_for_empty_body(
    transformer_api_client,
):
    client, _ = transformer_api_client

    response = client.put(
        "/transformers/snmp_normalizer/columns/map_utilization",
        json={},
    )

    assert response.status_code == 400
    assert "no fields" in response.json()["detail"].lower()


def test_transformer_columns_update_returns_400_for_unknown_operation(
    transformer_api_client,
):
    client, _ = transformer_api_client

    response = client.put(
        "/transformers/snmp_normalizer/columns/map_utilization",
        json={"operation": "does_not_exist"},
    )

    assert response.status_code == 400
    assert "unknown operation" in response.json()["detail"].lower()


# --- DELETE /transformers/{id}/columns/{column_id} ---


def test_transformer_columns_delete_success(transformer_api_client):
    client, fake_storage = transformer_api_client

    response = client.delete("/transformers/snmp_normalizer/columns/map_utilization")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "map_utilization"
    assert "deleted" in body["message"].lower()
    cmd = fake_storage.last_command
    assert "ALTER TABLE" in cmd["query"]
    assert "DELETE" in cmd["query"]
    assert cmd["parameters"]["id"] == "map_utilization"


def test_transformer_columns_delete_returns_404_when_transformer_missing(
    transformer_api_client,
):
    client, fake_storage = transformer_api_client
    fake_storage.transformer_by_id = None

    response = client.delete("/transformers/missing/columns/map_utilization")

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_transformer_columns_delete_returns_404_when_column_missing(
    transformer_api_client,
):
    client, _ = transformer_api_client

    response = client.delete("/transformers/snmp_normalizer/columns/missing_col")

    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


# --- Config Validation Tests ---


def test_transformer_columns_create_returns_400_for_missing_required_config_field(
    transformer_api_client,
):
    """Test that creating a column with missing required config fields fails."""
    client, _ = transformer_api_client

    # 'field' operation requires 'source' field in config
    response = client.post(
        "/transformers/snmp_normalizer/columns",
        json={
            "id": "bad_field_col",
            "target_column": "mapped_field",
            "operation": "field",
            "config": {"cast": "string"},  # Missing required 'source' field
        },
    )

    assert response.status_code == 400
    assert "invalid config" in response.json()["detail"].lower()
    assert "source" in response.json()["detail"].lower()


def test_transformer_columns_create_returns_400_for_invalid_config_type(
    transformer_api_client,
):
    """Test that creating a column with wrong config field types fails."""
    client, _ = transformer_api_client

    # 'concat' operation requires 'fields' to be a list and 'delimiter' to be a string
    response = client.post(
        "/transformers/snmp_normalizer/columns",
        json={
            "id": "bad_concat_col",
            "target_column": "concatenated",
            "operation": "concat",
            "config": {
                "fields": "not_a_list",  # Should be a list
                "delimiter": ",",
            },
        },
    )

    assert response.status_code == 400
    assert "invalid config" in response.json()["detail"].lower()
    assert "fields" in response.json()["detail"].lower()


def test_transformer_columns_create_succeeds_with_valid_config(
    transformer_api_client,
):
    """Test that creating a column with valid config succeeds."""
    client, fake_storage = transformer_api_client

    response = client.post(
        "/transformers/snmp_normalizer/columns",
        json={
            "id": "good_field_col",
            "target_column": "source_value",
            "operation": "field",
            "config": {
                "source": "interface_speed",
                "cast": "int",
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == "good_field_col"


def test_transformer_columns_update_returns_400_for_invalid_config_type(
    transformer_api_client,
):
    """Test that updating a column with wrong config field types fails."""
    client, _ = transformer_api_client

    # Try to update with invalid config for 'field' operation
    response = client.put(
        "/transformers/snmp_normalizer/columns/map_utilization",
        json={
            "operation": "static",
            "config": {"value": 123},  # Should be a string, not an int
        },
    )

    assert response.status_code == 400
    assert "invalid config" in response.json()["detail"].lower()


def test_transformer_columns_update_validates_config_against_existing_operation(
    transformer_api_client,
):
    """Test that config is validated against existing operation when operation is not provided."""
    client, _ = transformer_api_client

    # Update config for 'field' operation (from existing column) without changing operation
    response = client.put(
        "/transformers/snmp_normalizer/columns/map_utilization",
        json={
            "config": {"source": "new_source", "cast": "string"},
        },
    )

    assert response.status_code == 200


def test_transformer_columns_update_validates_config_against_new_operation(
    transformer_api_client,
):
    """Test that config is validated against new operation when operation is changed."""
    client, _ = transformer_api_client

    # Change operation to 'static' and provide valid config for it
    response = client.put(
        "/transformers/snmp_normalizer/columns/map_utilization",
        json={
            "operation": "static",
            "config": {"value": "static_value"},
        },
    )

    assert response.status_code == 200
