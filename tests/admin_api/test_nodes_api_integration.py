import pytest
from fastapi.testclient import TestClient

from admin_api.app import app
from admin_api.nodes.model import Node


class FakeStorage:
    def __init__(self):
        self.client = object()

    def close(self):
        pass


class FakeNodeService:
    """Stands in for NodeService so the routes are tested, not ClickHouse."""

    calls = []
    nodes = {}

    def __init__(self, storage):
        self.storage = storage

    async def get_nodes(self):
        return list(FakeNodeService.nodes.values())

    async def get_node_by_id(self, node_id):
        if node_id not in FakeNodeService.nodes:
            raise LookupError(f"Node with id '{node_id}' not found")
        return FakeNodeService.nodes[node_id]

    async def create_node(self, request):
        FakeNodeService.calls.append(("create", request))
        node = Node(node_id="generated-id", **request.model_dump())
        FakeNodeService.nodes[node.node_id] = node
        return node

    async def update_node(self, node_id, request):
        FakeNodeService.calls.append(("update", node_id, request))
        if node_id not in FakeNodeService.nodes:
            raise LookupError(f"Node with id '{node_id}' not found")
        node = Node(node_id=node_id, **request.model_dump())
        FakeNodeService.nodes[node_id] = node
        return node

    async def delete_node(self, node_id):
        FakeNodeService.calls.append(("delete", node_id))
        if node_id not in FakeNodeService.nodes:
            raise LookupError(f"Node with id '{node_id}' not found")
        del FakeNodeService.nodes[node_id]
        return {"message": f"Node '{node_id}' deleted", "node_id": node_id}


@pytest.fixture
def api_client(monkeypatch):
    fake_storage = FakeStorage()

    async def fake_create(cls):
        return fake_storage

    monkeypatch.setattr("admin_api.context.Clickhouse.create", classmethod(fake_create))
    monkeypatch.setattr("admin_api.nodes.router.NodeService", FakeNodeService)
    FakeNodeService.calls = []
    FakeNodeService.nodes = {}

    with TestClient(app) as client:
        yield client


def _body(**overrides):
    defaults = dict(
        host="10.0.0.1",
        port=161,
        community="public",
        name="core-rtr-1",
        make="Cisco",
        model="ASR9000",
    )
    defaults.update(overrides)
    return defaults


def test_create_route_assigns_a_server_side_node_id(api_client):
    response = api_client.post("/nodes/", json=_body())

    assert response.status_code == 200
    body = response.json()
    assert body["node_id"] == "generated-id"
    assert body["name"] == "core-rtr-1"


def test_create_route_rejects_a_node_id_in_the_body(api_client):
    response = api_client.post("/nodes/", json=_body(node_id="client-supplied"))

    assert response.status_code == 200
    body = response.json()
    # node_id in the request body is silently ignored, not honored
    assert body["node_id"] == "generated-id"


def test_create_route_rejects_missing_host(api_client):
    response = api_client.post("/nodes/", json=_body(host=None))

    assert response.status_code == 422
    assert FakeNodeService.calls == []


def test_get_route_returns_a_node(api_client):
    api_client.post("/nodes/", json=_body())

    response = api_client.get("/nodes/generated-id")

    assert response.status_code == 200
    assert response.json()["node_id"] == "generated-id"


def test_get_route_reports_missing_node_as_404(api_client):
    response = api_client.get("/nodes/missing")

    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


def test_list_route_returns_all_nodes(api_client):
    api_client.post("/nodes/", json=_body(name="a"))

    response = api_client.get("/nodes/")

    assert response.status_code == 200
    assert len(response.json()) == 1


def test_update_route_replaces_the_node(api_client):
    api_client.post("/nodes/", json=_body())

    response = api_client.put(
        "/nodes/generated-id", json=_body(port=162, community="private")
    )

    assert response.status_code == 200
    body = response.json()
    assert body["port"] == 162
    assert body["community"] == "private"


def test_update_route_reports_missing_node_as_404(api_client):
    response = api_client.put("/nodes/missing", json=_body())

    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


def test_delete_route_removes_the_node(api_client):
    api_client.post("/nodes/", json=_body())

    response = api_client.delete("/nodes/generated-id")

    assert response.status_code == 200
    assert response.json()["node_id"] == "generated-id"

    follow_up = api_client.get("/nodes/generated-id")
    assert follow_up.status_code == 404


def test_delete_route_reports_missing_node_as_404(api_client):
    response = api_client.delete("/nodes/missing")

    assert response.status_code == 404
    assert "not found" in response.json()["detail"]
