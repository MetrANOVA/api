import asyncio
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from admin_api.nodes.model import Node, NodeCreateRequest, NodeUpdateRequest
from admin_api.nodes.service import NodeService, _from_row, _to_row


class DummyClient:
    """Records writes and replays a scripted queue of query results."""

    def __init__(self, query_results=None):
        self.query_calls = []
        self.insert_calls = []
        self.command_calls = []
        self.query_results = list(query_results or [])

    async def query(self, query, parameters=None):
        self.query_calls.append((query, parameters))
        rows = self.query_results.pop(0) if self.query_results else []
        return SimpleNamespace(
            row_count=len(rows),
            named_results=lambda: iter(rows),
        )

    async def insert(self, **kwargs):
        self.insert_calls.append(kwargs)

    async def command(self, command, parameters=None):
        self.command_calls.append((command, parameters))


class DummyStorage:
    def __init__(self, query_results=None):
        self.database = "metranova"
        self.client = DummyClient(query_results)
        self.ensure_calls = 0

    async def ensure_nodes_table(self):
        self.ensure_calls += 1

    def _qualified_table_name(self, table_name: str) -> str:
        return f"`{self.database}`.`{table_name}`"


def _node(**overrides) -> Node:
    defaults = dict(
        node_id="0123456789abcdef0123456789abcdef",
        host="10.0.0.1",
        port=161,
        community="public",
        name="core-rtr-1",
        make="Cisco",
        model="ASR9000",
    )
    defaults.update(overrides)
    return Node(**defaults)


def _create_request(**overrides) -> NodeCreateRequest:
    defaults = dict(
        host="10.0.0.1",
        port=161,
        community="public",
        name="core-rtr-1",
        make="Cisco",
        model="ASR9000",
    )
    defaults.update(overrides)
    return NodeCreateRequest(**defaults)


def _stored_row(node: Node) -> dict:
    """A row as ClickHouse would hand it back, including updated_at."""
    row = _to_row(node)
    row["updated_at"] = "2026-08-11 00:00:00.000000"
    return row


def test_to_row_from_row_round_trip():
    original = _node()

    restored = _from_row(_stored_row(original))

    assert restored == original


def test_from_row_accepts_positional_tuples():
    row = _stored_row(_node())
    positional = tuple(row.values())

    restored = _from_row(positional)

    assert restored == _node()


def test_create_generates_node_id_and_inserts():
    storage = DummyStorage()
    service = NodeService(storage)

    result = asyncio.run(service.create_node(_create_request()))

    assert result.node_id
    assert result.name == "core-rtr-1"
    assert storage.ensure_calls == 1

    assert len(storage.client.insert_calls) == 1
    insert = storage.client.insert_calls[0]
    assert insert["table"] == "nodes"
    assert insert["database"] == "metranova"
    row = _to_row(result)
    assert insert["column_names"] == list(row.keys())
    assert insert["data"] == [list(row.values())]


def test_create_generates_unique_ids_across_calls():
    storage = DummyStorage()
    service = NodeService(storage)

    first = asyncio.run(service.create_node(_create_request()))
    second = asyncio.run(service.create_node(_create_request()))

    assert first.node_id != second.node_id


def test_create_rejects_missing_host():
    with pytest.raises(ValidationError, match="host"):
        NodeCreateRequest(port=161, community="public", name="n", make="m", model="m1")


def test_get_nodes_returns_latest_per_node_id():
    rows = [_stored_row(_node()), _stored_row(_node(node_id="other", name="other"))]
    storage = DummyStorage(query_results=[rows])
    service = NodeService(storage)

    nodes = asyncio.run(service.get_nodes())

    assert [n.node_id for n in nodes] == ["0123456789abcdef0123456789abcdef", "other"]
    query, _ = storage.client.query_calls[0]
    assert "LIMIT 1 BY node_id" in query
    assert "updated_at DESC" in query


def test_get_node_by_id_returns_model():
    storage = DummyStorage(query_results=[[_stored_row(_node())]])
    service = NodeService(storage)

    node = asyncio.run(service.get_node_by_id("0123456789abcdef0123456789abcdef"))

    assert node == _node()


def test_get_node_by_id_reports_missing():
    storage = DummyStorage(query_results=[[]])
    service = NodeService(storage)

    with pytest.raises(LookupError, match="not found"):
        asyncio.run(service.get_node_by_id("nope"))


def test_update_replaces_fields_and_inserts():
    node_id = "0123456789abcdef0123456789abcdef"
    storage = DummyStorage(query_results=[[_stored_row(_node())]])
    service = NodeService(storage)

    result = asyncio.run(
        service.update_node(
            node_id,
            NodeUpdateRequest(
                host="10.0.0.2",
                port=162,
                community="private",
                name="core-rtr-1b",
                make="Juniper",
                model="MX960",
            ),
        )
    )

    assert result.node_id == node_id
    assert result.port == 162
    assert result.make == "Juniper"
    assert len(storage.client.insert_calls) == 1


def test_update_reports_missing_node():
    storage = DummyStorage(query_results=[[]])
    service = NodeService(storage)

    with pytest.raises(LookupError, match="not found"):
        asyncio.run(
            service.update_node(
                "nope",
                NodeUpdateRequest(
                    host="10.0.0.2",
                    port=162,
                    community="private",
                    name="x",
                    make="x",
                    model="x",
                ),
            )
        )

    assert storage.client.insert_calls == []


def test_delete_removes_the_node():
    node_id = "0123456789abcdef0123456789abcdef"
    storage = DummyStorage(query_results=[[_stored_row(_node())]])
    service = NodeService(storage)

    result = asyncio.run(service.delete_node(node_id))

    assert result["node_id"] == node_id
    assert len(storage.client.command_calls) == 1
    command, parameters = storage.client.command_calls[0]
    assert "DELETE WHERE node_id = {node_id:String}" in command
    assert parameters == {"node_id": node_id}


def test_delete_reports_missing_node():
    storage = DummyStorage(query_results=[[]])
    service = NodeService(storage)

    with pytest.raises(LookupError, match="not found"):
        asyncio.run(service.delete_node("nope"))

    assert storage.client.command_calls == []
