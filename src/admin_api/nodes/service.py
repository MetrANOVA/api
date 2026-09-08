import logging
import uuid

from typing import TYPE_CHECKING, Any

from .model import Node, NodeCreateRequest, NodeUpdateRequest

if TYPE_CHECKING:
    from metranova.storage.clickhouse import Clickhouse

logger = logging.getLogger(__name__)

TABLE = "nodes"

COLUMNS = [
    "node_id",
    "host",
    "port",
    "community",
    "name",
    "make",
    "model",
    "updated_at",
]


def _to_row(node: Node) -> dict[str, Any]:
    """Flatten a Node into a ClickHouse row dict."""
    return {
        "node_id": node.node_id,
        "host": node.host,
        "port": node.port,
        "community": node.community,
        "name": node.name,
        "make": node.make,
        "model": node.model,
    }


def _from_row(row) -> Node:
    """Rebuild a Node from a stored row."""
    if not isinstance(row, dict):
        row = dict(zip(COLUMNS, row))

    return Node(
        node_id=row["node_id"],
        host=row["host"],
        port=row["port"],
        community=row["community"],
        name=row["name"],
        make=row["make"],
        model=row["model"],
    )


def _rows(result) -> list[dict]:
    if hasattr(result, "named_results"):
        return list(result.named_results())
    return [
        dict(zip(COLUMNS, row)) for row in (getattr(result, "result_rows", None) or [])
    ]


class NodeService:
    """Service class for handling node operations."""

    def __init__(self, storage: "Clickhouse"):
        self.storage = storage
        self.client = storage.client

    async def _insert(self, node: Node) -> Node:
        data = _to_row(node)
        await self.storage.client.insert(
            table=TABLE,
            database=self.storage.database,
            column_names=list(data.keys()),
            data=[list(data.values())],
        )
        return node

    async def get_nodes(self) -> list[Node]:
        """Return the latest record for every stored node."""
        await self.storage.ensure_nodes_table()

        table_name = self.storage._qualified_table_name(TABLE)
        result = await self.storage.client.query(
            f"SELECT {', '.join(COLUMNS)} FROM {table_name}"
            + " ORDER BY updated_at DESC LIMIT 1 BY node_id",
            parameters={},
        )

        return [_from_row(row) for row in _rows(result)]

    async def get_node_by_id(self, node_id: str) -> Node:
        """Return the latest record for a single node.

        Raises:
            LookupError: no node is stored under that id.
        """
        await self.storage.ensure_nodes_table()

        table_name = self.storage._qualified_table_name(TABLE)
        result = await self.storage.client.query(
            f"SELECT {', '.join(COLUMNS)} FROM {table_name}"
            + " WHERE node_id = {node_id:String} ORDER BY updated_at DESC LIMIT 1",
            parameters={"node_id": node_id},
        )

        rows = _rows(result)
        if not rows:
            raise LookupError(f"Node with id '{node_id}' not found")
        return _from_row(rows[0])

    async def create_node(self, request: NodeCreateRequest) -> Node:
        """Persist a new node under a freshly generated node_id."""
        await self.storage.ensure_nodes_table()

        node = Node(node_id=uuid.uuid4().hex, **request.model_dump())
        return await self._insert(node)

    async def update_node(self, node_id: str, request: NodeUpdateRequest) -> Node:
        """Replace every field of an existing node.

        Raises:
            LookupError: no node is stored under that id.
        """
        await self.storage.ensure_nodes_table()

        await self.get_node_by_id(node_id)

        node = Node(node_id=node_id, **request.model_dump())
        return await self._insert(node)

    async def delete_node(self, node_id: str) -> dict[str, str]:
        """Delete a node.

        Raises:
            LookupError: no node is stored under that id.
        """
        await self.storage.ensure_nodes_table()

        await self.get_node_by_id(node_id)

        table_name = self.storage._qualified_table_name(TABLE)
        await self.storage.client.command(
            f"ALTER TABLE {table_name} DELETE WHERE node_id = {{node_id:String}}",
            parameters={"node_id": node_id},
        )

        return {"message": f"Node '{node_id}' deleted", "node_id": node_id}
