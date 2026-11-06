import ipaddress
from typing import TYPE_CHECKING

from admin_api.nodes.model import Node
from admin_api.collector.model import ResourceConfiguration
from admin_api.collector.selectors import select_nodes

if TYPE_CHECKING:
    from metranova.storage.clickhouse import Clickhouse


TABLE = "resource_configuration_memberships"


def normalize_device_identity(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("device identity must not be empty")
    try:
        return ipaddress.ip_address(value).compressed
    except ValueError:
        try:
            identity = value.rstrip(".").encode("idna").decode("ascii").lower()
        except UnicodeError:
            raise ValueError("device identity is invalid") from None
        if not identity:
            raise ValueError("device identity must not be empty")
        return identity


def node_identity(node: Node) -> str:
    return normalize_device_identity(node.name if node.name.strip() else node.host)


def _result_rows(result) -> list:
    if hasattr(result, "named_results"):
        return list(result.named_results())
    return list(getattr(result, "result_rows", None) or [])


class MembershipService:
    """Append-only membership state for collector resource configurations."""

    def __init__(self, storage: "Clickhouse"):
        self.storage = storage
        self.client = storage.client

    async def _ensure_table(self) -> None:
        await self.storage.ensure_resource_configuration_memberships_table()

    async def _states(self, resource_config_id: str) -> dict[str, bool]:
        await self._ensure_table()
        table = self.storage._qualified_table_name(TABLE)
        result = await self.client.query(
            f"SELECT device_identity, active FROM {table} "
            "WHERE resource_config_id = {resource_config_id:String} "
            "ORDER BY updated_at DESC LIMIT 1 BY device_identity",
            parameters={"resource_config_id": resource_config_id},
        )
        rows = _result_rows(result)
        states = {}
        for row in rows:
            if isinstance(row, dict):
                identity, active = row["device_identity"], row["active"]
            else:
                identity, active = row[0], row[1]
            states[identity] = bool(active)
        return states

    async def has_history(self, resource_config_id: str) -> bool:
        return bool(await self._states(resource_config_id))

    async def device_ids(
        self, config: ResourceConfiguration, nodes: list[Node],
    ) -> list[str]:
        states = await self._states(config.id)
        if not states:
            identities = {node_identity(node) for node in select_nodes(nodes, config.node_selectors)}
            return sorted(identities)
        return sorted(identity for identity, active in states.items() if active)

    async def add(
        self, config: ResourceConfiguration, nodes: list[Node], device_id: str,
    ) -> None:
        identity = normalize_device_identity(device_id)
        matches = [node for node in nodes if node_identity(node) == identity]
        if not matches:
            raise LookupError(f"Device '{identity}' not found")
        if len(matches) > 1:
            raise ValueError(f"Device identity '{identity}' is ambiguous")

        states = await self._states(config.id)
        if not states:
            legacy_ids = {
                node_identity(node)
                for node in select_nodes(nodes, config.node_selectors)
            }
            for legacy_id in sorted(legacy_ids):
                await self._insert(config.id, legacy_id, True)
            states = {legacy_id: True for legacy_id in legacy_ids}
        if states.get(identity) is True:
            return
        await self._insert(config.id, identity, True)

    async def remove(
        self, config: ResourceConfiguration, nodes: list[Node], device_id: str,
    ) -> None:
        identity = normalize_device_identity(device_id)
        states = await self._states(config.id)
        if not states:
            legacy_ids = {
                node_identity(node)
                for node in select_nodes(nodes, config.node_selectors)
            }
            for legacy_id in sorted(legacy_ids):
                await self._insert(config.id, legacy_id, legacy_id != identity)
            states = {legacy_id: legacy_id != identity for legacy_id in legacy_ids}
        if states.get(identity) is False:
            return
        await self._insert(config.id, identity, False)

    async def _insert(
        self, resource_config_id: str, device_identity: str, active: bool,
    ) -> None:
        await self._ensure_table()
        await self.client.insert(
            table=TABLE,
            database=self.storage.database,
            column_names=["resource_config_id", "device_identity", "active"],
            data=[[resource_config_id, device_identity, active]],
        )

    async def selected_nodes(
        self, config: ResourceConfiguration, nodes: list[Node],
    ) -> list[Node]:
        states = await self._states(config.id)
        if not states:
            return select_nodes(nodes, config.node_selectors)

        wanted = {identity for identity, active in states.items() if active}
        by_identity: dict[str, list[Node]] = {}
        for node in nodes:
            by_identity.setdefault(node_identity(node), []).append(node)
        selected = []
        for identity in sorted(wanted):
            matches = by_identity.get(identity, [])
            if not matches:
                raise LookupError(f"Membership device '{identity}' no longer exists")
            if len(matches) > 1:
                raise ValueError(f"Membership device '{identity}' is ambiguous")
            selected.append(matches[0])
        return selected
