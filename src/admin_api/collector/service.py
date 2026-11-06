import logging
import re

from importlib.metadata import entry_points
from typing import TYPE_CHECKING, Any

from admin_api.nodes.service import NodeService
from admin_api.collector.membership import MembershipService
from admin_api.resource_type.router import (
    _existing_data_field_names,
    _existing_meta_field_names,
)

from .model import (
    ApplyResponse,
    FieldConfig,
    PlanResponse,
    ResourceConfiguration,
    ResourceConfigurationRequest,
    ResourceConfigurationUpdate,
    Selector,
)
from admin_api.collector.plugins.telegraf import TelegrafValidator

if TYPE_CHECKING:
    from metranova.storage.clickhouse import Clickhouse

logger = logging.getLogger(__name__)

TABLE = "resource_configuration"

COLUMNS = [
    "id",
    "ref",
    "name",
    "slug",
    "resource_type",
    "collector_plugin",
    "interval",
    "timeout",
    "field_mappings",
    "node_selectors",
    "source",
    "source_id",
    "updated_at",
]

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_-]+$")

# Order snapshots by the numeric suffix of `ref`, not by `updated_at`: DateTime
# has second granularity, so two versions written in the same second tie and the
# "latest" row is picked arbitrarily.
_VERSION_EXPR = r"toUInt32OrZero(extract(ref, '__v(\\d+)$'))"


def slugify(name: str) -> str:
    return name.lower().replace(" ", "_")


def _bump_ref(ref: str) -> str:
    """Return the next snapshot ref for an existing one ('x__v1' -> 'x__v2')."""
    parts = ref.rsplit("__v", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return f"{parts[0]}__v{int(parts[1]) + 1}"
    return f"{ref}__v2"


def _to_row(config: ResourceConfiguration) -> dict[str, Any]:
    """Flatten a ResourceConfiguration into a ClickHouse row dict."""
    field_mappings = [
        (
            field_name,
            fc.oid,
            bool(fc.is_tag),
            fc.secondary_index_table,
            fc.secondary_index_use,
        )
        for field_name, fc in sorted(config.field_mappings.items())
    ]
    node_selectors = [(s.type, s.value) for s in config.node_selectors]

    return {
        "id": config.id,
        "ref": config.ref,
        "name": config.name,
        "slug": slugify(config.name),
        "resource_type": config.resource_type,
        "collector_plugin": config.collector_plugin,
        "interval": config.interval,
        "timeout": config.timeout,
        "field_mappings": field_mappings,
        "node_selectors": node_selectors,
        "source": config.source,
        "source_id": config.source_id,
    }


def _tuple_get(value, key: str, index: int):
    """Read a named-tuple element from either a dict or a positional row."""
    if isinstance(value, dict):
        return value.get(key)
    return value[index]


def _from_row(row) -> ResourceConfiguration:
    """Rebuild a ResourceConfiguration from a stored row."""
    if not isinstance(row, dict):
        row = dict(zip(COLUMNS, row))

    field_mappings = {}
    for entry in row.get("field_mappings") or []:
        field_mappings[_tuple_get(entry, "field_name", 0)] = FieldConfig(
            oid=_tuple_get(entry, "oid", 1),
            is_tag=bool(_tuple_get(entry, "is_tag", 2)),
            secondary_index_table=_tuple_get(entry, "secondary_index_table", 3),
            secondary_index_use=_tuple_get(entry, "secondary_index_use", 4),
        )

    node_selectors = [
        Selector(type=_tuple_get(entry, "type", 0), value=_tuple_get(entry, "value", 1))
        for entry in row.get("node_selectors") or []
    ]

    return ResourceConfiguration(
        id=row["id"],
        ref=row["ref"],
        name=row["name"],
        resource_type=row["resource_type"],
        collector_plugin=row["collector_plugin"],
        interval=row["interval"],
        timeout=row["timeout"],
        field_mappings=field_mappings,
        node_selectors=node_selectors,
        source=row["source"],
        source_id=row["source_id"],
    )


def _rows(result) -> list[dict]:
    if hasattr(result, "named_results"):
        return list(result.named_results())
    return [
        dict(zip(COLUMNS, row)) for row in (getattr(result, "result_rows", None) or [])
    ]


class CollectorService:
    """Service class for handling collector operations."""

    def __init__(self, storage: "Clickhouse"):
        self.storage = storage
        self.client = storage.client

        self.plugins = {}
        for ep in entry_points(group="metranova.collector.plugins"):
            plugin_cls = ep.load()
            logger.info(
                f"Loaded {plugin_cls.plugin_type} plugin {plugin_cls.plugin_id}"
            )
            self.plugins[plugin_cls.plugin_id] = plugin_cls()
        logger.info(
            f"Loaded {len(self.plugins)} collector plugins: {list(self.plugins)}"
        )

    def get_plugin_names(self) -> list[str]:
        """Return the ids of every loaded collector plugin."""
        return list(self.plugins)

    async def _declared_field_names(self, resource_type: str) -> set[str]:
        """Field names declared by the referenced resource type definition."""
        definition = await self.storage.find_resource_type_by_slug(resource_type)
        if not definition:
            raise LookupError(f"No resource type with slug '{resource_type}' found")

        names = _existing_data_field_names(definition) | _existing_meta_field_names(
            definition
        )
        return {n for n in names if n}

    async def _validate_field_mappings(
        self, resource_type: str, field_mappings: dict[str, FieldConfig]
    ) -> None:
        declared = await self._declared_field_names(resource_type)
        unknown = sorted(set(field_mappings) - declared)
        if unknown:
            raise ValueError(
                f"Unknown field(s) for resource type '{resource_type}': "
                f"{', '.join(unknown)}. Declared fields: {', '.join(sorted(declared))}"
            )

    @staticmethod
    def _assert_owned_by(
        config: ResourceConfiguration, collector_plugin: str | None
    ) -> None:
        """Raise if `config` belongs to a plugin other than the one asked for.

        A configuration addressed through the wrong plugin's URL is reported as
        missing rather than forbidden — the plugin scopes the namespace.
        """
        if collector_plugin is not None and config.collector_plugin != collector_plugin:
            raise LookupError(
                f"Resource configuration with id '{config.id}' not found for "
                f"plugin '{collector_plugin}'"
            )

    async def _insert(self, config: ResourceConfiguration) -> ResourceConfiguration:
        data = _to_row(config)
        await self.storage.client.insert(
            table=TABLE,
            database=self.storage.database,
            column_names=list(data.keys()),
            data=[list(data.values())],
        )
        return config

    async def get_collector_configurations(
        self, collector_plugin: str | None = None
    ) -> list[ResourceConfiguration]:
        """Return the latest snapshot of every stored configuration."""
        await self.storage.ensure_resource_configuration_table()

        table_name = self.storage._qualified_table_name(TABLE)
        select = f"SELECT {', '.join(COLUMNS)} FROM {table_name}"

        if collector_plugin is not None:
            result = await self.storage.client.query(
                select
                + " WHERE collector_plugin = {plugin:String}"
                + f" ORDER BY {_VERSION_EXPR} DESC LIMIT 1 BY id",
                parameters={"plugin": collector_plugin},
            )
        else:
            result = await self.storage.client.query(
                select + f" ORDER BY {_VERSION_EXPR} DESC LIMIT 1 BY id",
                parameters={},
            )

        return [_from_row(row) for row in _rows(result)]

    async def get_resource_configuration_by_id(
        self, config_id: str
    ) -> ResourceConfiguration:
        """Return the latest snapshot for a single configuration id.

        Raises:
            LookupError: no configuration is stored under that id.
        """
        await self.storage.ensure_resource_configuration_table()

        table_name = self.storage._qualified_table_name(TABLE)
        result = await self.storage.client.query(
            f"SELECT {', '.join(COLUMNS)} FROM {table_name}"
            + f" WHERE id = {{id:String}} ORDER BY {_VERSION_EXPR} DESC LIMIT 1",
            parameters={"id": config_id},
        )

        rows = _rows(result)
        if not rows:
            raise LookupError(f"Resource configuration with id '{config_id}' not found")
        return _from_row(rows[0])

    async def create_resource_configuration(
        self, request: ResourceConfigurationRequest
    ) -> ResourceConfiguration:
        """Persist a new resource configuration as its first snapshot.

        Raises:
            ValueError: the name collides with an existing configuration, or a
                field mapping names a field the resource type does not declare.
            LookupError: the referenced resource type does not exist.
        """
        await self.storage.ensure_resource_configuration_table()

        slug = slugify(request.name)
        table_name = self.storage._qualified_table_name(TABLE)

        result = await self.storage.client.query(
            f"SELECT id FROM {table_name}"
            + " WHERE id = {s:String} OR slug = {s:String}",
            parameters={"s": slug},
        )
        if result.row_count != 0:
            raise ValueError("A record with that id or slug already exists")

        await self._validate_field_mappings(
            request.resource_type, request.field_mappings
        )

        config = ResourceConfiguration(
            id=slug, ref=f"{slug}__v1", **request.model_dump()
        )
        return await self._insert(config)

    async def update_resource_configuration(
        self,
        config_id: str,
        request: ResourceConfigurationUpdate,
        collector_plugin: str | None = None,
    ) -> ResourceConfiguration:
        """Append a new desired snapshot without activating it.

        `None` means "leave unchanged". The resource type is immutable — a
        configuration for a different type is a different configuration.

        Rendering and activation require separate preview/apply requests.

        Args:
            config_id: Stable id shared by all versions of the configuration.
            request: Fields to change; `None` leaves the current value.
            collector_plugin: When given, the update only applies to a
                configuration owned by that plugin.

        Raises:
            ValueError: the request carries no updates, or a field mapping names
                a field the resource type does not declare.
            LookupError: no configuration is stored under that id, or it belongs
                to a different plugin.
        """
        await self.storage.ensure_resource_configuration_table()

        current = await self.get_resource_configuration_by_id(config_id)
        self._assert_owned_by(current, collector_plugin)

        updates = request.model_dump(exclude_none=True)
        if not updates:
            raise ValueError("No fields provided to update")

        if "field_mappings" in updates:
            await self._validate_field_mappings(
                current.resource_type, request.field_mappings
            )

        # Merge through the constructor rather than model_copy: model_dump
        # flattens the nested models to dicts and model_copy does not
        # revalidate, which would leave raw dicts in field_mappings.
        config = ResourceConfiguration(
            **{**current.model_dump(), **updates, "ref": _bump_ref(current.ref)}
        )
        await self._insert(config)

        return config

    async def delete_resource_configuration(self, config_id: str) -> dict[str, str]:
        """Delete every snapshot of a configuration.

        Args:
            config_id: Stable id shared by all versions of the configuration.

        Raises:
            LookupError: no configuration is stored under that id.
        """
        await self.storage.ensure_resource_configuration_table()

        await self.get_resource_configuration_by_id(config_id)

        table_name = self.storage._qualified_table_name(TABLE)
        await self.storage.client.command(
            f"ALTER TABLE {table_name} DELETE WHERE id = {{id:String}}",
            parameters={"id": config_id},
        )

        return {
            "message": f"Resource configuration '{config_id}' deleted",
            "id": config_id,
        }

    async def generate_configuration(self, config: ResourceConfiguration) -> str:
        """Render the configuration for a given snapshot.

        Used primarily to verify syntactic validity of a new version
        before it becomes active.
        """
        plugin = self.plugins.get(config.collector_plugin)
        if plugin is None:
            raise LookupError(f"No collector plugin '{config.collector_plugin}' registered")

        if hasattr(plugin, "_safe_resource_type"):
            plugin._safe_resource_type(config.resource_type)

        nodes = await self._selected_nodes(config)
        return await plugin.render_config(config, nodes)

    async def _selected_nodes(self, config: ResourceConfiguration):
        all_nodes = await NodeService(self.storage).get_nodes()
        return await MembershipService(self.storage).selected_nodes(config, all_nodes)

    async def get_resource_config_devices(self, config_id: str) -> list[str]:
        config = await self.get_resource_configuration_by_id(config_id)
        nodes = await NodeService(self.storage).get_nodes()
        return await MembershipService(self.storage).device_ids(config, nodes)

    async def add_resource_config_device(
        self, config_id: str, device_id: str,
    ) -> list[str]:
        config = await self.get_resource_configuration_by_id(config_id)
        nodes = await NodeService(self.storage).get_nodes()
        membership = MembershipService(self.storage)
        await membership.add(config, nodes, device_id)
        return await membership.device_ids(config, nodes)

    async def remove_resource_config_device(
        self, config_id: str, device_id: str,
    ) -> list[str]:
        config = await self.get_resource_configuration_by_id(config_id)
        nodes = await NodeService(self.storage).get_nodes()
        membership = MembershipService(self.storage)
        await membership.remove(config, nodes, device_id)
        return await membership.device_ids(config, nodes)

    async def plan_configuration(self, config_id: str) -> PlanResponse:
        """Preview the effect of applying a configuration.

        Returns a plan indicating validation status and whether the
        configuration differs from the currently active one.
        """
        config = await self.get_resource_configuration_by_id(config_id)
        plugin = self.plugins.get(config.collector_plugin)
        if plugin is None:
            raise LookupError(f"No collector plugin '{config.collector_plugin}' registered")

        nodes = await self._selected_nodes(config)
        await self._check_owned_selection(config, nodes)
        rendered = await plugin.render_config(config, nodes)
        if not await plugin.validate_config(rendered):
            raise ValueError("Configuration validation failed")
        if not hasattr(plugin, "check_changed"):
            raise ValueError("Collector plugin does not support read-only change inspection")
        is_changed = await plugin.check_changed(rendered, config.id)

        return PlanResponse(
            config_id=config_id,
            resource_type=config.resource_type,
            is_valid=True,
            is_changed=is_changed,
        )

    async def _check_owned_selection(
        self, config: ResourceConfiguration, nodes,
    ) -> None:
        if config.source:
            membership_managed = await MembershipService(self.storage).has_history(
                config.id,
            )
            if (not config.node_selectors and not membership_managed) or not nodes:
                raise ValueError("Owned configuration requires explicit, nonempty node selection")
            if any(node.source != config.source for node in nodes):
                raise ValueError("Ownership collision in selected nodes")

    async def apply_configuration(self, config_id: str) -> ApplyResponse:
        """Validate and publish a configuration.

        Returns an apply response indicating if it was published and reloaded.
        """
        config = await self.get_resource_configuration_by_id(config_id)
        plugin = self.plugins.get(config.collector_plugin)
        if plugin is None:
            raise LookupError("Collector plugin is not registered")
        nodes = await self._selected_nodes(config)
        await self._check_owned_selection(config, nodes)
        rendered = await plugin.render_config(config, nodes)
        published = await plugin.publish_config(rendered, config.id)

        return ApplyResponse(
            config_id=config_id,
            published=published,
            reloaded=published,
        )
