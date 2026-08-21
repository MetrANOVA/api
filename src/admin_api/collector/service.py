import logging
import re

from importlib.metadata import entry_points
from typing import TYPE_CHECKING, Any

import tomli_w

from admin_api.resource_type.router import (
    _existing_data_field_names,
    _existing_meta_field_names,
)

from .model import (
    FieldConfig,
    ResourceConfiguration,
    ResourceConfigurationRequest,
    ResourceConfigurationUpdate,
    Selector,
)

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
        """Append a new snapshot of an existing configuration and re-render it.

        `None` means "leave unchanged". The resource type is immutable — a
        configuration for a different type is a different configuration.

        The snapshot is written first and rendered afterwards. A render failure
        is logged, not raised: the insert is append-only and cannot be rolled
        back, so the new version stands regardless.

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

        try:
            await self.generate_configuration(config)
        except Exception as e:
            logger.exception(f"Error rendering configuration '{config.ref}': {e}")

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

    async def generate_configuration(self, c: ResourceConfiguration):
        plugin = self.plugins.get(c.collector_plugin)
        if plugin is None:
            raise LookupError(
                f"No collector plugin '{c.collector_plugin}' is registered. "
                f"Available plugins: {', '.join(sorted(self.plugins)) or 'none'}"
            )
        return plugin.render_config(c)
        # config = self.generate_telegraf_config(
        #     c,
        #     agents=["udp://snmp-simulator:161"],
        #     version=2,
        #     community="public",
        # )

        # toml_config = tomli_w.dumps(config)

        # # resource_type reaches us from the create payload — keep it out of the path
        # # unless it is a plain name.
        # if not _SAFE_NAME.match(c.resource_type):
        #     raise ValueError(f"Unsafe resource type name: {c.resource_type!r}")

        # with open(f"/etc/telegraf/telegraf.d/{c.resource_type}.toml", "w") as f:
        #     f.write(toml_config)

        # return toml_config

    def generate_telegraf_config(
        self,
        config: ResourceConfiguration,
        agents: list[str],
        version: int = 2,
        community: str = "public",
    ) -> dict[str, Any]:
        """Build a Telegraf SNMP input config dict ready for TOML serialization.

        Args:
            agents: List of SNMP agent URIs (e.g. ["udp://host:161"]).
            version: SNMP version (default 2).
            community: SNMP community string (default "public").

        Returns:
            A plain dict matching the Telegraf SNMP input config structure.
        """

        scalar_fields = [{"name": "node", "oid": ".1.3.6.1.2.1.1.5.0", "is_tag": True}]
        table_fields = []

        for k, oid in config.field_mappings.items():
            is_scalar = oid.oid.endswith(".0")
            entry: dict[str, Any] = {
                "name": k,
                "oid": oid.oid,
            }

            if is_scalar:
                if bool(oid.is_tag):
                    entry["is_tag"] = True
                scalar_fields.append(entry)
            else:
                if bool(oid.is_tag):
                    entry["is_tag"] = True
                if bool(oid.secondary_index_use):
                    entry["secondary_index_use"] = True
                if bool(oid.secondary_index_table):
                    entry["secondary_index_table"] = True
                table_fields.append(entry)

        inherit_tags = [f["name"] for f in scalar_fields if f.get("is_tag")]

        snmp_block: dict[str, Any] = {
            "agents": agents,
            "version": version,
            "community": community,
            "interval": f"{config.interval}s",
            "timeout": f"{config.timeout}s",
        }

        if scalar_fields:
            snmp_block["field"] = scalar_fields

        if table_fields:
            table_block: dict[str, Any] = {
                "name": config.resource_type,
                "index_as_tag": True,
            }
            if inherit_tags:
                table_block["inherit_tags"] = inherit_tags
            table_block["field"] = table_fields
            snmp_block["table"] = [table_block]

        return {
            "agent": {
                "interval": f"{config.interval}s",
                "round_interval": True,
                "flush_interval": f"{config.interval}s",
            },
            "inputs": {"snmp": [snmp_block]},
            "outputs": {
                "kafka": [
                    {
                        "brokers": ["kafka:9092"],
                        "topic": "metranova_snmp",
                        "data_format": "json",
                        "version": "3.0.0",
                    }
                ],
                "file": [{"files": ["stdout"], "data_format": "influx"}],
            },
        }
