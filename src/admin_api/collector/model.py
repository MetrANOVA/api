from pydantic import BaseModel, Field


class FieldConfig(BaseModel):
    """

    If datasource fields can be different we need a better approach than the current fixed FieldConfig model.

    """

    oid: str
    is_tag: bool = False
    secondary_index_table: str | None = None
    secondary_index_use: bool | None = None


class Selector(BaseModel):
    type: str
    value: str


class ResourceConfiguration(BaseModel):
    """A stored snapshot of a collector resource configuration.

    Attributes:
        id: Stable identifier, shared by every version (e.g. 'example_telegraf').
        ref: Immutable snapshot identifier ('example_telegraf__v1').
        name: Human-readable name; the id is derived from it.
        resource_type: Slug of the resource type this configuration collects for.
        collector_plugin: The plugin that renders this configuration.
        interval: Polling interval in seconds.
        timeout: Per-poll timeout in seconds.
        field_mappings: Resource-type field name -> how to collect it.
        node_selectors: Nodes to collect from; empty means all of them.

    Usage:

        plugin.load(resource_configuration)
        logger.info(f"Collector configuration: {plugin.generate_configuration()}")

    """

    id: str
    ref: str
    name: str
    resource_type: str
    collector_plugin: str
    interval: int = 60
    timeout: int = 15
    field_mappings: dict[str, FieldConfig] = Field(default_factory=dict)
    node_selectors: list[Selector] = Field(default_factory=list)


class ResourceConfigurationRequest(BaseModel):
    """Client-supplied fields; id/ref are assigned by the server on insert."""

    name: str = Field(min_length=1)
    resource_type: str = Field(min_length=1)
    collector_plugin: str = Field(min_length=1)
    interval: int = Field(default=60, gt=0)
    timeout: int = Field(default=15, gt=0)
    field_mappings: dict[str, FieldConfig] = Field(default_factory=dict)
    node_selectors: list[Selector] = Field(default_factory=list)


class ResourceConfigurationUpdate(BaseModel):
    """Partial update; `None` leaves the current value unchanged.

    `resource_type` is absent by design — a configuration for a different
    resource type is a different configuration.
    """

    name: str | None = Field(default=None, min_length=1)
    collector_plugin: str | None = Field(default=None, min_length=1)
    interval: int | None = Field(default=None, gt=0)
    timeout: int | None = Field(default=None, gt=0)
    field_mappings: dict[str, FieldConfig] | None = None
    node_selectors: list[Selector] | None = None
