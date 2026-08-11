from pydantic import BaseModel, Field, field_validator, model_validator


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
    """Represents a resource configuration.
    Attributes:
        name: The name of the resource configuration.
        resource_type: The type of resource this configuration corresponds to.
        collector_plugin: The type of plugin used for this resource.
        interval: The polling interval for the resource in seconds.
        field_configs: A dictionary mapping field names to their configurations.
        selectors: A list of selectors applied to the resource.

    Usage:

        plugin.load(resource_configuration)
        logger.info(f"Collector configuration: {plugin.generate_configuration()})

    """

    id: str
    ref: str
    name: str
    # Check is valid
    resource_type: str
    collector_plugin: str
    interval: int = 60
    timeout: int = 15
    # Validate all required fields are present
    resource_type_field_mappings: dict[str, FieldConfig] = Field(default_factory=dict)
    # If empty everything is selected for the resource
    node_selectors: list[Selector] = Field(default_factory=list)
