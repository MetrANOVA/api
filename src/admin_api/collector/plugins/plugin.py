from abc import ABC, abstractmethod

from admin_api.collector.model import ResourceConfiguration


class DataSourcePlugin(ABC):

    plugin_id: str  # class-level constant, e.g. "telegraf"
    plugin_type: str = "datasource"

    @abstractmethod
    def render_config(self, datasources: list[ResourceConfiguration]) -> any:
        """Produce a collector config file from one or more DataSources.
        Called by both the CCE (preview) and the plugin container (apply).
        """
        ...

    @abstractmethod
    def reload(self) -> None:
        """Signal the collector process to reload its configuration.
        Called only by the plugin container after writing the config file.
        """
        ...

    def validate_access(self, node_id: str, datasource: ResourceConfiguration) -> bool:
        """Optional: probe device connectivity before adding a Collection.
        Called by the CCE API when validate_access=True is requested.
        """
        raise NotImplementedError
