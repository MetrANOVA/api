from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from admin_api.collector.model import ResourceConfiguration

if TYPE_CHECKING:
    from admin_api.nodes.model import Node


class DataSourcePlugin(ABC):

    plugin_id: str  # class-level constant, e.g. "telegraf"
    plugin_type: str = "datasource"

    @abstractmethod
    async def render_config(
        self,
        datasources: ResourceConfiguration,
        nodes: "list[Node] | None" = None,
    ) -> any:
        """Produce a collector config file from one or more DataSources.

        `nodes` is the set of nodes the configuration selects (empty selectors
        resolve to every node). Called by both the CCE (preview) and the plugin
        container (apply).
        """
        ...

    @abstractmethod
    async def publish_config(
        self,
        config_text: str,
        resource_type: str,
    ) -> None:
        """Persist the rendered configuration to the target environment.
        This is the mutation step.
        """
        ...

    @abstractmethod
    async def validate_config(
        self,
        config_text: str,
    ) -> bool:
        """Validate the rendered configuration.
        Returns True if valid, False otherwise.
        """
        ...

    def validate_access(self, node_id: str, datasource: ResourceConfiguration) -> bool:
        """Optional: probe device connectivity before adding a Collection.
        Called by the CCE API when validate_access=True is requested.
        """
        raise NotImplementedError
