import re
from io import StringIO
from typing import Any

import tomli_w
from kubernetes_asyncio.client import CoreV1Api, V1ConfigMap, V1ObjectMeta
from ruamel.yaml import YAML

from admin_api.collector.model import ResourceConfiguration
from admin_api.collector.plugins.kube_client import ApiException, get_core_v1_api
from admin_api.collector.plugins.plugin import DataSourcePlugin
from admin_api.nodes.model import Node
from admin_api.settings import get_settings

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_-]+$")
_CONFIGMAP_NAME = "metranova-collector-resource-configurations"


class TelegrafPlugin(DataSourcePlugin):
    """Shared rendering for every Telegraf-backed collector plugin."""

    def _safe_resource_type(self, resource_type: str) -> str:
        # resource_type reaches us from the create payload — keep it out of file
        # paths and ConfigMap keys unless it is a plain name.
        if not _SAFE_NAME.match(resource_type):
            raise ValueError(f"Unsafe resource type name: {resource_type!r}")
        return resource_type

    def generate_telegraf_config(
        self,
        config: ResourceConfiguration,
        nodes: list[Node] | None = None,
        version: int = 2,
    ) -> dict[str, Any]:
        """Build a Telegraf SNMP input config dict ready for TOML serialization.

        One ``[[inputs.snmp]]`` block is emitted per distinct community among
        `nodes`, its ``agents`` being ``udp://{host}:{port}`` for the nodes that
        share that community. No nodes means no SNMP inputs.
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

        table_block: dict[str, Any] | None = None
        if table_fields:
            table_block = {
                "name": config.resource_type,
                "index_as_tag": True,
            }
            if inherit_tags:
                table_block["inherit_tags"] = inherit_tags
            table_block["field"] = table_fields

        by_community: dict[str, list[Node]] = {}
        for node in nodes or []:
            by_community.setdefault(node.community, []).append(node)

        snmp_blocks: list[dict[str, Any]] = []
        for community, group in by_community.items():
            block: dict[str, Any] = {
                "agents": [f"udp://{n.host}:{n.port}" for n in group],
                "version": version,
                "community": community,
                "interval": f"{config.interval}s",
                "timeout": f"{config.timeout}s",
            }
            if scalar_fields:
                block["field"] = scalar_fields
            if table_block is not None:
                block["table"] = [table_block]
            snmp_blocks.append(block)

        return {
            "agent": {
                "interval": f"{config.interval}s",
                "round_interval": True,
                "flush_interval": f"{config.interval}s",
            },
            "inputs": {"snmp": snmp_blocks},
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


class TelegrafVscode(TelegrafPlugin):
    plugin_id = "telegraf_vscode"

    async def render_config(
        self, c: ResourceConfiguration, nodes: list[Node] | None = None
    ):
        print("Rendering telegraf-vscode plugin")

        config = self.generate_telegraf_config(c, nodes)
        toml_config = tomli_w.dumps(config)

        resource_type = self._safe_resource_type(c.resource_type)
        with open(f"/etc/telegraf/telegraf.d/{resource_type}.conf", "w") as f:
            f.write(toml_config)

        return toml_config

    def reload(self) -> None:
        print("Reloading telegraf-vscode plugin")
        return None


class TelegrafKube(TelegrafPlugin):
    plugin_id = "telegraf_kube"

    def __init__(
        self,
        core_v1_api: CoreV1Api | None = None,
        namespace: str | None = None,
    ):
        self._core_v1_api = core_v1_api
        self.namespace = namespace or get_settings().kube_namespace

    async def render_config(
        self, c: ResourceConfiguration, nodes: list[Node] | None = None
    ):
        """

        Example:

            ---
            apiVersion: v1
            kind: ConfigMap
            metadata:
            name: metranova-collector-resource-configurations
            data:
            telegraf_example.conf: |
                ... # Telegraf Configuration Example
            cpu.conf: |
                ... # CPU Configuration Example

        """
        print("Rendering telegraf-kube plugin")

        config = self.generate_telegraf_config(c, nodes)
        resource_type = self._safe_resource_type(c.resource_type)

        yaml = YAML()
        data = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": _CONFIGMAP_NAME},
            "data": {},
        }

        conf_contents = tomli_w.dumps(config)
        data["data"][f"{resource_type}.conf"] = conf_contents

        stream = StringIO()
        yaml.dump(data, stream)
        output = stream.getvalue()
        print(output)

        await self._push_configmap(resource_type, conf_contents)

        return output

    async def _push_configmap(self, resource_type: str, conf_contents: str) -> None:
        api = self._core_v1_api or await get_core_v1_api()
        key = f"{resource_type}.conf"
        try:
            await api.patch_namespaced_config_map(
                name=_CONFIGMAP_NAME,
                namespace=self.namespace,
                body={"data": {key: conf_contents}},
            )
        except ApiException as e:
            if e.status == 404:
                await api.create_namespaced_config_map(
                    namespace=self.namespace,
                    body=V1ConfigMap(
                        metadata=V1ObjectMeta(name=_CONFIGMAP_NAME),
                        data={key: conf_contents},
                    ),
                )
            else:
                raise

    def reload(self) -> None:
        print("Reloading telegraf-kube plugin")
        return None
