import re
from io import StringIO
from ruamel.yaml import YAML
from kubernetes_asyncio.client import CoreV1Api, V1ConfigMap, V1ObjectMeta
import tomli_w


from admin_api.collector.plugins.plugin import DataSourcePlugin
from admin_api.collector.plugins.kube_client import ApiException, get_core_v1_api
from admin_api.collector.model import ResourceConfiguration
from admin_api.settings import get_settings

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_-]+$")
_CONFIGMAP_NAME = "metranova-collector-resource-configurations"


class TelegrafKube(DataSourcePlugin):
    plugin_id = "telegraf_kube"

    def __init__(
        self,
        core_v1_api: CoreV1Api | None = None,
        namespace: str | None = None,
    ):
        self._core_v1_api = core_v1_api
        self.namespace = namespace or get_settings().kube_namespace

    async def render_config(self, c: ResourceConfiguration):
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

        config = self.generate_telegraf_config(
            c,
            agents=["udp://snmp-simulator:161"],
            version=2,
            community="public",
        )

        # resource_type reaches us from the create payload — keep it out of the path
        # unless it is a plain name.
        if not _SAFE_NAME.match(c.resource_type):
            raise ValueError(f"Unsafe resource type name: {c.resource_type!r}")

        yaml = YAML()
        data = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "metranova-collector-resource-configurations"},
            "data": {},
        }

        conf_contents = tomli_w.dumps(config)

        # Inject a comment next to a specific key
        data["data"][f"{c.resource_type}.conf"] = conf_contents

        stream = StringIO()
        yaml.dump(data, stream)
        output = stream.getvalue()
        print(output)

        await self._push_configmap(c.resource_type, conf_contents)

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

    def generate_telegraf_config(
        self,
        config: ResourceConfiguration,
        agents: list[str],
        version: int = 2,
        community: str = "public",
    ) -> dict[str, any]:
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
            entry: dict[str, any] = {
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

        snmp_block: dict[str, any] = {
            "agents": agents,
            "version": version,
            "community": community,
            "interval": f"{config.interval}s",
            "timeout": f"{config.timeout}s",
        }

        if scalar_fields:
            snmp_block["field"] = scalar_fields

        if table_fields:
            table_block: dict[str, any] = {
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
