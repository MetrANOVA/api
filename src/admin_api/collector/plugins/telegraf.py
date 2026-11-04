import re
import subprocess
import asyncio
import hashlib
from pathlib import Path
import tempfile
from typing import Any

import tomli_w
from kubernetes_asyncio.client import AppsV1Api, CoreV1Api

from admin_api.collector.model import ResourceConfiguration
from admin_api.collector.plugins.kube_client import ApiException, get_core_v1_api
from admin_api.collector.plugins.plugin import DataSourcePlugin
from admin_api.nodes.model import Node
from admin_api.settings import get_settings

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_-]+$")


class TelegrafError(ValueError):
    """Sanitized configuration validation or activation failure."""


class TelegrafValidator:
    """Validates Telegraf configuration using the telegraf binary."""

    def __init__(self):
        self.binary_path = get_settings().telegraf_binary_path
        self.expected_version = get_settings().telegraf_version
        self.base_config_path = get_settings().telegraf_base_config_path

    async def validate(self, config_text: str) -> bool:
        """Load and test the deployment base configuration and candidate."""
        return await asyncio.to_thread(self._validate, config_text)

    def _validate(self, config_text: str) -> bool:
        if not self.expected_version or not self.base_config_path:
            raise TelegrafError("Telegraf version and deployment base config must be configured.")
        try:
            version = subprocess.run(
                [self.binary_path, "--version"], capture_output=True, timeout=10,
                check=False,
            )
            if version.returncode or not re.search(
                rf"\b{re.escape(self.expected_version)}\b",
                version.stdout.decode("utf-8", errors="replace"),
            ):
                raise TelegrafError("Telegraf version does not match the deployment.")
            base = Path(self.base_config_path).read_text()
            with tempfile.TemporaryDirectory() as directory:
                candidate = Path(directory) / "candidate.conf"
                candidate.write_text(base + "\n" + config_text)
                result = subprocess.run(
                    [self.binary_path, "--config", str(candidate), "--test",
                     "--test-wait", "0"],
                    capture_output=True, check=False, timeout=10,
                )
                if result.returncode:
                    raise TelegrafError("Telegraf rejected the assembled configuration.")
        except (OSError, subprocess.TimeoutExpired):
            raise TelegrafError("Telegraf validation could not complete.") from None
        return True


class TelegrafPlugin(DataSourcePlugin):
    """Shared rendering for every Telegraf-backed collector plugin."""

    def _safe_resource_type(self, resource_type: str) -> str:
        # resource_type reaches us from the create payload — keep it out of file
        # paths and ConfigMap keys unless it is a plain name.
        if not _SAFE_NAME.fullmatch(resource_type):
            raise ValueError("Unsafe resource type name")
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

        scalar_fields = []
        table_fields = []

        # Sort field mappings by key to ensure deterministic output
        for k in sorted(config.field_mappings.keys()):
            oid = config.field_mappings[k]
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
        for node in (nodes or []):
            by_community.setdefault(node.community, []).append(node)

        snmp_blocks: list[dict[str, Any]] = []
        # Sort communities to ensure deterministic block order
        for community in sorted(by_community.keys()):
            group = by_community[community]
            # Sort nodes within community by host to ensure deterministic agent list
            sorted_agents = sorted(
                f"udp://[{n.host}]:{n.port}" if ":" in n.host
                else f"udp://{n.host}:{n.port}" for n in group
            )
            block: dict[str, Any] = {
                "agents": sorted_agents,
                "agent_host_tag": "device",
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
            "outputs": {},
        }


class TelegrafVscode(TelegrafPlugin):
    plugin_id = "telegraf_vscode"

    async def validate_config(self, config_text: str) -> bool:
        """Validate the rendered configuration."""
        return await TelegrafValidator().validate(config_text)

    async def render_config(
        self, c: ResourceConfiguration, nodes: list[Node] | None = None
    ):
        print("Rendering telegraf-vscode plugin")

        config = self.generate_telegraf_config(c, nodes)
        config["outputs"] = {
            "kafka": [
                {
                    "brokers": ["kafka:9092"],
                    "topic": "metranova_snmp",
                    "data_format": "json",
                    "version": "3.0.0",
                }
            ],
            "file": [{"files": ["stdout"], "data_format": "influx"}],
        }
        return tomli_w.dumps(config)

    async def publish_config(self, config_text: str, resource_type: str) -> None:
        resource_type = self._safe_resource_type(resource_type)
        with open(f"/etc/telegraf/telegraf.d/{resource_type}.conf", "w") as f:
            f.write(config_text)

    def reload(self) -> None:
        print("Reloading telegraf-vscode plugin")
        return None


class TelegrafKube(TelegrafPlugin):
    plugin_id = "telegraf_kube"

    def __init__(
        self,
        core_v1_api: CoreV1Api | None = None,
        namespace: str | None = None,
        apps_v1_api: AppsV1Api | None = None,
    ):
        self._core_v1_api = core_v1_api
        self._apps_v1_api = apps_v1_api
        self.release_namespace = namespace or get_settings().kube_release_namespace
        self.release_name = get_settings().kube_release_name
        self.validator = TelegrafValidator()

    async def render_config(
        self, c: ResourceConfiguration, nodes: list[Node] | None = None
    ):
        """Render the TOML configuration for a single resource."""
        self._safe_resource_type(c.resource_type)
        config = self.generate_telegraf_config(c, nodes)
        return tomli_w.dumps({"inputs": config["inputs"]})

    async def validate_config(self, config_text: str) -> bool:
        """Validate the rendered configuration."""
        return await self.validator.validate(config_text)

    async def _current(self):
        api = self._core_v1_api or await get_core_v1_api()
        cm_name = f"{self.release_name}-telegraf-resource-configurations"
        try:
            cm = await api.read_namespaced_config_map(cm_name, self.release_namespace)
        except ApiException:
            raise TelegrafError("Cannot read the preprovisioned Telegraf ConfigMap.") from None
        return cm

    async def check_changed(self, config_text: str, config_id: str) -> bool:
        self._safe_resource_type(config_id)
        cm = await self._current()
        return (cm.data or {}).get(f"{config_id}.conf") != config_text

    async def publish_config(self, config_text: str, config_id: str) -> bool:
        """Validate the complete candidate, then publish and request activation."""
        config_id = self._safe_resource_type(config_id)
        api = self._core_v1_api or await get_core_v1_api()
        cm = await self._current()
        data = dict(cm.data or {})
        changed = data.get(f"{config_id}.conf") != config_text
        data[f"{config_id}.conf"] = config_text
        if not all(key.endswith(".conf") for key in data):
            raise TelegrafError("Telegraf ConfigMap contains unsupported entries.")
        assembled = "\n".join(data[key] for key in sorted(data))
        if not await self.validate_config(assembled):
            raise TelegrafError("Telegraf configuration validation failed.")
        digest = hashlib.sha256(assembled.encode()).hexdigest()
        annotation = "metranova.org/telegraf-active-sha256"
        annotations = (cm.metadata.annotations or {}) if cm.metadata else {}
        if not changed and annotations.get(annotation) == digest:
            return False
        name = f"{self.release_name}-telegraf-resource-configurations"
        try:
            if changed:
                await api.patch_namespaced_config_map(
                    name=name, namespace=self.release_namespace,
                    body={"metadata": {"resourceVersion": cm.metadata.resource_version},
                          "data": data},
                )
            apps = self._apps_v1_api or AppsV1Api(api.api_client)
            await apps.patch_namespaced_deployment(
                name=f"{self.release_name}-telegraf",
                namespace=self.release_namespace,
                body={"spec": {"template": {"metadata": {"annotations": {
                    "metranova.org/telegraf-config-sha256": digest,
                }}}}},
            )
            await api.patch_namespaced_config_map(
                name=name,
                namespace=self.release_namespace,
                body={"metadata": {"annotations": {annotation: digest}}},
            )
        except ApiException:
            raise TelegrafError(
                "Telegraf publication or activation acknowledgement failed; "
                "configuration may be published. Rerun apply to recover."
            ) from None
        return True
