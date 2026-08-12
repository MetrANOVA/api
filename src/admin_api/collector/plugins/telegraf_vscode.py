import re
import tomli_w

from admin_api.collector.plugins.plugin import DataSourcePlugin
from admin_api.collector.model import ResourceConfiguration

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_-]+$")


class TelegrafVscode(DataSourcePlugin):
    plugin_id = "telegraf_vscode"

    def render_config(self, c: ResourceConfiguration):
        print("Rendering telegraf-vscode plugin")

        config = self.generate_telegraf_config(
            c,
            agents=["udp://snmp-simulator:161"],
            version=2,
            community="public",
        )

        toml_config = tomli_w.dumps(config)

        # resource_type reaches us from the create payload — keep it out of the path
        # unless it is a plain name.
        if not _SAFE_NAME.match(c.resource_type):
            raise ValueError(f"Unsafe resource type name: {c.resource_type!r}")

        with open(f"/etc/telegraf/telegraf.d/{c.resource_type}.conf", "w") as f:
            f.write(toml_config)

        return toml_config

    def reload(self) -> None:
        print("Reloading telegraf-vscode plugin")
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
