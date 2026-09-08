import asyncio

from kubernetes_asyncio.client.exceptions import ApiException

from admin_api.collector.model import FieldConfig, ResourceConfiguration
from admin_api.collector.plugins.telegraf import (
    TelegrafKube,
    TelegrafVscode,
    _CONFIGMAP_NAME,
)
from admin_api.nodes.model import Node


class DummyCoreV1Api:
    """Records patch/create calls; optionally simulates a missing ConfigMap."""

    def __init__(self, missing=False):
        self.missing = missing
        self.patch_calls = []
        self.create_calls = []

    async def patch_namespaced_config_map(self, name, namespace, body):
        self.patch_calls.append((name, namespace, body))
        if self.missing:
            raise ApiException(status=404, reason="Not Found")

    async def create_namespaced_config_map(self, namespace, body):
        self.create_calls.append((namespace, body))


def _config(**overrides) -> ResourceConfiguration:
    defaults = dict(
        id="example_cpu",
        ref="example_cpu__v1",
        name="example cpu",
        resource_type="cpu",
        collector_plugin="telegraf_kube",
        interval=10,
        timeout=15,
        field_mappings={
            "usage": FieldConfig(oid=".1.3.6.1.2.1.2.2.1.10"),
        },
    )
    defaults.update(overrides)
    return ResourceConfiguration(**defaults)


def _node(**overrides) -> Node:
    defaults = dict(
        node_id="n1",
        host="10.0.0.1",
        port=161,
        community="public",
        name="rtr-1",
        make="Cisco",
        model="ASR9000",
    )
    defaults.update(overrides)
    return Node(**defaults)


# --------------------------------------------------------------------------
# generate_telegraf_config
# --------------------------------------------------------------------------


def test_generate_telegraf_config_without_nodes_has_no_snmp_inputs():
    config = TelegrafVscode().generate_telegraf_config(_config(), nodes=[])

    assert config["inputs"]["snmp"] == []
    # the shell of the config is still there
    assert config["outputs"]["kafka"][0]["topic"] == "metranova_snmp"


def test_generate_telegraf_config_one_block_per_community():
    nodes = [
        _node(node_id="a", host="10.0.0.1", community="public"),
        _node(node_id="b", host="10.0.0.2", community="public"),
        _node(node_id="c", host="10.0.0.3", community="private"),
    ]

    blocks = TelegrafVscode().generate_telegraf_config(_config(), nodes=nodes)[
        "inputs"
    ]["snmp"]

    assert [b["community"] for b in blocks] == ["public", "private"]
    assert blocks[0]["agents"] == ["udp://10.0.0.1:161", "udp://10.0.0.2:161"]
    assert blocks[1]["agents"] == ["udp://10.0.0.3:161"]


def test_generate_telegraf_config_carries_field_and_table_mappings():
    config = _config(
        resource_type="interface",
        field_mappings={
            "intf": FieldConfig(oid=".1.3.6.1.2.1.2.2.1.2", is_tag=True),
            "input": FieldConfig(oid=".1.3.6.1.2.1.2.2.1.10"),
        },
    )

    block = TelegrafVscode().generate_telegraf_config(config, nodes=[_node()])[
        "inputs"
    ]["snmp"][0]

    assert {f["name"] for f in block["field"]} == {"node"}
    table = block["table"][0]
    assert table["name"] == "interface"
    assert {f["name"] for f in table["field"]} == {"intf", "input"}


def test_generate_telegraf_config_uses_node_port():
    block = TelegrafVscode().generate_telegraf_config(
        _config(), nodes=[_node(host="dev.example", port=1161)]
    )["inputs"]["snmp"][0]

    assert block["agents"] == ["udp://dev.example:1161"]


# --------------------------------------------------------------------------
# TelegrafKube.render_config
# --------------------------------------------------------------------------


def test_render_config_patches_existing_configmap():
    api = DummyCoreV1Api()
    plugin = TelegrafKube(core_v1_api=api, namespace="test-ns")

    output = asyncio.run(plugin.render_config(_config(), [_node()]))

    assert "cpu.conf" in output
    assert len(api.patch_calls) == 1
    name, namespace, body = api.patch_calls[0]
    assert name == _CONFIGMAP_NAME
    assert namespace == "test-ns"
    assert list(body["data"].keys()) == ["cpu.conf"]
    assert "udp://10.0.0.1:161" in body["data"]["cpu.conf"]
    assert api.create_calls == []


def test_render_config_creates_configmap_when_missing():
    api = DummyCoreV1Api(missing=True)
    plugin = TelegrafKube(core_v1_api=api, namespace="test-ns")

    asyncio.run(plugin.render_config(_config(), [_node()]))

    assert len(api.patch_calls) == 1
    assert len(api.create_calls) == 1
    namespace, body = api.create_calls[0]
    assert namespace == "test-ns"
    assert body.metadata.name == _CONFIGMAP_NAME
    assert list(body.data.keys()) == ["cpu.conf"]


def test_render_config_rejects_unsafe_resource_type():
    api = DummyCoreV1Api()
    plugin = TelegrafKube(core_v1_api=api, namespace="test-ns")

    try:
        asyncio.run(plugin.render_config(_config(resource_type="../../etc/passwd")))
    except ValueError as exc:
        assert "Unsafe resource type name" in str(exc)
    else:
        raise AssertionError("expected ValueError for unsafe resource_type")

    assert api.patch_calls == []
