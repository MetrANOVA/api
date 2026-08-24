import asyncio

from kubernetes_asyncio.client.exceptions import ApiException

from admin_api.collector.model import FieldConfig, ResourceConfiguration
from admin_api.collector.plugins.telegraf_kube import TelegrafKube, _CONFIGMAP_NAME


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


def test_render_config_patches_existing_configmap():
    api = DummyCoreV1Api()
    plugin = TelegrafKube(core_v1_api=api, namespace="test-ns")

    output = asyncio.run(plugin.render_config(_config()))

    assert "cpu.conf" in output
    assert len(api.patch_calls) == 1
    name, namespace, body = api.patch_calls[0]
    assert name == _CONFIGMAP_NAME
    assert namespace == "test-ns"
    assert list(body["data"].keys()) == ["cpu.conf"]
    assert api.create_calls == []


def test_render_config_creates_configmap_when_missing():
    api = DummyCoreV1Api(missing=True)
    plugin = TelegrafKube(core_v1_api=api, namespace="test-ns")

    asyncio.run(plugin.render_config(_config()))

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
