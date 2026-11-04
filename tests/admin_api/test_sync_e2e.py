import asyncio
from pathlib import Path
import subprocess
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from kubernetes_asyncio.client.exceptions import ApiException

from admin_api.collector.model import FieldConfig, ResourceConfiguration, Selector
from admin_api.collector.plugins.telegraf import (
    TelegrafError, TelegrafKube, TelegrafValidator,
)
from admin_api.collector.service import CollectorService
from admin_api.nodes.model import Node


@pytest.fixture
def flow(monkeypatch):
    config = ResourceConfiguration(
        id="sample", ref="sample__v1", name="sample", resource_type="sample",
        collector_plugin="telegraf_kube", source="inventory", source_id="sample",
        field_mappings={"counter": FieldConfig(oid="1.3.6.1.2.1.2.2.1.10")},
        node_selectors=[Selector(type="node_id", value="node1")],
    )
    node = Node(
        node_id="node1", name="node1", source="inventory", source_id="1",
        host="2001:db8::1", port=161, community="fixture", make="generic", model="generic",
    )
    cm = SimpleNamespace(
        data={}, metadata=SimpleNamespace(annotations={}, resource_version="1"),
    )
    core = SimpleNamespace(
        read_namespaced_config_map=AsyncMock(return_value=cm),
        patch_namespaced_config_map=AsyncMock(),
    )
    apps = SimpleNamespace(patch_namespaced_deployment=AsyncMock())

    async def patch_cm(**kwargs):
        cm.data.update(kwargs["body"].get("data", {}))
        cm.metadata.annotations.update(
            kwargs["body"].get("metadata", {}).get("annotations", {})
        )

    core.patch_namespaced_config_map.side_effect = patch_cm
    plugin = TelegrafKube(core_v1_api=core, apps_v1_api=apps, namespace="test")
    plugin.validate_config = AsyncMock(return_value=True)
    service = object.__new__(CollectorService)
    service.plugins = {"telegraf_kube": plugin}
    service.get_resource_configuration_by_id = AsyncMock(return_value=config)
    monkeypatch.setattr(
        "admin_api.collector.service.NodeService",
        lambda storage: SimpleNamespace(get_nodes=AsyncMock(return_value=[node])),
    )
    service.storage = object()
    return service, plugin, core, apps, cm, config, node


def test_apply_validates_publishes_and_is_idempotent(flow):
    service, plugin, core, apps, cm, _, _ = flow
    result = asyncio.run(service.apply_configuration("sample"))
    assert result.published and result.reloaded
    candidate = plugin.validate_config.call_args.args[0]
    assert candidate == cm.data["sample.conf"]
    assert "udp://[2001:db8::1]:161" in candidate
    assert "[[outputs" not in candidate
    assert not asyncio.run(service.apply_configuration("sample")).published
    assert apps.patch_namespaced_deployment.call_count == 1


def test_preview_has_no_publication_or_sensitive_output(flow):
    service, _, core, apps, _, _, _ = flow
    plan = asyncio.run(service.plan_configuration("sample"))
    assert plan.is_valid and plan.is_changed
    assert plan.rendered_config is None
    core.patch_namespaced_config_map.assert_not_called()
    apps.patch_namespaced_deployment.assert_not_called()


def test_validation_failure_preserves_current_config(flow):
    service, plugin, core, apps, cm, _, _ = flow
    cm.data = {"existing.conf": "existing"}
    plugin.validate_config.return_value = False
    with pytest.raises(TelegrafError, match="validation failed"):
        asyncio.run(service.apply_configuration("sample"))
    assert cm.data == {"existing.conf": "existing"}
    core.patch_namespaced_config_map.assert_not_called()
    apps.patch_namespaced_deployment.assert_not_called()


def test_restart_failure_recovers_without_republishing(flow):
    service, _, core, apps, cm, _, _ = flow
    apps.patch_namespaced_deployment.side_effect = ApiException(
        status=503, reason="sensitive-response",
    )
    with pytest.raises(TelegrafError, match="Rerun apply") as error:
        asyncio.run(service.apply_configuration("sample"))
    assert "sensitive-response" not in str(error.value)
    assert "sample.conf" in cm.data
    assert cm.metadata.annotations == {}
    apps.patch_namespaced_deployment.side_effect = None
    assert asyncio.run(service.apply_configuration("sample")).reloaded
    assert core.patch_namespaced_config_map.call_count == 2


@pytest.mark.parametrize("selection", ["empty", "unowned", "other-source"])
def test_owned_selection_fails_before_publication(flow, selection):
    service, _, core, apps, _, config, node = flow
    if selection == "empty":
        config.node_selectors = []
    else:
        node.source = None if selection == "unowned" else "other"
    with pytest.raises(ValueError):
        asyncio.run(service.apply_configuration("sample"))
    core.patch_namespaced_config_map.assert_not_called()
    apps.patch_namespaced_deployment.assert_not_called()


def test_multiple_collectors_for_same_resource_keep_separate_files(flow):
    service, plugin, _, _, cm, config, _ = flow
    asyncio.run(service.apply_configuration("sample"))
    config.id = "second"
    config.interval = 300
    asyncio.run(service.apply_configuration("second"))
    assert set(cm.data) == {"sample.conf", "second.conf"}
    assert "sample.conf" not in plugin.validate_config.call_args.args[0]
    assert cm.data["sample.conf"] in plugin.validate_config.call_args.args[0]


@pytest.mark.parametrize("failure", [None, "version", "missing", "timeout", "invalid"])
def test_validator_checks_version_and_actual_private_candidate(monkeypatch, tmp_path, failure):
    base = tmp_path / "base.conf"
    base.write_text('[agent]\ninterval = "60s"\n')
    monkeypatch.setattr(
        "admin_api.collector.plugins.telegraf.get_settings",
        lambda: SimpleNamespace(
            telegraf_binary_path="telegraf", telegraf_version="1.40.0",
            telegraf_base_config_path=str(base),
        ),
    )
    candidate_paths = []

    def run(command, **kwargs):
        assert kwargs["timeout"] == 10
        if failure == "missing":
            raise FileNotFoundError("sensitive")
        if failure == "timeout":
            raise subprocess.TimeoutExpired(command, 10, stderr="sensitive")
        if command[-1] == "--version":
            return subprocess.CompletedProcess(
                command, 0, stdout=b"Telegraf 1.39.0" if failure == "version"
                else b"Telegraf 1.40.0", stderr=b"",
            )
        candidate = Path(command[command.index("--config") + 1])
        assert candidate.read_text() == base.read_text() + "\n[[inputs.snmp]]\n"
        assert candidate.parent.stat().st_mode & 0o077 == 0
        assert command[-2:] == ["--test-wait", "0"]
        candidate_paths.append(candidate)
        return subprocess.CompletedProcess(
            command, 1 if failure == "invalid" else 0,
            stdout=b"", stderr=b"sensitive",
        )

    monkeypatch.setattr("admin_api.collector.plugins.telegraf.subprocess.run", run)
    validator = TelegrafValidator()
    if failure:
        with pytest.raises(TelegrafError) as error:
            asyncio.run(validator.validate("[[inputs.snmp]]\n"))
        assert "sensitive" not in str(error.value)
    else:
        assert asyncio.run(validator.validate("[[inputs.snmp]]\n"))
    assert all(not path.exists() for path in candidate_paths)


def test_validator_requires_deployment_settings(monkeypatch):
    monkeypatch.setattr(
        "admin_api.collector.plugins.telegraf.get_settings",
        lambda: SimpleNamespace(
            telegraf_binary_path="telegraf", telegraf_version="",
            telegraf_base_config_path="",
        ),
    )
    with pytest.raises(TelegrafError, match="must be configured"):
        asyncio.run(TelegrafValidator().validate("candidate"))
