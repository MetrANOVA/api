import asyncio
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from admin_api.app import app
from admin_api.collector.membership import MembershipService
from admin_api.collector.model import ResourceConfiguration, Selector
from admin_api.nodes.model import Node
from metranova.storage.clickhouse import Clickhouse


class MembershipClient:
    def __init__(self):
        self.events = []
        self.insert_calls = []

    async def query(self, query, parameters=None):
        resource_config_id = parameters["resource_config_id"]
        latest = {}
        for event in self.events:
            if event[0] == resource_config_id:
                latest[event[1]] = event[2]
        rows = [
            {"device_identity": identity, "active": active}
            for identity, active in sorted(latest.items())
        ]
        return SimpleNamespace(named_results=lambda: iter(rows))

    async def insert(self, **kwargs):
        self.insert_calls.append(kwargs)
        resource_config_id, identity, active = kwargs["data"][0]
        self.events.append((resource_config_id, identity, active))


class MembershipStorage:
    database = "metranova"

    def __init__(self):
        self.client = MembershipClient()

    async def ensure_resource_configuration_memberships_table(self):
        pass

    def _qualified_table_name(self, table_name):
        return f"`metranova`.`{table_name}`"


def _node(node_id, name, host):
    return Node(
        node_id=node_id, name=name, host=host, port=161, community="private",
        make="Vendor", model="Model",
    )


def _config(**changes):
    values = dict(
        id="resource-a", ref="resource-a__v1", name="resource a",
        resource_type="interface", collector_plugin="telegraf_kube",
        node_selectors=[Selector(type="make", value="vendor")],
    )
    values.update(changes)
    return ResourceConfiguration(**values)


def test_membership_reads_legacy_selector_state_before_first_write():
    storage = MembershipStorage()
    service = MembershipService(storage)
    nodes = [
        _node("one", "Router.Example.", "192.0.2.1"),
        _node("two", "other.example", "192.0.2.2"),
    ]

    devices = asyncio.run(service.device_ids(_config(), nodes))

    assert devices == ["other.example", "router.example"]


def test_add_initializes_legacy_state_and_is_idempotent():
    storage = MembershipStorage()
    service = MembershipService(storage)
    nodes = [
        _node("one", "router.example", "192.0.2.1"),
        _node("two", "new.example", "192.0.2.2"),
    ]
    config = _config()

    asyncio.run(service.add(config, nodes, "new.example"))
    asyncio.run(service.add(config, nodes, "NEW.EXAMPLE."))

    assert asyncio.run(service.device_ids(config, nodes)) == [
        "new.example", "router.example",
    ]
    assert storage.client.events == [
        ("resource-a", "new.example", True),
        ("resource-a", "router.example", True),
    ]


def test_remove_is_idempotent_and_preserves_other_legacy_members():
    storage = MembershipStorage()
    service = MembershipService(storage)
    nodes = [
        _node("one", "router.example", "192.0.2.1"),
        _node("two", "switch.example", "192.0.2.2"),
    ]
    config = _config()

    asyncio.run(service.remove(config, nodes, "router.example"))
    asyncio.run(service.remove(config, nodes, "ROUTER.EXAMPLE."))

    assert asyncio.run(service.device_ids(config, nodes)) == ["switch.example"]
    assert storage.client.events == [
        ("resource-a", "router.example", False),
        ("resource-a", "switch.example", True),
    ]


def test_membership_managed_empty_config_selects_no_nodes():
    storage = MembershipStorage()
    storage.client.events.append(("resource-a", "router.example", False))
    service = MembershipService(storage)
    nodes = [_node("one", "router.example", "192.0.2.1")]

    assert asyncio.run(service.selected_nodes(_config(node_selectors=[]), nodes)) == []


class FakeStorage:
    def __init__(self):
        self.client = object()

    def close(self):
        pass


class FakeMembershipCollectorService:
    calls = []

    def __init__(self, storage):
        pass

    async def get_resource_config_devices(self, resource_id):
        self.calls.append(("get", resource_id))
        if resource_id == "missing":
            raise LookupError("resource config not found")
        return ["router.example"]

    async def add_resource_config_device(self, resource_id, device_id):
        self.calls.append(("post", resource_id, device_id))
        return sorted({"router.example", device_id})

    async def remove_resource_config_device(self, resource_id, device_id):
        self.calls.append(("delete", resource_id, device_id))
        return []


@pytest.fixture
def api_client(monkeypatch):
    async def fake_create(cls):
        return FakeStorage()

    monkeypatch.setattr("admin_api.context.Clickhouse.create", classmethod(fake_create))
    monkeypatch.setattr(
        "admin_api.resource_configs.router.CollectorService",
        FakeMembershipCollectorService,
    )
    FakeMembershipCollectorService.calls = []
    with TestClient(app) as client:
        yield client


def test_membership_routes_return_complete_devices_and_are_idempotent(api_client):
    response = api_client.get("/resource-configs/resource-a")
    assert response.status_code == 200
    assert response.json() == {"devices": ["router.example"]}

    response = api_client.post(
        "/resource-configs/resource-a/devices/router.example", json={},
    )
    assert response.status_code == 200
    assert response.json() == {"devices": ["router.example"]}

    response = api_client.delete("/resource-configs/resource-a/devices/router.example")
    assert response.status_code == 200
    assert response.json() == {"devices": []}
    assert FakeMembershipCollectorService.calls == [
        ("get", "resource-a"),
        ("post", "resource-a", "router.example"),
        ("delete", "resource-a", "router.example"),
    ]


def test_membership_add_requires_empty_json_object(api_client):
    response = api_client.post(
        "/resource-configs/resource-a/devices/router.example",
        json={"unexpected": True},
    )
    assert response.status_code == 422
    assert FakeMembershipCollectorService.calls == []


def test_membership_get_reports_unknown_resource_config(api_client):
    response = api_client.get("/resource-configs/missing")
    assert response.status_code == 404


def test_storage_creates_membership_table_idempotently():
    commands = []

    async def command(query):
        commands.append(query)

    storage = SimpleNamespace(
        client=SimpleNamespace(command=command),
        metadata_engine="MergeTree",
        is_connected=lambda: _async_value(True),
        _qualified_table_name=lambda name: f"`metranova`.`{name}`",
        _get_on_cluster_clause=lambda engine: _async_value(""),
        _validated_engine_name=lambda engine: engine,
    )

    asyncio.run(Clickhouse.ensure_resource_configuration_memberships_table(storage))

    assert len(commands) == 1
    assert "CREATE TABLE IF NOT EXISTS `metranova`.`resource_configuration_memberships`" in commands[0]
    assert "active Bool" in commands[0]
    assert "updated_at DateTime64(9) DEFAULT now64(9)" in commands[0]


async def _async_value(value):
    return value
