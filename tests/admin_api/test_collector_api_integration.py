import pytest
from fastapi.testclient import TestClient

from admin_api.app import app
from admin_api.collector.model import ResourceConfiguration


class FakeStorage:
    def __init__(self):
        self.client = object()

    def close(self):
        pass


class FakeCollectorService:
    """Stands in for CollectorService so the routes are tested, not ClickHouse."""

    calls = []

    def __init__(self, storage):
        self.storage = storage

    async def update_resource_configuration(
        self, config_id, request, collector_plugin=None
    ):
        FakeCollectorService.calls.append((config_id, request, collector_plugin))

        if config_id == "missing":
            raise LookupError(f"Resource configuration with id '{config_id}' not found")

        updates = request.model_dump(exclude_none=True)
        if not updates:
            raise ValueError("No fields provided to update")

        return ResourceConfiguration(
            id=config_id,
            ref=f"{config_id}__v2",
            name="example telegraf",
            resource_type="interface",
            collector_plugin=collector_plugin or "telegraf_vscode",
            **updates,
        )


@pytest.fixture
def api_client(monkeypatch):
    fake_storage = FakeStorage()

    async def fake_create(cls):
        return fake_storage

    monkeypatch.setattr("admin_api.context.Clickhouse.create", classmethod(fake_create))
    monkeypatch.setattr(
        "admin_api.collector.router.CollectorService", FakeCollectorService
    )
    FakeCollectorService.calls = []

    with TestClient(app) as client:
        yield client


def test_update_route_appends_a_version(api_client):
    response = api_client.post(
        "/collector/plugins/telegraf_vscode/resource_configurations/example_telegraf",
        json={"interval": 30},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["ref"] == "example_telegraf__v2"
    assert body["interval"] == 30
    # the rendered config is a side effect, not part of the response
    assert "rendered" not in body

    # the plugin in the path scopes the update
    config_id, _, collector_plugin = FakeCollectorService.calls[0]
    assert (config_id, collector_plugin) == ("example_telegraf", "telegraf_vscode")


def test_update_route_reports_missing_configuration_as_404(api_client):
    response = api_client.post(
        "/collector/plugins/telegraf_vscode/resource_configurations/missing",
        json={"interval": 30},
    )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


def test_update_route_rejects_an_empty_body(api_client):
    response = api_client.post(
        "/collector/plugins/telegraf_vscode/resource_configurations/example_telegraf",
        json={},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "No fields provided to update"


def test_update_route_validates_the_body_before_the_service(api_client):
    response = api_client.post(
        "/collector/plugins/telegraf_vscode/resource_configurations/example_telegraf",
        json={"interval": 0},
    )

    assert response.status_code == 422
    assert FakeCollectorService.calls == []
