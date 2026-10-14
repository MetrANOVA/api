from urllib.parse import quote

import pytest
from fastapi.testclient import TestClient

from admin_api.app import app
from admin_api.metadata.service import MetadataService

INTERFACE_TYPE = {
    "name": "interface",
    "slug": "interface",
    "identifier": ["node", "intf"],
    "meta_fields": [],
}

SLASH_ID = "zag-edge1::xe-2/0/2"


class FakeStorage:
    database = "metranova"
    client = None

    def close(self):
        pass


@pytest.fixture
def client(monkeypatch):
    async def fake_create(cls):
        return FakeStorage()

    monkeypatch.setattr("admin_api.context.Clickhouse.create", classmethod(fake_create))
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def calls(monkeypatch):
    seen = {}

    async def history(self, slug, _id):
        seen["history"] = (slug, _id)
        return []

    async def metadata_type(self, slug):
        return INTERFACE_TYPE

    async def validate(self, definition, record):
        return definition

    async def update(self, definition, record, version):
        seen["update"] = (record["id"], version)

    monkeypatch.setattr(MetadataService, "get_metadata_record_history", history)
    monkeypatch.setattr(MetadataService, "get_metadata_type", metadata_type)
    monkeypatch.setattr(MetadataService, "validate_metadata_record", validate)
    monkeypatch.setattr(MetadataService, "update_metadata_record", update)
    return seen


@pytest.mark.parametrize("path_id", [quote(SLASH_ID, safe=""), SLASH_ID])
def test_record_history_accepts_ids_with_slashes(client, calls, path_id):
    response = client.get(f"/metadata/interface/{path_id}")

    assert response.status_code == 200
    assert calls["history"] == ("interface", SLASH_ID)


def test_record_update_accepts_ids_with_slashes(client, calls):
    record = {"node": "zag-edge1", "intf": "xe-2/0/2", "ref": f"{SLASH_ID}__v3"}

    response = client.put(
        f"/metadata/interface/{quote(SLASH_ID, safe='')}/3", json=record
    )

    assert response.status_code == 200
    assert response.json() == {"id": SLASH_ID, "ref": f"{SLASH_ID}__v3"}
    assert calls["update"] == (SLASH_ID, "3")


def test_record_update_still_refuses_a_changed_identifier(client, calls):
    record = {"node": "zag-edge1", "intf": "xe-9/9/9", "ref": f"{SLASH_ID}__v1"}

    response = client.put(
        f"/metadata/interface/{quote(SLASH_ID, safe='')}/1", json=record
    )

    assert response.status_code == 400
    assert "update" not in calls


def test_trailing_slash_still_redirects_to_the_record_list(client, calls):
    response = client.get("/metadata/interface/", follow_redirects=False)

    assert response.status_code == 307
    assert response.headers["location"].endswith("/metadata/interface")
    assert "history" not in calls
