import asyncio

import pytest
from fastapi import HTTPException

from admin_api.resource_type.router import (
    batch_create_or_update_resource_types,
    get_resource_type_by_slug,
    get_resource_type_schema_by_slug,
    update_resource_type_by_slug,
)
from admin_api.resource_type.model import (
    BatchCreateResourceTypeRequest,
    UpdateResourceTypeRequest,
)


class DummyStorage:
    def __init__(self, result):
        self.result = result
        self.calls = []
        self.client = object()
        self.existing = None
        self.create_result = (True, "created")
        self.add_missing_result = (True, "updated", True)

    async def find_resource_type_by_slug(self, slug: str):
        self.calls.append(slug)
        return self.result

    async def find_resource_type_schema_by_slug(self, slug: str):
        self.calls.append(slug)
        return self.result

    async def update_resource_type(
        self,
        slug: str,
        fields,
        meta_fields=None,
        consumer_config_updates=None,
        ext_updates=None,
    ):
        self.calls.append(
            {
                "slug": slug,
                "fields": fields,
                "meta_fields": meta_fields,
                "consumer_config_updates": consumer_config_updates,
                "ext_updates": ext_updates,
            }
        )
        return self.result

    async def create_resource_type(
        self,
        name,
        slug,
        data_fields,
        meta_fields,
        identifier,
        ttl,
        engine_type="CoalescingMergeTree",
    ):
        self.calls.append(
            {
                "name": name,
                "slug": slug,
                "data_fields": data_fields,
                "meta_fields": meta_fields,
                "identifier": identifier,
                "ttl": ttl,
                "engine_type": engine_type,
                "op": "create",
            }
        )
        return self.create_result


def test_get_resource_type_by_slug_returns_dict_from_tuple_result():
    storage = DummyStorage(
        (
            "def_interface-traffic",
            "def_interface-traffic__v1",
            "Interface Traffic",
            "interface-traffic",
            [("if_name", "String", True, "")],
            [("if_name", "String", True)],
            ["if_name"],
            "365 DAY",
            "MergeTree()",
            True,
            "2026-03-31 00:00:00",
        )
    )

    result = asyncio.run(get_resource_type_by_slug("interface-traffic", storage))

    assert storage.calls == ["interface-traffic"]
    assert isinstance(result, dict)
    assert result["slug"] == "interface-traffic"
    assert result["data_fields"] == [("if_name", "String", True)]


def test_get_resource_type_by_slug_returns_dict_unchanged():
    expected = {
        "id": "def_interface-traffic",
        "slug": "interface-traffic",
        "data_fields": [("if_name", "String", True)],
    }
    storage = DummyStorage(expected)

    result = asyncio.run(get_resource_type_by_slug("interface-traffic", storage))

    assert result == expected


def test_get_resource_type_by_slug_raises_404_when_not_found():
    storage = DummyStorage(None)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(get_resource_type_by_slug("missing-slug", storage))

    assert exc_info.value.status_code == 404
    assert "missing-slug" in exc_info.value.detail


def test_get_resource_type_schema_by_slug_returns_schema_dict():
    expected = {
        "slug": "interface-traffic",
        "type": "data",
        "table": "metranova.data_interface-traffic",
        "columns": [
            {"name": "collector_id", "type": "LowCardinality(String)"},
            {"name": "if_name", "type": "String"},
        ],
    }
    storage = DummyStorage(expected)

    result = asyncio.run(get_resource_type_schema_by_slug("interface-traffic", storage))

    assert storage.calls == ["interface-traffic"]
    assert result == expected


def test_get_resource_type_schema_by_slug_raises_404_when_not_found():
    storage = DummyStorage(None)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(get_resource_type_schema_by_slug("missing-slug", storage))

    assert exc_info.value.status_code == 404
    assert "missing-slug" in exc_info.value.detail


def test_update_resource_type_by_slug_returns_success_message():
    storage = DummyStorage(
        (True, "Resource type 'ip_address' updated to def_ip_address__v2")
    )
    request = UpdateResourceTypeRequest(
        data_fields=[
            {"field_name": "hostname", "field_type": "String", "nullable": True}
        ],
        meta_fields=[
            {"field_name": "hostname", "field_type": "String", "nullable": True}
        ],
        consumer_config={"topic": "new-topic"},
        ext={"owner": "network-team"},
    )

    result = asyncio.run(update_resource_type_by_slug("ip_address", request, storage))

    assert "updated to" in result["message"]
    assert storage.calls[0]["slug"] == "ip_address"
    assert storage.calls[0]["fields"][0].field_name == "hostname"
    assert storage.calls[0]["meta_fields"][0].name == "hostname"
    assert storage.calls[0]["consumer_config_updates"]["topic"] == "new-topic"
    assert storage.calls[0]["ext_updates"]["owner"] == "network-team"


def test_update_resource_type_by_slug_raises_404_for_missing_slug():
    storage = DummyStorage((False, "Resource type with slug 'missing' not found"))
    request = UpdateResourceTypeRequest()

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(update_resource_type_by_slug("missing", request, storage))

    assert exc_info.value.status_code == 404


def test_update_resource_type_by_slug_raises_400_for_non_additive_error():
    storage = DummyStorage((False, "Field 'host' already exists"))
    request = UpdateResourceTypeRequest()

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(update_resource_type_by_slug("ip_address", request, storage))

    assert exc_info.value.status_code == 400


def test_batch_create_or_update_resource_types_reports_created_updated_failed():
    storage = DummyStorage(None)

    state = {"count": 0}

    async def fake_find(slug: str):
        state["count"] += 1
        if slug == "new_type":
            return None
        return {"slug": slug}

    storage.find_resource_type_by_slug = fake_find

    async def fake_create_resource_type(*args, **kwargs):
        return True, "Type New Type has been successfully created"

    storage.create_resource_type = fake_create_resource_type

    async def fake_update_resource_type(
        slug,
        fields,
        meta_fields=None,
        consumer_config_updates=None,
        ext_updates=None,
    ):
        if slug == "existing_type":
            return (
                True,
                "Resource type 'existing_type' updated to def_existing_type__v2",
            )
        return False, "schema update failed"

    storage.update_resource_type = fake_update_resource_type

    request = BatchCreateResourceTypeRequest(
        definitions=[
            {
                "name": "New Type",
                "data_fields": [
                    {"field_name": "a", "field_type": "String", "nullable": True}
                ],
                "meta_fields": [
                    {"field_name": "a", "field_type": "String", "nullable": True}
                ],
                "identifier": ["a"],
                "ttl": "30 DAY",
            },
            {
                "name": "Existing Type",
                "data_fields": [
                    {"field_name": "b", "field_type": "String", "nullable": True}
                ],
                "meta_fields": [
                    {"field_name": "b", "field_type": "String", "nullable": True}
                ],
                "identifier": ["b"],
                "ttl": "30 DAY",
            },
            {
                "name": "Broken Type",
                "data_fields": [
                    {"field_name": "c", "field_type": "String", "nullable": True}
                ],
                "meta_fields": [
                    {"field_name": "c", "field_type": "String", "nullable": True}
                ],
                "identifier": ["c"],
                "ttl": "30 DAY",
            },
        ]
    )

    result = asyncio.run(batch_create_or_update_resource_types(request, storage))

    assert [item["slug"] for item in result["created"]] == ["new_type"]
    assert [item["slug"] for item in result["updated"]] == ["existing_type"]
    assert [item["slug"] for item in result["failed"]] == ["broken_type"]


def test_batch_create_or_update_resource_types_continues_on_exception():
    storage = DummyStorage(None)

    async def fake_find(slug: str):
        if slug == "boom":
            raise RuntimeError("unexpected")
        return None

    storage.find_resource_type_by_slug = fake_find
    storage.create_result = (True, "ok")

    request = BatchCreateResourceTypeRequest(
        definitions=[
            {
                "name": "Boom",
                "data_fields": [
                    {"field_name": "a", "field_type": "String", "nullable": True}
                ],
                "meta_fields": [
                    {"field_name": "a", "field_type": "String", "nullable": True}
                ],
                "identifier": ["a"],
                "ttl": "7 DAY",
            },
            {
                "name": "Next",
                "data_fields": [
                    {"field_name": "b", "field_type": "String", "nullable": True}
                ],
                "meta_fields": [
                    {"field_name": "b", "field_type": "String", "nullable": True}
                ],
                "identifier": ["b"],
                "ttl": "7 DAY",
            },
        ]
    )

    result = asyncio.run(batch_create_or_update_resource_types(request, storage))

    assert len(result["failed"]) == 1
    assert result["failed"][0]["slug"] == "boom"
    assert len(result["created"]) == 1
    assert result["created"][0]["slug"] == "next"


def test_batch_create_or_update_resource_types_meta_only_uses_metadata_service(
    monkeypatch,
):
    storage = DummyStorage(None)

    called = {"value": False}

    async def fake_create_metadata_type(self, name, identifier, fields):
        called["value"] = True
        assert name == "POP"
        assert identifier == ["pop_id"]
        assert fields[0].name == "pop_id"

    monkeypatch.setattr(
        "admin_api.resource_type.router.MetadataService.create_metadata_type",
        fake_create_metadata_type,
    )

    request = BatchCreateResourceTypeRequest(
        definitions=[
            {
                "name": "POP",
                "data_fields": [],
                "meta_fields": [
                    {"field_name": "pop_id", "field_type": "String", "nullable": False}
                ],
                "identifier": ["pop_id"],
                "ttl": "7 DAY",
            }
        ]
    )

    result = asyncio.run(batch_create_or_update_resource_types(request, storage))

    assert called["value"] is True
    assert [item["slug"] for item in result["created"]] == ["pop"]
    assert result["updated"] == []
    assert result["failed"] == []
