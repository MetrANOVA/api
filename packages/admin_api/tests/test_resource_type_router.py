import asyncio

import pytest
from fastapi import HTTPException

from admin_api.routers.resource_type import (
    get_resource_type_by_slug,
    get_resource_type_schema_by_slug,
    update_resource_type_by_slug,
)
from admin_api.models.resource_type import UpdateResourceTypeRequest


class DummyStorage:
    def __init__(self, result):
        self.result = result
        self.calls = []

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
        consumer_config_updates,
        ext_updates,
    ):
        self.calls.append(
            {
                "slug": slug,
                "fields": fields,
                "consumer_config_updates": consumer_config_updates,
                "ext_updates": ext_updates,
            }
        )
        return self.result


def test_get_resource_type_by_slug_returns_dict_from_tuple_result():
    storage = DummyStorage(
        (
            "def_interface-traffic",
            "def_interface-traffic__v1",
            "Interface Traffic",
            "interface-traffic",
            "data",
            "kafka",
            '{"topic":"snmp.metrics"}',
            [("if_name", "String", True)],
            ["if_name"],
            "toYYYYMM(timestamp)",
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
    assert result["type"] == "data"


def test_get_resource_type_by_slug_returns_dict_unchanged():
    expected = {
        "id": "def_interface-traffic",
        "slug": "interface-traffic",
        "type": "data",
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
        fields=[{"field_name": "hostname", "field_type": "String", "nullable": True}],
        consumer_config={"topic": "new-topic"},
        ext={"owner": "network-team"},
    )

    result = asyncio.run(update_resource_type_by_slug("ip_address", request, storage))

    assert "updated to" in result["message"]
    assert storage.calls[0]["slug"] == "ip_address"
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
