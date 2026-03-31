import asyncio

import pytest
from fastapi import HTTPException

from admin_api.routers.resource_type import (
    get_resource_type_by_slug,
    get_resource_type_schema_by_slug,
)


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
