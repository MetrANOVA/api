import pytest
from pydantic import ValidationError

from admin_api.models.resource_type import CreateResourceTypeRequest


def build_valid_payload():
    return {
        "name": "Interface Traffic",
        "slug": "interface-traffic",
        "collection_type": "data",
        "consumer_type": "kafka",
        "consumer_config": {"topic": "snmp.metrics"},
        "fields": [
            {"field_name": "host", "field_type": "String", "nullable": False},
            {"field_name": "timestamp", "field_type": "DateTime64", "nullable": False},
        ],
        "primary_key": ["host", "timestamp"],
        "partition_by": "toYYYYMM(timestamp)",
        "ttl": "365 DAY",
    }


def test_create_resource_type_request_accepts_valid_payload():
    model = CreateResourceTypeRequest(**build_valid_payload())
    assert model.slug == "interface-traffic"
    assert model.engine_type == "CoalescingMergeTree"
    assert model.is_replicated is True


def test_create_resource_type_request_rejects_missing_primary_key_field():
    payload = build_valid_payload()
    payload["primary_key"] = ["missing_field"]

    with pytest.raises(ValidationError):
        CreateResourceTypeRequest(**payload)


def test_create_resource_type_request_rejects_duplicate_field_names():
    payload = build_valid_payload()
    payload["fields"] = [
        {"field_name": "host", "field_type": "String", "nullable": False},
        {"field_name": "host", "field_type": "String", "nullable": False},
    ]

    with pytest.raises(ValidationError):
        CreateResourceTypeRequest(**payload)
