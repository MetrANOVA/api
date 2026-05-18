import pytest
from pydantic import ValidationError

from admin_api.resource_type.model import (
    BatchResourceTypeDefinitionRequest,
    CreateResourceTypeRequest,
)


def build_valid_payload():
    return {
        "name": "Interface Traffic",
        "data_fields": [
            {"field_name": "host", "field_type": "String", "nullable": False},
            {"field_name": "timestamp", "field_type": "DateTime64", "nullable": False},
        ],
        "meta_fields": [
            {"field_name": "host", "field_type": "String", "nullable": False},
            {"field_name": "timestamp", "field_type": "DateTime64", "nullable": False},
        ],
        "identifier": ["host", "timestamp"],
        "ttl": "365 DAY",
    }


def test_create_resource_type_request_accepts_valid_payload():
    model = CreateResourceTypeRequest(**build_valid_payload())
    assert model.name == "Interface Traffic"
    assert len(model.data_fields) == 2
    assert len(model.meta_fields) == 2


def test_create_resource_type_request_accepts_payload_without_meta_fields():
    payload = build_valid_payload()
    payload.pop("meta_fields")

    model = CreateResourceTypeRequest(**payload)
    assert model.name == "Interface Traffic"
    assert len(model.data_fields) == 2
    assert model.meta_fields == []


def test_create_resource_type_request_rejects_missing_primary_key_field():
    payload = build_valid_payload()
    payload["identifier"] = ["missing_field"]

    with pytest.raises(ValidationError):
        CreateResourceTypeRequest(**payload)


def test_create_resource_type_request_rejects_duplicate_field_names():
    payload = build_valid_payload()
    payload["data_fields"] = [
        {"field_name": "host", "field_type": "String", "nullable": False},
        {"field_name": "host", "field_type": "String", "nullable": False},
    ]

    with pytest.raises(ValidationError):
        CreateResourceTypeRequest(**payload)


def test_create_resource_type_request_rejects_identifier_not_in_data_fields_without_meta():
    payload = build_valid_payload()
    payload.pop("meta_fields")
    payload["identifier"] = ["missing_field"]

    with pytest.raises(ValidationError) as exc_info:
        CreateResourceTypeRequest(**payload)

    assert "identifier fields must exist in data fields" in str(exc_info.value)


@pytest.mark.parametrize(
    "raw_type",
    [
        "string",
        "float64",
        "datetime64",
        "array(string)",
        "nullable(string)",
        "lowcardinality(string)",
        "String",
        "Float64",
    ],
)
def test_resource_field_request_preserves_field_type_as_provided(raw_type):
    from admin_api.resource_type.model import ResourceFieldRequest

    field = ResourceFieldRequest(field_name="col", field_type=raw_type)
    assert field.field_type == raw_type


@pytest.mark.parametrize(
    "ttl",
    [
        "365 DAY",
        "30 MONTH",
        "1 YEAR",
        "24 HOUR",
        "60 MINUTE",
        "3600 SECOND",
        "52 WEEK",
        "2 QUARTER",
    ],
)
def test_create_resource_type_request_accepts_valid_ttl_formats(ttl):
    payload = build_valid_payload()
    payload["ttl"] = ttl
    model = CreateResourceTypeRequest(**payload)
    assert model.ttl == ttl


@pytest.mark.parametrize(
    "ttl",
    [
        "365",  # missing time unit
        "DAY",  # missing number
        "365 DAYS",  # invalid time unit (plural)
        "365 days",  # lowercase (should be uppercase after validation)
        "-365 DAY",  # negative number
        "365.5 DAY",  # decimal number
        "abc DAY",  # non-numeric number
    ],
)
def test_create_resource_type_request_rejects_invalid_ttl_formats(ttl):
    payload = build_valid_payload()
    payload["ttl"] = ttl

    with pytest.raises(ValidationError) as exc_info:
        CreateResourceTypeRequest(**payload)
    assert "ttl must be a valid ClickHouse interval format" in str(exc_info.value)


def test_batch_definition_request_accepts_meta_only_definition():
    model = BatchResourceTypeDefinitionRequest(
        name="POP",
        meta_fields=[
            {"field_name": "pop_id", "field_type": "String", "nullable": False}
        ],
    )

    assert model.data_fields == []
    assert len(model.meta_fields) == 1
    assert model.identifier == []


def test_batch_definition_request_rejects_without_data_and_meta_fields():
    with pytest.raises(ValidationError) as exc_info:
        BatchResourceTypeDefinitionRequest(
            name="POP",
            identifier=["pop_id"],
        )

    assert "at least one of data_fields or meta_fields is required" in str(
        exc_info.value
    )


def test_batch_definition_request_rejects_data_fields_without_identifier():
    with pytest.raises(ValidationError) as exc_info:
        BatchResourceTypeDefinitionRequest(
            name="Interface Traffic",
            data_fields=[
                {"field_name": "host", "field_type": "String", "nullable": False}
            ],
            ttl="365 DAY",
        )

    assert "identifier is required when data_fields are provided" in str(exc_info.value)
