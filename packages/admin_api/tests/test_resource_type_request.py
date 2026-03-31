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


@pytest.mark.parametrize(
    "raw_type, normalized_type",
    [
        ("string", "String"),
        ("float64", "Float64"),
        ("datetime64", "Datetime64"),
        ("array(string)", "Array(String)"),
        ("nullable(string)", "Nullable(String)"),
        ("lowcardinality(string)", "Lowcardinality(String)"),
        ("String", "String"),  # already correct — unchanged
        ("Float64", "Float64"),  # already correct — unchanged
    ],
)
def test_resource_field_request_normalizes_field_type_casing(raw_type, normalized_type):
    from admin_api.models.resource_type import ResourceFieldRequest

    field = ResourceFieldRequest(field_name="col", field_type=raw_type)
    assert field.field_type == normalized_type


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


@pytest.mark.parametrize(
    "partition_by",
    [
        "toYYYYMM(timestamp)",
        "toYYYYMMDD(timestamp)",
        "toDate(timestamp)",
        "toMonth(timestamp)",
        "toQuarter(timestamp)",
        "toYear(timestamp)",
        "toStartOfMonth(timestamp)",
        "intDiv(user_id, 100)",
        "intHash32(user_id)",
        "cityHash64(user_id)",
        "user_id",
        "region_id",
        "country_code",
        "sipHash128(user_id)",
        "murmurHash3_32(user_id)",
    ],
)
def test_create_resource_type_request_accepts_valid_partition_by_formats(partition_by):
    payload = build_valid_payload()
    payload["partition_by"] = partition_by
    model = CreateResourceTypeRequest(**payload)
    assert model.partition_by == partition_by


@pytest.mark.parametrize(
    "partition_by",
    [
        "",  # empty string (caught by Field min_length)
        "(",  # unbalanced parenthesis
        ")",  # unbalanced parenthesis
        "toYYYYMM(timestamp",  # missing closing paren
        "toYYYYMM timestamp)",  # missing opening paren
        "toYYYYMM((timestamp)",  # extra opening paren
        "func@name(col)",  # invalid character (@)
        "toYYYYMM(#timestamp)",  # invalid character (#)
        "() () ()",  # no identifiers
        "((()))",  # no identifiers, only parens
    ],
)
def test_create_resource_type_request_rejects_invalid_partition_by_formats(
    partition_by,
):
    payload = build_valid_payload()
    payload["partition_by"] = partition_by

    with pytest.raises(ValidationError) as exc_info:
        CreateResourceTypeRequest(**payload)
    assert "partition_by" in str(exc_info.value)


@pytest.mark.parametrize(
    "slug",
    [
        "interface-traffic",
        "cpu_metrics",
        "host_metrics_v2",
        "snmp-data",
        "metrics1",
        "a",
        "a1b2c3",
        "metric_123_abc",
        "test-case-1",
        "under_score_test",
        "mixedcase-and_underscore",
    ],
)
def test_create_resource_type_request_accepts_valid_slug_formats(slug):
    payload = build_valid_payload()
    payload["slug"] = slug
    model = CreateResourceTypeRequest(**payload)
    assert model.slug == slug


@pytest.mark.parametrize(
    "slug",
    [
        "Interface-Traffic",  # uppercase letters
        "interface_TRAFFIC",  # uppercase in middle
        "METRICS",  # all uppercase
        "interface traffic",  # spaces
        "interface.traffic",  # periods
        "interface@traffic",  # special character (@)
        "interface#metrics",  # special character (#)
        "interface$data",  # special character ($)
        "-interface",  # starts with hyphen
        "_metrics",  # starts with underscore
        "interface-",  # ends with hyphen
        "metrics_",  # ends with underscore
        "interface--traffic",  # double hyphen (consecutive)
        "metrics__data",  # double underscore (consecutive)
        "interface.metrics",  # dot in middle
    ],
)
def test_create_resource_type_request_rejects_invalid_slug_formats(slug):
    payload = build_valid_payload()
    payload["slug"] = slug

    with pytest.raises(ValidationError) as exc_info:
        CreateResourceTypeRequest(**payload)
    assert "slug must be URL-safe" in str(exc_info.value)
