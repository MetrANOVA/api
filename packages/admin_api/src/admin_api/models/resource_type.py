from typing import Any, ClassVar
import re

from pydantic import BaseModel, Field, field_validator, model_validator

from metranova.storage.base import CollectionType, ConsumerType


class ResourceFieldRequest(BaseModel):
    field_name: str = Field(min_length=1)
    field_type: str = Field(min_length=1)
    nullable: bool = True

    _CLICKHOUSE_TYPE_MAP: ClassVar[dict[str, str]] = {
        "string": "String",
        "bool": "Bool",
        "boolean": "Bool",
        "uuid": "UUID",
        "ipv4": "IPv4",
        "ipv6": "IPv6",
        "date": "Date",
        "date32": "Date32",
        "datetime": "DateTime",
        "datetime32": "DateTime32",
        "datetime64": "DateTime64",
        "int8": "Int8",
        "int16": "Int16",
        "int32": "Int32",
        "int64": "Int64",
        "int128": "Int128",
        "int256": "Int256",
        "uint8": "UInt8",
        "uint16": "UInt16",
        "uint32": "UInt32",
        "uint64": "UInt64",
        "uint128": "UInt128",
        "uint256": "UInt256",
        "float32": "Float32",
        "float64": "Float64",
        "decimal": "Decimal",
        "decimal32": "Decimal32",
        "decimal64": "Decimal64",
        "decimal128": "Decimal128",
        "decimal256": "Decimal256",
        "enum8": "Enum8",
        "enum16": "Enum16",
        "json": "JSON",
        "object": "Object",
        "nothing": "Nothing",
    }
    _CLICKHOUSE_WRAPPER_MAP: ClassVar[dict[str, str]] = {
        "array": "Array",
        "nullable": "Nullable",
        "lowcardinality": "LowCardinality",
        "map": "Map",
        "tuple": "Tuple",
        "nested": "Nested",
        "simpleaggregatefunction": "SimpleAggregateFunction",
        "aggregatefunction": "AggregateFunction",
    }

    @classmethod
    def _split_type_arguments(cls, args: str) -> list[str]:
        parts: list[str] = []
        current: list[str] = []
        depth = 0
        for char in args:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif char == "," and depth == 0:
                parts.append("".join(current).strip())
                current = []
                continue
            current.append(char)
        if current:
            parts.append("".join(current).strip())
        return parts

    @classmethod
    def _normalize_clickhouse_type(cls, value: str) -> str:
        value = value.strip()
        lowered = value.lower()
        if lowered in cls._CLICKHOUSE_TYPE_MAP:
            return cls._CLICKHOUSE_TYPE_MAP[lowered]
        match = re.fullmatch(r"([A-Za-z][A-Za-z0-9]*)\s*\((.*)\)", value)
        if not match:
            return value
        type_name, inner = match.groups()
        normalized_name = cls._CLICKHOUSE_WRAPPER_MAP.get(
            type_name.lower(),
            cls._CLICKHOUSE_TYPE_MAP.get(type_name.lower(), type_name),
        )
        normalized_args = ", ".join(
            cls._normalize_clickhouse_type(part)
            for part in cls._split_type_arguments(inner)
        )
        return f"{normalized_name}({normalized_args})"

    @field_validator("field_type")
    @classmethod
    def normalize_field_type(cls, v: str) -> str:
        """Normalize ClickHouse type names to their canonical case.
        Converts e.g. 'string' -> 'String', 'array(string)' -> 'Array(String)',
        'nullable(datetime64)' -> 'Nullable(DateTime64)',
        'lowcardinality(string)' -> 'LowCardinality(String)'.
        """
        return cls._normalize_clickhouse_type(v)


class CreateResourceTypeRequest(BaseModel):
    name: str = Field(min_length=1, examples=["Interface Traffic"])
    slug: str = Field(min_length=1, examples=["interface-traffic"])
    collection_type: CollectionType
    consumer_type: ConsumerType
    consumer_config: dict[str, Any] = Field(default_factory=dict)
    fields: list[ResourceFieldRequest] = Field(min_length=1)
    primary_key: list[str] = Field(min_length=1)
    partition_by: str = Field(min_length=1, examples=["toYYYYMM(timestamp)"])
    ttl: str = Field(min_length=1, examples=["365 DAY"])
    engine_type: str = Field(default="CoalescingMergeTree", min_length=1)
    is_replicated: bool = True

    @field_validator("partition_by")
    @classmethod
    def validate_partition_by_format(cls, v: str) -> str:
        """Validate that partition_by is a valid ClickHouse PARTITION BY expression."""
        # Check basic structure: alphanumeric, underscores, parentheses, commas, operators, dots, spaces
        if not re.match(r"^[\w,\s+\-*/()\.]+$", v):
            raise ValueError(
                "partition_by contains invalid characters. "
                "Valid characters: alphanumerics, underscores, parentheses, commas, "
                "operators (+, -, *, /), dots, and spaces."
            )

        # Check for balanced parentheses
        if v.count("(") != v.count(")"):
            raise ValueError(
                "partition_by has unbalanced parentheses. "
                "Number of '(' must equal number of ')'."
            )

        # Check for at least one valid identifier or number
        if not re.search(r"[\w]", v):
            raise ValueError(
                "partition_by must contain at least one function call or column name. "
                "Examples: 'toYYYYMM(timestamp)', 'intDiv(user_id, 10)', 'user_id'"
            )

        return v

    @field_validator("slug")
    @classmethod
    def validate_slug_format(cls, v: str) -> str:
        """Validate that slug is URL-safe (lowercase alphanumerics, hyphens, underscores)."""
        pattern = r"^[a-z0-9]+(?:[-_][a-z0-9]+)*$"
        if not re.match(pattern, v):
            raise ValueError(
                "slug must be URL-safe. "
                "Use only lowercase letters (a-z), numbers (0-9), hyphens (-), and underscores (_). "
                "Must start and end with alphanumeric characters. "
                "Examples: 'interface-traffic', 'cpu_metrics', 'host_metrics_v2'"
            )
        return v

    @field_validator("ttl")
    @classmethod
    def validate_ttl_format(cls, v: str) -> str:
        """Validate that TTL is a valid ClickHouse interval format."""
        pattern = r"^\d+\s+(SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|QUARTER|YEAR)$"
        if not re.match(pattern, v.upper()):
            raise ValueError(
                "ttl must be a valid ClickHouse interval format "
                "(e.g., '365 DAY', '30 MONTH', '1 YEAR'). "
                "Format: <number> <TIME_UNIT> where TIME_UNIT is one of: "
                "SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR"
            )
        return v.upper()

    @model_validator(mode="after")
    def validate_primary_key_fields(self):
        field_names = [field.field_name for field in self.fields]
        if len(field_names) != len(set(field_names)):
            raise ValueError("fields must have unique field_name values")

        missing_primary_keys = [
            key for key in self.primary_key if key not in field_names
        ]
        if missing_primary_keys:
            raise ValueError("primary_key values must exist in fields.field_name")

        return self


class UpdateResourceTypeRequest(BaseModel):
    fields: list[ResourceFieldRequest] = Field(default_factory=list)
    consumer_config: dict[str, Any] = Field(default_factory=dict)
    ext: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_unique_fields(self):
        field_names = [field.field_name for field in self.fields]
        if len(field_names) != len(set(field_names)):
            raise ValueError("fields must have unique field_name values")
        return self
