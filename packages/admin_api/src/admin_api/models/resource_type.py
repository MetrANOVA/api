from typing import Any
import re

from pydantic import BaseModel, Field, field_validator, model_validator

from metranova.storage.base import CollectionType, ConsumerType


class ResourceFieldRequest(BaseModel):
    field_name: str = Field(min_length=1)
    field_type: str = Field(min_length=1)
    nullable: bool = True

    @field_validator("field_type")
    @classmethod
    def normalize_field_type(cls, v: str) -> str:
        """Capitalize the first letter of each ClickHouse type identifier.

        Converts e.g. 'string' -> 'String', 'array(string)' -> 'Array(String)',
        'nullable(datetime64)' -> 'Nullable(Datetime64)'.
        ClickHouse type names are case-sensitive and must start with a capital letter.
        """
        return re.sub(r"(?<![a-zA-Z])([a-z])", lambda m: m.group(1).upper(), v)


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
        return v

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
