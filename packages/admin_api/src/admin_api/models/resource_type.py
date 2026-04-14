from typing import Any, ClassVar
import re

from pydantic import BaseModel, Field, field_validator, model_validator

from metranova_core.storage.base import CollectionType, ConsumerType


class ResourceFieldRequest(BaseModel):
    field_name: str = Field(min_length=1)
    field_type: str = Field(min_length=1)
    nullable: bool = True

class CreateResourceTypeRequest(BaseModel):
    name: str = Field(min_length=1, examples=["Interface Traffic"])
    collection_type: CollectionType
    consumer_type: ConsumerType
    consumer_config: dict[str, Any] = Field(default_factory=dict)
    fields: list[ResourceFieldRequest] = Field(min_length=1)
    primary_key: list[str] = Field(min_length=1)
    ttl: str = Field(min_length=1, examples=["365 DAY"])
    engine_type: str = Field(default="CoalescingMergeTree", min_length=1)
    is_replicated: bool = True


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
