from typing import Any

from pydantic import BaseModel, Field, model_validator

from metranova.storage.base import CollectionType, ConsumerType


class ResourceFieldRequest(BaseModel):
    field_name: str = Field(min_length=1)
    field_type: str = Field(min_length=1)
    nullable: bool = True


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
