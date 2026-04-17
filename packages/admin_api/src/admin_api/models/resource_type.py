from typing import Any, ClassVar, Optional
import re

from pydantic import BaseModel, Field, field_validator, model_validator

from metranova.storage.base import CollectionType, ConsumerType


class ResourceFieldRequest(BaseModel):
    field_name: str = Field(min_length=1)
    field_type: str = Field(min_length=1)
    nullable: bool = True
    
class MetaFieldRequest(ResourceFieldRequest):
    table: str | None = None

class DataFields(BaseModel):
    fields: list[ResourceFieldRequest] = Field(min_length=1)
    
class MetaFields(BaseModel):
    fields: list[MetaFieldRequest] = Field(min_length=1)

class CreateResourceTypeRequest(BaseModel):
    name: str = Field(min_length=1, examples=["Interface Traffic"])
    data: DataFields = Field()
    meta: MetaFields = Field()
    identifier: list[str] = Field(min_length=1)
    ttl: str = Field(min_length=1, examples=["365 DAY"])

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
    def validate_fields_and_identifiers(self):
        data_field_names = [f.field_name for f in self.data.fields]
        if len(data_field_names) != len(set(data_field_names)):
            raise ValueError("data fields must have unique field_name values")

        meta_field_names = [f.field_name for f in self.meta.fields]
        if len(meta_field_names) != len(set(meta_field_names)):
            raise ValueError("meta fields must have unique field_name values")

        missing_identifiers = [
            key for key in self.identifier if key not in meta_field_names
        ]
        if missing_identifiers:
            raise ValueError(
                f"identifier fields must exist in meta fields: {missing_identifiers}"
            )

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
