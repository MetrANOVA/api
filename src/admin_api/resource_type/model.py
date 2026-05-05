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
    data_fields: list[ResourceFieldRequest] = Field(min_length=1)
    meta_fields: list[MetaFieldRequest] = Field(default_factory=list)
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
        data_field_names = [f.field_name for f in self.data_fields]
        if len(data_field_names) != len(set(data_field_names)):
            raise ValueError("data fields must have unique field_name values")

        meta_field_names = [f.field_name for f in self.meta_fields]
        if len(meta_field_names) != len(set(meta_field_names)):
            raise ValueError("meta fields must have unique field_name values")

        identifier_scope = meta_field_names if meta_field_names else data_field_names
        missing_identifiers = [
            key for key in self.identifier if key not in identifier_scope
        ]
        if missing_identifiers:
            scope_label = "meta fields" if meta_field_names else "data fields"
            raise ValueError(
                f"identifier fields must exist in {scope_label}: {missing_identifiers}"
            )

        return self


class BatchResourceTypeDefinitionRequest(BaseModel):
    name: str = Field(min_length=1, examples=["Interface Traffic"])
    data_fields: list[ResourceFieldRequest] = Field(default_factory=list)
    meta_fields: list[MetaFieldRequest] = Field(default_factory=list)
    identifier: list[str] = Field(default_factory=list)
    ttl: str | None = Field(default=None, min_length=1, examples=["365 DAY"])

    # @field_validator("ttl")
    # @classmethod
    # def validate_ttl_format(cls, v: str | None) -> str | None:
    #     if v is None:
    #         return None
    #     pattern = r"^\d+\s+(SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|QUARTER|YEAR)$"
    #     if not re.match(pattern, v.upper()):
    #         raise ValueError(
    #             "ttl must be a valid ClickHouse interval format "
    #             "(e.g., '365 DAY', '30 MONTH', '1 YEAR'). "
    #             "Format: <number> <TIME_UNIT> where TIME_UNIT is one of: "
    #             "SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR"
    #         )
    #     return v.upper()

    @model_validator(mode="after")
    def validate_fields_and_identifiers(self):
        if not self.data_fields and not self.meta_fields:
            raise ValueError("at least one of data_fields or meta_fields is required")

        if self.data_fields and self.ttl is None:
            raise ValueError("ttl is required when data_fields are provided")

        data_field_names = [f.field_name for f in self.data_fields]
        if len(data_field_names) != len(set(data_field_names)):
            raise ValueError("data fields must have unique field_name values")

        meta_field_names = [f.field_name for f in self.meta_fields]
        if len(meta_field_names) != len(set(meta_field_names)):
            raise ValueError("meta fields must have unique field_name values")

        if not self.identifier and self.data_fields:
            raise ValueError("identifier is required when data_fields are provided")

        identifier_scope = meta_field_names if meta_field_names else data_field_names
        missing_identifiers = [
            key for key in self.identifier if key not in identifier_scope
        ]
        if missing_identifiers:
            scope_label = "meta fields" if meta_field_names else "data fields"
            raise ValueError(
                f"identifier fields must exist in {scope_label}: {missing_identifiers}"
            )

        return self


class UpdateResourceTypeRequest(BaseModel):
    data_fields: list[ResourceFieldRequest] = Field(default_factory=list)
    meta_fields: list[MetaFieldRequest] = Field(default_factory=list)
    consumer_config: dict[str, Any] = Field(default_factory=dict)
    ext: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_unique_fields(self):
        data_field_names = [field.field_name for field in self.data_fields]
        if len(data_field_names) != len(set(data_field_names)):
            raise ValueError("data fields must have unique field_name values")

        meta_field_names = [field.field_name for field in self.meta_fields]
        if len(meta_field_names) != len(set(meta_field_names)):
            raise ValueError("meta fields must have unique field_name values")
        return self


class BatchCreateResourceTypeRequest(BaseModel):
    definitions: list[BatchResourceTypeDefinitionRequest] = Field(min_length=1)
