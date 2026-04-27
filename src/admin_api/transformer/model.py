from typing import Optional
from pydantic import BaseModel, Field


class CreateTransformerRequest(BaseModel):
    name: str = Field(min_length=1)
    definition_ref: str = Field(min_length=1)
    description: str = Field(default="")
    match_field: str = Field(min_length=1)


class UpdateTransformerRequest(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1)
    description: Optional[str] = Field(default=None)
    match_field: Optional[str] = Field(default=None, min_length=1)


class CreateTransformerColumnRequest(BaseModel):
    id: str = Field(min_length=1)
    target_column: str = Field(min_length=1)
    match_value: Optional[str] = Field(default=None)
    vendor_match_field: Optional[str] = Field(default=None)
    vendor_match_value: Optional[str] = Field(default=None)
    operation: str = Field(min_length=1)
    config: dict = Field(default_factory=dict)
    default_value: Optional[str] = Field(default=None)
    order: int = Field(default=1, ge=0)


class UpdateTransformerColumnRequest(BaseModel):
    target_column: Optional[str] = Field(default=None, min_length=1)
    match_value: Optional[str] = Field(default=None)
    vendor_match_field: Optional[str] = Field(default=None)
    vendor_match_value: Optional[str] = Field(default=None)
    operation: Optional[str] = Field(default=None, min_length=1)
    config: Optional[dict] = Field(default=None)
    default_value: Optional[str] = Field(default=None)
    order: Optional[int] = Field(default=None, ge=0)
