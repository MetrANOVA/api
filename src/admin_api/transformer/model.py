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
