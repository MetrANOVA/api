import logging
from fastapi import APIRouter, HTTPException, Depends

from metranova.storage.clickhouse import Clickhouse
from metranova.storage.base import CollectionField
from ..models.resource_type import CreateResourceTypeRequest
from ..context import get_clickhouse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/")
async def create_resource_type(
    request: CreateResourceTypeRequest,
    se: Clickhouse = Depends(get_clickhouse),
):
    fields = [
        CollectionField(
            field_name=field.field_name,
            field_type=field.field_type,
            nullable=field.nullable,
        )
        for field in request.fields
    ]

    (success, msg) = await se.create_resource_type(
        request.name,
        request.slug,
        request.collection_type,
        request.consumer_type,
        request.consumer_config,
        fields,
        request.primary_key,
        request.partition_by,
        request.ttl,
        request.engine_type,
        request.is_replicated,
    )

    if success:
        return {"message": msg}
    else:
        raise HTTPException(
            status_code=500,
            detail=msg,
        )
