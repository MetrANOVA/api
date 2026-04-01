import logging
from fastapi import APIRouter, HTTPException, Depends

from metranova.storage.clickhouse import Clickhouse
from metranova.storage.base import CollectionField
from ..models.resource_type import CreateResourceTypeRequest, UpdateResourceTypeRequest
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


@router.get("/")
async def get_all_resource_types(
    se: Clickhouse = Depends(get_clickhouse),
):
    results = await se.find_all_resource_types()
    if results is None:
        raise HTTPException(status_code=500, detail="Error fetching all resource types")
    return results


@router.get("/{slug}")
async def get_resource_type_by_slug(
    slug: str,
    se: Clickhouse = Depends(get_clickhouse),
):
    result = await se.find_resource_type_by_slug(slug)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"Resource type with slug '{slug}' not found",
        )

    if isinstance(result, dict):
        return result

    column_names = [
        "id",
        "ref",
        "name",
        "slug",
        "type",
        "consumer_type",
        "consumer_config",
        "fields",
        "primary_key",
        "partition_by",
        "ttl",
        "engine_type",
        "is_replicated",
        "updated_at",
    ]
    return dict(zip(column_names, result))


@router.get("/{slug}/schema")
async def get_resource_type_schema_by_slug(
    slug: str,
    se: Clickhouse = Depends(get_clickhouse),
):
    schema = await se.find_resource_type_schema_by_slug(slug)
    if schema is None:
        raise HTTPException(
            status_code=404,
            detail=f"Schema for resource type with slug '{slug}' not found",
        )
    return schema


@router.put("/{slug}")
async def update_resource_type_by_slug(
    slug: str,
    request: UpdateResourceTypeRequest,
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

    success, message = await se.update_resource_type(
        slug=slug,
        fields=fields,
        consumer_config_updates=request.consumer_config,
        ext_updates=request.ext,
    )

    if not success:
        if "not found" in message.lower():
            raise HTTPException(status_code=404, detail=message)
        raise HTTPException(status_code=400, detail=message)

    return {"message": message}
