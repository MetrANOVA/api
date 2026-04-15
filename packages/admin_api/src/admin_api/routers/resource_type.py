import logging
from fastapi import APIRouter, HTTPException, Depends

from metranova_core.storage.clickhouse import Clickhouse
from metranova_core.storage.base import CollectionField, MetaCollectionField
from ..models.resource_type import CreateResourceTypeRequest, UpdateResourceTypeRequest
from ..context import get_clickhouse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/")
async def create_resource_type(
    request: CreateResourceTypeRequest,
    se: Clickhouse = Depends(get_clickhouse),
):
    data_fields = [
        CollectionField(
            field_name=f.field_name,
            field_type=f.field_type,
            nullable=f.nullable,
        )
        for f in request.data.fields
    ]
    meta_fields = [
        MetaCollectionField(
            field_name=f.field_name,
            field_type=f.field_type,
            nullable=f.nullable,
            table=f.table,
        )
        for f in request.meta.fields
    ]

    slug = request.name.lower().replace(' ', '-')
    try:
        (success, msg) = await se.create_resource_type(
            name=request.name,
            slug=slug,
            data_fields=data_fields,
            meta_fields=meta_fields,
            identifier=request.identifier,
            ttl=request.ttl,
        )
    except Exception as e:
        logger.exception(e)
        success = False
        msg = "error creating resource type " + str(e)

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
    try:
        results = await se.find_all_resource_types()
    except Exception as e:
        logger.exception(e)
        results = None
    if results is None:
        raise HTTPException(status_code=500, detail="Error fetching all resource types")
    return results


@router.get("/{slug}")
async def get_resource_type_by_slug(
    slug: str,
    se: Clickhouse = Depends(get_clickhouse),
):
    try:
        result = await se.find_resource_type_by_slug(slug)
    except Exception as e:
        logger.exception(e)
        result = None
        
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
        "meta_fields",
        "data_fields",
        "identifier",
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
    try:
        schema = await se.find_resource_type_schema_by_slug(slug)
    except Exception as e:
        logger.exception(e)
        schema = None
        
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
    try:
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
    except Exception as e:
        logger.exception(e)
        success = False
        message = "error updating resource type " + str(e)

    if not success:
        if "not found" in message.lower():
            raise HTTPException(status_code=404, detail=message)
        raise HTTPException(status_code=400, detail=message)

    return {"message": message}
