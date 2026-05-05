import logging
from fastapi import APIRouter, HTTPException, Depends

from metranova.storage.clickhouse import Clickhouse
from metranova.storage.base import CollectionField
from admin_api.metadata.service import MetadataField, MetadataService, slugify
from .model import (
    BatchCreateResourceTypeRequest,
    CreateResourceTypeRequest,
    UpdateResourceTypeRequest,
)
from ..context import get_clickhouse

logger = logging.getLogger(__name__)

router = APIRouter()


def _to_collection_fields(fields):
    return [
        CollectionField(
            field_name=f.field_name,
            field_type=f.field_type,
            nullable=f.nullable,
        )
        for f in fields
    ]


def _to_metadata_fields(fields):
    return [
        MetadataField(
            name=f.field_name,
            type=f.field_type,
            nullable=f.nullable,
            table=f.table,
        )
        for f in fields
    ]


def _existing_data_field_names(definition) -> set[str]:
    values = (
        definition.get("data_fields") if isinstance(definition, dict) else definition[5]
    )
    values = values or []
    names = set()
    for value in values:
        if isinstance(value, dict):
            names.add(value.get("field_name"))
        else:
            names.add(value[0])
    return names


def _existing_meta_field_names(definition) -> set[str]:
    values = (
        definition.get("meta_fields") if isinstance(definition, dict) else definition[4]
    )
    values = values or []
    names = set()
    for value in values:
        if isinstance(value, dict):
            names.add(value.get("field_name"))
        else:
            names.add(value[0])
    return names


@router.post("/")
async def create_resource_type(
    request: CreateResourceTypeRequest,
    se: Clickhouse = Depends(get_clickhouse),
):
    data_fields = _to_collection_fields(request.data_fields)
    meta_fields = _to_metadata_fields(request.meta_fields)

    slug = request.name.lower().replace(" ", "_")
    try:
        success, msg = await se.create_resource_type(
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


@router.post("/batch")
async def batch_create_or_update_resource_types(
    request: BatchCreateResourceTypeRequest,
    se: Clickhouse = Depends(get_clickhouse),
):
    created = []
    updated = []
    failed = []

    for definition in request.definitions:
        slug = definition.name.lower().replace(" ", "_")
        data_fields = _to_collection_fields(definition.data_fields)
        meta_fields = _to_metadata_fields(definition.meta_fields)

        try:

            existing = await se.find_resource_type_by_slug(slug)
            if existing is None:
                if not data_fields and meta_fields:
                    metadata_slug = slugify(definition.name)
                    metadata_service = MetadataService(se)
                    await metadata_service.create_metadata_type(
                        name=definition.name,
                        identifier=definition.identifier,
                        fields=meta_fields,
                    )
                    created.append(
                        {
                            "name": definition.name,
                            "slug": metadata_slug,
                            "message": f"Metadata type {definition.name} has been successfully created",
                        }
                    )
                    continue
                success, msg = await se.create_resource_type(
                    name=definition.name,
                    slug=slug,
                    data_fields=data_fields,
                    meta_fields=meta_fields,
                    identifier=definition.identifier,
                    ttl=definition.ttl,
                )

                if success:
                    created.append(
                        {"name": definition.name, "slug": slug, "message": msg}
                    )
                else:
                    logger.error(
                        "Batch type creation failed for slug '%s': %s",
                        slug,
                        msg,
                    )
                    failed.append({"name": definition.name, "slug": slug, "error": msg})
                continue

            existing_data_names = _existing_data_field_names(existing)
            existing_meta_names = _existing_meta_field_names(existing)
            missing_data_fields = [
                field
                for field in data_fields
                if field.field_name not in existing_data_names
            ]
            missing_meta_fields = [
                field for field in meta_fields if field.name not in existing_meta_names
            ]

            if not missing_data_fields and not missing_meta_fields:
                continue

            success, msg = await se.update_resource_type(
                slug=slug,
                fields=missing_data_fields,
                meta_fields=missing_meta_fields,
            )

            if success:
                updated.append({"name": definition.name, "slug": slug, "message": msg})
                continue

            logger.error(
                "Batch type update failed for slug '%s': %s",
                slug,
                msg,
            )
            failed.append({"name": definition.name, "slug": slug, "error": msg})
        except Exception as e:
            logger.exception(
                "Batch type upsert failed for slug '%s' with unexpected error",
                slug,
            )
            failed.append({"name": definition.name, "slug": slug, "error": str(e)})

    return {
        "created": created,
        "updated": updated,
        "failed": failed,
    }


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
        data_fields = _to_collection_fields(request.data_fields)
        meta_fields = _to_metadata_fields(request.meta_fields)

        success, message = await se.update_resource_type(
            slug=slug,
            fields=data_fields,
            meta_fields=meta_fields,
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
