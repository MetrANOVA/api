"""router for metadata management

slug: short for metadata type, e.g. node, interface, temperature. Called "slug" in design documents.
"""

import logging

from fastapi import APIRouter, Body, Request, HTTPException
from metranova.storage.clickhouse import Clickhouse, MetadataField
from pydantic import BaseModel
from admin_api.metadata.service import MetadataService, slugify

logger = logging.getLogger(__name__)
router = APIRouter(tags=["metadata resources"])


class CreateMetadataTypeReq(BaseModel):
    name: str
    identifier: list[str]
    fields: list[MetadataField]


class UpdateMetadataTypeReq(BaseModel):
    fields: list[MetadataField]


@router.post("/")
async def create_metadata_type(req: Request, body: CreateMetadataTypeReq):
    """Create a new metadata type.

    This registers a new metadata type in the system by adding an entry to the resource_types table.
    The slug is auto-generated from the name by lowercasing and replacing spaces with underscores.
    """
    for key in body.identifier:
        if key not in [f.name for f in body.fields]:
            raise HTTPException(
                status_code=400, detail=f"Identifier field '{key}' not found in fields."
            )

    try:
        metadata = MetadataService(req.app.state.se)
        await metadata.create_metadata_type(body.name, body.identifier, body.fields)
    except ValueError as ve:
        logger.warning(f"Validation error creating metadata type '{body.name}': {ve}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.exception(f"Error creating metadata type '{body.name}': {e}")
        raise HTTPException(
            status_code=400, detail=f"Error creating metadata type '{body.name}': {e}"
        )

    return {"type": slugify(body.name)}


@router.get("/")
async def get_metadata_types(req: Request):
    """List all registered metadata types.

    Returns the slug of each resource type where type is 'metadata'.
    """
    try:
        metadata = MetadataService(req.app.state.se)
        return await metadata.get_metadata_types()
    except Exception as e:
        logger.exception(f"Error fetching metadata types: {e}")
        raise HTTPException(
            status_code=400, detail=f"Error fetching metadata types: {e}"
        )


@router.get("/{slug}")
async def get_metadata(slug: str, req: Request):
    """Get the latest version of every record for a given metadata type.

    Uses max(created_at) per id to return only the most recent version of each record.
    """
    try:
        metadata = MetadataService(req.app.state.se)
        return await metadata.get_metadata_records(slug)
    except Exception as e:
        logger.exception(f"Error fetching metadata records for type '{slug}': {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Error fetching metadata records for type '{slug}': {e}",
        )


@router.put("/{slug}")
async def update_metadata_type(slug: str, req: Request, body: UpdateMetadataTypeReq):
    """Update a metadata type's schema by adding or removing non-reserved, non-identifier fields."""
    metadata = MetadataService(req.app.state.se)
    try:
        await metadata.update_metadata_type(slug, body.fields)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error updating metadata type '{slug}': {e}")
        raise HTTPException(status_code=400, detail=str(e))
    return {"type": slug}


@router.delete("/{slug}")
async def delete_metadata_type(slug: str, req: Request):
    """Delete a metadata type and all associated records.

    This drops the backing ClickHouse table and removes the resource type definition.
    """
    try:
        metadata = MetadataService(req.app.state.se)
        await metadata.delete_metadata_type(slug)
        return {
            "detail": f"Metadata type '{slug}' and all associated records have been deleted."
        }
    except Exception as e:
        logger.exception(f"Error deleting metadata type '{slug}': {e}")
        raise HTTPException(
            status_code=400, detail=f"Error deleting metadata type '{slug}': {e}"
        )


@router.post("/{slug}")
async def create_metadata(slug: str, req: Request):
    """Create a new versioned metadata record.

    Validates that the metadata type exists and the primary key structure matches.
    Creates the backing table if it doesn't exist, then inserts a new row with an
    auto-incremented version ref (e.g. 'my-id__v1').
    """
    metadata = MetadataService(req.app.state.se)
    record = await req.json()

    type_def = await metadata.get_metadata_type(slug)
    if type_def is None:
        raise HTTPException(
            status_code=404,
            detail=f"Metadata type '{slug}' not found in resource types.",
        )

    try:
        await metadata.validate_metadata_record(type_def, record)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        result = await metadata.create_metadata_record(type_def, record)
        return {"type": slug, **result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{slug}/{mid}")
async def get_metadata(slug: str, mid: str, req: Request):
    """Get the version history for a specific metadata record.

    Returns all versions of the record identified by `mid`, showing the latest
    update for each version ordered by created_at descending.
    """
    se: Clickhouse = req.app.state.se

    try:
        metadata = MetadataService(se)
        return await metadata.get_metadata_record_history(slug, mid)
    except Exception as e:
        logger.exception(
            f"Error fetching metadata history for '{mid}' in type '{slug}': {e}"
        )
        raise HTTPException(
            status_code=400,
            detail=f"Error fetching metadata history for '{mid}' in type '{slug}': {e}",
        )


@router.put("/{slug}/{mid}/{version}")
async def update_metadata_version(
    slug: str, mid: str, version: str, req: Request, record: dict = Body(...)
):
    """Update a specific version of a metadata record.

    Accepts a full record body, validates it, preserves the original created_at, and
    inserts a new row with a fresh updated_at. Follows ClickHouse's append-only pattern.
    """
    metadata = MetadataService(req.app.state.se)

    type_def = await metadata.get_metadata_type(slug)
    if type_def is None:
        raise HTTPException(
            status_code=404, detail=f"Metadata type '{slug}' not found."
        )

    try:
        assert mid == "::".join([record[i] for i in type_def["identifier"]])
        record["id"] = mid
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Metadata primary keys cannot be modified."
        )

    try:
        await metadata.validate_metadata_record(type_def, record)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        await metadata.update_metadata_record(type_def, record, version)
        return {"id": record["id"], "ref": record["ref"]}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(
            f"Error updating metadata '{mid}__v{version}' in type '{slug}': {e}"
        )
        raise HTTPException(status_code=400, detail=str(e))
