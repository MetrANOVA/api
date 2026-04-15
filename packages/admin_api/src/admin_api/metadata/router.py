"""router for metadata management

slug: short for metadata type, e.g. node, interface, temperature. Called "slug" in design documents.
"""
import logging

from fastapi import APIRouter, Request, HTTPException
from metranova.storage.clickhouse import Clickhouse, MetadataField
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(tags=["metadata resources"])


class CreateMetadataTypeReq(BaseModel):
    name: str
    identifier: list[str]
    fields: list[MetadataField]


@router.post("/")
async def create_metadata_type(req: Request, body: CreateMetadataTypeReq):
    """Create a new metadata type.

    This registers a new metadata type in the system by adding an entry to the resource_types table.
    The slug is auto-generated from the name by lowercasing and replacing spaces with underscores.
    """
    se: Clickhouse = req.app.state.se

    for key in body.identifier:
        if key not in [f.name for f in body.fields]:
            raise HTTPException(status_code=400, detail=f"Identifier field '{key}' not found in fields.")

    slug = body.name.lower().replace(" ", "_")
    existing = await se.find_resource_type_by_slug(slug)
    if existing is not None:
        raise HTTPException(status_code=400, detail=f"Metadata type with slug '{slug}' already exists.")

    try:
        await se.create_meta_table(slug, body.fields, "engine", "created_at", body.identifier)
        await se.client.insert(
            database=se.database,
            table="definition",
            data=[
                [
                    f"def_{slug}",
                    f"def_{slug}__v1",
                    body.name,
                    slug,
                    "metadata",
                    "kafka",
                    "{}",
                    [(f.name, f.type, f.nullable) for f in body.fields],
                    body.identifier,
                    "created_at",
                    "ttl",
                ]
            ],
            column_names=[
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
            ],
        )
    except Exception as e:
        logger.exception(
            f"Error creating metadata type definition '{slug}': {e}"
        )
        raise HTTPException(status_code=400, detail=f"Error creating metadata type definition '{slug}': {e}")
    return {"type": slug}


@router.get("/")
async def get_metadata_slugs(req: Request):
    """List all registered metadata types.

    Returns the slug of each resource type where type is 'metadata'.
    """
    se: Clickhouse = req.app.state.se
    resources = await se.find_all_resource_types()
    return [{"type": r["slug"]} for r in filter(lambda x: x["type"] == "metadata", resources)]


@router.get("/{slug}")
async def get_metadata(slug: str, req: Request):
    """Get the latest version of every record for a given metadata type.

    Uses max(created_at) per id to return only the most recent version of each record.
    """
    se: Clickhouse = req.app.state.se

    result = await se.client.query("""
        SELECT t.* FROM {db:Identifier}.{table:Identifier} t
        INNER JOIN (
            SELECT id, max(created_at) AS max_created_at
            FROM {db:Identifier}.{table:Identifier}
            GROUP BY id
        ) latest ON t.id = latest.id AND t.created_at = latest.max_created_at
        """,
        parameters={"db": "metranova", "table": f"meta_{slug}"}
    )

    return list(result.named_results())


class MetadataReqBody(BaseModel):
    id: str


@router.delete("/{slug}")
async def delete_metadata_type(slug: str, req: Request):
    """Delete a metadata type and all associated records.

    This drops the backing ClickHouse table and removes the resource type definition.
    """
    se: Clickhouse = req.app.state.se

    await se.client.command(f"DROP TABLE IF EXISTS {se._qualified_table_name('meta_'+slug)}", parameters=[])
    await se.client.command("DELETE FROM definition WHERE slug = %s AND type = 'metadata'", parameters=[slug])
    return {"detail": f"Metadata type '{slug}' and all associated records have been deleted."}


@router.post("/{slug}")
async def create_metadata(slug: str, req: Request, metadata: MetadataReqBody):
    """Create a new versioned metadata record.

    Validates that the metadata type exists and the primary key structure matches.
    Creates the backing table if it doesn't exist, then inserts a new row with an
    auto-incremented version ref (e.g. 'my-id__v1').
    """
    se: Clickhouse = req.app.state.se

    resource = await se.find_resource_type_by_slug(slug)
    if resource is None:
        raise HTTPException(status_code=400, detail=f"Metadata type '{slug}' not found in resource types.")

    if len(metadata.id.split("::")) != len(resource["primary_key"]):
        raise HTTPException(status_code=400, detail=f"Invalid primary key for metadata type '{slug}'.")

    # # If you want to reset the table for testing, uncomment the following line. This will delete all existing metadata of this type.
    await se.client.command(f"DROP TABLE IF EXISTS {se._qualified_table_name('meta_'+slug)}", parameters=[])

    # TODO: In theory we can create the table if it doesn't exist, but this is currently
    # not supported.
    # await se.create_meta_table(slug, [MetadataField(name=f["name"], type=f["type"], nullable=f["nullable"]) for f in resource["fields"]], "engine", "created_at", resource["primary_key"])

    result = await se.client.query(
        f"SELECT id, ref FROM {se._qualified_table_name('meta_'+slug)} WHERE id = %s ORDER BY created_at DESC LIMIT 1",
        parameters=[metadata.id],
    )
    records = list(result.named_results())
    version = 1
    if len(records) > 0:
        version = int(records[0]["ref"].split("__v")[-1]) + 1

    await se.client.command(
        f"INSERT INTO {se._qualified_table_name('meta_'+slug)} (id, ref, created_at, updated_at) VALUES (%s, %s, now(), now())",
        parameters=[metadata.id, f"{metadata.id}__v{version}"],
    )

    return {"type": slug, "id": metadata.id, "ref": f"{metadata.id}__v{version}"}


@router.get("/{slug}/{mid}")
async def get_metadata(slug: str, mid: str, req: Request):
    """Get the version history for a specific metadata record.

    Returns all versions of the record identified by `mid`, showing the latest
    update for each version ordered by created_at descending.
    """
    se: Clickhouse = req.app.state.se

    result = await se.client.query(f"""
        SELECT * FROM {se._qualified_table_name("meta_"+slug)} WHERE (id, updated_at) IN (
            SELECT id, max(updated_at) FROM {se._qualified_table_name("meta_"+slug)} WHERE id=%s GROUP BY (id, created_at)
        ) ORDER BY created_at DESC
    """,
        parameters=[mid],
    )
    return list(result.named_results())


@router.put("/{slug}/{mid}/{version}")
async def update_metadata_version(slug: str, mid: str, version: str, req: Request):
    """Update a specific version of a metadata record.

    Inserts a new row with the same id, ref, and created_at but a fresh updated_at
    timestamp. This follows ClickHouse's append-only pattern instead of in-place updates.
    """
    se: Clickhouse = req.app.state.se

    result = await se.client.query(f"""
        SELECT * FROM {se._qualified_table_name("meta_"+slug)} WHERE (id, updated_at) IN (
            SELECT id, max(updated_at) FROM {se._qualified_table_name("meta_"+slug)} WHERE ref=%s GROUP BY (id, created_at)
        ) ORDER BY created_at DESC
    """,
        parameters=[f"{mid}__v{version}"],
    )
    record = next(result.named_results(), None)
    if record is None:
        raise HTTPException(status_code=400, detail=f"Failed to find metadata for '{mid}__v{version}' in metadata type '{slug}'.")

    original_created_at = record["created_at"]
    await se.client.command(
        f"INSERT INTO {se._qualified_table_name("meta_"+slug)} (id, ref, created_at, updated_at) VALUES (%s, %s, %s, now())",
        parameters=[mid, f"{mid}__v{version}", original_created_at],
    )
    return {"id": mid, "ref": f"{mid}__v{version}"}
