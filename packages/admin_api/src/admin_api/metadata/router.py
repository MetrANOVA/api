"""router for metadata management

slug: short for metadata type, e.g. node, interface, temperature. Called "slug" in design documents.
"""

from fastapi import APIRouter, Request, HTTPException
from metranova_core.storage.clickhouse import Clickhouse
from pydantic import BaseModel


router = APIRouter(tags=["metadata resources"])


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

    result = await se.client.query(f"""
        SELECT * FROM %s WHERE (id, created_at) IN (
            SELECT id, max(created_at) FROM %s GROUP BY id
        )
    """, parameters=[se._qualified_table_name(slug), se._qualified_table_name(slug)])
    return list(result.named_results())


class MetadataReqBody(BaseModel):
    id: str


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

    # If you want to reset the table for testing, uncomment the following line. This will delete all existing metadata of this type.
    # await se.client.command(f"DROP TABLE IF EXISTS `%s`", parameters=[se._qualified_table_name(slug)])

    # Tiny bit of pre-validation
    await se.client.command(
        f"CREATE TABLE IF NOT EXISTS `%s` (id String, ref String, created_at DateTime, updated_at DateTime) ENGINE = MergeTree() ORDER BY id",
        parameters=[se._qualified_table_name(slug)],
    )

    result = await se.client.query(
        f"SELECT id, ref FROM `%s` WHERE id = %s ORDER BY created_at DESC LIMIT 1",
        parameters=[se._qualified_table_name(slug), metadata.id],
    )
    records = list(result.named_results())
    version = 1
    if len(records) > 0:
        version = int(records[0]["ref"].split("__v")[-1]) + 1

    await se.client.command(
        f"INSERT INTO `%s` (id, ref, created_at, updated_at) VALUES (%s, %s, now(), now())",
        parameters=[se._qualified_table_name(slug), metadata.id, f"{metadata.id}__v{version}"],
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
        SELECT * FROM `%s` WHERE (id, updated_at) IN (
            SELECT id, max(updated_at) FROM `%s` WHERE id=%s GROUP BY (id, created_at)
        ) ORDER BY created_at DESC
    """,
        parameters=[se._qualified_table_name(slug), se._qualified_table_name(slug), mid],
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
        SELECT * FROM `%s` WHERE (id, updated_at) IN (
            SELECT id, max(updated_at) FROM `%s` WHERE ref=%s GROUP BY (id, created_at)
        ) ORDER BY created_at DESC
    """,
        parameters=[se._qualified_table_name(slug), se._qualified_table_name(slug), f"{mid}__v{version}"],
    )
    record = next(result.named_results(), None)
    if record is None:
        raise HTTPException(status_code=400, detail=f"Failed to find metadata for '{mid}__v{version}' in metadata type '{slug}'.")

    original_created_at = record["created_at"]
    await se.client.command(
        f"INSERT INTO `%s` (id, ref, created_at, updated_at) VALUES (%s, %s, %s, now())",
        parameters=[se._qualified_table_name(slug), mid, f"{mid}__v{version}", original_created_at],
    )
    return {"id": mid, "ref": f"{mid}__v{version}"}
