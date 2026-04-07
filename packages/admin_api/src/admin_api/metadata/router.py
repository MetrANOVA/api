"""router for metadata management

mtype: short for metadata type, e.g. node, interface, temperature. Called "slug" in design documents.
"""

from fastapi import APIRouter, Request, HTTPException
from metranova.storage.clickhouse import Clickhouse
from pydantic import BaseModel


router = APIRouter(tags=["metadata resources"])


@router.get("/")
async def get_metadata_mtypes(req: Request):
    """List all registered metadata types.

    Returns the slug of each resource type where type is 'metadata'.
    """
    se: Clickhouse = req.app.state.se
    resources = await se.find_all_resource_types()
    return [{"type": r["slug"]} for r in filter(lambda x: x["type"] == "metadata", resources)]


@router.get("/{mtype}")
async def get_metadata(mtype: str, req: Request):
    """Get the latest version of every record for a given metadata type.

    Uses max(created_at) per id to return only the most recent version of each record.
    """
    se: Clickhouse = req.app.state.se

    result = await se.client.query(f"""
        SELECT * FROM {se._qualified_table_name(mtype)} WHERE (id, created_at) IN (
            SELECT id, max(created_at) FROM {se._qualified_table_name(mtype)} GROUP BY id
        )
    """)
    return list(result.named_results())


class MetadataReqBody(BaseModel):
    id: str


@router.post("/{mtype}")
async def create_metadata(mtype: str, req: Request, metadata: MetadataReqBody):
    """Create a new versioned metadata record.

    Validates that the metadata type exists and the primary key structure matches.
    Creates the backing table if it doesn't exist, then inserts a new row with an
    auto-incremented version ref (e.g. 'my-id__v1').
    """
    se: Clickhouse = req.app.state.se

    resource = await se.find_resource_type_by_slug(mtype)
    if resource is None:
        raise HTTPException(status_code=400, detail=f"Metadata type '{mtype}' not found in resource types.")

    if len(metadata.id.split("::")) != len(resource["primary_key"]):
        raise HTTPException(status_code=400, detail=f"Invalid primary key for metadata type '{mtype}'.")

    # If you want to reset the table for testing, uncomment the following line. This will delete all existing metadata of this type.
    # await se.client.command(f"DROP TABLE IF EXISTS {se._qualified_table_name(mtype)}")

    # Tiny bit of pre-validation
    await se.client.command(
        f"CREATE TABLE IF NOT EXISTS {se._qualified_table_name(mtype)} (id String, ref String, created_at DateTime, updated_at DateTime) ENGINE = MergeTree() ORDER BY id"
    )

    result = await se.client.query(f"SELECT id, ref FROM {se._qualified_table_name(mtype)} WHERE id = '{metadata.id}' ORDER BY created_at DESC LIMIT 1")
    records = list(result.named_results())
    version = 1
    if len(records) > 0:
        version = int(records[0]["ref"].split("__v")[-1]) + 1

    await se.client.command(
        f"INSERT INTO {se._qualified_table_name(mtype)} (id, ref, created_at, updated_at) VALUES ('{metadata.id}', '{metadata.id}__v{version}', now(), now())"
    )

    return {"type": mtype, "id": metadata.id, "ref": f"{metadata.id}__v{version}"}


@router.get("/{mtype}/{mid}")
async def get_metadata(mtype: str, mid: str, req: Request):
    """Get the version history for a specific metadata record.

    Returns all versions of the record identified by `mid`, showing the latest
    update for each version ordered by created_at descending.
    """
    se: Clickhouse = req.app.state.se

    result = await se.client.query(f"""
        SELECT * FROM {se._qualified_table_name(mtype)} WHERE (id, updated_at) IN (
            SELECT id, max(updated_at) FROM {se._qualified_table_name(mtype)} WHERE id='{mid}' GROUP BY (id, created_at)
        ) ORDER BY created_at DESC
    """)
    return list(result.named_results())


@router.put("/{mtype}/{mid}/{version}")
async def update_metadata_version(mtype: str, mid: str, version: str, req: Request):
    """Update a specific version of a metadata record.

    Inserts a new row with the same id, ref, and created_at but a fresh updated_at
    timestamp. This follows ClickHouse's append-only pattern instead of in-place updates.
    """
    se: Clickhouse = req.app.state.se

    result = await se.client.query(f"""
        SELECT * FROM {se._qualified_table_name(mtype)} WHERE (id, updated_at) IN (
            SELECT id, max(updated_at) FROM {se._qualified_table_name(mtype)} WHERE ref='{mid}__v{version}' GROUP BY (id, created_at)
        ) ORDER BY created_at DESC
    """)
    record = next(result.named_results(), None)
    if record is None:
        raise HTTPException(status_code=400, detail=f"Failed to find metadata for '{mid}__v{version}' in metadata type '{mtype}'.")

    original_created_at = record["created_at"]
    await se.client.command(
        f"INSERT INTO {se._qualified_table_name(mtype)} (id, ref, created_at, updated_at) VALUES ('{mid}', '{mid}__v{version}', '{original_created_at}', now())"
    )
    return {"id": mid, "ref": f"{mid}__v{version}"}
