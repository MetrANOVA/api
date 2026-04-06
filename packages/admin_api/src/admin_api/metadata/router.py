"""router for metadata management

mtype: short for metadata type, e.g. node, interface, temperature. Called "slug" in design documents.
"""

from fastapi import APIRouter, Request
from metranova.storage.clickhouse import Clickhouse

router = APIRouter(tags=["metadata"])


@router.get("/")
async def get_metadata_mtypes(req: Request):
    se: Clickhouse = req.app.state.se
    print(await se.find_all_resource_types())
    return [{"type": "node"}, {"type": "interface"}, {"type": "temperature"}]


@router.get("/{mtype}")
async def get_metadata(mtype: str, req: Request):
    return [
        {"type": mtype, "id": "node1::intf1"},
        {"type": mtype, "id": "node1::intf2"},
    ]


@router.post("/{mtype}")
async def create_metadata(mtype: str):
    return {"type": mtype, "id": "node1::intf1", "version": "v1"}


@router.get("/{mtype}/{mid}")
async def get_metadata(mtype: str, mid: str):
    """support limit and offset"""
    return {"type": mtype, "id": mid}


@router.put("/{mtype}/{mid}/{version}")
async def update_metadata_version(mtype: str, mid: str, version: str):
    return {"type": mtype, "id": mid, "version": version}
