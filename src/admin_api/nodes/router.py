import logging

from fastapi import APIRouter, Request, HTTPException
from admin_api.nodes.model import NodeCreateRequest, NodeUpdateRequest
from admin_api.nodes.service import NodeService

logger = logging.getLogger(__name__)
router = APIRouter(tags=["nodes"])


@router.get("/")
async def get_nodes(req: Request):
    """List every stored node."""
    try:
        service = NodeService(req.app.state.se)
        return await service.get_nodes()
    except Exception as e:
        logger.exception(f"Error fetching nodes: {e}")
        raise HTTPException(status_code=400, detail=f"Error fetching nodes: {e}")


@router.get("/{node_id}")
async def get_node(req: Request, node_id: str):
    """Fetch a single node by id."""
    try:
        service = NodeService(req.app.state.se)
        return await service.get_node_by_id(node_id)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(f"Error fetching node: {e}")
        raise HTTPException(status_code=400, detail=f"Error fetching node: {e}")


@router.post("/")
async def create_node(req: Request, node: NodeCreateRequest):
    """Create a new node. `node_id` is assigned by the server."""
    try:
        service = NodeService(req.app.state.se)
        return await service.create_node(node)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error creating node: {e}")
        raise HTTPException(status_code=400, detail=f"Error creating node: {e}")


@router.put("/{node_id}")
async def update_node(req: Request, node_id: str, node: NodeUpdateRequest):
    """Replace every field of an existing node."""
    try:
        service = NodeService(req.app.state.se)
        return await service.update_node(node_id, node)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error updating node: {e}")
        raise HTTPException(status_code=400, detail=f"Error updating node: {e}")


@router.delete("/{node_id}")
async def delete_node(req: Request, node_id: str):
    """Delete a node."""
    try:
        service = NodeService(req.app.state.se)
        return await service.delete_node(node_id)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(f"Error deleting node: {e}")
        raise HTTPException(status_code=400, detail=f"Error deleting node: {e}")
