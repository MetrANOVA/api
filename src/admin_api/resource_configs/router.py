import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict

from admin_api.collector.service import CollectorService

logger = logging.getLogger(__name__)
router = APIRouter(tags=["resource-config-memberships"])


class MembershipAddRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")


@router.get("/resource-configs/{resource_id}")
async def get_resource_config_devices(req: Request, resource_id: str):
    try:
        devices = await CollectorService(req.app.state.se).get_resource_config_devices(
            resource_id,
        )
        return {"devices": devices}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        logger.exception("Error reading resource-config memberships")
        raise HTTPException(status_code=400, detail=f"Error reading memberships: {exc}")


@router.post("/resource-configs/{resource_id}/devices/{device_id}")
async def add_resource_config_device(
    req: Request,
    resource_id: str,
    device_id: str,
    body: MembershipAddRequest,
):
    try:
        devices = await CollectorService(req.app.state.se).add_resource_config_device(
            resource_id, device_id,
        )
        return {"devices": devices}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except Exception as exc:
        logger.exception("Error adding resource-config membership")
        raise HTTPException(status_code=400, detail=f"Error adding membership: {exc}")


@router.delete("/resource-configs/{resource_id}/devices/{device_id}")
async def remove_resource_config_device(req: Request, resource_id: str, device_id: str):
    try:
        devices = await CollectorService(req.app.state.se).remove_resource_config_device(
            resource_id, device_id,
        )
        return {"devices": devices}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("Error removing resource-config membership")
        raise HTTPException(status_code=400, detail=f"Error removing membership: {exc}")
