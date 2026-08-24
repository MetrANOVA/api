import logging

from fastapi import APIRouter, Request, HTTPException
from admin_api.collector.model import (
    ResourceConfigurationRequest,
    ResourceConfigurationUpdate,
)
from admin_api.collector.service import CollectorService

logger = logging.getLogger(__name__)
router = APIRouter(tags=["collector"])


@router.get("/plugins")
async def get_collector_plugins(req: Request):
    """List the names of all loaded collector plugins."""
    try:
        metadata = CollectorService(req.app.state.se)
        return metadata.get_plugin_names()
    except Exception as e:
        logger.exception(f"Error fetching collector plugins: {e}")
        raise HTTPException(
            status_code=400, detail=f"Error fetching collector plugins: {e}"
        )


@router.get("/resource_configurations")
async def get_collector_resource_configurations(
    req: Request, plugin_id: str | None = None
):
    """List all registered collector configurations.

    Returns the slug of each resource type where type is 'collector'.
    """
    try:
        metadata = CollectorService(req.app.state.se)
        return await metadata.get_collector_configurations(collector_plugin=plugin_id)
    except Exception as e:
        logger.exception(f"Error fetching collector configurations: {e}")
        raise HTTPException(
            status_code=400, detail=f"Error fetching collector configurations: {e}"
        )


@router.post("/resource_configurations")
async def create_collector_resource_configuration(
    req: Request, config: ResourceConfigurationRequest
):
    """Create a new collector resource configuration.

    Expects a JSON body with the configuration details. `id`/`ref` are assigned
    by the server.
    """
    try:
        metadata = CollectorService(req.app.state.se)
        return await metadata.create_resource_configuration(config)
    except (ValueError, LookupError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error creating collector resource configuration: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Error creating collector resource configuration: {e}",
        )


@router.post("/resource_configurations/{config_id}")
async def update_collector_resource_configuration(
    req: Request, config_id: str, config: ResourceConfigurationUpdate
):
    """Append a new version of a collector resource configuration.

    Omitted (or null) fields keep their current value. This is a POST rather
    than a PUT because it is not a pure write: the new version is re-rendered
    into the collector's configuration afterwards.
    """
    try:
        metadata = CollectorService(req.app.state.se)
        return await metadata.update_resource_configuration(config_id, config)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error updating collector resource configuration: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Error updating collector resource configuration: {e}",
        )


@router.delete("/resource_configurations/{config_id}")
async def delete_collector_resource_configuration(req: Request, config_id: str):
    """Delete a collector resource configuration and all of its snapshots."""
    try:
        metadata = CollectorService(req.app.state.se)
        return await metadata.delete_resource_configuration(config_id)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(f"Error deleting collector resource configuration: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Error deleting collector resource configuration: {e}",
        )


@router.get("/resource_configurations/{config_id}/render")
async def render_collector_resource_configuration(req: Request, config_id: str):
    """Render a single collector resource configuration."""
    try:
        metadata = CollectorService(req.app.state.se)
        config = await metadata.get_resource_configuration_by_id(config_id)
        return await metadata.generate_configuration(config)
    except LookupError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(f"Error rendering collector resource configuration: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Error rendering collector resource configuration: {e}",
        )
