import logging

from fastapi import APIRouter, Body, Request, HTTPException
from admin_api.collector.model import FieldConfig, Selector
from admin_api.collector.service import CollectorService

logger = logging.getLogger(__name__)
router = APIRouter(tags=["collector"])


@router.get("/plugins/{name}/resource_configurations")
async def get_collector_resource_configurations(req: Request, name: str):
    """List all registered collector configurations.

    Returns the slug of each resource type where type is 'collector'.
    """
    try:
        metadata = CollectorService(req.app.state.se)
        return await metadata.get_collector_configurations(collector_plugin=name)
    except Exception as e:
        logger.exception(f"Error fetching collector configurations: {e}")
        raise HTTPException(
            status_code=400, detail=f"Error fetching collector configurations: {e}"
        )


@router.post("/plugins/{name}/resource_configurations")
async def create_collector_resource_configuration(
    req: Request, name: str, config: dict = Body(...)
):
    """Create a new collector resource configuration.

    Expects a JSON body with the configuration details. The plugin name comes
    from the path; `id`/`ref` are assigned by the server.
    """
    try:
        metadata = CollectorService(req.app.state.se)

        field_mappings = {
            k: FieldConfig(**v) for k, v in (config.get("field_mappings") or {}).items()
        }
        node_selectors = [Selector(**s) for s in (config.get("node_selectors") or [])]

        success, result = await metadata.create_resource_configuration(
            name=config["name"],
            resource_type=config["resource_type"],
            collector_plugin=config.get("collector_plugin", name),
            field_mappings=field_mappings,
            node_selectors=node_selectors,
            interval=config.get("interval", 60),
            timeout=config.get("timeout", 15),
        )
        if not success:
            raise HTTPException(status_code=400, detail=result["message"])

        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error creating collector resource configuration: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Error creating collector resource configuration: {e}",
        )


@router.get("/plugins/{name}/resource_configurations/example")
async def get_collector_resource_configuration_example(req: Request, name: str):
    """List all registered collector configurations.

    Returns the slug of each resource type where type is 'collector'.
    """
    try:
        metadata = CollectorService(req.app.state.se)
        configs = await metadata.get_collector_configurations(collector_plugin=name)
        if not configs:
            raise HTTPException(
                status_code=404,
                detail=f"No resource configurations stored for plugin '{name}'",
            )
        return await metadata.generate_configuration(configs[0])
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error fetching collector configurations: {e}")
        raise HTTPException(
            status_code=400, detail=f"Error fetching collector configurations: {e}"
        )
