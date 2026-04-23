import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from .model import CreateTransformerRequest, UpdateTransformerRequest
from .service import TransformerService

logger = logging.getLogger(__name__)

router = APIRouter(tags=["transformer"])


@router.post("/", tags=["transformer"])
async def create_transformer(
    request: CreateTransformerRequest,
    service: TransformerService = Depends(),
):
    try:
        success, data = await service.create_transformer(
            name=request.name,
            definition_ref=request.definition_ref,
            description=request.description,
            match_field=request.match_field,
        )

        if not success:
            raise HTTPException(
                status_code=400, detail=data.get("message", "Unknown error")
            )

        return data
    except:
        raise HTTPException(status_code=500, detail="Error creating transformer")


@router.get("/", tags=["transformer"])
async def get_all_transformers(
    definition_ref: Optional[str] = Query(default=None),
    service: TransformerService = Depends(),
):
    try:
        return await service.get_all_transformers(definition_ref=definition_ref)
    except Exception:
        raise HTTPException(status_code=500, detail="Error fetching transformers")


@router.get("/{transformer_id}", tags=["transformer"])
async def get_transformer_by_id(
    transformer_id: str,
    service: TransformerService = Depends(),
):
    try:
        success, data = await service.get_transformer_by_id(transformer_id)
        if not success:
            detail = data.get("message", "Unknown error")
            if "not found" in detail.lower():
                raise HTTPException(status_code=404, detail=detail)
            raise HTTPException(status_code=500, detail=detail)
        return data
    except HTTPException:
        raise
    except:
        raise HTTPException(status_code=500, detail="Error fetching transformer")


@router.put("/{transformer_id}", tags=["transformer"])
async def update_transformer(
    transformer_id: str,
    request: UpdateTransformerRequest,
    service: TransformerService = Depends(),
):
    try:
        success, data = await service.update_transformer(
            transformer_id=transformer_id,
            name=request.name,
            description=request.description,
            match_field=request.match_field,
        )
        if not success:
            detail = data.get("message", "Unknown error")
            if "not found" in detail.lower():
                raise HTTPException(status_code=404, detail=detail)
            if "no fields" in detail.lower():
                raise HTTPException(status_code=400, detail=detail)
            raise HTTPException(status_code=500, detail=detail)
        return data
    except HTTPException:
        raise
    except:
        raise HTTPException(status_code=500, detail="Error updating transformer")
