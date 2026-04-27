import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from .model import (
    CreateTransformerRequest,
    UpdateTransformerRequest,
    CreateTransformerColumnRequest,
    UpdateTransformerColumnRequest,
)
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
    except Exception:
        raise HTTPException(status_code=500, detail="Error updating transformer")


@router.post("/{transformer_id}/columns", tags=["transformer"])
async def create_transformer_column(
    transformer_id: str,
    request: CreateTransformerColumnRequest,
    service: TransformerService = Depends(),
):
    try:
        found, transformer = await service.get_transformer_by_id(transformer_id)
        if not found:
            detail = transformer.get("message", "Unknown error")
            if "not found" in detail.lower():
                raise HTTPException(status_code=404, detail=detail)
            raise HTTPException(status_code=500, detail=detail)

        success, data = await service.create_transformer_column(
            id=request.id,
            transformer_ref=transformer["ref"],
            target_column=request.target_column,
            match_value=request.match_value,
            vendor_match_field=request.vendor_match_field,
            vendor_match_value=request.vendor_match_value,
            operation=request.operation,
            config=request.config,
            default_value=request.default_value,
            order=request.order,
        )
        if not success:
            raise HTTPException(
                status_code=400, detail=data.get("message", "Unknown error")
            )

        return data
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Error creating transformer column")


@router.get("/{transformer_id}/columns/", tags=["transformer"])
async def get_transformer_columns(
    transformer_id: str,
    service: TransformerService = Depends(),
):
    try:
        found, transformer = await service.get_transformer_by_id(transformer_id)
        if not found:
            detail = transformer.get("message", "Unknown error")
            if "not found" in detail.lower():
                raise HTTPException(status_code=404, detail=detail)
            raise HTTPException(status_code=500, detail=detail)

        return await service.get_transformer_columns(transformer_ref=transformer["ref"])
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(e)
        raise HTTPException(
            status_code=500, detail="Error fetching transformer columns"
        )


@router.get("/{transformer_id}/columns/{column_id}", tags=["transformer"])
async def get_transformer_column_by_id(
    transformer_id: str,
    column_id: str,
    service: TransformerService = Depends(),
):
    try:
        found, transformer = await service.get_transformer_by_id(transformer_id)
        if not found:
            detail = transformer.get("message", "Unknown error")
            if "not found" in detail.lower():
                raise HTTPException(status_code=404, detail=detail)
            raise HTTPException(status_code=500, detail=detail)

        found, column = await service.get_transformer_column_by_id(
            transformer_ref=transformer["ref"],
            column_id=column_id,
        )
        if not found:
            detail = column.get("message", "Unknown error")
            if "not found" in detail.lower():
                raise HTTPException(status_code=404, detail=detail)
            raise HTTPException(status_code=500, detail=detail)

        return column
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Error fetching transformer column")


@router.put("/{transformer_id}/columns/{column_id}", tags=["transformer"])
async def update_transformer_column(
    transformer_id: str,
    column_id: str,
    request: UpdateTransformerColumnRequest,
    service: TransformerService = Depends(),
):
    try:
        found, transformer = await service.get_transformer_by_id(transformer_id)
        if not found:
            detail = transformer.get("message", "Unknown error")
            if "not found" in detail.lower():
                raise HTTPException(status_code=404, detail=detail)
            raise HTTPException(status_code=500, detail=detail)

        found, column = await service.update_transformer_column(
            transformer_ref=transformer["ref"],
            column_id=column_id,
            target_column=request.target_column,
            match_value=request.match_value,
            vendor_match_field=request.vendor_match_field,
            vendor_match_value=request.vendor_match_value,
            operation=request.operation,
            config=request.config,
            default_value=request.default_value,
            order=request.order,
        )
        if not found:
            detail = column.get("message", "Unknown error")
            if "not found" in detail.lower():
                raise HTTPException(status_code=404, detail=detail)
            if (
                "no fields" in detail.lower()
                or "unknown operation" in detail.lower()
                or "invalid config" in detail.lower()
            ):
                raise HTTPException(status_code=400, detail=detail)
            raise HTTPException(status_code=500, detail=detail)

        return column
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Error updating transformer column")


@router.delete("/{transformer_id}/columns/{column_id}", tags=["transformer"])
async def delete_transformer_column(
    transformer_id: str,
    column_id: str,
    service: TransformerService = Depends(),
):
    try:
        found, transformer = await service.get_transformer_by_id(transformer_id)
        if not found:
            detail = transformer.get("message", "Unknown error")
            if "not found" in detail.lower():
                raise HTTPException(status_code=404, detail=detail)
            raise HTTPException(status_code=500, detail=detail)

        found, result = await service.delete_transformer_column(
            transformer_ref=transformer["ref"],
            column_id=column_id,
        )
        if not found:
            detail = result.get("message", "Unknown error")
            if "not found" in detail.lower():
                raise HTTPException(status_code=404, detail=detail)
            raise HTTPException(status_code=500, detail=detail)

        return result
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Error deleting transformer column")
