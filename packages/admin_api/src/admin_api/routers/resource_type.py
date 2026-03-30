import logging
import os
from fastapi import APIRouter, HTTPException, Depends

from metranova.storage.clickhouse import Clickhouse
from metranova.storage.base import ConsumerType, CollectionType, CollectionField
from ..context import get_clickhouse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/")
async def create_resource_type(
    name: str,
    slug: str,
    type: CollectionType,
    consumer_type: ConsumerType,
    consumer_config: dict,
    fields: list[CollectionField],
    primary_key: list[str],
    partition_by: str,
    ttl: str,
    engine_type="CoalescingMergeTree",
    is_replicated=True,
    se: Clickhouse = Depends(get_clickhouse),
):
    success = await se.create_resource_type(
        name,
        slug,
        type,
        consumer_type,
        consumer_config,
        fields,
        primary_key,
        partition_by,
        ttl,
        engine_type,
        is_replicated,
    )

    if success:
        return {"success": True}
    else:
        raise HTTPException(
            status_code=500,
            detail="Unknown error while creating type definition",
        )
