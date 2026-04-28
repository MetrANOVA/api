import os
import logging
import sys
from contextlib import asynccontextmanager
from typing import Annotated
from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

import admin_api.logs as logs
from .settings import get_settings

from admin_api.resource_type.router import (
    router as resource_type_router,
)
from admin_api.metadata.router import router as metadata_router
from .context import lifespan, get_clickhouse

description = """
# MetrANOVA Admin API
Use this API to manage MetrANOVA.
"""
sys.setrecursionlimit(10000)

settings = get_settings()


logs.configure(format="text")
logs.set_level("admin_api", os.getenv("LOG_LEVEL", "INFO"))
logs.set_level("uvicorn", os.getenv("LOG_LEVEL", "INFO"))

logger = logging.getLogger(__name__)

is_pytest = "pytest" in sys.modules


app = FastAPI(title="MetrANOVA Admin API", description=description, lifespan=lifespan)

app.root_path = settings.root_path
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=settings.cors_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def index():
    return {"name": "MetrANOVA Admin API", "version": "0.0.1"}


@app.get("/health")
async def health(clickhouse: Annotated[any, Depends(get_clickhouse)]):
    try:
        connected = await clickhouse.client.ping()
    except Exception:
        connected = False
    return {"healthy": connected}


# Add routers here
app.include_router(resource_type_router, prefix="/type", tags=["resource_type"])
app.include_router(metadata_router, prefix="/metadata")
