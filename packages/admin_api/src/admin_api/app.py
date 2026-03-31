import os
import logging
import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import admin_api.logs as logs
from .settings import get_settings
from .routers.resource_type import (
    router as resource_type_router,
    schema_router as resource_type_schema_router,
)
from .context import lifespan

description = """
# MetrANOVA Admin API
Use this API to manage MetrANOVA.
"""
sys.recursionlimit = 10000

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
    return {"message": "Hello World!"}


# Add routers here
app.include_router(resource_type_router, prefix="/type", tags=["resource_type"])
app.include_router(resource_type_schema_router, prefix="/types", tags=["resource_type"])
