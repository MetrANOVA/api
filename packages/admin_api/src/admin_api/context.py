import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request

from metranova.storage.clickhouse import Clickhouse

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    se = None
    storage_type = os.environ.get("STORAGE_TYPE", "clickhouse")
    if storage_type == "clickhouse":
        try:
            se = await Clickhouse.create()
        except Exception as e:
            logger.error("Error connecting to Clickhouse")
            logger.exception(e)
    else:
        logger.error(f"Unsupported storage type {storage_type}")
    if se is None:
        logger.error("Error initializing storage service")
        raise Exception("Unknown error intializing storage engine")

    app.state.se = se
    try:
        yield
    finally:
        se.close()


def get_clickhouse(request: Request) -> Clickhouse:
    return request.app.state.se
