from functools import lru_cache
from pydantic_settings import BaseSettings

import os


class Settings(BaseSettings):
    cors_regex: str = r"^(https://((?!-)[A-Za-z0-9-]{1,63}(?!<-)\.)+grnoc\.iu\.edu)$"
    root_path: str = ""
    db_url: str = ""
    kube_namespace: str = os.getenv("METRANOVA_KUBE_NAMESPACE", "metranova-test")


@lru_cache()
def get_settings() -> Settings:
    return Settings()
