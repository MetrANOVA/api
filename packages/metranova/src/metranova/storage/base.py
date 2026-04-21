from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from admin_api.metadata.service import MetadataField


class CollectionType(StrEnum):
    DATA = "data"
    METADATA = "metadata"


class ConsumerType(StrEnum):
    KAFKA = "kafka"


@dataclass
class CollectionField:
    field_name: str
    field_type: str
    nullable: bool = True


class StorageEngine(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    async def connect(self) -> None:
        pass

    @abstractmethod
    async def is_connected(self) -> bool:
        pass

    @abstractmethod
    async def close(self):
        pass

    @abstractmethod
    async def create_resource_type(
        self,
        name: str,
        slug: str | None = None,
        data_fields: list[CollectionField] | None = None,
        meta_fields: list[MetadataField] | None = None,
        identifier: list[str] | None = None,
        ttl: str = "",
        engine_type: str = "CoalescingMergeTree",
    ) -> tuple[bool, str]:
        pass

    @abstractmethod
    async def find_all_resource_types(self) -> list | None:
        pass

    @abstractmethod
    async def find_resource_type_by_slug(self, slug: str) -> dict[str, Any] | tuple | None:
        pass

    @abstractmethod
    async def update_resource_type(
        self,
        slug: str,
        fields: list[CollectionField] | None = None,
        consumer_config_updates: dict | None = None,
        ext_updates: dict | None = None,
    ) -> tuple[bool, str]:
        pass
