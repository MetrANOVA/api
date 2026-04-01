from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


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
    def close(self):
        pass

    @abstractmethod
    async def create_resource_type(
        self,
        name: str,
        slug: str,
        collection_type: CollectionType,
        consumer_type: ConsumerType,
        consumer_config: dict,
        fields: list[CollectionField],
        primary_key: list[str],
        partition_by: str,
        ttl: str,
        engine_type: str = "CoalescingMergeTree",
        is_replicated: bool = True,
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
