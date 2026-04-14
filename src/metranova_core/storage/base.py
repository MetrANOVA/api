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


@dataclass
class MetaCollectionField(CollectionField):
    """A metadata collection field. Supports 'reference' as a field_type
    in addition to all standard ClickHouse types."""
    table: str | None = None


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
        data_fields: list[CollectionField],
        meta_fields: list[MetaCollectionField],
        identifier: list[str],
        ttl: str,
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
