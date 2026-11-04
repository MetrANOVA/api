import asyncio

import pytest

from metranova.storage.clickhouse import Clickhouse


class RecordingClient:
    def __init__(self):
        self.commands = []

    async def command(self, query):
        self.commands.append(query)


class DDLClickhouse(Clickhouse):
    def __init__(self):
        self.client = RecordingClient()
        self.database = "test"
        self.data_engine = "MergeTree"

    async def _get_on_cluster_clause(self, engine):
        return ""


@pytest.mark.parametrize(
    ("field_type", "nullable", "expected_type"),
    [
        ("UInt64", True, "Nullable(UInt64)"),
        ("Nullable(UInt64)", True, "Nullable(UInt64)"),
        ("Nullable(UInt64)", False, "UInt64"),
    ],
)
def test_create_data_table_uses_nullable_flag(field_type, nullable, expected_type):
    storage = DDLClickhouse()

    asyncio.run(
        storage.create_data_table(
            "sample",
            ["node"],
            "1 DAY",
            [("counter", field_type, nullable)],
        )
    )

    column = f"`counter` {expected_type}"
    if not nullable:
        column += " NOT NULL"
    assert column in storage.client.commands[0]


@pytest.mark.parametrize(
    ("field_type", "nullable", "expected_type"),
    [
        ("UInt64", True, "Nullable(UInt64)"),
        ("Nullable(UInt64)", True, "Nullable(UInt64)"),
        ("Nullable(UInt64)", False, "UInt64"),
    ],
)
def test_added_columns_use_nullable_flag(field_type, nullable, expected_type):
    storage = DDLClickhouse()

    asyncio.run(
        storage._add_columns_to_table(
            "data_sample",
            [("counter", field_type, nullable)],
        )
    )

    assert (
        f"ADD COLUMN IF NOT EXISTS `counter` {expected_type}"
        in storage.client.commands[0]
    )
